use std::{fmt, future::Future, marker::PhantomData, rc::Rc};

use ntex::io::{IoBoxed, Timer};
use ntex::router::{IntoPattern, Router, RouterBuilder};
use ntex::service::{boxed, into_service, IntoService, Service};
use ntex::time::{sleep, Millis, Seconds};
use ntex::util::{Either, Ready};

use crate::error::MqttError;
use crate::io::Dispatcher;
use crate::v3::{shared::MqttShared, sink::MqttSink};
use crate::v3::{ControlResult, Publish};

use super::control::ControlMessage;
use super::dispatcher::create_dispatcher;

/// Mqtt client
pub struct Client {
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    disconnect_timeout: Seconds,
    session_present: bool,
    max_receive: usize,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::Client")
            .field("keepalive", &self.keepalive)
            .field("disconnect_timeout", &self.disconnect_timeout)
            .field("session_present", &self.session_present)
            .field("max_receive", &self.max_receive)
            .finish()
    }
}

impl Client {
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: IoBoxed,
        shared: Rc<MqttShared>,
        session_present: bool,
        keepalive_timeout: Seconds,
        disconnect_timeout: Seconds,
        max_receive: usize,
    ) -> Self {
        Client {
            io,
            shared,
            session_present,
            disconnect_timeout,
            max_receive,
            keepalive: keepalive_timeout,
        }
    }
}

impl Client {
    #[inline]
    /// Get client sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.session_present
    }

    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, U, E>(self, address: T, service: F) -> ClientRouter<E, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U, Publish>,
        U: Service<Publish, Response = ()> + 'static,
        E: From<U::Error>,
    {
        let mut builder = Router::build();
        builder.path(address, 0);
        let handlers = vec![boxed::service(service.into_service())];

        ClientRouter {
            builder,
            handlers,
            io: self.io,
            shared: self.shared,
            keepalive: self.keepalive,
            disconnect_timeout: self.disconnect_timeout,
            max_receive: self.max_receive,
            _t: PhantomData,
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) {
        if self.keepalive.non_zero() {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            into_service(|pkt| Ready::Ok(Either::Right(pkt))),
            into_service(|msg: ControlMessage<()>| Ready::<_, ()>::Ok(msg.disconnect())),
        );

        let _ = Dispatcher::new(
            self.io,
            self.shared.clone(),
            dispatcher,
            Timer::new(Millis::ONE_SEC),
        )
        .keepalive_timeout(Seconds::ZERO)
        .disconnect_timeout(self.disconnect_timeout)
        .await;
    }

    /// Run client with provided control messages handler
    pub async fn start<F, S, E>(self, service: F) -> Result<(), MqttError<E>>
    where
        E: 'static,
        F: IntoService<S, ControlMessage<E>> + 'static,
        S: Service<ControlMessage<E>, Response = ControlResult, Error = E> + 'static,
    {
        if self.keepalive.non_zero() {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            into_service(|pkt| Ready::Ok(Either::Right(pkt))),
            service.into_service(),
        );

        Dispatcher::new(self.io, self.shared.clone(), dispatcher, Timer::new(Millis::ONE_SEC))
            .keepalive_timeout(Seconds::ZERO)
            .disconnect_timeout(self.disconnect_timeout)
            .await
    }
}

type Handler<E> = boxed::BoxService<Publish, (), E>;

/// Mqtt client with routing capabilities
pub struct ClientRouter<Err, PErr> {
    builder: RouterBuilder<usize>,
    handlers: Vec<Handler<PErr>>,
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    disconnect_timeout: Seconds,
    max_receive: usize,
    _t: PhantomData<Err>,
}

impl<Err, PErr> fmt::Debug for ClientRouter<Err, PErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::ClientRouter")
            .field("keepalive", &self.keepalive)
            .field("disconnect_timeout", &self.disconnect_timeout)
            .field("max_receive", &self.max_receive)
            .finish()
    }
}

impl<Err, PErr> ClientRouter<Err, PErr>
where
    Err: From<PErr> + 'static,
    PErr: 'static,
{
    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, S>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoService<S, Publish>,
        S: Service<Publish, Response = (), Error = PErr> + 'static,
    {
        self.builder.path(address, self.handlers.len());
        self.handlers.push(boxed::service(service.into_service()));
        self
    }

    /// Run client with default control messages handler
    pub async fn start_default(self) {
        if self.keepalive.non_zero() {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            dispatch(self.builder.finish(), self.handlers),
            into_service(|msg: ControlMessage<Err>| Ready::<_, Err>::Ok(msg.disconnect())),
        );

        let _ = Dispatcher::new(
            self.io,
            self.shared.clone(),
            dispatcher,
            Timer::new(Millis::ONE_SEC),
        )
        .keepalive_timeout(Seconds::ZERO)
        .disconnect_timeout(self.disconnect_timeout)
        .await;
    }

    /// Run client and handle control messages
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<Err>>
    where
        F: IntoService<S, ControlMessage<Err>>,
        S: Service<ControlMessage<Err>, Response = ControlResult, Error = Err> + 'static,
    {
        if self.keepalive.non_zero() {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            dispatch(self.builder.finish(), self.handlers),
            service.into_service(),
        );

        Dispatcher::new(self.io, self.shared.clone(), dispatcher, Timer::new(Millis::ONE_SEC))
            .keepalive_timeout(Seconds::ZERO)
            .disconnect_timeout(self.disconnect_timeout)
            .await
    }
}

fn dispatch<Err, PErr>(
    router: Router<usize>,
    handlers: Vec<Handler<PErr>>,
) -> impl Service<Publish, Response = Either<(), Publish>, Error = Err>
where
    PErr: 'static,
    Err: From<PErr>,
{
    into_service(move |mut req: Publish| {
        if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
            // exec handler
            let fut = call(req, &handlers[*idx]);
            Either::Left(async move { fut.await })
        } else {
            Either::Right(Ready::<_, Err>::Ok(Either::Right(req)))
        }
    })
}

fn call<S, Err, PErr>(
    req: Publish,
    srv: &S,
) -> impl Future<Output = Result<Either<(), Publish>, Err>>
where
    S: Service<Publish, Response = (), Error = PErr>,
    Err: From<PErr>,
{
    let fut = srv.call(req);

    async move {
        match fut.await {
            Ok(_) => Ok(Either::Left(())),
            Err(err) => Err(err.into()),
        }
    }
}

async fn keepalive(sink: MqttSink, timeout: Seconds) {
    log::debug!("start mqtt client keep-alive task");

    let keepalive = Millis::from(timeout);
    loop {
        sleep(keepalive).await;

        if !sink.ping() {
            // connection is closed
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }
    }
}
