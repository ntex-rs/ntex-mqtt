#![allow(clippy::let_underscore_future)]
use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_io::IoBoxed;
use ntex_router::{IntoPattern, Router, RouterBuilder};
use ntex_service::{IntoService, Pipeline, Service, boxed, fn_service};
use ntex_util::future::{Either, Ready};
use ntex_util::time::{Millis, Seconds, sleep};

use crate::v3::{ControlAck, Publish, codec, shared::MqttShared, sink::MqttSink};
use crate::{error::MqttError, io::Dispatcher};

use super::{control::Control, dispatcher::create_dispatcher};

/// Mqtt client
pub struct Client {
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    session_present: bool,
    max_receive: usize,
    max_buffer_size: usize,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::Client")
            .field("keepalive", &self.keepalive)
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
        keepalive: Seconds,
        max_receive: usize,
        max_buffer_size: usize,
    ) -> Self {
        Client { io, shared, session_present, keepalive, max_receive, max_buffer_size }
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
    pub fn resource<T, F, U>(self, address: T, service: F) -> ClientRouter<U::Error, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U, Publish>,
        U: Service<Publish, Response = ()> + 'static,
    {
        let mut builder = Router::build();
        builder.path(address, 0);
        let handlers = vec![Pipeline::new(boxed::service(service.into_service()))];

        ClientRouter {
            builder,
            handlers,
            io: self.io,
            shared: self.shared,
            keepalive: self.keepalive,
            max_receive: self.max_receive,
            max_buffer_size: self.max_buffer_size,
            _t: PhantomData,
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) {
        if self.keepalive.non_zero() {
            let _ =
                ntex_util::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.shared.clone(),
            self.max_receive,
            self.max_buffer_size,
            fn_service(|pkt| Ready::Ok(Either::Right(pkt))),
            fn_service(|msg: Control<()>| Ready::<_, ()>::Ok(msg.disconnect())),
        );

        let _ = Dispatcher::new(self.io, self.shared.clone(), dispatcher).await;
    }

    /// Run client with provided control messages handler
    pub async fn start<F, S, E>(self, service: F) -> Result<(), MqttError<E>>
    where
        E: 'static,
        F: IntoService<S, Control<E>> + 'static,
        S: Service<Control<E>, Response = ControlAck, Error = E> + 'static,
    {
        if self.keepalive.non_zero() {
            let _ =
                ntex_util::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.shared.clone(),
            self.max_receive,
            self.max_buffer_size,
            fn_service(|pkt| Ready::Ok(Either::Right(pkt))),
            service.into_service(),
        );

        Dispatcher::new(self.io, self.shared.clone(), dispatcher).await
    }

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

type Handler<E> = boxed::BoxService<Publish, (), E>;

/// Mqtt client with routing capabilities
pub struct ClientRouter<Err, PErr> {
    builder: RouterBuilder<usize>,
    handlers: Vec<Pipeline<Handler<PErr>>>,
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    max_receive: usize,
    max_buffer_size: usize,
    _t: PhantomData<Err>,
}

impl<Err, PErr> fmt::Debug for ClientRouter<Err, PErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::ClientRouter")
            .field("keepalive", &self.keepalive)
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
        self.handlers.push(Pipeline::new(boxed::service(service.into_service())));
        self
    }

    /// Run client with default control messages handler
    pub async fn start_default(self) {
        if self.keepalive.non_zero() {
            let _ =
                ntex_util::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.shared.clone(),
            self.max_receive,
            self.max_buffer_size,
            dispatch(self.builder.finish(), self.handlers),
            fn_service(|msg: Control<Err>| Ready::<_, Err>::Ok(msg.disconnect())),
        );

        let _ = Dispatcher::new(self.io, self.shared.clone(), dispatcher).await;
    }

    /// Run client and handle control messages
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<Err>>
    where
        F: IntoService<S, Control<Err>>,
        S: Service<Control<Err>, Response = ControlAck, Error = Err> + 'static,
    {
        if self.keepalive.non_zero() {
            let _ =
                ntex_util::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.shared.clone(),
            self.max_receive,
            self.max_buffer_size,
            dispatch(self.builder.finish(), self.handlers),
            service.into_service(),
        );

        Dispatcher::new(self.io, self.shared.clone(), dispatcher).await
    }
}

fn dispatch<Err, PErr>(
    router: Router<usize>,
    handlers: Vec<Pipeline<Handler<PErr>>>,
) -> impl Service<Publish, Response = Either<(), Publish>, Error = Err>
where
    PErr: 'static,
    Err: From<PErr>,
{
    let handlers = Rc::new(handlers);

    fn_service(move |mut req: Publish| {
        if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
            // exec handler
            let idx = *idx;
            let handlers = handlers.clone();
            Either::Left(async move { call(req, handlers[idx].clone()).await })
        } else {
            Either::Right(Ready::<_, Err>::Ok(Either::Right(req)))
        }
    })
}

async fn call<S, Err, PErr>(req: Publish, srv: Pipeline<S>) -> Result<Either<(), Publish>, Err>
where
    S: Service<Publish, Response = (), Error = PErr>,
    Err: From<PErr>,
{
    match srv.call(req).await {
        Ok(_) => Ok(Either::Left(())),
        Err(err) => Err(err.into()),
    }
}

async fn keepalive(sink: MqttSink, timeout: Seconds) {
    log::debug!("start mqtt client keep-alive task");

    let keepalive = Millis::from(timeout);
    loop {
        sleep(keepalive).await;

        if !sink.is_open() || !sink.ping() {
            // connection is closed
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }
    }
}
