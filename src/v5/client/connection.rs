use std::time::{Duration, Instant};
use std::{cell::RefCell, convert::TryFrom, future::Future, marker, num::NonZeroU16, rc::Rc};

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::router::{IntoPattern, Path, Router, RouterBuilder};
use ntex::rt::time::{delay_until, Instant as RtInstant};
use ntex::service::boxed::BoxService;
use ntex::service::{into_service, IntoService, Service};
use ntex::util::{ByteString, Either, HashMap, Ready};

use crate::error::MqttError;
use crate::io::{Dispatcher, Timer};
use crate::v5::publish::{Publish, PublishAck};
use crate::v5::{codec, shared::MqttShared, sink::MqttSink, ControlResult};

use super::control::ControlMessage;
use super::dispatcher::create_dispatcher;

/// Mqtt client
pub struct Client<Io> {
    io: Io,
    shared: Rc<MqttShared>,
    keepalive: u16,
    disconnect_timeout: u16,
    max_receive: usize,
    pkt: Box<codec::ConnectAck>,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: T,
        shared: Rc<MqttShared>,
        pkt: Box<codec::ConnectAck>,
        max_receive: u16,
        keepalive: u16,
        disconnect_timeout: u16,
    ) -> Self {
        Client {
            io,
            pkt,
            shared,
            keepalive,
            disconnect_timeout,
            max_receive: max_receive as usize,
        }
    }
}

impl<Io> Client<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    #[inline]
    /// Get client sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.pkt.session_present
    }

    #[inline]
    /// Get reference to `ConnectAck` packet
    pub fn packet(&self) -> &codec::ConnectAck {
        &self.pkt
    }

    #[inline]
    /// Get mutable reference to `ConnectAck` packet
    pub fn packet_mut(&mut self) -> &mut codec::ConnectAck {
        &mut self.pkt
    }

    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, U, E>(self, address: T, service: F) -> ClientRouter<Io, E, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U>,
        U: Service<Request = Publish, Response = PublishAck> + 'static,
        E: From<U::Error>,
        PublishAck: TryFrom<U::Error, Error = E>,
    {
        let mut builder = Router::build();
        builder.path(address, 0);
        let handlers = vec![ntex::boxed::service(service.into_service())];

        ClientRouter {
            builder,
            handlers,
            io: self.io,
            shared: self.shared,
            keepalive: self.keepalive,
            disconnect_timeout: self.disconnect_timeout,
            max_receive: self.max_receive,
            _t: marker::PhantomData,
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            16,
            into_service(|pkt| Ready::Ok(Either::Left(pkt))),
            into_service(|msg: ControlMessage<()>| {
                Ready::Ok(msg.disconnect(codec::Disconnect::default()))
            }),
        );

        let _ = Dispatcher::with(
            self.io,
            self.shared.state.clone(),
            self.shared,
            dispatcher,
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await;
    }

    /// Run client with provided control messages handler
    pub async fn start<F, S, E>(self, service: F) -> Result<(), MqttError<E>>
    where
        E: 'static,
        F: IntoService<S> + 'static,
        S: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E> + 'static,
    {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            16,
            into_service(|pkt| Ready::Ok(Either::Left(pkt))),
            service.into_service(),
        );

        Dispatcher::with(
            self.io,
            self.shared.state.clone(),
            self.shared,
            dispatcher,
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await
    }
}

type Handler<E> = BoxService<Publish, PublishAck, E>;

/// Mqtt client with routing capabilities
pub struct ClientRouter<Io, Err, PErr> {
    builder: RouterBuilder<usize>,
    handlers: Vec<Handler<PErr>>,
    io: Io,
    shared: Rc<MqttShared>,
    keepalive: u16,
    disconnect_timeout: u16,
    max_receive: usize,
    _t: marker::PhantomData<Err>,
}

impl<Io, Err, PErr> ClientRouter<Io, Err, PErr>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    Err: From<PErr> + 'static,
    PublishAck: TryFrom<PErr, Error = Err>,
    PErr: 'static,
{
    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, S>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoService<S>,
        S: Service<Request = Publish, Response = PublishAck, Error = PErr> + 'static,
    {
        self.builder.path(address, self.handlers.len());
        self.handlers.push(ntex::boxed::service(service.into_service()));
        self
    }

    /// Run client with default control messages handler
    pub async fn start_default(self) {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            16,
            dispatch(self.builder.finish(), self.handlers),
            into_service(|msg: ControlMessage<Err>| {
                Ready::Ok(msg.disconnect(codec::Disconnect::default()))
            }),
        );

        let _ = Dispatcher::with(
            self.io,
            self.shared.state.clone(),
            self.shared,
            dispatcher,
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await;
    }

    /// Run client and handle control messages
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<Err>>
    where
        F: IntoService<S> + 'static,
        S: Service<Request = ControlMessage<Err>, Response = ControlResult, Error = Err>
            + 'static,
    {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            MqttSink::new(self.shared.clone()),
            self.max_receive,
            16,
            dispatch(self.builder.finish(), self.handlers),
            service.into_service(),
        );

        Dispatcher::with(
            self.io,
            self.shared.state.clone(),
            self.shared,
            dispatcher,
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await
    }
}

fn dispatch<Err, PErr>(
    router: Router<usize>,
    handlers: Vec<Handler<PErr>>,
) -> impl Service<Request = Publish, Response = Either<Publish, PublishAck>, Error = Err>
where
    PErr: 'static,
    PublishAck: TryFrom<PErr, Error = Err>,
{
    let aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>> =
        RefCell::new(HashMap::default());

    into_service(move |mut req: Publish| {
        if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    aliases.borrow_mut().insert(alias, (*idx, req.topic().clone()));
                }

                // exec handler
                return Either::Left(call(req, &handlers[*idx]));
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = aliases.borrow();
            if let Some(item) = aliases.get(alias) {
                *req.topic_mut() = item.1.clone();
                return Either::Left(call(req, &handlers[item.0]));
            } else {
                log::error!("Unknown topic alias: {:?}", alias);
            }
        }

        Either::Right(Ready::<_, Err>::Ok(Either::Left(req)))
    })
}

fn call<S, Err>(
    req: Publish,
    srv: &S,
) -> impl Future<Output = Result<Either<Publish, PublishAck>, Err>>
where
    S: Service<Request = Publish, Response = PublishAck>,
    PublishAck: TryFrom<S::Error, Error = Err>,
{
    let fut = srv.call(req);

    async move {
        match fut.await {
            Ok(ack) => Ok(Either::Right(ack)),
            Err(err) => match PublishAck::try_from(err) {
                Ok(ack) => Ok(Either::Right(ack)),
                Err(err) => Err(err),
            },
        }
    }
}

async fn keepalive(sink: MqttSink, timeout: u16) {
    log::debug!("start mqtt client keep-alive task");

    let keepalive = Duration::from_secs(timeout as u64);
    loop {
        let expire = RtInstant::from_std(Instant::now() + keepalive);
        delay_until(expire).await;

        if !sink.ping() {
            // connection is closed
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }
    }
}
