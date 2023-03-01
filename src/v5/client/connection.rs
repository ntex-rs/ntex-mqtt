use std::{cell::RefCell, convert::TryFrom, fmt, marker, num::NonZeroU16, rc::Rc};

use ntex::io::IoBoxed;
use ntex::router::{IntoPattern, Path, Router, RouterBuilder};
use ntex::service::{boxed, into_service, IntoService, Service};
use ntex::time::{sleep, Millis, Seconds};
use ntex::util::{ByteString, Either, HashMap, Ready};

use crate::error::MqttError;
use crate::io::Dispatcher;
use crate::v5::publish::{Publish, PublishAck};
use crate::v5::{codec, shared::MqttShared, sink::MqttSink, ControlResult};

use super::control::ControlMessage;
use super::dispatcher::create_dispatcher;

/// Mqtt client
pub struct Client {
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    disconnect_timeout: Seconds,
    max_receive: usize,
    pkt: Box<codec::ConnectAck>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::Client")
            .field("keepalive", &self.keepalive)
            .field("disconnect_timeout", &self.disconnect_timeout)
            .field("max_receive", &self.max_receive)
            .field("connect", &self.pkt)
            .finish()
    }
}

impl Client {
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: IoBoxed,
        shared: Rc<MqttShared>,
        pkt: Box<codec::ConnectAck>,
        max_receive: u16,
        keepalive: Seconds,
        disconnect_timeout: Seconds,
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

impl Client {
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
    pub fn resource<T, F, U, E>(self, address: T, service: F) -> ClientRouter<E, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U, Publish>,
        U: Service<Publish, Response = PublishAck> + 'static,
        E: From<U::Error>,
        PublishAck: TryFrom<U::Error, Error = E>,
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
            _t: marker::PhantomData,
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
            16,
            into_service(|pkt| Ready::Ok(Either::Left(pkt))),
            into_service(|msg: ControlMessage<()>| {
                Ready::Ok(msg.disconnect(codec::Disconnect::default()))
            }),
        );

        let _ = Dispatcher::new(self.io, self.shared, dispatcher)
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
            16,
            into_service(|pkt| Ready::Ok(Either::Left(pkt))),
            service.into_service(),
        );

        Dispatcher::new(self.io, self.shared, dispatcher)
            .keepalive_timeout(Seconds::ZERO)
            .disconnect_timeout(self.disconnect_timeout)
            .await
    }

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

type Handler<E> = boxed::BoxService<Publish, PublishAck, E>;

/// Mqtt client with routing capabilities
pub struct ClientRouter<Err, PErr> {
    io: IoBoxed,
    builder: RouterBuilder<usize>,
    handlers: Vec<Handler<PErr>>,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    disconnect_timeout: Seconds,
    max_receive: usize,
    _t: marker::PhantomData<Err>,
}

impl<Err, PErr> fmt::Debug for ClientRouter<Err, PErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::ClientRouter")
            .field("keepalive", &self.keepalive)
            .field("disconnect_timeout", &self.disconnect_timeout)
            .field("max_receive", &self.max_receive)
            .finish()
    }
}

impl<Err, PErr> ClientRouter<Err, PErr>
where
    Err: From<PErr> + 'static,
    PublishAck: TryFrom<PErr, Error = Err>,
    PErr: 'static,
{
    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, S>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoService<S, Publish>,
        S: Service<Publish, Response = PublishAck, Error = PErr> + 'static,
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
            16,
            dispatch(self.builder.finish(), self.handlers),
            into_service(|msg: ControlMessage<Err>| {
                Ready::Ok(msg.disconnect(codec::Disconnect::default()))
            }),
        );

        let _ = Dispatcher::new(self.io, self.shared, dispatcher)
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
            16,
            dispatch(self.builder.finish(), self.handlers),
            service.into_service(),
        );

        Dispatcher::new(self.io, self.shared, dispatcher)
            .keepalive_timeout(Seconds::ZERO)
            .disconnect_timeout(self.disconnect_timeout)
            .await
    }

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

fn dispatch<Err, PErr>(
    router: Router<usize>,
    handlers: Vec<Handler<PErr>>,
) -> impl Service<Publish, Response = Either<Publish, PublishAck>, Error = Err>
where
    PErr: 'static,
    PublishAck: TryFrom<PErr, Error = Err>,
{
    // let handlers =
    let aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>> =
        RefCell::new(HashMap::default());
    let handlers = Rc::new(handlers);

    into_service(move |mut req: Publish| {
        let idx = if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    aliases.borrow_mut().insert(alias, (*idx, req.topic().clone()));
                }
                *idx
            } else {
                return Either::Right(Ready::<_, Err>::Ok(Either::Left(req)));
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = aliases.borrow();
            if let Some(item) = aliases.get(alias) {
                *req.topic_mut() = item.1.clone();
                item.0
            } else {
                log::error!("Unknown topic alias: {:?}", alias);
                return Either::Right(Ready::<_, Err>::Ok(Either::Left(req)));
            }
        } else {
            return Either::Right(Ready::<_, Err>::Ok(Either::Left(req)));
        };

        // exec handler
        let handlers = handlers.clone();
        Either::Left(async move { call(req, &handlers[idx]).await })
    })
}

async fn call<S, Err>(req: Publish, srv: &S) -> Result<Either<Publish, PublishAck>, Err>
where
    S: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<S::Error, Error = Err>,
{
    match srv.call(req).await {
        Ok(ack) => Ok(Either::Right(ack)),
        Err(err) => match PublishAck::try_from(err) {
            Ok(ack) => Ok(Either::Right(ack)),
            Err(err) => Err(err),
        },
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
