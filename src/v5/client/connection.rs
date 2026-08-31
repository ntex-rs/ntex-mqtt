use std::{cell::RefCell, fmt, marker, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_io::IoBoxed;
use ntex_router::{IntoPattern, Path, Router, RouterBuilder};
use ntex_service::pipeline::{Pipeline, PipelineState};
use ntex_service::{IntoService, Service, cfg::Cfg, fn_service, fn_service_st};
use ntex_util::time::{Millis, Seconds, sleep};
use ntex_util::{HashMap, future::Either};

use crate::v5::default::ControlService;
use crate::v5::publish::{Publish, PublishAck};
use crate::v5::{ProtocolMessageAck, Session, codec, shared::MqttShared, sink::MqttSink};
use crate::{MqttServiceConfig, control, error::MqttError, io::Dispatcher};

use super::{control::ProtocolMessage, dispatcher::create_dispatcher};

/// Mqtt client
pub struct Client {
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    max_receive: usize,
    cfg: Cfg<MqttServiceConfig>,
    pkt: Box<codec::ConnectAck>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::Client")
            .field("keepalive", &self.keepalive)
            .field("max_receive", &self.max_receive)
            .field("cfg", &self.cfg)
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
        cfg: Cfg<MqttServiceConfig>,
    ) -> Self {
        Client {
            io,
            pkt,
            shared,
            cfg,
            keepalive,
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
        F: IntoService<U, Session<()>, Publish>,
        U: Service<Session<()>, Publish, Res = PublishAck> + 'static,
        E: From<U::Error>,
        PublishAck: TryFrom<U::Error, Error = E>,
    {
        let mut builder = Router::build();
        builder.path(address, 0);
        let handlers = vec![PipelineState::new(service.into_service())];

        ClientRouter {
            builder,
            handlers,
            io: self.io,
            shared: self.shared,
            keepalive: self.keepalive,
            max_receive: self.max_receive,
            cfg: self.cfg,
            _t: marker::PhantomData,
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) {
        let sink = MqttSink::new(self.shared.clone());

        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::with(
            Session::new((), sink.clone()),
            create_dispatcher(
                self.shared.clone(),
                fn_service(async |pkt| Ok(Either::Left(pkt))),
                fn_service(async |msg: ProtocolMessage| {
                    Ok::<_, ()>(msg.disconnect(codec::Disconnect::default()))
                }),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::with(
            Session::new((), sink),
            ControlService::new(
                control::DefaultControlService::<(), codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        let _ = Dispatcher::new(self.io, self.shared, dispatcher, control).await;
    }

    /// Run client with provided control messages handler
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<()>>
    where
        F: IntoService<S, Session<()>, ProtocolMessage> + 'static,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = ()> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());

        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::with(
            Session::new((), sink.clone()),
            create_dispatcher(
                self.shared.clone(),
                fn_service(async |pkt| Ok(Either::Left(pkt))),
                service.into_service(),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::with(
            Session::new((), sink),
            ControlService::new(
                control::DefaultControlService::<(), codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Run client with provided control messages handler
    pub async fn start_with_control<F, S, C, E>(
        self,
        service: F,
        control: C,
    ) -> Result<(), MqttError<C::Error>>
    where
        E: fmt::Debug + 'static,
        F: IntoService<S, Session<()>, ProtocolMessage> + 'static,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = E> + 'static,
        C: Service<Session<()>, control::Control<E>, Res = Option<codec::Encoded>> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());
        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::with(
            Session::new((), sink.clone()),
            create_dispatcher(
                self.shared.clone(),
                fn_service(async |pkt| Ok(Either::Left(pkt))),
                service.into_service(),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::with(
            Session::new((), sink),
            ControlService::new(control, self.shared.clone()),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

/// Mqtt client with routing capabilities
pub struct ClientRouter<Err, PErr> {
    io: IoBoxed,
    builder: RouterBuilder<usize>,
    handlers: Vec<PipelineState<Session<()>, Publish, PublishAck, PErr>>,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    max_receive: usize,
    cfg: Cfg<MqttServiceConfig>,
    _t: marker::PhantomData<(Err, PErr)>,
}

impl<Err, PErr> fmt::Debug for ClientRouter<Err, PErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::ClientRouter")
            .field("keepalive", &self.keepalive)
            .field("max_receive", &self.max_receive)
            .finish()
    }
}

impl<Err, PErr> ClientRouter<Err, PErr>
where
    Err: From<PErr> + fmt::Debug + 'static,
    PublishAck: TryFrom<PErr, Error = PErr>,
    PErr: fmt::Debug + 'static,
{
    #[must_use]
    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, S>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoService<S, Session<()>, Publish>,
        S: Service<Session<()>, Publish, Res = PublishAck, Error = PErr> + 'static,
    {
        self.builder.path(address, self.handlers.len());
        self.handlers
            .push(PipelineState::new(service.into_service()));
        self
    }

    /// Run client with default control messages handler
    pub async fn start_default(self) {
        let sink = MqttSink::new(self.shared.clone());
        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::with(
            Session::new((), sink.clone()),
            create_dispatcher(
                self.shared.clone(),
                dispatch(self.builder.finish(), self.handlers),
                fn_service(async |msg: ProtocolMessage| {
                    Ok(msg.disconnect(codec::Disconnect::default()))
                }),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::with(
            Session::new((), sink),
            ControlService::new(
                control::DefaultControlService::<Err, codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        let _ = Dispatcher::new(self.io, self.shared, dispatcher, control).await;
    }

    /// Run client and handle control messages
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<Err>>
    where
        F: IntoService<S, Session<()>, ProtocolMessage>,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = PErr> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());
        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::with(
            Session::new((), sink.clone()),
            create_dispatcher(
                self.shared.clone(),
                dispatch(self.builder.finish(), self.handlers),
                service.into_service(),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::with(
            Session::new((), sink),
            ControlService::new(
                control::DefaultControlService::<Err, codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

fn dispatch<PErr>(
    router: Router<usize>,
    handlers: Vec<PipelineState<Session<()>, Publish, PublishAck, PErr>>,
) -> impl Service<Session<()>, Publish, Res = Either<Publish, PublishAck>, Error = PErr>
where
    PErr: 'static,
    PublishAck: TryFrom<PErr, Error = PErr>,
{
    // let handlers =
    let aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>> =
        RefCell::new(HashMap::default());
    let handlers = Rc::new(handlers);

    fn_service_st(async move |st: &Session<()>, mut req: Publish| {
        let idx = if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    aliases
                        .borrow_mut()
                        .insert(alias, (*idx, req.topic().clone()));
                }
                *idx
            } else {
                return Ok::<_, PErr>(Either::Left(req));
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = aliases.borrow();
            if let Some(item) = aliases.get(alias) {
                *req.topic_mut() = item.1.clone();
                item.0
            } else {
                log::error!("Unknown topic alias: {alias:?}");
                return Ok(Either::Left(req));
            }
        } else {
            return Ok(Either::Left(req));
        };

        // exec handler
        match handlers[idx].call(req, st).await {
            Ok(ack) => Ok(Either::Right(ack)),
            Err(err) => match PublishAck::try_from(err) {
                Ok(ack) => Ok(Either::Right(ack)),
                Err(err) => Err(err),
            },
        }
    })
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
