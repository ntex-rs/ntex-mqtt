use std::{fmt, future::Future, marker::PhantomData, rc::Rc};

use ntex::io::{DispatchItem, DispatcherConfig, IoBoxed};
use ntex::service::{IntoServiceFactory, Service, ServiceCtx, ServiceFactory};
use ntex::time::{timeout_checked, Millis, Seconds};
use ntex::util::{BoxFuture, Either};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::{io::Dispatcher, service, types::QoS};

use super::control::{ControlMessage, ControlResult};
use super::default::{DefaultControlService, DefaultPublishService};
use super::handshake::{Handshake, HandshakeAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, dispatcher::factory, MqttSink, Publish, Session};

/// Mqtt v3.1.1 server
///
/// `St` - connection state
/// `H` - handshake service
/// `C` - service for handling control messages
/// `P` - service for handling publish
///
/// Every mqtt connection is handled in several steps. First step is handshake. Server calls
/// handshake service with `Handshake` message, during this step service can authenticate connect
/// packet, it must return instance of connection state `St`.
///
/// Handshake service could be expressed as simple function:
///
/// ```rust,ignore
/// use ntex_mqtt::v3::{Handshake, HandshakeAck};
///
/// async fn handshake(hnd: Handshake) -> Result<HandshakeAkc<MyState>, MyError> {
///     Ok(hnd.ack(MyState::new(), false))
/// }
/// ```
///
/// During next stage, control and publish services get constructed,
/// both factories receive `Session<St>` state object as an argument. Publish service
/// handles `Publish` packet. On success, server server sends `PublishAck` packet to
/// the client, in case of error connection get closed. Control service receives all
/// other packets, like `Subscribe`, `Unsubscribe` etc. Also control service receives
/// errors from publish service and connection disconnect.
pub struct MqttServer<St, H, C, P> {
    handshake: H,
    control: C,
    publish: P,
    max_qos: QoS,
    max_size: u32,
    max_inflight: u16,
    max_inflight_size: usize,
    connect_timeout: Seconds,
    config: DispatcherConfig,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, H>
    MqttServer<St, H, DefaultControlService<St, H::Error>, DefaultPublishService<St, H::Error>>
where
    St: 'static,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<H, Handshake>,
    {
        let config = DispatcherConfig::default();
        config.set_disconnect_timeout(Seconds(3));

        MqttServer {
            config,
            handshake: handshake.into_factory(),
            control: DefaultControlService::default(),
            publish: DefaultPublishService::default(),
            max_qos: QoS::AtLeastOnce,
            max_size: 0,
            max_inflight: 16,
            max_inflight_size: 65535,
            connect_timeout: Seconds::ZERO,
            pool: Default::default(),
            _t: PhantomData,
        }
    }
}

impl<St, H, C, P> MqttServer<St, H, C, P>
where
    St: 'static,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C: ServiceFactory<ControlMessage<H::Error>, Session<St>, Response = ControlResult>
        + 'static,
    P: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
    H::Error:
        From<C::Error> + From<C::InitError> + From<P::Error> + From<P::InitError> + fmt::Debug,
{
    /// Set client timeout for first `Connect` frame.
    ///
    /// Defines a timeout for reading `Connect` frame. If a client does not transmit
    /// the entire frame within this time, the connection is terminated with
    /// Mqtt::Handshake(HandshakeError::Timeout) error.
    ///
    /// By default, connect timeout is disabled.
    pub fn connect_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout;
        self
    }

    #[deprecated(since = "0.12.5")]
    #[doc(hidden)]
    pub fn conenct_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout;
        self
    }

    #[deprecated(since = "0.12.1")]
    #[doc(hidden)]
    /// Set handshake timeout.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set server connection disconnect timeout.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(self, val: Seconds) -> Self {
        self.config.set_disconnect_timeout(val);
        self
    }

    /// Set read rate parameters for single frame.
    ///
    /// Set max timeout for reading single frame. If the client
    /// sends `rate` amount of data, increase the timeout by 1 second for every.
    /// But no more than `max_timeout` timeout.
    ///
    /// By default frame read rate is disabled.
    pub fn frame_read_rate(self, timeout: Seconds, max_timeout: Seconds, rate: u16) -> Self {
        self.config.set_frame_read_rate(timeout, max_timeout, rate);
        self
    }

    /// Set max allowed QoS.
    ///
    /// If peer sends publish with higher qos then ProtocolError::MaxQoSViolated(..)
    /// By default max qos is set to `ExactlyOnce`.
    pub fn max_qos(mut self, qos: QoS) -> Self {
        self.max_qos = qos;
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(mut self, size: u32) -> Self {
        self.max_size = size;
        self
    }

    /// Number of in-flight concurrent messages.
    ///
    /// By default in-flight is set to 16 messages
    pub fn inflight(mut self, val: u16) -> Self {
        self.max_inflight = val;
        self
    }

    /// Total size of in-flight messages.
    ///
    /// By default total in-flight size is set to 64Kb
    pub fn inflight_size(mut self, val: usize) -> Self {
        self.max_inflight_size = val;
        self
    }

    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<St, H, Srv, P>
    where
        F: IntoServiceFactory<Srv, ControlMessage<H::Error>, Session<St>>,
        Srv: ServiceFactory<ControlMessage<H::Error>, Session<St>, Response = ControlResult>
            + 'static,
        H::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            handshake: self.handshake,
            publish: self.publish,
            control: service.into_factory(),
            config: self.config,
            max_qos: self.max_qos,
            max_size: self.max_size,
            max_inflight: self.max_inflight,
            max_inflight_size: self.max_inflight_size,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<F, Srv>(self, publish: F) -> MqttServer<St, H, C, Srv>
    where
        F: IntoServiceFactory<Srv, Publish, Session<St>>,
        Srv: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
        H::Error: From<Srv::Error> + From<Srv::InitError> + fmt::Debug,
    {
        MqttServer {
            handshake: self.handshake,
            publish: publish.into_factory(),
            control: self.control,
            config: self.config,
            max_qos: self.max_qos,
            max_size: self.max_size,
            max_inflight: self.max_inflight,
            max_inflight_size: self.max_inflight_size,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Finish server configuration and create mqtt server factory
    pub fn finish(
        self,
    ) -> service::MqttServer<
        Session<St>,
        impl ServiceFactory<
            IoBoxed,
            Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds),
            Error = MqttError<H::Error>,
            InitError = H::InitError,
        >,
        impl ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            Session<St>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<H::Error>,
            InitError = MqttError<H::Error>,
        >,
        Rc<MqttShared>,
    > {
        service::MqttServer::new(
            HandshakeFactory {
                factory: self.handshake,
                max_size: self.max_size,
                connect_timeout: self.connect_timeout,
                pool: self.pool.clone(),
                _t: PhantomData,
            },
            factory(
                self.publish,
                self.control,
                self.max_inflight,
                self.max_inflight_size,
                self.max_qos,
            ),
            self.config,
        )
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn finish_selector<F, R>(
        self,
        check: F,
    ) -> impl ServiceFactory<
        Handshake,
        Response = Either<Handshake, ()>,
        Error = MqttError<H::Error>,
        InitError = H::InitError,
    >
    where
        F: Fn(&Handshake) -> R + 'static,
        R: Future<Output = Result<bool, H::Error>> + 'static,
    {
        ServerSelector {
            check: Rc::new(check),
            handshake: self.handshake,
            handler: Rc::new(factory(
                self.publish,
                self.control,
                self.max_inflight,
                self.max_inflight_size,
                self.max_qos,
            )),
            max_size: self.max_size,
            config: self.config,
            _t: PhantomData,
        }
    }
}

struct HandshakeFactory<St, H> {
    factory: H,
    max_size: u32,
    connect_timeout: Seconds,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, H> ServiceFactory<IoBoxed> for HandshakeFactory<St, H>
where
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
{
    type Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds);
    type Error = MqttError<H::Error>;

    type Service = HandshakeService<St, H::Service>;
    type InitError = H::InitError;
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Self::InitError>> where Self: 'f;

    fn create(&self, _: ()) -> Self::Future<'_> {
        Box::pin(async move {
            Ok(HandshakeService {
                max_size: self.max_size,
                pool: self.pool.clone(),
                service: self.factory.create(()).await?,
                connect_timeout: self.connect_timeout.into(),
                _t: PhantomData,
            })
        })
    }
}

struct HandshakeService<St, H> {
    service: H,
    max_size: u32,
    pool: Rc<MqttSinkPool>,
    connect_timeout: Millis,
    _t: PhantomData<St>,
}

impl<St, H> Service<IoBoxed> for HandshakeService<St, H>
where
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
{
    type Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds);
    type Error = MqttError<H::Error>;
    type Future<'f> = BoxFuture<'f, Result<Self::Response, Self::Error>> where Self: 'f;

    ntex::forward_poll_ready!(service, MqttError::Service);
    ntex::forward_poll_shutdown!(service);

    fn call<'a>(&'a self, io: IoBoxed, ctx: ServiceCtx<'a, Self>) -> Self::Future<'a> {
        log::trace!("Starting mqtt v3 handshake");

        Box::pin(async move {
            let codec = mqtt::Codec::default();
            codec.set_max_size(self.max_size);
            let shared =
                Rc::new(MqttShared::new(io.get_ref(), codec, false, self.pool.clone()));

            // read first packet
            let packet = timeout_checked(self.connect_timeout, io.recv(&shared.codec))
                .await
                .map_err(|_| MqttError::Handshake(HandshakeError::Timeout))?
                .map_err(|err| {
                    log::trace!("Error is received during mqtt handshake: {:?}", err);
                    MqttError::Handshake(HandshakeError::from(err))
                })?
                .ok_or_else(|| {
                    log::trace!("Server mqtt is disconnected during handshake");
                    MqttError::Handshake(HandshakeError::Disconnected(None))
                })?;

            match packet {
                (mqtt::Packet::Connect(connect), size) => {
                    // authenticate mqtt connection
                    let ack = ctx
                        .call(&self.service, Handshake::new(connect, size, io, shared))
                        .await
                        .map_err(MqttError::Service)?;

                    match ack.session {
                        Some(session) => {
                            let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
                                session_present: ack.session_present,
                                return_code: mqtt::ConnectAckReason::ConnectionAccepted,
                            });

                            log::trace!("Sending success handshake ack: {:#?}", pkt);

                            ack.shared.set_cap(ack.inflight as usize);
                            ack.io.encode(pkt, &ack.shared.codec)?;
                            Ok((
                                ack.io,
                                ack.shared.clone(),
                                Session::new(session, MqttSink::new(ack.shared)),
                                ack.keepalive,
                            ))
                        }
                        None => {
                            let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
                                session_present: false,
                                return_code: ack.return_code,
                            });

                            log::trace!("Sending failed handshake ack: {:#?}", pkt);
                            ack.io.encode(pkt, &ack.shared.codec)?;
                            let _ = ack.io.shutdown().await;

                            Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                        }
                    }
                }
                (packet, _) => {
                    log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {:?}", packet);
                    Err(MqttError::Handshake(HandshakeError::Protocol(
                        ProtocolError::unexpected_packet(
                            packet.packet_type(),
                            "MQTT-3.1.0-1: Expected CONNECT packet",
                        ),
                    )))
                }
            }
        })
    }
}

pub(crate) struct ServerSelector<St, H, T, F, R> {
    handshake: H,
    handler: Rc<T>,
    check: Rc<F>,
    config: DispatcherConfig,
    max_size: u32,
    _t: PhantomData<(St, R)>,
}

impl<St, H, T, F, R> ServiceFactory<Handshake> for ServerSelector<St, H, T, F, R>
where
    St: 'static,
    F: Fn(&Handshake) -> R + 'static,
    R: Future<Output = Result<bool, H::Error>>,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            Session<St>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<H::Error>,
            InitError = MqttError<H::Error>,
        > + 'static,
{
    type Response = Either<Handshake, ()>;
    type Error = MqttError<H::Error>;
    type InitError = H::InitError;
    type Service = ServerSelectorImpl<St, H::Service, T, F, R>;
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Self::InitError>> where Self: 'f;

    fn create(&self, _: ()) -> Self::Future<'_> {
        // create handshake service and then create service impl
        Box::pin(async move {
            Ok(ServerSelectorImpl {
                handler: self.handler.clone(),
                check: self.check.clone(),
                config: self.config.clone(),
                max_size: self.max_size,
                handshake: self.handshake.create(()).await?,
                _t: PhantomData,
            })
        })
    }
}

pub(crate) struct ServerSelectorImpl<St, H, T, F, R> {
    check: Rc<F>,
    handshake: H,
    handler: Rc<T>,
    max_size: u32,
    config: DispatcherConfig,
    _t: PhantomData<(St, R)>,
}

impl<St, H, T, F, R> Service<Handshake> for ServerSelectorImpl<St, H, T, F, R>
where
    St: 'static,
    F: Fn(&Handshake) -> R + 'static,
    R: Future<Output = Result<bool, H::Error>>,
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            Session<St>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<H::Error>,
            InitError = MqttError<H::Error>,
        > + 'static,
{
    type Response = Either<Handshake, ()>;
    type Error = MqttError<H::Error>;
    type Future<'f> = BoxFuture<'f, Result<Self::Response, Self::Error>> where Self: 'f;

    ntex::forward_poll_ready!(handshake, MqttError::Service);
    ntex::forward_poll_shutdown!(handshake);

    #[inline]
    fn call<'a>(&'a self, hnd: Handshake, ctx: ServiceCtx<'a, Self>) -> Self::Future<'a> {
        Box::pin(async move {
            log::trace!("Start connection handshake");

            let result = (*self.check)(&hnd).await;
            if !result.map_err(|e| MqttError::Handshake(HandshakeError::Service(e)))? {
                Ok(Either::Left(hnd))
            } else {
                // authenticate mqtt connection
                let ack = ctx.call(&self.handshake, hnd).await.map_err(|e| {
                    log::trace!("Connection handshake failed: {:?}", e);
                    MqttError::Handshake(HandshakeError::Service(e))
                })?;

                match ack.session {
                    Some(session) => {
                        let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
                            session_present: ack.session_present,
                            return_code: mqtt::ConnectAckReason::ConnectionAccepted,
                        });
                        log::trace!(
                            "Connection handshake succeeded, sending handshake ack: {:#?}",
                            pkt
                        );

                        ack.shared.set_cap(ack.inflight as usize);
                        ack.shared.codec.set_max_size(self.max_size);
                        ack.io.encode(pkt, &ack.shared.codec)?;

                        let session = Session::new(session, MqttSink::new(ack.shared.clone()));
                        let handler = self.handler.create(session).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Dispatcher::new(ack.io, ack.shared, handler, &self.config)
                            .keepalive_timeout(ack.keepalive)
                            .await?;
                        Ok(Either::Right(()))
                    }
                    None => {
                        let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
                            session_present: false,
                            return_code: ack.return_code,
                        });

                        log::trace!("Sending failed handshake ack: {:#?}", pkt);
                        ack.io.encode(pkt, &ack.shared.codec)?;
                        let _ = ack.io.shutdown().await;

                        Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                    }
                }
            }
        })
    }
}
