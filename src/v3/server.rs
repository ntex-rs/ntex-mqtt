use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_io::{DispatchItem, IoBoxed};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Identity, IntoServiceFactory, Service, ServiceCtx, ServiceFactory, Stack};
use ntex_util::time::{Seconds, timeout_checked};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::{MqttServiceConfig, service};

use super::control::{Control, ControlAck};
use super::default::{DefaultControlService, InFlightService};
use super::handshake::{Handshake, HandshakeAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{MqttSink, Publish, Session, codec as mqtt, dispatcher::factory};

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
pub struct MqttServer<St, H, C, M = Identity> {
    handshake: H,
    control: C,
    middleware: M,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, H> MqttServer<St, H, DefaultControlService<St, H::Error>, InFlightService>
where
    St: 'static,
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<H, Handshake, SharedCfg>,
    {
        MqttServer {
            handshake: handshake.into_factory(),
            control: DefaultControlService::default(),
            middleware: InFlightService,
            pool: Default::default(),
            _t: PhantomData,
        }
    }
}

impl<St, H, C, M> MqttServer<St, H, C, M>
where
    St: 'static,
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>> + 'static,
    C: ServiceFactory<Control<H::Error>, Session<St>, Response = ControlAck> + 'static,
    H::Error: From<C::Error> + From<C::InitError> + fmt::Debug,
{
    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<St, H, Srv, M>
    where
        F: IntoServiceFactory<Srv, Control<H::Error>, Session<St>>,
        Srv: ServiceFactory<Control<H::Error>, Session<St>, Response = ControlAck> + 'static,
        H::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            handshake: self.handshake,
            control: service.into_factory(),
            middleware: self.middleware,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Registers middleware, in the form of a middleware component (type),
    /// that runs during inbound and/or outbound processing in the request
    /// lifecycle (request -> response), modifying request/response as
    /// necessary, across all requests managed by the *Server*.
    ///
    /// Use middleware when you need to read or modify *every* request or
    /// response in some way.
    pub fn middleware<U>(self, mw: U) -> MqttServer<St, H, C, Stack<M, U>> {
        MqttServer {
            middleware: Stack::new(self.middleware, mw),
            handshake: self.handshake,
            control: self.control,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Replace middlewares
    pub fn replace_middlewares<U>(self, mw: U) -> MqttServer<St, H, C, U> {
        MqttServer {
            middleware: mw,
            handshake: self.handshake,
            control: self.control,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<F, Srv>(
        self,
        publish: F,
    ) -> service::MqttServer<
        Session<St>,
        impl ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds),
            Error = MqttError<H::Error>,
            InitError = H::InitError,
        >,
        impl ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            (SharedCfg, Session<St>),
            Response = Option<mqtt::Encoded>,
            Error = MqttError<H::Error>,
            InitError = MqttError<H::Error>,
        >,
        M,
        Rc<MqttShared>,
    >
    where
        H::Error: From<C::Error>
            + From<C::InitError>
            + From<Srv::Error>
            + From<Srv::InitError>
            + fmt::Debug,
        F: IntoServiceFactory<Srv, Publish, Session<St>>,
        Srv: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
        H::Error: From<Srv::Error> + From<Srv::InitError> + fmt::Debug,
    {
        service::MqttServer::new(
            HandshakeFactory {
                factory: self.handshake,
                pool: self.pool.clone(),
                _t: PhantomData,
            },
            factory(publish.into_factory(), self.control),
            self.middleware,
        )
    }
}

struct HandshakeFactory<St, H> {
    factory: H,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, H> ServiceFactory<IoBoxed, SharedCfg> for HandshakeFactory<St, H>
where
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
{
    type Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds);
    type Error = MqttError<H::Error>;

    type Service = HandshakeService<St, H::Service>;
    type InitError = H::InitError;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        Ok(HandshakeService {
            cfg: cfg.get(),
            pool: self.pool.clone(),
            service: self.factory.create(cfg).await?,
            _t: PhantomData,
        })
    }
}

struct HandshakeService<St, H> {
    service: H,
    cfg: Cfg<MqttServiceConfig>,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, H> Service<IoBoxed> for HandshakeService<St, H>
where
    H: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    H::Error: fmt::Debug,
{
    type Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds);
    type Error = MqttError<H::Error>;

    ntex_service::forward_ready!(service, MqttError::Service);
    ntex_service::forward_poll!(service, MqttError::Service);
    ntex_service::forward_shutdown!(service);

    async fn call(
        &self,
        io: IoBoxed,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Starting mqtt v3 handshake");

        let codec = mqtt::Codec::default();
        codec.set_max_size(self.cfg.max_size);
        codec.set_min_chunk_size(self.cfg.min_chunk_size);
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, self.pool.clone()));

        // read first packet
        let packet = timeout_checked(self.cfg.connect_timeout, io.recv(&shared.codec))
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
            mqtt::Decoded::Packet(mqtt::Packet::Connect(connect), size) => {
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

                        ack.shared.set_cap(ack.max_send.unwrap_or(self.cfg.max_send) as usize);
                        if let Some(max_packet_size) = ack.max_packet_size {
                            ack.shared.codec.set_max_size(max_packet_size.get());
                        }
                        ack.io.encode(mqtt::Encoded::Packet(pkt), &ack.shared.codec)?;
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
                        ack.io.encode(mqtt::Encoded::Packet(pkt), &ack.shared.codec)?;
                        let _ = ack.io.shutdown().await;

                        Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                    }
                }
            }
            mqtt::Decoded::Packet(packet, _) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {:?}", packet);
                Err(MqttError::Handshake(HandshakeError::Protocol(
                    ProtocolError::unexpected_packet(
                        packet.packet_type(),
                        "MQTT-3.1.0-1: Expected CONNECT packet",
                    ),
                )))
            }
            mqtt::Decoded::Publish(..) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received PUBLISH");
                Err(MqttError::Handshake(HandshakeError::Protocol(
                    ProtocolError::unexpected_packet(
                        crate::types::packet_type::PUBLISH_START,
                        "Expected CONNECT packet [MQTT-3.1.0-1]",
                    ),
                )))
            }
            mqtt::Decoded::PayloadChunk(..) => unreachable!(),
        }
    }
}
