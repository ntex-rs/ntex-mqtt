use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_io::{DispatchItem, IoBoxed};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Identity, IntoServiceFactory, Service, ServiceCtx, ServiceFactory, Stack};
use ntex_util::time::{Seconds, timeout_checked};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::{MqttServiceConfig, service};

use super::codec::{self as mqtt, Decoded, Encoded, Packet};
use super::control::{Control, ControlAck};
use super::default::{DefaultControlService, InFlightService};
use super::handshake::{Handshake, HandshakeAck};
use super::publish::{Publish, PublishAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{MqttSink, Session, dispatcher::factory};

/// Mqtt Server
pub struct MqttServer<St, C, Cn, M = Identity> {
    handshake: C,
    control: Cn,
    middleware: M,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, C> MqttServer<St, C, DefaultControlService<St, C::Error>, InFlightService>
where
    C: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>>,
    C::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<C, Handshake, SharedCfg>,
    {
        MqttServer {
            handshake: handshake.into_factory(),
            control: DefaultControlService::default(),
            middleware: InFlightService,
            pool: Rc::new(MqttSinkPool::default()),
            _t: PhantomData,
        }
    }
}

impl<St, C, Cn, M> MqttServer<St, C, Cn, M>
where
    St: 'static,
    C: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    Cn: ServiceFactory<Control<C::Error>, Session<St>, Response = ControlAck> + 'static,
{
    /// Remove all middlewares
    pub fn reset_middlewares(self) -> MqttServer<St, C, Cn, Identity> {
        MqttServer {
            middleware: Identity,
            control: self.control,
            handshake: self.handshake,
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
    pub fn middleware<U>(self, mw: U) -> MqttServer<St, C, Cn, Stack<M, U>> {
        MqttServer {
            middleware: Stack::new(self.middleware, mw),
            handshake: self.handshake,
            control: self.control,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<St, C, Srv, M>
    where
        F: IntoServiceFactory<Srv, Control<C::Error>, Session<St>>,
        Srv: ServiceFactory<Control<C::Error>, Session<St>, Response = ControlAck> + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            handshake: self.handshake,
            control: service.into_factory(),
            middleware: self.middleware,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    /// and create mqtt server factory
    pub fn publish<F, Srv>(
        self,
        publish: F,
    ) -> service::MqttServer<
        Session<St>,
        impl ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds),
            Error = MqttError<C::Error>,
            InitError = C::InitError,
        >,
        impl ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            (SharedCfg, Session<St>),
            Response = Option<mqtt::Encoded>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        >,
        M,
        Rc<MqttShared>,
    >
    where
        F: IntoServiceFactory<Srv, Publish, Session<St>>,
        C::Error:
            From<Cn::Error> + From<Cn::InitError> + From<Srv::Error> + From<Srv::InitError>,
        Srv: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
        Srv::Error: fmt::Debug,
        PublishAck: TryFrom<Srv::Error, Error = C::Error>,
    {
        service::MqttServer::new(
            HandshakeFactory { factory: self.handshake, pool: self.pool, _t: PhantomData },
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
            service: self.factory.create(cfg).await?,
            pool: self.pool.clone(),
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
    ntex_service::forward_shutdown!(service);

    async fn call(
        &self,
        io: IoBoxed,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Starting mqtt v5 handshake");

        let codec = mqtt::Codec::default();
        codec.set_max_inbound_size(self.cfg.max_size);
        codec.set_min_chunk_size(self.cfg.min_chunk_size);

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, self.pool.clone()));
        shared.set_max_qos(self.cfg.max_qos);
        shared.set_receive_max(self.cfg.max_receive);
        shared.set_topic_alias_max(self.cfg.max_topic_alias);

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
            Decoded::Packet(Packet::Connect(connect), size) => {
                // set max outbound (encoder) packet size
                if let Some(size) = connect.max_packet_size {
                    shared.codec.set_max_outbound_size(size.get());
                }
                let keep_alive = connect.keep_alive;
                let peer_receive_max =
                    connect.receive_max.map(|v| v.get()).unwrap_or(16) as usize;

                // authenticate mqtt connection
                let mut ack = ctx
                    .call(&self.service, Handshake::new(connect, size, io, shared))
                    .await
                    .map_err(|e| MqttError::Handshake(HandshakeError::Service(e)))?;

                match ack.session {
                    Some(session) => {
                        log::trace!("Sending: {:#?}", ack.packet);
                        let shared = ack.shared;

                        shared.set_max_qos(ack.packet.max_qos);
                        shared.set_receive_max(ack.packet.receive_max.get());
                        shared.set_topic_alias_max(ack.packet.topic_alias_max);
                        shared
                            .codec
                            .set_max_inbound_size(ack.packet.max_packet_size.unwrap_or(0));
                        shared.codec.set_retain_available(ack.packet.retain_available);
                        shared.codec.set_sub_ids_available(
                            ack.packet.subscription_identifiers_available,
                        );
                        if ack.packet.server_keepalive_sec.is_none()
                            && (keep_alive > ack.keepalive)
                        {
                            ack.packet.server_keepalive_sec = Some(ack.keepalive);
                        }
                        shared.set_cap(peer_receive_max);

                        ack.io.encode(
                            Encoded::Packet(Packet::ConnectAck(Box::new(ack.packet))),
                            &shared.codec,
                        )?;

                        Ok((
                            ack.io,
                            shared.clone(),
                            Session::new(session, MqttSink::new(shared)),
                            Seconds(ack.keepalive),
                        ))
                    }
                    None => {
                        log::trace!("Failed to complete handshake: {:#?}", ack.packet);

                        ack.io.encode(
                            Encoded::Packet(Packet::ConnectAck(Box::new(ack.packet))),
                            &ack.shared.codec,
                        )?;
                        let _ = ack.io.shutdown().await;
                        Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                    }
                }
            }
            Decoded::Packet(packet, _) => {
                log::info!(
                    "MQTT-3.1.0-1: Expected CONNECT packet, received {}",
                    packet.packet_type()
                );
                Err(MqttError::Handshake(HandshakeError::Protocol(
                    ProtocolError::unexpected_packet(
                        packet.packet_type(),
                        "Expected CONNECT packet [MQTT-3.1.0-1]",
                    ),
                )))
            }
            Decoded::Publish(..) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received PUBLISH");
                Err(MqttError::Handshake(HandshakeError::Protocol(
                    ProtocolError::unexpected_packet(
                        crate::types::packet_type::PUBLISH_START,
                        "Expected CONNECT packet [MQTT-3.1.0-1]",
                    ),
                )))
            }
            Decoded::PayloadChunk(..) => unreachable!(),
        }
    }
}
