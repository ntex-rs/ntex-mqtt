#![allow(clippy::type_complexity)]
use std::{cmp, fmt, marker::PhantomData, num::NonZero, rc::Rc};

use ntex_io::IoBoxed;
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Identity, IntoServiceFactory, Service, ServiceCtx, ServiceFactory, Stack};
use ntex_util::time::{Seconds, timeout_checked};

use crate::error::{DispatcherError, HandshakeError, MqttError, ProtocolError};
use crate::{MqttServiceConfig, control, control::Control, service};

use super::codec::{self as mqtt, Decoded, Encoded, Packet};
use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::default::{DefaultProtocolService, InFlightService};
use super::handshake::{Handshake, HandshakeAck};
use super::publish::{Publish, PublishAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{MqttSink, Session, ToPublishAck, dispatcher::ControlFactory, dispatcher::factory};

/// Mqtt Server
pub struct MqttServer<St, E, H, P, C, M = Identity> {
    control: C,
    handshake: H,
    protocol: P,
    middleware: M,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<(St, E)>,
}

impl<St, E, H, P, C, M> fmt::Debug for MqttServer<St, E, H, P, C, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::MqttServer").finish()
    }
}

impl<St, E, H>
    MqttServer<
        St,
        E,
        H,
        DefaultProtocolService<Session<St>, E>,
        ControlFactory<
            control::DefaultControlService<Session<St>, E, Encoded, H::Error>,
            St,
            E,
        >,
        InFlightService,
    >
where
    E: From<H::Error> + fmt::Debug,
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>>,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<H, Handshake, SharedCfg>,
    {
        MqttServer {
            handshake: handshake.into_factory(),
            protocol: DefaultProtocolService::default(),
            middleware: InFlightService,
            control: ControlFactory::new(control::DefaultControlService::default()),
            pool: Rc::new(MqttSinkPool::default()),
            _t: PhantomData,
        }
    }
}

impl<St, E, H, P, C, M> MqttServer<St, E, H, P, C, M>
where
    St: 'static,
    E: From<H::Error> + fmt::Debug,
    H: ServiceFactory<Handshake, SharedCfg, Response = HandshakeAck<St>> + 'static,
    P: ServiceFactory<ProtocolMessage, Session<St>, Response = ProtocolMessageAck> + 'static,
    C: ServiceFactory<Control<E>, Session<St>, Response = Option<Encoded>> + 'static,
{
    /// Remove all middlewares
    pub fn reset_middlewares(self) -> MqttServer<St, E, H, P, C, Identity> {
        MqttServer {
            middleware: Identity,
            handshake: self.handshake,
            control: self.control,
            protocol: self.protocol,
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
    pub fn middleware<U>(self, mw: U) -> MqttServer<St, E, H, P, C, Stack<M, U>> {
        MqttServer {
            middleware: Stack::new(self.middleware, mw),
            handshake: self.handshake,
            protocol: self.protocol,
            control: self.control,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Replace middlewares
    pub fn replace_middlewares<U>(self, mw: U) -> MqttServer<St, E, H, P, C, U> {
        MqttServer {
            middleware: mw,
            handshake: self.handshake,
            protocol: self.protocol,
            control: self.control,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Service to handle protocol control messages
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn protocol<F, Srv>(self, service: F) -> MqttServer<St, E, H, Srv, C, M>
    where
        F: IntoServiceFactory<Srv, ProtocolMessage, Session<St>>,
        Srv: ServiceFactory<ProtocolMessage, Session<St>, Response = ProtocolMessageAck>
            + 'static,
        E: From<Srv::Error>,
        H::Error: From<Srv::InitError>,
    {
        MqttServer {
            handshake: self.handshake,
            protocol: service.into_factory(),
            middleware: self.middleware,
            control: self.control,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Service to handle connection control messages
    pub fn control<F, Srv>(
        self,
        service: F,
    ) -> MqttServer<
        St,
        E,
        H,
        P,
        impl ServiceFactory<
            Control<E>,
            Session<St>,
            Response = Option<Encoded>,
            Error = H::Error,
            InitError = H::Error,
        >,
        M,
    >
    where
        F: IntoServiceFactory<Srv, Control<E>, Session<St>>,
        Srv: ServiceFactory<Control<E>, Session<St>, Response = Option<Encoded>> + 'static,
        H::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            handshake: self.handshake,
            protocol: self.protocol,
            control: ControlFactory::new(service.into_factory())
                .map_err(H::Error::from)
                .map_init_err(H::Error::from),
            middleware: self.middleware,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets
    ///
    /// And create mqtt server factory
    pub fn publish<F, Srv>(
        self,
        publish: F,
    ) -> service::MqttServer<
        Session<St>,
        E,
        H::Error,
        impl ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds),
            Error = MqttError<H::Error>,
        >,
        impl ServiceFactory<
            Decoded,
            (SharedCfg, Session<St>),
            Response = Option<Encoded>,
            Error = DispatcherError<E>,
            InitError = H::Error,
        >,
        M,
        C,
        Rc<MqttShared>,
    >
    where
        F: IntoServiceFactory<Srv, Publish, Session<St>>,
        H::Error: From<P::InitError>
            + From<C::Error>
            + From<C::InitError>
            + From<Srv::InitError>
            + fmt::Debug,
        E: From<P::Error> + 'static,
        Srv: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
        Srv::Error: ToPublishAck<Error = E>,
    {
        service::MqttServer::new(
            HandshakeFactory { factory: self.handshake, pool: self.pool, _t: PhantomData },
            factory(publish.into_factory().map_init_err(H::Error::from), self.protocol),
            self.middleware,
            self.control,
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
    ntex_service::forward_poll!(service, MqttError::Service);
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
            .map_err(|()| MqttError::Handshake(HandshakeError::Timeout))?
            .map_err(|err| {
                log::trace!("{}: Error is received during mqtt handshake: {err:?}", io.tag());
                MqttError::Handshake(HandshakeError::from(err))
            })?
            .ok_or_else(|| {
                log::trace!("{}: Server mqtt is disconnected during handshake", io.tag());
                MqttError::Handshake(HandshakeError::Disconnected(None))
            })?;

        match packet {
            Decoded::Packet(Packet::Connect(connect), size) => {
                // set max outbound (encoder) packet size
                if let Some(size) = connect.max_packet_size {
                    shared.codec.set_max_outbound_size(size.get());
                }
                let keep_alive = connect.keep_alive;
                let peer_receive_max = connect.receive_max.map(NonZero::get);
                if connect.session_expiry_interval_secs == 0 {
                    shared.set_zero_session_expiry();
                }

                // authenticate mqtt connection
                let mut ack = ctx
                    .call(&self.service, Handshake::new(connect, size, io, shared))
                    .await
                    .map_err(|e| MqttError::Handshake(HandshakeError::Service(e)))?;

                if let Some(session) = ack.session {
                    log::trace!("Sending: {:#?}", ack.packet);
                    let shared = ack.shared;

                    shared.set_max_qos(ack.packet.max_qos);
                    shared.set_receive_max(ack.packet.receive_max.get());
                    shared.set_topic_alias_max(ack.packet.topic_alias_max);
                    shared.codec.set_max_inbound_size(ack.packet.max_packet_size.unwrap_or(0));
                    shared.codec.set_retain_available(ack.packet.retain_available);
                    shared
                        .codec
                        .set_sub_ids_available(ack.packet.subscription_identifiers_available);
                    if ack.packet.server_keepalive_sec.is_none() && (keep_alive > ack.keepalive)
                    {
                        ack.packet.server_keepalive_sec = Some(ack.keepalive);
                    }

                    // outbound receive max
                    let max_send_cfg = ack.max_send.unwrap_or(self.cfg.max_send);
                    let max_send = peer_receive_max
                        .map_or(max_send_cfg, |val| cmp::min(max_send_cfg, val));
                    shared.set_cap(max_send as usize);

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
                } else {
                    log::trace!("Failed to complete handshake: {:#?}", ack.packet);

                    ack.io.encode(
                        Encoded::Packet(Packet::ConnectAck(Box::new(ack.packet))),
                        &ack.shared.codec,
                    )?;
                    let _ = ack.io.shutdown().await;
                    Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
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

#[cfg(test)]
mod tests {
    use ntex_service::fn_factory;
    use ntex_util::future::Ready;

    use super::*;

    #[test]
    fn test_debug() {
        let server = MqttServer::<(), (), _, _, _, _>::new(fn_factory(|| async {
            Ok::<_, ()>(ntex_service::fn_service(|h: Handshake| {
                Ready::<HandshakeAck<()>, ()>::Ok(h.ack(()))
            }))
        }));
        assert!(format!("{server:?}").contains("v5::MqttServer"));
    }
}
