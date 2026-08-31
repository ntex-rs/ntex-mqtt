#![allow(clippy::type_complexity)]
use std::{cmp, error::Error, fmt, marker::PhantomData, num::NonZero, rc::Rc};

use ntex_io::IoBoxed;
use ntex_service::cfg::Configuration;
use ntex_service::pipeline::PipelineFactory;
use ntex_service::{
    Ctx, Identity, IntoService, IntoServiceFactory, Service, ServiceFactory, Stack,
};
use ntex_util::{time::Seconds, time::timeout_checked};

use crate::error::{DispatcherError, HandshakeError, MqttError, ProtocolError};
use crate::types::{DefaultMapper, InputMapper};
use crate::{HandshakePipeline, MqttServiceConfig, control, control::Control, service};

use super::codec::{self as mqtt, Decoded, Encoded, Packet};
use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::default::{ControlFactory, DefaultProtocolService, InFlightService};
use super::handshake::{Handshake, HandshakeAck};
use super::publish::{Publish, PublishAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{Connection, MqttSink, Session, ToPublishAck, dispatcher::factory};

type ControlPipeline<AppSt, E, Err> = PipelineFactory<
    Session<AppSt>,
    Control<E>,
    Option<Encoded>,
    MqttError<Err>,
    Connection<AppSt>,
    Box<dyn Error>,
>;

/// Mqtt Server
pub struct MqttServer<Im, AppSt, Err, E, Pub, P, M = Identity> {
    im: Im,
    publish: Pub,
    protocol: P,
    middleware: M,
    control: ControlPipeline<AppSt, E, Err>,
    pool: Rc<MqttSinkPool>,
}

impl<Im, AppSt, Err, E, Pub, P, M> fmt::Debug for MqttServer<Im, AppSt, Err, E, Pub, P, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::MqttServer").finish()
    }
}

impl<AppSt, Err, E, Pub>
    MqttServer<DefaultMapper, AppSt, Err, E, Pub, DefaultProtocolService<E>, InFlightService>
where
    AppSt: 'static,
    Err: 'static,
    E: 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Connection<AppSt>, Res = PublishAck> + 'static,
    Pub::Error: ToPublishAck<Error = E>,
    Pub::InitError: Into<Box<dyn Error>> + 'static,
{
    /// Create mqtt v5 server and provide publish service
    pub fn new<I>(publish: I) -> Self
    where
        I: IntoServiceFactory<Pub, Session<AppSt>, Publish, Connection<AppSt>>,
    {
        Self::with_state(DefaultMapper, publish)
    }
}

impl<Im, AppSt, Err, E, Pub>
    MqttServer<Im, AppSt, Err, E, Pub, DefaultProtocolService<E>, InFlightService>
where
    Im: InputMapper,
    AppSt: 'static,
    Err: 'static,
    E: 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Connection<AppSt>, Res = PublishAck> + 'static,
    Pub::Error: ToPublishAck<Error = E>,
    Pub::InitError: Into<Box<dyn Error>> + 'static,
{
    /// Create mqtt v5 server with state
    pub fn with_state<I>(im: Im, publish: I) -> Self
    where
        I: IntoServiceFactory<Pub, Session<AppSt>, Publish, Connection<AppSt>>,
    {
        MqttServer {
            im,
            publish: publish.into_factory(),
            protocol: DefaultProtocolService::default(),
            middleware: InFlightService,
            pool: Rc::new(MqttSinkPool::default()),
            control: ControlPipeline::new(ControlFactory::new(control::DefaultControlService::<
                Err,
                _,
            >::default())),
        }
    }
}

impl<Im, AppSt, Err, E, Pub, P, M> MqttServer<Im, AppSt, Err, E, Pub, P, M>
where
    Im: InputMapper,
    AppSt: 'static,
    Err: 'static,
    E: From<P::Error> + 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Connection<AppSt>, Res = PublishAck> + 'static,
    Pub::Error: ToPublishAck<Error = E>,
    Pub::InitError: Into<Box<dyn Error>> + 'static,
    P: ServiceFactory<Session<AppSt>, ProtocolMessage, Connection<AppSt>, Res = ProtocolMessageAck>
        + 'static,
    P::InitError: Error,
{
    #[must_use]
    /// Registers middleware, in the form of a middleware component (type),
    /// that runs during inbound and/or outbound processing in the request
    /// lifecycle (request -> response), modifying request/response as
    /// necessary, across all requests managed by the *Server*.
    ///
    /// Use middleware when you need to read or modify *every* request or
    /// response in some way.
    pub fn middleware<U>(self, mw: U) -> MqttServer<Im, AppSt, Err, E, Pub, P, Stack<M, U>> {
        MqttServer {
            im: self.im,
            middleware: Stack::new(self.middleware, mw),
            publish: self.publish,
            protocol: self.protocol,
            control: self.control,
            pool: self.pool,
        }
    }

    #[must_use]
    /// Replace middlewares
    pub fn replace_middlewares<U>(self, mw: U) -> MqttServer<Im, AppSt, Err, E, Pub, P, U> {
        MqttServer {
            im: self.im,
            middleware: mw,
            publish: self.publish,
            protocol: self.protocol,
            control: self.control,
            pool: self.pool,
        }
    }

    #[must_use]
    /// Service to handle protocol control messages
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn protocol<F, Srv>(self, service: F) -> MqttServer<Im, AppSt, Err, E, Pub, Srv, M>
    where
        F: IntoServiceFactory<Srv, Session<AppSt>, ProtocolMessage, Connection<AppSt>>,
        Srv: ServiceFactory<
                Session<AppSt>,
                ProtocolMessage,
                Connection<AppSt>,
                Res = ProtocolMessageAck,
            > + 'static,
        Srv::InitError: Error,
        E: From<Srv::Error> + 'static,
    {
        MqttServer {
            im: self.im,
            publish: self.publish,
            protocol: service.into_factory(),
            middleware: self.middleware,
            control: self.control,
            pool: self.pool,
        }
    }

    #[must_use]
    /// Service to handle connection control messages
    pub fn control<Srv>(
        self,
        f: impl IntoServiceFactory<Srv, Session<AppSt>, Control<E>, Connection<AppSt>>,
    ) -> MqttServer<Im, AppSt, Err, E, Pub, P, M>
    where
        Srv: ServiceFactory<Session<AppSt>, Control<E>, Connection<AppSt>, Res = Option<Encoded>>
            + 'static,
        Srv::Error: Into<Err>,
        Srv::InitError: Error + 'static,
    {
        MqttServer {
            im: self.im,
            publish: self.publish,
            protocol: self.protocol,
            middleware: self.middleware,
            control: ControlPipeline::new(ControlFactory::new(
                f.into_factory().map_err(Into::into),
            )),
            pool: self.pool,
        }
    }

    /// Set service to handle handshake and create mqtt server
    pub fn connect<H, St>(
        self,
        handshake: impl IntoService<H, St, Handshake<Im::State>>,
    ) -> service::MqttServer<
        St,
        Im,
        AppSt,
        Rc<MqttShared>,
        MqttSink,
        Err,
        E,
        impl ServiceFactory<
            Session<AppSt>,
            Decoded,
            Connection<AppSt>,
            Res = Option<Encoded>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        >,
        M,
    >
    where
        H: Service<St, Handshake<Im::State>, Res = HandshakeAck<AppSt>, Error = Err> + 'static,
        St: 'static,
    {
        let handshake = HandshakePipeline::new(HandshakeService::<Im::State, AppSt, _> {
            svc: handshake.into_service().map_err(Into::into),
            pool: self.pool.clone(),
            _t: PhantomData,
        });

        service::MqttServer::new(
            self.im,
            handshake,
            factory(self.publish, self.protocol),
            self.middleware,
            self.control,
        )
    }
}

struct HandshakeService<ImSt, AppSt, H> {
    svc: H,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<(ImSt, AppSt)>,
}

impl<Hst, ImSt, AppSt, H> Service<Hst, (IoBoxed, ImSt)> for HandshakeService<ImSt, AppSt, H>
where
    Hst: 'static,
    H: Service<Hst, Handshake<ImSt>, Res = HandshakeAck<AppSt>> + 'static,
{
    type Res = (
        IoBoxed,
        Rc<MqttShared>,
        Session<AppSt>,
        Connection<AppSt>,
        Seconds,
    );
    type Error = MqttError<H::Error>;

    ntex_service::forward_ready!(Hst, svc, MqttError::Service);
    ntex_service::forward_shutdown!(Hst, svc);

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        (io, st): (IoBoxed, ImSt),
        ctx: Ctx<'_, Self, Hst>,
    ) -> Result<Self::Res, Self::Error> {
        log::trace!("Starting mqtt v5 handshake");

        let cfg = io.cfg().ctx().get::<MqttServiceConfig>();

        let codec = mqtt::Codec::default();
        codec.set_max_inbound_size(cfg.max_size);
        codec.set_min_chunk_size(cfg.min_chunk_size);

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, self.pool.clone()));
        shared.set_max_qos(cfg.max_qos);
        shared.set_receive_max(cfg.max_receive);
        shared.set_topic_alias_max(cfg.max_topic_alias);

        // read first packet
        let packet = timeout_checked(cfg.connect_timeout, io.recv(&shared.codec))
            .await
            .map_err(|()| MqttError::Handshake(HandshakeError::Timeout))?
            .map_err(|err| {
                log::trace!(
                    "{}: Error is received during mqtt handshake: {err:?}",
                    io.tag()
                );
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
                    .call(&self.svc, Handshake::new(connect, size, io, st, shared))
                    .await
                    .map_err(|e| MqttError::Handshake(HandshakeError::Service(e)))?;

                if let Some(session) = ack.session {
                    log::trace!("Sending: {:#?}", ack.packet);
                    let shared = ack.shared;

                    shared.set_max_qos(ack.packet.max_qos);
                    shared.set_receive_max(ack.packet.receive_max.get());
                    shared.set_topic_alias_max(ack.packet.topic_alias_max);
                    shared
                        .codec
                        .set_max_inbound_size(ack.packet.max_packet_size.unwrap_or(0));
                    shared
                        .codec
                        .set_retain_available(ack.packet.retain_available);
                    shared
                        .codec
                        .set_sub_ids_available(ack.packet.subscription_identifiers_available);
                    if ack.packet.server_keepalive_sec.is_none() && (keep_alive > ack.keepalive) {
                        ack.packet.server_keepalive_sec = Some(ack.keepalive);
                    }

                    // outbound receive max
                    let max_send_cfg = ack.max_send.unwrap_or(cfg.max_send);
                    let max_send =
                        peer_receive_max.map_or(max_send_cfg, |val| cmp::min(max_send_cfg, val));
                    shared.set_cap(max_send as usize);

                    ack.io.encode(
                        Encoded::Packet(Packet::ConnectAck(Box::new(ack.packet))),
                        &shared.codec,
                    )?;
                    let con = Connection::new(session.clone(), ack.io.shared());

                    Ok((ack.io, shared.clone(), session, con, Seconds(ack.keepalive)))
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
    use ntex_service::fn_service;
    use std::convert::Infallible;

    use super::*;

    #[derive(Debug)]
    struct TestError;

    impl From<Infallible> for TestError {
        fn from(_: Infallible) -> Self {
            TestError
        }
    }

    impl TryFrom<TestError> for PublishAck {
        type Error = TestError;

        fn try_from(err: TestError) -> Result<Self, Self::Error> {
            Err(err)
        }
    }

    #[test]
    fn test_debug() {
        let server = MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()));
        assert!(format!("{server:?}").contains("v5::MqttServer"));

        let _ = server.build(fn_service(async |h: Handshake| {
            Ok::<HandshakeAck<()>, TestError>(h.ack(()))
        }));
    }
}
