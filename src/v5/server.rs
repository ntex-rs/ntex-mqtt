#![allow(clippy::type_complexity)]
use std::{cmp, fmt, marker::PhantomData, num::NonZero, rc::Rc};

use ntex_error::{Error, ErrorDiagnostic, ErrorInfo};
use ntex_io::IoBoxed;
use ntex_service::cfg::Configuration;
use ntex_service::pipeline::PipelineFactory;
use ntex_service::{
    Ctx, Identity, IntoService, IntoServiceFactory, Service, ServiceFactory, Stack,
};
use ntex_util::{time::Seconds, time::timeout_checked};

use crate::error::{DispatcherError, MqttConnectError, MqttError, MqttProtocolError};
use crate::{ConnectPipeline, MqttServiceConfig, control, control::Control, service};

use super::codec::{self as mqtt, Decoded, Encoded, Packet};
use super::connect::{Connect, ConnectAck};
use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::default::{ControlFactory, DefaultProtoSrv, InFlightService};
use super::publish::{Publish, PublishAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{MqttSink, Session, ToPublishAck, dispatcher::factory};

type ControlPipeline<AppSt, E, Err> =
    PipelineFactory<Session<AppSt>, Control<E>, Option<Encoded>, MqttError<Err>, ErrorInfo>;

/// Mqtt Server
pub struct MqttServer<Im, AppSt, Err, E, Pub, P, M = Identity> {
    publish: Pub,
    protocol: P,
    middleware: M,
    control: ControlPipeline<AppSt, E, Err>,
    pool: Rc<MqttSinkPool>,
    st: PhantomData<Im>,
}

impl<Im, AppSt, Err, E, Pub, P, M> fmt::Debug for MqttServer<Im, AppSt, Err, E, Pub, P, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::MqttServer").finish()
    }
}

impl<AppSt, Err, E, Pub> MqttServer<(), AppSt, Err, E, Pub, DefaultProtoSrv<E>, InFlightService>
where
    AppSt: 'static,
    Err: 'static,
    E: 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Res = PublishAck> + 'static,
    Pub::Error: ToPublishAck<Error = E>,
    Pub::InitError: ErrorDiagnostic,
{
    /// Create mqtt v5 server and provide publish service
    pub fn new<I>(publish: I) -> Self
    where
        I: IntoServiceFactory<Pub, Session<AppSt>, Publish>,
    {
        Self::with(publish)
    }
}

impl<Im, AppSt, Err, E, Pub> MqttServer<Im, AppSt, Err, E, Pub, DefaultProtoSrv<E>, InFlightService>
where
    Im: 'static,
    AppSt: 'static,
    Err: 'static,
    E: 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Res = PublishAck> + 'static,
    Pub::Error: ToPublishAck<Error = E>,
    Pub::InitError: ErrorDiagnostic,
{
    /// Create mqtt v5 server with state
    pub fn with<I>(publish: I) -> Self
    where
        I: IntoServiceFactory<Pub, Session<AppSt>, Publish>,
    {
        MqttServer {
            publish: publish.into_factory(),
            protocol: DefaultProtoSrv::default(),
            middleware: InFlightService,
            pool: Rc::new(MqttSinkPool::default()),
            control: ControlPipeline::new(ControlFactory::new(control::DefaultControlService::<
                Err,
                _,
            >::default())),
            st: PhantomData,
        }
    }
}

impl<Im, AppSt, Err, E, Pub, P, M> MqttServer<Im, AppSt, Err, E, Pub, P, M>
where
    Im: 'static,
    AppSt: 'static,
    Err: 'static,
    E: From<P::Error> + 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Res = PublishAck> + 'static,
    Pub::Error: ToPublishAck<Error = E>,
    Pub::InitError: ErrorDiagnostic,
    P: ServiceFactory<Session<AppSt>, ProtocolMessage, Res = ProtocolMessageAck> + 'static,
    P::InitError: ErrorDiagnostic,
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
            middleware: Stack::new(self.middleware, mw),
            publish: self.publish,
            protocol: self.protocol,
            control: self.control,
            pool: self.pool,
            st: self.st,
        }
    }

    #[must_use]
    /// Replace middlewares
    pub fn replace_middlewares<U>(self, mw: U) -> MqttServer<Im, AppSt, Err, E, Pub, P, U> {
        MqttServer {
            middleware: mw,
            publish: self.publish,
            protocol: self.protocol,
            control: self.control,
            pool: self.pool,
            st: self.st,
        }
    }

    #[must_use]
    /// Service to handle protocol control messages
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn protocol<F, Srv>(self, service: F) -> MqttServer<Im, AppSt, Err, E, Pub, Srv, M>
    where
        F: IntoServiceFactory<Srv, Session<AppSt>, ProtocolMessage>,
        E: From<Srv::Error> + 'static,
        Srv: ServiceFactory<Session<AppSt>, ProtocolMessage, Res = ProtocolMessageAck> + 'static,
        Srv::InitError: ErrorDiagnostic,
    {
        MqttServer {
            publish: self.publish,
            protocol: service.into_factory(),
            middleware: self.middleware,
            control: self.control,
            pool: self.pool,
            st: self.st,
        }
    }

    #[must_use]
    /// Service to handle connection control messages
    pub fn control<Srv>(
        self,
        f: impl IntoServiceFactory<Srv, Session<AppSt>, Control<E>>,
    ) -> MqttServer<Im, AppSt, Err, E, Pub, P, M>
    where
        Srv: ServiceFactory<Session<AppSt>, Control<E>, Res = Option<Encoded>> + 'static,
        Srv::Error: Into<Err>,
        Srv::InitError: ErrorDiagnostic,
    {
        MqttServer {
            publish: self.publish,
            protocol: self.protocol,
            middleware: self.middleware,
            control: ControlPipeline::new(ControlFactory::new(
                f.into_factory()
                    .map_err(Into::into)
                    .map_init_err(|e| ErrorInfo::from(Error::from(e))),
            )),
            pool: self.pool,
            st: self.st,
        }
    }

    /// Set service to handle Connect packet and create mqtt server
    pub fn build<S, St>(
        self,
        connect: impl IntoService<S, St, Connect<Im>>,
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
            Res = Option<Encoded>,
            Error = DispatcherError<E>,
            InitError = ErrorInfo,
        >,
        M,
    >
    where
        S: Service<St, Connect<Im>, Res = ConnectAck<AppSt>, Error = Err> + 'static,
        St: 'static,
    {
        let connect = ConnectPipeline::new(ConnectService::<Im, AppSt, _> {
            svc: connect.into_service().map_err(Into::into),
            pool: self.pool.clone(),
            _t: PhantomData,
        });

        service::MqttServer::new(
            connect,
            factory(self.publish, self.protocol),
            self.middleware,
            self.control,
        )
    }
}

struct ConnectService<ImSt, AppSt, S> {
    svc: S,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<(ImSt, AppSt)>,
}

impl<Hst, ImSt, AppSt, S> Service<Hst, (IoBoxed, ImSt)> for ConnectService<ImSt, AppSt, S>
where
    Hst: 'static,
    S: Service<Hst, Connect<ImSt>, Res = ConnectAck<AppSt>> + 'static,
{
    type Res = (IoBoxed, Rc<MqttShared>, Session<AppSt>, Seconds);
    type Error = MqttError<S::Error>;

    ntex_service::forward_ready!(Hst, svc, MqttError::Service);
    ntex_service::forward_shutdown!(Hst, svc);

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        (io, st): (IoBoxed, ImSt),
        ctx: Ctx<'_, Self, Hst>,
    ) -> Result<Self::Res, Self::Error> {
        log::trace!("Starting mqtt v5 Connect");

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
            .map_err(|()| MqttError::Connect(MqttConnectError::Timeout))?
            .map_err(|err| {
                log::trace!(
                    "{}: Error is received during mqtt Connect: {err:?}",
                    io.tag()
                );
                MqttError::Connect(MqttConnectError::from(err))
            })?
            .ok_or_else(|| {
                log::trace!("{}: Server mqtt is disconnected during Connect", io.tag());
                MqttError::Connect(MqttConnectError::Disconnected(None))
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
                    .call(&self.svc, Connect::new(connect, size, io, st, shared))
                    .await
                    .map_err(|e| MqttError::Connect(MqttConnectError::Service(e)))?;

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

                    Ok((ack.io, shared.clone(), session, Seconds(ack.keepalive)))
                } else {
                    log::trace!("Failed to complete Connect: {:#?}", ack.packet);

                    ack.io.encode(
                        Encoded::Packet(Packet::ConnectAck(Box::new(ack.packet))),
                        &ack.shared.codec,
                    )?;
                    let _ = ack.io.shutdown().await;
                    Err(MqttError::Connect(MqttConnectError::Disconnected(None)))
                }
            }
            Decoded::Packet(packet, _) => {
                log::info!(
                    "MQTT-3.1.0-1: Expected CONNECT packet, received {}",
                    packet.packet_type()
                );
                Err(MqttError::Connect(MqttConnectError::Protocol(
                    MqttProtocolError::unexpected_packet(
                        packet.packet_type(),
                        "Expected CONNECT packet [MQTT-3.1.0-1]",
                    ),
                )))
            }
            Decoded::Publish(..) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received PUBLISH");
                Err(MqttError::Connect(MqttConnectError::Protocol(
                    MqttProtocolError::unexpected_packet(
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
    use std::{convert::Infallible, io};

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
        let server = MqttServer::<(), (), io::Error, _, _, _, _>::new(ntex_service::fn_service(
            async |p: Publish| Ok::<_, TestError>(p.ack()),
        ));
        assert!(format!("{server:?}").contains("v5::MqttServer"));
    }
}
