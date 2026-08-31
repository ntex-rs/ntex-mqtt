#![allow(clippy::type_complexity)]
use std::{error::Error, fmt, marker::PhantomData, rc::Rc};

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

use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::default::{ControlFactory, DefaultProtocolService, InFlightService};
use super::handshake::{Handshake, HandshakeAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{Connection, MqttSink, Publish, Session, codec as mqtt, dispatcher::factory};

type ControlPipeline<AppSt, E, Err> = PipelineFactory<
    Session<AppSt>,
    Control<E>,
    Option<mqtt::Encoded>,
    MqttError<Err>,
    Connection<AppSt>,
    Box<dyn Error>,
>;

/// Mqtt v3.1.1 server
///
/// `St` - connection state
/// `H` - handshake service
/// `P` - service for handling protocol control messages
/// `C` - service for handling connection control messages
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
pub struct MqttServer<Im, AppSt, Err, E, Pub, P, M = Identity> {
    im: Im,
    publish: Pub,
    protocol: P,
    middleware: M,
    control: ControlPipeline<AppSt, E, Err>,
    pub(super) pool: Rc<MqttSinkPool>,
}

impl<Im, AppSt, Err, E, Pub, P, M> fmt::Debug for MqttServer<Im, AppSt, Err, E, Pub, P, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::MqttServer").finish()
    }
}

impl<AppSt, Err, E>
    MqttServer<DefaultMapper, AppSt, Err, E, (), DefaultProtocolService<E>, InFlightService>
where
    AppSt: 'static,
    Err: 'static,
    E: 'static,
{
    /// Create server builder and provide publish service
    pub fn new<Pub, I>(
        publish: I,
    ) -> MqttServer<DefaultMapper, AppSt, Err, E, Pub, DefaultProtocolService<E>, InFlightService>
    where
        I: IntoServiceFactory<Pub, Session<AppSt>, Publish, Connection<AppSt>>,
        Pub: ServiceFactory<Session<AppSt>, Publish, Connection<AppSt>, Res = ()> + 'static,
        Pub::InitError: Into<Box<dyn Error>> + 'static,
    {
        Self::with_state(DefaultMapper, publish)
    }
}

impl<Im, AppSt, Err, E>
    MqttServer<Im, AppSt, Err, E, (), DefaultProtocolService<E>, InFlightService>
where
    Im: InputMapper,
    AppSt: 'static,
    Err: 'static,
    E: 'static,
{
    /// Create server builder with state
    pub fn with_state<Pub>(
        im: Im,
        publish: impl IntoServiceFactory<Pub, Session<AppSt>, Publish, Connection<AppSt>>,
    ) -> MqttServer<Im, AppSt, Err, E, Pub, DefaultProtocolService<E>, InFlightService>
    where
        Pub: ServiceFactory<Session<AppSt>, Publish, Connection<AppSt>, Res = ()> + 'static,
        Pub::InitError: Into<Box<dyn Error>> + 'static,
    {
        MqttServer {
            im,
            publish: publish.into_factory(),
            protocol: DefaultProtocolService::default(),
            middleware: InFlightService,
            control: ControlPipeline::new(
                ControlFactory::new(control::DefaultControlService::<Err, _>::default())
                    .map_err(MqttError::Service),
            ),
            pool: Rc::new(MqttSinkPool::default()),
        }
    }
}

impl<Im, AppSt, Err, E, Pub, P, M> MqttServer<Im, AppSt, Err, E, Pub, P, M>
where
    Im: InputMapper,
    AppSt: 'static,
    Err: 'static,
    E: From<Pub::Error> + From<P::Error> + 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Connection<AppSt>, Res = ()> + 'static,
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
    /// Service to handle protocol control messages.
    ///
    /// All control messages are processed sequentially, max number of buffered
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
        Srv::InitError: Error + 'static,
        E: From<Srv::Error>,
    {
        MqttServer {
            im: self.im,
            publish: self.publish,
            protocol: service.into_factory(),
            control: self.control,
            middleware: self.middleware,
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
        Srv: ServiceFactory<
                Session<AppSt>,
                Control<E>,
                Connection<AppSt>,
                Res = Option<mqtt::Encoded>,
            > + 'static,
        Srv::Error: Into<Err>,
        Srv::InitError: Error + 'static,
    {
        MqttServer {
            im: self.im,
            publish: self.publish,
            protocol: self.protocol,
            middleware: self.middleware,
            control: ControlPipeline::new(
                ControlFactory::new(f.into_factory().map_err(Into::into))
                    .map_err(MqttError::Service),
            ),
            pool: self.pool,
        }
    }

    /// Set service to handle connect and create mqtt server
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
            mqtt::Decoded,
            Connection<AppSt>,
            Res = Option<mqtt::Encoded>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        >,
        M,
    >
    where
        H: Service<St, Handshake<Im::State>, Res = HandshakeAck<AppSt>, Error = Err> + 'static,
        St: 'static,
    {
        let handshake = HandshakePipeline::new(HandshakeService {
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

struct HandshakeService<Im, AppSt, H> {
    svc: H,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<(Im, AppSt)>,
}

impl<St, Im, AppSt, H> Service<St, (IoBoxed, Im)> for HandshakeService<Im, AppSt, H>
where
    H: Service<St, Handshake<Im>, Res = HandshakeAck<AppSt>> + 'static,
{
    type Res = (
        IoBoxed,
        Rc<MqttShared>,
        Session<AppSt>,
        Connection<AppSt>,
        Seconds,
    );
    type Error = MqttError<H::Error>;

    ntex_service::forward_ready!(St, svc, MqttError::Service);
    ntex_service::forward_shutdown!(St, svc);

    async fn call(
        &self,
        (io, st): (IoBoxed, Im),
        ctx: Ctx<'_, Self, St>,
    ) -> Result<Self::Res, Self::Error> {
        log::trace!("Starting mqtt v3 handshake");

        let cfg = io.cfg().ctx().get::<MqttServiceConfig>();

        let codec = mqtt::Codec::default();
        codec.set_max_size(cfg.max_size);
        codec.set_min_chunk_size(cfg.min_chunk_size);
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec,
            false,
            self.pool.clone(),
        ));

        // read first packet
        let packet = timeout_checked(cfg.connect_timeout, io.recv(&shared.codec))
            .await
            .map_err(|()| MqttError::Handshake(HandshakeError::Timeout))?
            .map_err(|err| {
                log::trace!("Error is received during mqtt handshake: {err:?}");
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
                    .call(&self.svc, Handshake::new(connect, size, io, st, shared))
                    .await
                    .map_err(MqttError::Service)?;

                if let Some(session) = ack.session {
                    let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
                        session_present: ack.session_present,
                        return_code: mqtt::ConnectAckReason::ConnectionAccepted,
                    });

                    log::trace!("Sending success handshake ack: {pkt:#?}");

                    ack.shared
                        .set_cap(ack.max_send.unwrap_or(cfg.max_send) as usize);
                    if let Some(max_packet_size) = ack.max_packet_size {
                        ack.shared.codec.set_max_size(max_packet_size.get());
                    }
                    ack.io
                        .encode(mqtt::Encoded::Packet(pkt), &ack.shared.codec)?;

                    let con = Connection::new(session.clone(), ack.io.shared());

                    Ok((ack.io, ack.shared.clone(), session, con, ack.keepalive))
                } else {
                    let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
                        session_present: false,
                        return_code: ack.return_code,
                    });

                    log::trace!("Sending failed handshake ack: {pkt:#?}");
                    ack.io
                        .encode(mqtt::Encoded::Packet(pkt), &ack.shared.codec)?;
                    let _ = ack.io.shutdown().await;

                    Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                }
            }
            mqtt::Decoded::Packet(packet, _) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {packet:?}");
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

#[cfg(test)]
mod tests {
    use ntex_service::fn_service;

    use super::*;

    #[test]
    fn test_debug() {
        let server = MqttServer::new(async |_| Ok::<_, ()>(()));
        assert!(format!("{server:?}").contains("v3::MqttServer"));

        let _ = server.build(fn_service(async |h: Handshake| {
            Ok::<HandshakeAck<()>, ()>(h.ack((), false))
        }));
    }
}
