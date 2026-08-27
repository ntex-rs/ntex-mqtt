#![allow(clippy::type_complexity)]
use std::{error::Error, fmt, marker::PhantomData, rc::Rc};

use ntex_io::IoBoxed;
use ntex_service::cfg::Configuration;
use ntex_service::{
    Ctx, Identity, IntoService, IntoServiceFactory, Service, ServiceFactory, Stack,
};
use ntex_util::time::{Seconds, timeout_checked};

use crate::HandshakePipeline;
use crate::error::{DispatcherError, HandshakeError, MqttError, ProtocolError};
use crate::{MqttServiceConfig, control, control::Control, service};

use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::default::{ControlFactory, DefaultProtocolService, InFlightService};
use super::handshake::{Handshake, HandshakeAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{Connection, MqttSink, Publish, Session, codec as mqtt, dispatcher::factory};

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
pub struct MqttServer<St, AppSt, Err, E, P, C, M = Identity> {
    handshake: HandshakePipeline<St, AppSt, Rc<MqttShared>, MqttSink, MqttError<Err>>,
    protocol: P,
    control: C,
    middleware: M,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<(AppSt, E)>,
}

impl<St, AppSt, Err, E, P, C, M> fmt::Debug for MqttServer<St, AppSt, Err, E, P, C, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::MqttServer").finish()
    }
}

impl<AppSt, Err, E>
    MqttServer<
        (),
        AppSt,
        Err,
        E,
        DefaultProtocolService<Session<AppSt>, E>,
        ControlFactory<
            (),
            AppSt,
            control::DefaultControlService<Session<AppSt>, E, mqtt::Encoded>,
            E,
        >,
        InFlightService,
    >
where
    AppSt: 'static,
    Err: 'static,
{
    /// Create server factory and provide handshake service
    pub fn new<H>(handshake: impl IntoService<H, (), Handshake>) -> Self
    where
        H: Service<(), Handshake, Res = HandshakeAck<AppSt>, Error = Err> + 'static,
    {
        let pool = Rc::new(MqttSinkPool::default());

        let handshake = HandshakeService {
            svc: handshake.into_service(),
            pool: pool.clone(),
            _t: PhantomData,
        };

        MqttServer {
            pool,
            handshake: HandshakePipeline::new(handshake),
            protocol: DefaultProtocolService::default(),
            middleware: InFlightService,
            control: ControlFactory::new(control::DefaultControlService::default()),
            _t: PhantomData,
        }
    }
}

impl<St, AppSt, Err, E, P, C, M> MqttServer<St, AppSt, Err, E, P, C, M>
where
    St: 'static,
    AppSt: 'static,
    P: ServiceFactory<Session<AppSt>, ProtocolMessage, Connection<St>, Res = ProtocolMessageAck>
        + 'static,
    P::InitError: Error,
    C: ServiceFactory<
            Session<AppSt>,
            Control<Err>,
            Connection<St>,
            Res = Option<mqtt::Encoded>,
            Error = MqttError<Err>,
            InitError = Box<dyn Error>,
        > + 'static,
{
    /// Registers middleware, in the form of a middleware component (type),
    /// that runs during inbound and/or outbound processing in the request
    /// lifecycle (request -> response), modifying request/response as
    /// necessary, across all requests managed by the *Server*.
    ///
    /// Use middleware when you need to read or modify *every* request or
    /// response in some way.
    pub fn middleware<U>(self, mw: U) -> MqttServer<St, AppSt, Err, E, P, C, Stack<M, U>> {
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
    pub fn replace_middlewares<U>(self, mw: U) -> MqttServer<St, AppSt, Err, E, P, C, U> {
        MqttServer {
            middleware: mw,
            handshake: self.handshake,
            protocol: self.protocol,
            control: self.control,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Service to handle protocol control messages.
    ///
    /// All control messages are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn protocol<F, Srv>(self, service: F) -> MqttServer<St, AppSt, Err, E, Srv, C, M>
    where
        F: IntoServiceFactory<Srv, Session<AppSt>, ProtocolMessage, Connection<St>>,
        Srv: ServiceFactory<
                Session<AppSt>,
                ProtocolMessage,
                Connection<St>,
                Res = ProtocolMessageAck,
            > + 'static,
        Srv::InitError: Error + 'static,
        E: From<Srv::Error>,
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
    pub fn control<Srv>(
        self,
        f: impl IntoServiceFactory<Srv, Session<AppSt>, Control<E>, Connection<St>>,
    ) -> MqttServer<St, AppSt, Err, E, P, ControlFactory<St, AppSt, Srv, E>, M>
    where
        Err: From<Srv::Error>,
        Srv: ServiceFactory<Session<AppSt>, Control<E>, Connection<St>, Res = Option<mqtt::Encoded>>
            + 'static,
        Srv::InitError: Error + 'static,
    {
        MqttServer {
            handshake: self.handshake,
            protocol: self.protocol,
            middleware: self.middleware,
            control: ControlFactory::new(f.into_factory()),
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<Srv>(
        self,
        publish: impl IntoServiceFactory<Srv, Session<AppSt>, Publish, Connection<St>>,
    ) -> service::MqttServer<
        St,
        AppSt,
        Rc<MqttShared>,
        MqttSink,
        Err,
        E,
        impl ServiceFactory<
            Session<AppSt>,
            mqtt::Decoded,
            Connection<St>,
            Res = Option<mqtt::Encoded>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        >,
        M,
        C,
    >
    where
        E: From<P::Error> + From<Srv::Error> + 'static,
        Srv: ServiceFactory<Session<AppSt>, Publish, Connection<St>, Res = ()> + 'static,
        Srv::InitError: Into<Box<dyn Error>> + 'static,
    {
        service::MqttServer::new(
            self.handshake,
            factory(publish.into_factory(), self.protocol),
            self.middleware,
            self.control,
        )
    }
}

struct HandshakeService<AppSt, H> {
    svc: H,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<AppSt>,
}

impl<St, AppSt, H> Service<St, IoBoxed> for HandshakeService<AppSt, H>
where
    St: Clone,
    H: Service<St, Handshake, Res = HandshakeAck<AppSt>> + 'static,
{
    type Res = (
        IoBoxed,
        Rc<MqttShared>,
        Connection<St>,
        Session<AppSt>,
        Seconds,
    );
    type Error = MqttError<H::Error>;

    ntex_service::forward_ready!(St, svc, MqttError::Service);
    ntex_service::forward_shutdown!(St, svc);

    async fn call(&self, io: IoBoxed, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
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
                    .call(&self.svc, Handshake::new(connect, size, io, shared))
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

                    let con = Connection::new(
                        ctx.st().clone(),
                        MqttSink::new(ack.shared.clone()),
                        ack.io.shared(),
                    );

                    Ok((
                        ack.io,
                        ack.shared.clone(),
                        con,
                        Session::new(session, MqttSink::new(ack.shared)),
                        ack.keepalive,
                    ))
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
        let server = MqttServer::new(fn_service(async |h: Handshake| {
            Ok::<HandshakeAck<()>, ()>(h.ack((), false))
        }));
        assert!(format!("{server:?}").contains("v3::MqttServer"));

        let _ = server.publish(async |_| Ok(()));
    }
}
