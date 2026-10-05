#![allow(clippy::type_complexity)]
use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_error::{Failure, IntoFailure};
use ntex_io::IoBoxed;
use ntex_service::cfg::Configuration;
use ntex_service::pipeline::PipelineFactory;
use ntex_service::{
    Ctx, Identity, IntoService, IntoServiceFactory, Service, ServiceFactory, Stack,
};
use ntex_util::{future::Either, time::Seconds, time::timeout_checked};

use crate::error::{DecodeError, DispatcherError, MqttConnectError, MqttError, MqttProtocolError};
use crate::{ConnectPipeline, MqttServiceConfig, control, control::Control, service};

use super::connect::{Connect, ConnectAck};
use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::default::{ControlFactory, DefaultProtoSrv, InFlightService};
use super::shared::{MqttShared, MqttSinkPool};
use super::{MqttSink, Publish, Session, codec as mqtt, dispatcher::factory};

type ControlPipeline<AppSt, E, Err> =
    PipelineFactory<Session<AppSt>, Control<E>, Option<mqtt::Encoded>, MqttError<Err>, Failure>;

/// Mqtt v3.1.1 server
///
/// * `St` - connection state, available before the handshake
/// * `AppSt` - session state, returned by the connect (handshake) service
/// * `Err` - application error type
/// * `Pub` - service for handling mqtt publish messages
/// * `P` - service for handling protocol messages
/// * `M` - middleware applied to the publish service
///
/// Every mqtt connection is handled in several steps. First step is connect. Server calls
/// connect service with `Connect` message, during this step service can authenticate connect
/// packet, it must return instance of connection state `AppSt`.
///
/// Connect service could be expressed as simple function:
///
/// ```rust,ignore
/// use ntex_mqtt::v3::{Connect, ConnectAck};
///
/// async fn connect(hnd: Connect) -> Result<ConnectAck<MyState>, MyError> {
///     Ok(hnd.ack(MyState::new(), false))
/// }
/// ```
///
/// During next stage, protocol, control and publish services get constructed,
/// factories receive `Session<AppSt>` state object as an argument. Publish service
/// handles `Publish` packet. On success, server sends `PublishAck` packet for `QoS 1`
/// or `PublishReceived` packet for `QoS 2` to the client, nothing is sent for `QoS 0`.
/// In case of error connection get closed. Protocol service receives all
/// other packets, like `Subscribe`, `Unsubscribe` etc. Control service receives
/// errors from publish service and connection disconnect.
pub struct MqttServer<St, AppSt, Err, Pub, P, M = Identity>
where
    Pub: ServiceFactory<Session<AppSt>, Publish>,
{
    publish: Pub,
    protocol: P,
    middleware: M,
    control: ControlPipeline<AppSt, Pub::Error, Err>,
    pub(super) pool: Rc<MqttSinkPool>,
    st: PhantomData<St>,
}

impl<St, AppSt, Err, Pub, P, M> fmt::Debug for MqttServer<St, AppSt, Err, Pub, P, M>
where
    Pub: ServiceFactory<Session<AppSt>, Publish>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::MqttServer").finish()
    }
}

impl<AppSt, Err, Pub> MqttServer<(), AppSt, Err, Pub, DefaultProtoSrv<Pub::Error>, InFlightService>
where
    AppSt: 'static,
    Err: 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Res = ()> + 'static,
    Pub::InitError: IntoFailure,
{
    /// Create server builder and provide publish service
    pub fn new<I>(publish: I) -> Self
    where
        I: IntoServiceFactory<Pub, Session<AppSt>, Publish>,
    {
        Self::with(publish)
    }
}

impl<St, AppSt, Err, Pub>
    MqttServer<St, AppSt, Err, Pub, DefaultProtoSrv<Pub::Error>, InFlightService>
where
    St: 'static,
    AppSt: 'static,
    Err: 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Res = ()> + 'static,
    Pub::InitError: IntoFailure,
{
    /// Create server builder with state
    pub fn with<I>(publish: I) -> Self
    where
        I: IntoServiceFactory<Pub, Session<AppSt>, Publish>,
    {
        MqttServer::<St, AppSt, Err, Pub, DefaultProtoSrv<Pub::Error>, InFlightService> {
            publish: publish.into_factory(),
            protocol: DefaultProtoSrv::default(),
            middleware: InFlightService,
            control: ControlPipeline::new(
                ControlFactory::new(control::DefaultControlService::<Err, _>::default())
                    .map_err(MqttError::Service),
            ),
            pool: Rc::new(MqttSinkPool::default()),
            st: PhantomData,
        }
    }
}

impl<St, AppSt, Err, Pub, P, M> MqttServer<St, AppSt, Err, Pub, P, M>
where
    St: 'static,
    AppSt: 'static,
    Err: 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Res = ()> + 'static,
    Pub::InitError: IntoFailure,
    P: ServiceFactory<Session<AppSt>, ProtocolMessage, Res = ProtocolMessageAck> + 'static,
    P::Error: Into<Pub::Error>,
    P::InitError: IntoFailure,
{
    #[must_use]
    /// Registers middleware, in the form of a middleware component (type),
    /// that runs during inbound and/or outbound processing in the request
    /// lifecycle (request -> response), modifying request/response as
    /// necessary, across all requests managed by the *Server*.
    ///
    /// Use middleware when you need to read or modify *every* request or
    /// response in some way.
    pub fn middleware<U>(self, mw: U) -> MqttServer<St, AppSt, Err, Pub, P, Stack<M, U>> {
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
    pub fn replace_middlewares<U>(self, mw: U) -> MqttServer<St, AppSt, Err, Pub, P, U> {
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
    /// Service to handle protocol control messages.
    ///
    /// All control messages are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn protocol<F, Srv>(self, service: F) -> MqttServer<St, AppSt, Err, Pub, Srv, M>
    where
        F: IntoServiceFactory<Srv, Session<AppSt>, ProtocolMessage>,
        Srv: ServiceFactory<Session<AppSt>, ProtocolMessage, Res = ProtocolMessageAck> + 'static,
        Srv::Error: Into<Pub::Error>,
        Srv::InitError: IntoFailure,
    {
        MqttServer {
            publish: self.publish,
            protocol: service.into_factory(),
            control: self.control,
            middleware: self.middleware,
            pool: self.pool,
            st: self.st,
        }
    }

    #[must_use]
    /// Service to handle connection control messages
    pub fn control<Srv>(
        self,
        f: impl IntoServiceFactory<Srv, Session<AppSt>, Control<Pub::Error>>,
    ) -> MqttServer<St, AppSt, Err, Pub, P, M>
    where
        Srv: ServiceFactory<Session<AppSt>, Control<Pub::Error>, Res = Option<mqtt::Encoded>>
            + 'static,
        Srv::Error: Into<Err>,
        Srv::InitError: IntoFailure,
    {
        MqttServer {
            publish: self.publish,
            protocol: self.protocol,
            middleware: self.middleware,
            control: ControlPipeline::new(ControlFactory::new(
                f.into_factory()
                    .map_err(|e| MqttError::Service(e.into()))
                    .map_init_err(IntoFailure::fail),
            )),
            pool: self.pool,
            st: self.st,
        }
    }

    /// Set service to handle connect and create mqtt server
    pub fn build<H, Hst>(
        self,
        connect: impl IntoService<H, Hst, Connect<St>>,
    ) -> service::MqttServer<
        Hst,
        St,
        AppSt,
        Rc<MqttShared>,
        MqttSink,
        Err,
        Pub::Error,
        impl ServiceFactory<
            Session<AppSt>,
            mqtt::Decoded,
            Res = Option<mqtt::Packet>,
            Error = DispatcherError<Pub::Error>,
            InitError = Failure,
        >,
        M,
    >
    where
        H: Service<Hst, Connect<St>, Res = ConnectAck<AppSt>, Error = Err> + 'static,
        Hst: 'static,
    {
        let connect = ConnectPipeline::new(ConnectService {
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

struct ConnectService<St, AppSt, S> {
    svc: S,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<(St, AppSt)>,
}

impl<Hst, St, AppSt, S> Service<Hst, (IoBoxed, St)> for ConnectService<St, AppSt, S>
where
    S: Service<Hst, Connect<St>, Res = ConnectAck<AppSt>> + 'static,
{
    type Res = (IoBoxed, Rc<MqttShared>, Session<AppSt>, Seconds);
    type Error = MqttError<S::Error>;

    ntex_service::forward_ready!(Hst, svc, MqttError::Service);
    ntex_service::forward_shutdown!(Hst, svc);

    async fn call(
        &self,
        (io, st): (IoBoxed, St),
        ctx: Ctx<'_, Self, Hst>,
    ) -> Result<Self::Res, Self::Error> {
        log::trace!("Starting mqtt v3 connect handshake");

        let cfg = io.cfg().ctx().get::<MqttServiceConfig>();

        let codec = mqtt::Codec::from_config(&cfg);
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec,
            false,
            self.pool.clone(),
        ));

        // read first packet
        let packet = match timeout_checked(cfg.connect_timeout, io.recv(&shared.codec))
            .await
            .map_err(|()| MqttError::Connect(MqttConnectError::Timeout))?
        {
            Ok(Some(packet)) => packet,
            Ok(None) => {
                log::trace!("Server mqtt is disconnected during handshake");
                return Err(MqttError::Connect(MqttConnectError::Disconnected(None)));
            }
            Err(err) => {
                log::trace!("Error is received during mqtt connect handshake: {err:?}");
                if let Either::Left(ref err) = err {
                    reject_connect(&io, &shared.codec, err).await;
                }
                return Err(MqttError::Connect(MqttConnectError::from(err)));
            }
        };

        match packet {
            mqtt::Decoded::Packet(mqtt::Packet::Connect(connect), size) => {
                // the Server must close the connection without CONNACK,
                // [MQTT-3.1.4-1] (MQTT 3.1.1, 3.1.4)
                if let Some(will) = &connect.last_will
                    && let Err(err) = crate::topic::check_will_topic(&will.topic)
                {
                    log::info!("{}: {err}", io.tag());
                    let _ = io.shutdown().await;
                    return Err(MqttError::Connect(MqttConnectError::Protocol(
                        MqttProtocolError::spec(err),
                    )));
                }
                // authenticate mqtt connection
                let ack = ctx
                    .call(&self.svc, Connect::new(connect, size, io, st, shared))
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

                    Ok((ack.io, ack.shared.clone(), session, ack.keepalive))
                } else {
                    let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
                        session_present: false,
                        return_code: ack.return_code,
                    });

                    log::trace!("Sending failed handshake ack: {pkt:#?}");
                    ack.io
                        .encode(mqtt::Encoded::Packet(pkt), &ack.shared.codec)?;
                    let _ = ack.io.shutdown().await;

                    Err(MqttError::Connect(MqttConnectError::Disconnected(None)))
                }
            }
            mqtt::Decoded::Packet(packet, _) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {packet:?}");
                Err(MqttError::Connect(MqttConnectError::Protocol(
                    MqttProtocolError::unexpected_packet(
                        packet.packet_type(),
                        "MQTT-3.1.0-1: Expected CONNECT packet",
                    ),
                )))
            }
            mqtt::Decoded::Publish(..) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received PUBLISH");
                Err(MqttError::Connect(MqttConnectError::Protocol(
                    MqttProtocolError::unexpected_packet(
                        crate::types::packet_type::PUBLISH_START,
                        "Expected CONNECT packet [MQTT-3.1.0-1]",
                    ),
                )))
            }
            mqtt::Decoded::PayloadChunk(..) => unreachable!(),
        }
    }
}

/// Send CONNACK for a CONNECT packet rejected by the decoder, and close the connection
pub(crate) async fn reject_connect(io: &IoBoxed, codec: &mqtt::Codec, err: &DecodeError) {
    let return_code = match err {
        // [MQTT-3.1.2-2] (MQTT 3.1.1, 3.1.2.2)
        DecodeError::UnsupportedProtocolLevel => {
            mqtt::ConnectAckReason::UnacceptableProtocolVersion
        }
        // [MQTT-3.1.3-8] (MQTT 3.1.1, 3.1.3.1)
        DecodeError::InvalidClientId => mqtt::ConnectAckReason::IdentifierRejected,
        _ => return,
    };
    let pkt = mqtt::Packet::ConnectAck(mqtt::ConnectAck {
        session_present: false,
        return_code,
    });
    log::trace!("Sending failed handshake ack: {pkt:#?}");
    if io.encode(mqtt::Encoded::Packet(pkt), codec).is_ok() {
        let _ = io.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug() {
        let server = MqttServer::<(), (), (), _, _, _>::new(ntex_service::fn_service(async |_| {
            Ok::<_, ()>(())
        }));
        assert!(format!("{server:?}").contains("v3::MqttServer"));
    }
}
