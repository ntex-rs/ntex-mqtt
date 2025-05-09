use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_io::{DispatchItem, DispatcherConfig, IoBoxed};
use ntex_service::{Identity, IntoServiceFactory, Service, ServiceCtx, ServiceFactory, Stack};
use ntex_util::time::{timeout_checked, Millis, Seconds};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::{service, types::QoS, InFlightService};

use super::control::{Control, ControlAck};
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
pub struct MqttServer<St, H, C, P, M = Identity> {
    handshake: H,
    control: C,
    publish: P,
    middleware: M,
    max_qos: QoS,
    max_size: u32,
    max_send: u16,
    max_send_size: (u32, u32),
    min_chunk_size: u32,
    handle_qos_after_disconnect: Option<QoS>,
    connect_timeout: Seconds,
    config: DispatcherConfig,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, H>
    MqttServer<
        St,
        H,
        DefaultControlService<St, H::Error>,
        DefaultPublishService<St, H::Error>,
        InFlightService,
    >
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
            middleware: InFlightService::new(16, 65535),
            max_qos: QoS::AtLeastOnce,
            max_size: 0,
            max_send: 16,
            max_send_size: (65535, 512),
            min_chunk_size: 32 * 1024,
            handle_qos_after_disconnect: None,
            connect_timeout: Seconds::ZERO,
            pool: Default::default(),
            _t: PhantomData,
        }
    }
}

impl<St, H, C, P> MqttServer<St, H, C, P, InFlightService> {
    /// Number of inbound in-flight concurrent messages.
    ///
    /// By default inbound is set to 16 messages
    pub fn max_receive(mut self, val: u16) -> Self {
        self.middleware = self.middleware.max_receive(val);
        self
    }

    /// Total size of inbound in-flight messages.
    ///
    /// By default total inbound in-flight size is set to 64Kb
    pub fn max_receive_size(mut self, val: usize) -> Self {
        self.middleware = self.middleware.max_receive_size(val);
        self
    }
}

impl<St, H, C, P, M> MqttServer<St, H, C, P, M>
where
    St: 'static,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C: ServiceFactory<Control<H::Error>, Session<St>, Response = ControlAck> + 'static,
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
    /// Set read timeout, max timeout and rate for reading payload. If the client
    /// sends `rate` amount of data within `timeout` period of time, extend timeout by `timeout` seconds.
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

    /// Number of outgoing concurrent messages.
    ///
    /// By default outgoing is set to 16 messages
    pub fn max_send(mut self, val: u16) -> Self {
        self.max_send = val;
        self
    }

    /// Total size of outgoing messages.
    ///
    /// By default total outgoing size is set to 64Kb
    pub fn max_send_size(mut self, val: u32) -> Self {
        self.max_send_size = (val, val / 10);
        self
    }

    /// Set min payload chunk size.
    ///
    /// If the minimum size is set to `0`, incoming payload chunks
    /// will be processed immediately. Otherwise, the codec will
    /// accumulate chunks until the total size reaches the specified minimum.
    /// By default min size is set to `0`
    pub fn min_chunk_size(mut self, size: u32) -> Self {
        self.min_chunk_size = size;
        self
    }

    /// Handle max received QoS messages after client disconnect.
    ///
    /// By default, messages received before dispatched to the publish service will be dropped if
    /// the client disconnect is detected on the server.
    ///
    /// If this option is set to `Some(QoS::AtMostOnce)`, only the received QoS 0 messages will
    /// always be handled by the server's publish service no matter if the client is disconnected
    /// or not.
    ///
    /// If this option is set to `Some(QoS::AtLeastOnce)`, the received QoS 0 and QoS 1 messages
    /// will always be handled by the server's publish service no matter if the client
    /// is disconnected or not. The QoS 2 messages will be dropped if the client disconnecting is
    /// detected before the server dispatches them to the publish service.
    ///
    /// If this option is set to `Some(QoS::ExactlyOnce)`, all the messages received will always
    /// be handled by the server's publish service no matter if the client is disconnected or not.
    ///
    /// The received messages which QoS larger than the `max_handle_qos` will not be guaranteed to
    /// be handled or not after the client disconnect. It depends on the network condition.
    ///
    /// By default handle-qos-after-disconnect is set to `None`
    pub fn handle_qos_after_disconnect(mut self, max_handle_qos: Option<QoS>) -> Self {
        self.handle_qos_after_disconnect = max_handle_qos;
        self
    }

    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<St, H, Srv, P, M>
    where
        F: IntoServiceFactory<Srv, Control<H::Error>, Session<St>>,
        Srv: ServiceFactory<Control<H::Error>, Session<St>, Response = ControlAck> + 'static,
        H::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            handshake: self.handshake,
            publish: self.publish,
            control: service.into_factory(),
            config: self.config,
            middleware: self.middleware,
            max_qos: self.max_qos,
            max_size: self.max_size,
            max_send: self.max_send,
            max_send_size: self.max_send_size,
            min_chunk_size: self.min_chunk_size,
            handle_qos_after_disconnect: self.handle_qos_after_disconnect,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<F, Srv>(self, publish: F) -> MqttServer<St, H, C, Srv, M>
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
            middleware: self.middleware,
            max_qos: self.max_qos,
            max_size: self.max_size,
            max_send: self.max_send,
            max_send_size: self.max_send_size,
            min_chunk_size: self.min_chunk_size,
            handle_qos_after_disconnect: self.handle_qos_after_disconnect,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Remove all middlewares
    pub fn reset_middlewares(self) -> MqttServer<St, H, C, P, Identity> {
        MqttServer {
            middleware: Identity,
            handshake: self.handshake,
            publish: self.publish,
            control: self.control,
            config: self.config,
            max_qos: self.max_qos,
            max_size: self.max_size,
            max_send: self.max_send,
            max_send_size: self.max_send_size,
            min_chunk_size: self.min_chunk_size,
            handle_qos_after_disconnect: self.handle_qos_after_disconnect,
            connect_timeout: self.connect_timeout,
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
    pub fn middleware<U>(self, mw: U) -> MqttServer<St, H, C, P, Stack<M, U>> {
        MqttServer {
            middleware: Stack::new(self.middleware, mw),
            handshake: self.handshake,
            publish: self.publish,
            control: self.control,
            config: self.config,
            max_qos: self.max_qos,
            max_size: self.max_size,
            max_send: self.max_send,
            max_send_size: self.max_send_size,
            min_chunk_size: self.min_chunk_size,
            handle_qos_after_disconnect: self.handle_qos_after_disconnect,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }
}

impl<St, H, C, P, M> MqttServer<St, H, C, P, M>
where
    St: 'static,
    H: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C: ServiceFactory<Control<H::Error>, Session<St>, Response = ControlAck> + 'static,
    P: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
    H::Error:
        From<C::Error> + From<C::InitError> + From<P::Error> + From<P::InitError> + fmt::Debug,
{
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
            Response = Option<mqtt::Encoded>,
            Error = MqttError<H::Error>,
            InitError = MqttError<H::Error>,
        >,
        M,
        Rc<MqttShared>,
    > {
        service::MqttServer::new(
            HandshakeFactory {
                factory: self.handshake,
                max_size: self.max_size,
                max_send: self.max_send,
                max_send_size: self.max_send_size,
                min_chunk_size: self.min_chunk_size,
                connect_timeout: self.connect_timeout,
                pool: self.pool.clone(),
                _t: PhantomData,
            },
            factory(self.publish, self.control, self.max_qos, self.handle_qos_after_disconnect),
            self.middleware,
            self.config,
        )
    }
}

struct HandshakeFactory<St, H> {
    factory: H,
    max_size: u32,
    max_send: u16,
    max_send_size: (u32, u32),
    min_chunk_size: u32,
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

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        Ok(HandshakeService {
            max_size: self.max_size,
            max_send: self.max_send,
            max_send_size: self.max_send_size,
            min_chunk_size: self.min_chunk_size,
            pool: self.pool.clone(),
            service: self.factory.create(()).await?,
            connect_timeout: self.connect_timeout.into(),
            _t: PhantomData,
        })
    }
}

struct HandshakeService<St, H> {
    service: H,
    max_size: u32,
    max_send: u16,
    max_send_size: (u32, u32),
    min_chunk_size: u32,
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

    ntex_service::forward_ready!(service, MqttError::Service);
    ntex_service::forward_shutdown!(service);

    async fn call(
        &self,
        io: IoBoxed,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Starting mqtt v3 handshake");

        let (h, l) = self.max_send_size;
        io.memory_pool().set_write_params(h, l);

        let codec = mqtt::Codec::default();
        codec.set_max_size(self.max_size);
        codec.set_min_chunk_size(self.min_chunk_size);
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, self.pool.clone()));

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

                        ack.shared.set_cap(ack.max_send.unwrap_or(self.max_send) as usize);
                        ack.io.encode(mqtt::Encoded::Packet(pkt.into()), &ack.shared.codec)?;
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
                        ack.io.encode(mqtt::Encoded::Packet(pkt.into()), &ack.shared.codec)?;
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
