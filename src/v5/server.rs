use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_io::{DispatchItem, DispatcherConfig, IoBoxed};
use ntex_service::{Identity, IntoServiceFactory, Service, ServiceCtx, ServiceFactory, Stack};
use ntex_util::time::{timeout_checked, Millis, Seconds};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::{service, types::QoS};

use super::control::{Control, ControlAck};
use super::default::{DefaultControlService, DefaultPublishService};
use super::handshake::{Handshake, HandshakeAck};
use super::publish::{Publish, PublishAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, dispatcher::factory, MqttSink, Session};

/// Mqtt Server
pub struct MqttServer<St, C, Cn, P, M = Identity> {
    handshake: C,
    srv_control: Cn,
    srv_publish: P,
    middleware: M,
    max_qos: QoS,
    max_size: u32,
    max_receive: u16,
    max_receive_size: usize,
    max_topic_alias: u16,
    handle_qos_after_disconnect: Option<QoS>,
    connect_timeout: Seconds,
    config: DispatcherConfig,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, C>
    MqttServer<St, C, DefaultControlService<St, C::Error>, DefaultPublishService<St, C::Error>>
where
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>>,
    C::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<C, Handshake>,
    {
        let config = DispatcherConfig::default();
        config.set_disconnect_timeout(Seconds(3));

        MqttServer {
            config,
            handshake: handshake.into_factory(),
            srv_control: DefaultControlService::default(),
            srv_publish: DefaultPublishService::default(),
            middleware: Identity,
            max_qos: QoS::AtLeastOnce,
            max_size: 0,
            max_receive: 15,
            max_receive_size: 65535,
            max_topic_alias: 32,
            handle_qos_after_disconnect: None,
            connect_timeout: Seconds::ZERO,
            pool: Rc::new(MqttSinkPool::default()),
            _t: PhantomData,
        }
    }
}

impl<St, C, Cn, P, M> MqttServer<St, C, Cn, P, M>
where
    St: 'static,
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    Cn: ServiceFactory<Control<C::Error>, Session<St>, Response = ControlAck> + 'static,
    P: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
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

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(mut self, size: u32) -> Self {
        self.max_size = size;
        self
    }

    /// Set `receive max`
    ///
    /// Number of in-flight publish packets. By default receive max is set to 15 packets.
    /// To disable timeout set value to 0.
    pub fn max_receive(mut self, val: u16) -> Self {
        self.max_receive = val;
        self
    }

    /// Total size of received in-flight messages.
    ///
    /// By default total in-flight size is set to 64Kb
    pub fn max_receive_size(mut self, val: usize) -> Self {
        self.max_receive_size = val;
        self
    }

    #[deprecated]
    #[doc(hidden)]
    pub fn receive_max(mut self, val: u16) -> Self {
        self.max_receive = val;
        self
    }

    /// Number of topic aliases.
    ///
    /// By default value is set to 32
    pub fn max_topic_alias(mut self, val: u16) -> Self {
        self.max_topic_alias = val;
        self
    }

    /// Set server max qos setting.
    ///
    /// By default max qos is not set.
    pub fn max_qos(mut self, qos: QoS) -> Self {
        self.max_qos = qos;
        self
    }

    #[deprecated]
    #[doc(hidden)]
    pub fn max_inflight_size(mut self, val: usize) -> Self {
        self.max_receive_size = val;
        self
    }

    /// Handle max received QoS messages after client disconnect.
    ///
    /// By default, messages received before dispatched to the publish service will be dropped if
    /// the client disconnect immediately.
    ///
    /// If this option is set to `Some(QoS::AtMostOnce)`, only the QoS 0 messages received will
    /// always be handled by the server's publish service no matter if the client is disconnected
    /// or not.
    ///
    /// If this option is set to `Some(QoS::AtLeastOnce)`, only the QoS 0 and QoS 1 messages
    /// received will always be handled by the server's publish service no matter if the client
    /// is disconnected or not. The QoS 2 messages will be dropped if the client is disconnected
    /// before the server dispatches them to the publish service.
    ///
    /// If this option is set to `Some(QoS::ExactlyOnce)`, all the messages received will always
    /// be handled by the server's publish service no matter if the client is disconnected or not.
    ///
    /// By default handle-qos-after-disconnect is set to `None`
    pub fn handle_qos_after_disconnect(mut self, max_qos: Option<QoS>) -> Self {
        self.handle_qos_after_disconnect = max_qos;
        self
    }

    /// Registers middleware, in the form of a middleware component (type),
    /// that runs during inbound and/or outbound processing in the request
    /// lifecycle (request -> response), modifying request/response as
    /// necessary, across all requests managed by the *Server*.
    ///
    /// Use middleware when you need to read or modify *every* request or
    /// response in some way.
    pub fn middleware<U>(self, mw: U) -> MqttServer<St, C, Cn, P, Stack<M, U>> {
        MqttServer {
            middleware: Stack::new(self.middleware, mw),
            config: self.config,
            handshake: self.handshake,
            srv_publish: self.srv_publish,
            srv_control: self.srv_control,
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            max_receive_size: self.max_receive_size,
            handle_qos_after_disconnect: self.handle_qos_after_disconnect,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<St, C, Srv, P, M>
    where
        F: IntoServiceFactory<Srv, Control<C::Error>, Session<St>>,
        Srv: ServiceFactory<Control<C::Error>, Session<St>, Response = ControlAck> + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            config: self.config,
            handshake: self.handshake,
            srv_publish: self.srv_publish,
            srv_control: service.into_factory(),
            middleware: self.middleware,
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            max_receive_size: self.max_receive_size,
            handle_qos_after_disconnect: self.handle_qos_after_disconnect,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<F, Srv>(self, publish: F) -> MqttServer<St, C, Cn, Srv, M>
    where
        F: IntoServiceFactory<Srv, Publish, Session<St>>,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
        Srv: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
        Srv::Error: fmt::Debug,
        PublishAck: TryFrom<Srv::Error, Error = C::Error>,
    {
        MqttServer {
            config: self.config,
            handshake: self.handshake,
            srv_publish: publish.into_factory(),
            srv_control: self.srv_control,
            middleware: self.middleware,
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            max_receive_size: self.max_receive_size,
            handle_qos_after_disconnect: self.handle_qos_after_disconnect,
            connect_timeout: self.connect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }
}

impl<St, C, Cn, P, M> MqttServer<St, C, Cn, P, M>
where
    St: 'static,
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: From<Cn::Error>
        + From<Cn::InitError>
        + From<P::Error>
        + From<P::InitError>
        + fmt::Debug,
    Cn: ServiceFactory<Control<C::Error>, Session<St>, Response = ControlAck> + 'static,
    P: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
    P::Error: fmt::Debug,
    PublishAck: TryFrom<P::Error, Error = C::Error>,
{
    /// Finish server configuration and create mqtt server factory
    pub fn finish(
        self,
    ) -> service::MqttServer<
        Session<St>,
        impl ServiceFactory<
            IoBoxed,
            Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds),
            Error = MqttError<C::Error>,
            InitError = C::InitError,
        >,
        impl ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            Session<St>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        >,
        M,
        Rc<MqttShared>,
    > {
        service::MqttServer::new(
            HandshakeFactory {
                factory: self.handshake,
                max_size: self.max_size,
                max_receive: self.max_receive,
                max_topic_alias: self.max_topic_alias,
                max_qos: self.max_qos,
                connect_timeout: self.connect_timeout.into(),
                pool: self.pool,
                _t: PhantomData,
            },
            factory(self.srv_publish, self.srv_control, self.handle_qos_after_disconnect),
            self.middleware,
            self.config,
        )
    }
}

struct HandshakeFactory<St, H> {
    factory: H,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    max_qos: QoS,
    connect_timeout: Millis,
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
            service: self.factory.create(()).await?,
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            pool: self.pool.clone(),
            connect_timeout: self.connect_timeout,
            _t: PhantomData,
        })
    }
}

struct HandshakeService<St, H> {
    service: H,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    max_qos: QoS,
    connect_timeout: Millis,
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
        codec.set_max_inbound_size(self.max_size);
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, self.pool.clone()));
        shared.set_max_qos(self.max_qos);
        shared.set_receive_max(self.max_receive);
        shared.set_topic_alias_max(self.max_topic_alias);

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
                            mqtt::Packet::ConnectAck(Box::new(ack.packet)),
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
                            mqtt::Packet::ConnectAck(Box::new(ack.packet)),
                            &ack.shared.codec,
                        )?;
                        let _ = ack.io.shutdown().await;
                        Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                    }
                }
            }
            (packet, _) => {
                log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {}", 1);
                Err(MqttError::Handshake(HandshakeError::Protocol(
                    ProtocolError::unexpected_packet(
                        packet.packet_type(),
                        "Expected CONNECT packet [MQTT-3.1.0-1]",
                    ),
                )))
            }
        }
    }
}
