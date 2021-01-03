use std::{convert::TryFrom, fmt, marker::PhantomData, rc::Rc, time::Duration};

use futures::future::TryFutureExt;
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::rt::time::Delay;
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::util::timeout::{Timeout, TimeoutError};

use crate::error::{MqttError, ProtocolError};
use crate::service::{FactoryBuilder, FactoryBuilder2};
use crate::{io::IoState, types::QoS};

use super::codec as mqtt;
use super::connect::{Connect, ConnectAck};
use super::control::{ControlMessage, ControlResult};
use super::default::{DefaultControlService, DefaultPublishService};
use super::dispatcher::factory;
use super::publish::{Publish, PublishAck};
use super::sink::{MqttSink, MqttSinkPool};
use super::Session;

/// Mqtt Server
pub struct MqttServer<Io, St, C: ServiceFactory, Cn: ServiceFactory, P: ServiceFactory> {
    connect: C,
    srv_control: Cn,
    srv_publish: P,
    max_size: u32,
    max_receive: u16,
    max_qos: Option<QoS>,
    handshake_timeout: u16,
    disconnect_timeout: u16,
    max_topic_alias: u16,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St, C>
    MqttServer<
        Io,
        St,
        C,
        DefaultControlService<St, C::Error>,
        DefaultPublishService<St, C::Error>,
    >
where
    St: 'static,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>
        + 'static,
    C::Error: fmt::Debug,
{
    /// Create server factory and provide connect service
    pub fn new<F>(connect: F) -> Self
    where
        F: IntoServiceFactory<C>,
    {
        MqttServer {
            connect: connect.into_factory(),
            srv_control: DefaultControlService::default(),
            srv_publish: DefaultPublishService::default(),
            max_size: 0,
            max_receive: 15,
            max_qos: None,
            handshake_timeout: 0,
            disconnect_timeout: 3000,
            max_topic_alias: 32,
            pool: Rc::new(MqttSinkPool::default()),
            _t: PhantomData,
        }
    }
}

impl<Io, St, C, Cn, P> MqttServer<Io, St, C, Cn, P>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    St: 'static,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>
        + 'static,
    C::Error: fmt::Debug,
    Cn: ServiceFactory<
            Config = Session<St>,
            Request = ControlMessage<C::Error>,
            Response = ControlResult,
        > + 'static,
    P: ServiceFactory<Config = Session<St>, Request = Publish, Response = PublishAck> + 'static,
{
    /// Set handshake timeout in millis.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: u16) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set server connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(mut self, val: u16) -> Self {
        self.disconnect_timeout = val;
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
    /// By default max qos is not set`
    pub fn max_qos(mut self, qos: QoS) -> Self {
        self.max_qos = Some(qos);
        self
    }

    /// Service to handle control messages
    pub fn control<F, Srv>(self, service: F) -> MqttServer<Io, St, C, Srv, P>
    where
        F: IntoServiceFactory<Srv>,
        Srv: ServiceFactory<
                Config = Session<St>,
                Request = ControlMessage<C::Error>,
                Response = ControlResult,
            > + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            connect: self.connect,
            srv_publish: self.srv_publish,
            srv_control: service.into_factory(),
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<F, Srv>(self, publish: F) -> MqttServer<Io, St, C, Cn, Srv>
    where
        F: IntoServiceFactory<Srv> + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
        Srv: ServiceFactory<Config = Session<St>, Request = Publish, Response = PublishAck>
            + 'static,
        Srv::Error: fmt::Debug,
        PublishAck: TryFrom<Srv::Error, Error = C::Error>,
    {
        MqttServer {
            connect: self.connect,
            srv_publish: publish.into_factory(),
            srv_control: self.srv_control,
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }
}

impl<Io, St, C, Cn, P> MqttServer<Io, St, C, Cn, P>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    St: 'static,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>
        + 'static,
    C::Error: From<Cn::Error>
        + From<Cn::InitError>
        + From<P::Error>
        + From<P::InitError>
        + fmt::Debug,
    Cn: ServiceFactory<
            Config = Session<St>,
            Request = ControlMessage<C::Error>,
            Response = ControlResult,
        > + 'static,
    P: ServiceFactory<Config = Session<St>, Request = Publish, Response = PublishAck> + 'static,
    P::Error: fmt::Debug,
    PublishAck: TryFrom<P::Error, Error = C::Error>,
{
    /// Set service to handle publish packets and create mqtt server factory
    pub fn finish(
        self,
    ) -> impl ServiceFactory<Config = (), Request = Io, Response = (), Error = MqttError<C::Error>>
    {
        let connect = self.connect;
        let publish = self.srv_publish.map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .srv_control
            .map_err(<C::Error>::from)
            .map_init_err(|e| MqttError::Service(e.into()));

        ntex::unit_config(
            FactoryBuilder::new(handshake_service_factory(
                connect,
                self.max_size,
                self.max_receive,
                self.max_topic_alias,
                self.max_qos,
                self.handshake_timeout,
                self.pool,
            ))
            .disconnect_timeout(self.disconnect_timeout)
            .build(factory(publish, control)),
        )
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn inner_finish(
        self,
    ) -> impl ServiceFactory<
        Config = (),
        Request = (Io, IoState<mqtt::Codec>, Option<Delay>),
        Response = (),
        Error = MqttError<C::Error>,
        InitError = C::InitError,
    > {
        let connect = self.connect;
        let publish = self.srv_publish.map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .srv_control
            .map_err(<C::Error>::from)
            .map_init_err(|e| MqttError::Service(e.into()));

        ntex::unit_config(
            FactoryBuilder2::new(handshake_service_factory2(
                connect,
                self.max_size,
                self.max_receive,
                self.max_topic_alias,
                self.max_qos,
                self.handshake_timeout,
                self.pool,
            ))
            .disconnect_timeout(self.disconnect_timeout)
            .build(factory(publish, control)),
        )
    }
}

fn handshake_service_factory<Io, St, C>(
    factory: C,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    max_qos: Option<QoS>,
    handshake_timeout: u16,
    pool: Rc<MqttSinkPool>,
) -> impl ServiceFactory<
    Config = (),
    Request = Io,
    Response = (Io, IoState<mqtt::Codec>, Session<St>, u16),
    Error = MqttError<C::Error>,
>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>,
    C::Error: fmt::Debug,
{
    ntex::apply(
        Timeout::new(Duration::from_millis(handshake_timeout as u64)),
        ntex::fn_factory(move || {
            let pool = pool.clone();

            factory.new_service(()).map_ok(move |service| {
                let pool = pool.clone();
                let service = Rc::new(service.map_err(MqttError::Service));
                ntex::apply_fn(service, move |io: Io, service| {
                    handshake(
                        io,
                        None,
                        service.clone(),
                        max_size,
                        max_receive,
                        max_topic_alias,
                        max_qos,
                        pool.clone(),
                    )
                })
            })
        }),
    )
    .map_err(|e| match e {
        TimeoutError::Service(e) => e,
        TimeoutError::Timeout => MqttError::HandshakeTimeout,
    })
}

fn handshake_service_factory2<Io, St, C>(
    factory: C,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    max_qos: Option<QoS>,
    handshake_timeout: u16,
    pool: Rc<MqttSinkPool>,
) -> impl ServiceFactory<
    Config = (),
    Request = (Io, IoState<mqtt::Codec>),
    Response = (Io, IoState<mqtt::Codec>, Session<St>, u16),
    Error = MqttError<C::Error>,
    InitError = C::InitError,
>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>,
    C::Error: fmt::Debug,
{
    ntex::apply(
        Timeout::new(Duration::from_millis(handshake_timeout as u64)),
        ntex::fn_factory(move || {
            let pool = pool.clone();
            factory.new_service(()).map_ok(move |service| {
                let pool = pool.clone();
                let service = Rc::new(service.map_err(MqttError::Service));
                ntex::apply_fn(service, move |(io, state), service| {
                    handshake(
                        io,
                        Some(state),
                        service.clone(),
                        max_size,
                        max_receive,
                        max_topic_alias,
                        max_qos,
                        pool.clone(),
                    )
                })
            })
        }),
    )
    .map_err(|e| match e {
        TimeoutError::Service(e) => e,
        TimeoutError::Timeout => MqttError::HandshakeTimeout,
    })
}

#[allow(clippy::too_many_arguments)]
async fn handshake<Io, S, St, E>(
    mut io: Io,
    state: Option<IoState<mqtt::Codec>>,
    service: S,
    max_size: u32,
    mut max_receive: u16,
    mut max_topic_alias: u16,
    max_qos: Option<QoS>,
    pool: Rc<MqttSinkPool>,
) -> Result<(Io, IoState<mqtt::Codec>, Session<St>, u16), S::Error>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    S: Service<Request = Connect<Io>, Response = ConnectAck<Io, St>, Error = MqttError<E>>,
{
    log::trace!("Starting mqtt v5 handshake");

    // set max inbound (decoder) packet size
    let state = state.unwrap_or_else(|| IoState::new(mqtt::Codec::default()));
    state.with_codec(|codec| codec.set_max_inbound_size(max_size));

    // read first packet
    let packet = state
        .next(&mut io)
        .await
        .map_err(|err| {
            log::trace!("Error is received during mqtt handshake: {:?}", err);
            MqttError::from(err)
        })
        .and_then(|res| {
            res.ok_or_else(|| {
                log::trace!("Server mqtt is disconnected during handshake");
                MqttError::Disconnected
            })
        })?;

    match packet {
        mqtt::Packet::Connect(connect) => {
            let sink = MqttSink::new(
                state.clone(),
                connect.receive_max.map(|v| v.get()).unwrap_or(16) as usize,
                pool,
            );

            // set max outbound (encoder) packet size
            if let Some(size) = connect.max_packet_size {
                state.with_codec(|codec| codec.set_max_outbound_size(size.get()));
            }

            let keep_alive = connect.keep_alive;

            // authenticate mqtt connection
            let mut ack = service
                .call(Connect::new(
                    connect,
                    io,
                    sink,
                    state,
                    max_size,
                    max_receive,
                    max_topic_alias,
                ))
                .await?;

            match ack.session {
                Some(session) => {
                    log::trace!("Sending: {:#?}", ack.packet);
                    let sink = ack.sink;

                    max_topic_alias = ack.packet.topic_alias_max;

                    if ack.packet.max_qos.is_none() {
                        ack.packet.max_qos = max_qos;
                    }

                    if let Some(num) = ack.packet.receive_max {
                        max_receive = num.get();
                    } else {
                        max_receive = 0;
                    }
                    if let Some(size) = ack.packet.max_packet_size {
                        ack.state.with_codec(|codec| codec.set_max_inbound_size(size));
                    }
                    if ack.packet.server_keepalive_sec.is_none()
                        && (keep_alive > ack.keepalive as u16)
                    {
                        ack.packet.server_keepalive_sec = Some(ack.keepalive as u16);
                    }
                    ack.state.send(&mut ack.io, mqtt::Packet::ConnectAck(ack.packet)).await?;

                    Ok((
                        ack.io,
                        ack.state,
                        Session::new_v5(session, sink, max_receive, max_topic_alias),
                        ack.keepalive,
                    ))
                }
                None => {
                    log::trace!("Failed to complete handshake: {:#?}", ack.packet);

                    ack.state.send(&mut ack.io, mqtt::Packet::ConnectAck(ack.packet)).await?;
                    Err(MqttError::Disconnected)
                }
            }
        }
        packet => {
            log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {}", 1);
            Err(MqttError::Protocol(ProtocolError::Unexpected(
                packet.packet_type(),
                "MQTT-3.1.0-1: Expected CONNECT packet",
            )))
        }
    }
}
