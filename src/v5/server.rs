use std::num::NonZeroU16;
use std::{convert::TryFrom, fmt, marker::PhantomData, rc::Rc, time::Duration};

use futures::{future::TryFutureExt, SinkExt, StreamExt};
use ntex::channel::mpsc;
use ntex::rt::time::Delay;
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::util::timeout::{Timeout, TimeoutError};
use ntex_codec::{AsyncRead, AsyncWrite, Framed};

use crate::error::{MqttError, ProtocolError};
use crate::handshake::{Handshake, HandshakeResult};
use crate::service::{FactoryBuilder, FactoryBuilder2};

use super::codec as mqtt;
use super::connect::{Connect, ConnectAck};
use super::control::{ControlPacket, ControlResult};
use super::default::{DefaultControlService, DefaultPublishService};
use super::dispatcher::factory;
use super::publish::{Publish, PublishAck};
use super::sink::MqttSink;
use super::Session;

/// Mqtt Server
pub struct MqttServer<Io, St, C: ServiceFactory, Cn: ServiceFactory, P: ServiceFactory> {
    connect: C,
    srv_control: Cn,
    srv_publish: P,
    max_size: u32,
    max_receive: u16,
    handshake_timeout: usize,
    disconnect_timeout: usize,
    max_topic_alias: u16,
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
            handshake_timeout: 0,
            disconnect_timeout: 3000,
            max_topic_alias: 32,
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
            Request = ControlPacket<C::Error>,
            Response = ControlResult,
        > + 'static,
    P: ServiceFactory<Config = Session<St>, Request = Publish, Response = PublishAck> + 'static,
{
    /// Set handshake timeout in millis.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: usize) -> Self {
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
    pub fn disconnect_timeout(mut self, val: usize) -> Self {
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

    /// Service to handle control messages
    pub fn control<F, Srv>(self, service: F) -> MqttServer<Io, St, C, Srv, P>
    where
        F: IntoServiceFactory<Srv>,
        Srv: ServiceFactory<
                Config = Session<St>,
                Request = ControlPacket<C::Error>,
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
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
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
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
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
            Request = ControlPacket<C::Error>,
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
                self.handshake_timeout,
            ))
            .disconnect_timeout(self.disconnect_timeout)
            .build(factory(
                publish,
                control,
                self.max_receive,
                self.max_topic_alias,
            )),
        )
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn inner_finish(
        self,
    ) -> impl ServiceFactory<
        Config = (),
        Request = (Framed<Io, mqtt::Codec>, Option<Delay>),
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
                self.handshake_timeout,
            ))
            .disconnect_timeout(self.disconnect_timeout)
            .build(factory(
                publish,
                control,
                self.max_receive,
                self.max_topic_alias,
            )),
        )
    }
}

fn handshake_service_factory<Io, St, C>(
    factory: C,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    handshake_timeout: usize,
) -> impl ServiceFactory<
    Config = (),
    Request = Handshake<Io, mqtt::Codec>,
    Response = HandshakeResult<Io, Session<St>, mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
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
            factory.new_service(()).map_ok(move |service| {
                let service = Rc::new(service.map_err(MqttError::Service));
                ntex::apply_fn(service, move |conn: Handshake<Io, mqtt::Codec>, service| {
                    handshake(
                        conn.codec(mqtt::Codec::new()),
                        service.clone(),
                        max_size,
                        max_receive,
                        max_topic_alias,
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
    handshake_timeout: usize,
) -> impl ServiceFactory<
    Config = (),
    Request = HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
    Response = HandshakeResult<Io, Session<St>, mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
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
            factory.new_service(()).map_ok(move |service| {
                let service = Rc::new(service.map_err(MqttError::Service));
                ntex::apply_fn(service, move |conn, service| {
                    handshake(conn, service.clone(), max_size, max_receive, max_topic_alias)
                })
            })
        }),
    )
    .map_err(|e| match e {
        TimeoutError::Service(e) => e,
        TimeoutError::Timeout => MqttError::HandshakeTimeout,
    })
}

async fn handshake<Io, S, St, E>(
    mut framed: HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
    service: S,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
) -> Result<HandshakeResult<Io, Session<St>, mqtt::Codec, mpsc::Receiver<mqtt::Packet>>, S::Error>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    S: Service<Request = Connect<Io>, Response = ConnectAck<Io, St>, Error = MqttError<E>>,
{
    log::trace!("Starting mqtt handshake");

    framed.get_codec_mut().set_max_size(max_size);

    // read first packet
    let packet = framed
        .next()
        .await
        .ok_or_else(|| {
            log::trace!("Server mqtt is disconnected during handshake");
            MqttError::Disconnected
        })
        .and_then(|res| {
            res.map_err(|e| {
                log::trace!("Error is received during mqtt handshake: {:?}", e);
                MqttError::from(e)
            })
        })?;

    match packet {
        mqtt::Packet::Connect(connect) => {
            let (tx, rx) = mpsc::channel();
            let sink =
                MqttSink::new(tx, connect.receive_max.map(|v| v.get()).unwrap_or(16) as usize);

            // authenticate mqtt connection
            let mut ack = service.call(Connect::new(connect, framed, sink)).await?;

            match ack.session {
                Some(session) => {
                    log::trace!("Sending: {:#?}", ack.packet);
                    let sink = ack.sink;

                    if max_receive != 0 {
                        ack.packet.receive_max = Some(NonZeroU16::new(max_receive).unwrap());
                    }
                    ack.packet.max_packet_size = Some(max_size);
                    ack.packet.topic_alias_max = max_topic_alias;
                    ack.packet.max_qos = Some(mqtt::QoS::AtLeastOnce);
                    if ack.packet.server_keepalive_sec.is_none() {
                        ack.packet.server_keepalive_sec = Some(ack.io.keepalive as u16);
                    }
                    ack.io.send(mqtt::Packet::ConnectAck(ack.packet)).await?;

                    Ok(ack.io.out(rx).state(Session::new(session, sink)))
                }
                None => {
                    log::trace!("Failed to complete handshake: {:#?}", ack.packet);

                    ack.io.send(mqtt::Packet::ConnectAck(ack.packet)).await?;
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
