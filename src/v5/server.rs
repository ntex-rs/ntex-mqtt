use std::{convert::TryFrom, fmt, future::Future, marker::PhantomData, rc::Rc};

use ntex::io::{DispatchItem, IoBoxed};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::time::{timeout_checked, Millis, Seconds};
use ntex::util::{select, BoxFuture, Either};

use crate::error::{MqttError, ProtocolError};
use crate::{io::Dispatcher, service, types::QoS};

use super::control::{ControlMessage, ControlResult};
use super::default::{DefaultControlService, DefaultPublishService};
use super::handshake::{Handshake, HandshakeAck};
use super::publish::{Publish, PublishAck};
use super::selector::SelectItem;
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, dispatcher::factory, MqttSink, Session};

/// Mqtt Server
pub struct MqttServer<St, C, Cn, P> {
    handshake: C,
    srv_control: Cn,
    srv_publish: P,
    max_size: u32,
    max_receive: u16,
    max_qos: Option<QoS>,
    max_inflight_size: usize,
    handshake_timeout: Seconds,
    disconnect_timeout: Seconds,
    max_topic_alias: u16,
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
        MqttServer {
            handshake: handshake.into_factory(),
            srv_control: DefaultControlService::default(),
            srv_publish: DefaultPublishService::default(),
            max_size: 0,
            max_receive: 15,
            max_qos: None,
            max_inflight_size: 65535,
            handshake_timeout: Seconds::ZERO,
            disconnect_timeout: Seconds(3),
            max_topic_alias: 32,
            pool: Rc::new(MqttSinkPool::default()),
            _t: PhantomData,
        }
    }
}

impl<St, C, Cn, P> MqttServer<St, C, Cn, P>
where
    St: 'static,
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    Cn: ServiceFactory<ControlMessage<C::Error>, Session<St>, Response = ControlResult>
        + 'static,
    P: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
{
    /// Set handshake timeout.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout;
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
    pub fn disconnect_timeout(mut self, val: Seconds) -> Self {
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
    /// By default max qos is not set.
    pub fn max_qos(mut self, qos: QoS) -> Self {
        self.max_qos = Some(qos);
        self
    }

    /// Total size of in-flight messages.
    ///
    /// By default total in-flight size is set to 64Kb
    pub fn max_inflight_size(mut self, val: usize) -> Self {
        self.max_inflight_size = val;
        self
    }

    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<St, C, Srv, P>
    where
        F: IntoServiceFactory<Srv, ControlMessage<C::Error>, Session<St>>,
        Srv: ServiceFactory<ControlMessage<C::Error>, Session<St>, Response = ControlResult>
            + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        MqttServer {
            handshake: self.handshake,
            srv_publish: self.srv_publish,
            srv_control: service.into_factory(),
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            max_inflight_size: self.max_inflight_size,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<F, Srv>(self, publish: F) -> MqttServer<St, C, Cn, Srv>
    where
        F: IntoServiceFactory<Srv, Publish, Session<St>>,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
        Srv: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
        Srv::Error: fmt::Debug,
        PublishAck: TryFrom<Srv::Error, Error = C::Error>,
    {
        MqttServer {
            handshake: self.handshake,
            srv_publish: publish.into_factory(),
            srv_control: self.srv_control,
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            max_inflight_size: self.max_inflight_size,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }
}

impl<St, C, Cn, P> MqttServer<St, C, Cn, P>
where
    St: 'static,
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: From<Cn::Error>
        + From<Cn::InitError>
        + From<P::Error>
        + From<P::InitError>
        + fmt::Debug,
    Cn: ServiceFactory<ControlMessage<C::Error>, Session<St>, Response = ControlResult>
        + 'static,
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
        Rc<MqttShared>,
    > {
        service::MqttServer::new(
            HandshakeFactory {
                factory: self.handshake,
                max_size: self.max_size,
                max_receive: self.max_receive,
                max_topic_alias: self.max_topic_alias,
                max_qos: self.max_qos,
                handshake_timeout: self.handshake_timeout.into(),
                pool: self.pool,
                _t: PhantomData,
            },
            factory(
                self.srv_publish,
                self.srv_control,
                self.max_qos.unwrap_or(QoS::ExactlyOnce),
                self.max_inflight_size,
            ),
            self.disconnect_timeout,
        )
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn finish_selector<F, R>(
        self,
        check: F,
    ) -> impl ServiceFactory<
        SelectItem,
        Response = Either<SelectItem, ()>,
        Error = MqttError<C::Error>,
        InitError = C::InitError,
    >
    where
        F: Fn(&Handshake) -> R + 'static,
        R: Future<Output = Result<bool, C::Error>> + 'static,
    {
        ServerSelector::<St, _, _, _, _> {
            check: Rc::new(check),
            connect: self.handshake,
            handler: Rc::new(factory(
                self.srv_publish,
                self.srv_control,
                self.max_qos.unwrap_or(QoS::ExactlyOnce),
                self.max_inflight_size,
            )),
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            disconnect_timeout: self.disconnect_timeout,
            _t: PhantomData,
        }
    }
}

struct HandshakeFactory<St, H> {
    factory: H,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    max_qos: Option<QoS>,
    handshake_timeout: Millis,
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
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Self::InitError>> where Self: 'f;

    fn create(&self, _: ()) -> Self::Future<'_> {
        let fut = self.factory.create(());
        let max_size = self.max_size;
        let max_receive = self.max_receive;
        let max_topic_alias = self.max_topic_alias;
        let max_qos = self.max_qos;
        let pool = self.pool.clone();
        let handshake_timeout = self.handshake_timeout;

        Box::pin(async move {
            let service = fut.await?;
            Ok(HandshakeService {
                max_size,
                max_receive,
                max_topic_alias,
                max_qos,
                handshake_timeout,
                pool,
                service: Rc::new(service),
                _t: PhantomData,
            })
        })
    }
}

struct HandshakeService<St, H> {
    service: Rc<H>,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    max_qos: Option<QoS>,
    handshake_timeout: Millis,
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
    type Future<'f> = BoxFuture<'f, Result<Self::Response, Self::Error>> where Self: 'f;

    ntex::forward_poll_ready!(service, MqttError::Service);
    ntex::forward_poll_shutdown!(service);

    fn call(&self, io: IoBoxed) -> Self::Future<'_> {
        log::trace!("Starting mqtt v5 handshake");

        let service = self.service.clone();
        let codec = mqtt::Codec::default().max_inbound_size(self.max_size);
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, self.pool.clone()));

        let max_size = self.max_size;
        let mut max_receive = self.max_receive;
        let mut max_topic_alias = self.max_topic_alias;
        let max_qos = self.max_qos;
        let handshake_timeout = self.handshake_timeout;

        let f = async move {
            // read first packet
            let packet = io
                .recv(&shared.codec)
                .await
                .map_err(|err| {
                    log::trace!("Error is received during mqtt handshake: {:?}", err);
                    MqttError::from(err)
                })?
                .ok_or_else(|| {
                    log::trace!("Server mqtt is disconnected during handshake");
                    MqttError::Disconnected(None)
                })?;

            match packet {
                mqtt::Packet::Connect(connect) => {
                    // set max outbound (encoder) packet size
                    if let Some(size) = connect.max_packet_size {
                        shared.codec.set_max_outbound_size(size.get());
                    }
                    let keep_alive = connect.keep_alive;
                    let peer_receive_max =
                        connect.receive_max.map(|v| v.get()).unwrap_or(16) as usize;

                    // authenticate mqtt connection
                    let mut ack = service
                        .call(Handshake::new(
                            connect,
                            io,
                            shared,
                            max_size,
                            max_receive,
                            max_topic_alias,
                        ))
                        .await
                        .map_err(MqttError::Service)?;

                    match ack.session {
                        Some(session) => {
                            log::trace!("Sending: {:#?}", ack.packet);
                            let shared = ack.shared;

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
                                shared.codec.set_max_inbound_size(size);
                            }
                            if ack.packet.server_keepalive_sec.is_none()
                                && (keep_alive > ack.keepalive)
                            {
                                ack.packet.server_keepalive_sec = Some(ack.keepalive);
                            }
                            shared.set_cap(peer_receive_max);

                            ack.io
                                .send(
                                    mqtt::Packet::ConnectAck(Box::new(ack.packet)),
                                    &shared.codec,
                                )
                                .await?;

                            Ok((
                                ack.io,
                                shared.clone(),
                                Session::new_v5(
                                    session,
                                    MqttSink::new(shared),
                                    max_receive,
                                    max_topic_alias,
                                ),
                                Seconds(ack.keepalive),
                            ))
                        }
                        None => {
                            log::trace!("Failed to complete handshake: {:#?}", ack.packet);

                            ack.io
                                .send(
                                    mqtt::Packet::ConnectAck(Box::new(ack.packet)),
                                    &ack.shared.codec,
                                )
                                .await?;
                            let _ = ack.io.shutdown().await;
                            Err(MqttError::Disconnected(None))
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
        };

        Box::pin(async move {
            if let Ok(val) = timeout_checked(handshake_timeout, f).await {
                val
            } else {
                Err(MqttError::HandshakeTimeout)
            }
        })
    }
}

pub(crate) struct ServerSelector<St, C, T, F, R> {
    connect: C,
    handler: Rc<T>,
    check: Rc<F>,
    max_size: u32,
    max_receive: u16,
    max_qos: Option<QoS>,
    disconnect_timeout: Seconds,
    max_topic_alias: u16,
    _t: PhantomData<(St, R)>,
}

impl<St, C, T, F, R> ServiceFactory<SelectItem> for ServerSelector<St, C, T, F, R>
where
    St: 'static,
    F: Fn(&Handshake) -> R + 'static,
    R: Future<Output = Result<bool, C::Error>>,
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            Session<St>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        > + 'static,
{
    type Response = Either<SelectItem, ()>;
    type Error = MqttError<C::Error>;
    type InitError = C::InitError;
    type Service = ServerSelectorImpl<St, C::Service, T, F, R>;
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Self::InitError>> where Self: 'f;

    fn create(&self, _: ()) -> Self::Future<'_> {
        let fut = self.connect.create(());
        let handler = self.handler.clone();
        let check = self.check.clone();
        let max_size = self.max_size;
        let max_receive = self.max_receive;
        let max_qos = self.max_qos;
        let max_topic_alias = self.max_topic_alias;
        let disconnect_timeout = self.disconnect_timeout;

        // create connect service and then create service impl
        Box::pin(async move {
            Ok(ServerSelectorImpl {
                handler,
                check,
                max_size,
                max_receive,
                max_qos,
                max_topic_alias,
                disconnect_timeout,
                connect: Rc::new(fut.await?),
                _t: PhantomData,
            })
        })
    }
}

pub(crate) struct ServerSelectorImpl<St, C, T, F, R> {
    check: Rc<F>,
    connect: Rc<C>,
    handler: Rc<T>,
    max_size: u32,
    max_receive: u16,
    max_qos: Option<QoS>,
    disconnect_timeout: Seconds,
    max_topic_alias: u16,
    _t: PhantomData<(St, R)>,
}

impl<St, C, T, F, R> Service<SelectItem> for ServerSelectorImpl<St, C, T, F, R>
where
    St: 'static,
    F: Fn(&Handshake) -> R + 'static,
    R: Future<Output = Result<bool, C::Error>>,
    C: Service<Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Rc<MqttShared>>,
            Session<St>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        > + 'static,
{
    type Response = Either<SelectItem, ()>;
    type Error = MqttError<C::Error>;
    type Future<'f> = BoxFuture<'f, Result<Self::Response, Self::Error>> where Self: 'f;

    ntex::forward_poll_ready!(connect, MqttError::Service);
    ntex::forward_poll_shutdown!(connect);

    #[inline]
    fn call(&self, req: SelectItem) -> Self::Future<'_> {
        log::trace!("Start connection handshake");

        let check = self.check.clone();
        let connect = self.connect.clone();
        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let max_qos = self.max_qos;
        let max_size = self.max_size;
        let mut max_receive = self.max_receive;
        let mut max_topic_alias = self.max_topic_alias;

        Box::pin(async move {
            let (mut hnd, mut delay) = req;

            let result = match select((*check)(&hnd), &mut delay).await {
                Either::Left(res) => res,
                Either::Right(_) => return Err(MqttError::HandshakeTimeout),
            };

            if !result.map_err(MqttError::Service)? {
                Ok(Either::Left((hnd, delay)))
            } else {
                // set max outbound (encoder) packet size
                if let Some(size) = hnd.packet().max_packet_size {
                    hnd.shared.codec.set_max_outbound_size(size.get());
                }
                let keep_alive = hnd.packet().keep_alive;
                let peer_receive_max =
                    hnd.packet().receive_max.map(|v| v.get()).unwrap_or(16) as usize;
                hnd.max_size = max_size;
                hnd.max_receive = max_receive;
                hnd.max_topic_alias = max_topic_alias;

                // authenticate mqtt connection
                let mut ack = match select(connect.call(hnd), &mut delay).await {
                    Either::Left(res) => res.map_err(|e| {
                        log::trace!("Connection handshake failed: {:?}", e);
                        MqttError::Service(e)
                    })?,
                    Either::Right(_) => return Err(MqttError::HandshakeTimeout),
                };

                match ack.session {
                    Some(session) => {
                        log::trace!("Sending: {:#?}", ack.packet);
                        let shared = ack.shared;

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
                            shared.codec.set_max_inbound_size(size);
                        }
                        if ack.packet.server_keepalive_sec.is_none()
                            && (keep_alive > ack.keepalive)
                        {
                            ack.packet.server_keepalive_sec = Some(ack.keepalive);
                        }
                        shared.set_cap(peer_receive_max);
                        ack.io
                            .send(mqtt::Packet::ConnectAck(Box::new(ack.packet)), &shared.codec)
                            .await?;

                        let session = Session::new_v5(
                            session,
                            MqttSink::new(shared.clone()),
                            max_receive,
                            max_topic_alias,
                        );
                        let handler = handler.create(session).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Dispatcher::new(ack.io, shared, handler)
                            .keepalive_timeout(Seconds(ack.keepalive))
                            .disconnect_timeout(timeout)
                            .await?;
                        Ok(Either::Right(()))
                    }
                    None => {
                        log::trace!("Failed to complete handshake: {:#?}", ack.packet);

                        ack.io
                            .send(
                                mqtt::Packet::ConnectAck(Box::new(ack.packet)),
                                &ack.shared.codec,
                            )
                            .await?;
                        let _ = ack.io.shutdown().await;
                        Err(MqttError::Disconnected(None))
                    }
                }
            }
        })
    }
}
