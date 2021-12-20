use std::task::{Context, Poll};
use std::{cell::RefCell, convert::TryFrom, fmt, future::Future, marker, pin::Pin, rc::Rc};

use ntex::io::{into_boxed, DispatchItem, Filter, Io, IoBoxed, Timer};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::time::{Millis, Seconds, Sleep};
use ntex::util::timeout::{Timeout, TimeoutError};
use ntex::util::{Either, PoolId, PoolRef};

use crate::error::{MqttError, ProtocolError};
use crate::io::Dispatcher;
use crate::service::{FramedService, FramedService2};
use crate::types::QoS;

use super::control::{ControlMessage, ControlResult};
use super::default::{DefaultControlService, DefaultPublishService};
use super::handshake::{Handshake, HandshakeAck};
use super::publish::{Publish, PublishAck};
use super::selector::SelectItem;
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, dispatcher::factory, MqttSink, Session};

/// Mqtt Server
pub struct MqttServer<St, C: ServiceFactory, Cn: ServiceFactory, P: ServiceFactory> {
    handshake: C,
    srv_control: Cn,
    srv_publish: P,
    max_size: u32,
    max_receive: u16,
    max_qos: Option<QoS>,
    handshake_timeout: Seconds,
    disconnect_timeout: Seconds,
    max_topic_alias: u16,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: marker::PhantomData<St>,
}

impl<St, C>
    MqttServer<St, C, DefaultControlService<St, C::Error>, DefaultPublishService<St, C::Error>>
where
    St: 'static,
    C: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<C>,
    {
        MqttServer {
            handshake: handshake.into_factory(),
            srv_control: DefaultControlService::default(),
            srv_publish: DefaultPublishService::default(),
            max_size: 0,
            max_receive: 15,
            max_qos: None,
            handshake_timeout: Seconds::ZERO,
            disconnect_timeout: Seconds(3),
            max_topic_alias: 32,
            pool: Rc::new(MqttSinkPool::default()),
            _t: marker::PhantomData,
        }
    }
}

impl<St, C, Cn, P> MqttServer<St, C, Cn, P>
where
    St: 'static,
    C: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    Cn: ServiceFactory<
            Config = Session<St>,
            Request = ControlMessage<C::Error>,
            Response = ControlResult,
        > + 'static,
    P: ServiceFactory<Config = Session<St>, Request = Publish, Response = PublishAck> + 'static,
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
    /// By default max qos is not set`
    pub fn max_qos(mut self, qos: QoS) -> Self {
        self.max_qos = Some(qos);
        self
    }

    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max number of buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<St, C, Srv, P>
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
            handshake: self.handshake,
            srv_publish: self.srv_publish,
            srv_control: service.into_factory(),
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            pool: self.pool,
            _t: marker::PhantomData,
        }
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub fn publish<F, Srv>(self, publish: F) -> MqttServer<St, C, Cn, Srv>
    where
        F: IntoServiceFactory<Srv> + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
        Srv: ServiceFactory<Config = Session<St>, Request = Publish, Response = PublishAck>
            + 'static,
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
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            pool: self.pool,
            _t: marker::PhantomData,
        }
    }
}

impl<St, C, Cn, P> MqttServer<St, C, Cn, P>
where
    St: 'static,
    C: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>> + 'static,
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
    pub fn finish<F: Filter>(
        self,
    ) -> impl ServiceFactory<Config = (), Request = Io<F>, Response = (), Error = MqttError<C::Error>>
    {
        let handshake = self.handshake;
        let publish = self.srv_publish.map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .srv_control
            .map_err(<C::Error>::from)
            .map_init_err(|e| MqttError::Service(e.into()));

        into_boxed(FramedService::new(
            handshake_service_factory(
                handshake,
                self.max_size,
                self.max_receive,
                self.max_topic_alias,
                self.max_qos,
                self.handshake_timeout,
                self.pool,
            ),
            factory(publish, control),
            self.disconnect_timeout,
        ))
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn inner_finish(
        self,
    ) -> impl ServiceFactory<
        Config = (),
        Request = (IoBoxed, Option<Sleep>),
        Response = (),
        Error = MqttError<C::Error>,
        InitError = C::InitError,
    > {
        let handshake = self.handshake;
        let publish = self.srv_publish.map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .srv_control
            .map_err(<C::Error>::from)
            .map_init_err(|e| MqttError::Service(e.into()));

        FramedService2::new(
            handshake_service_factory(
                handshake,
                self.max_size,
                self.max_receive,
                self.max_topic_alias,
                self.max_qos,
                self.handshake_timeout,
                self.pool,
            ),
            factory(publish, control),
            self.disconnect_timeout,
        )
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn finish_selector<F, R>(
        self,
        check: F,
    ) -> impl ServiceFactory<
        Config = (),
        Request = SelectItem,
        Response = Either<SelectItem, ()>,
        Error = MqttError<C::Error>,
        InitError = C::InitError,
    >
    where
        F: Fn(&Handshake) -> R + 'static,
        R: Future<Output = Result<bool, C::Error>> + 'static,
    {
        let publish = self.srv_publish.map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .srv_control
            .map_err(<C::Error>::from)
            .map_init_err(|e| MqttError::Service(e.into()));

        ServerSelector::<St, _, _, _, _> {
            check: Rc::new(check),
            connect: self.handshake,
            handler: Rc::new(factory(publish, control)),
            max_size: self.max_size,
            max_receive: self.max_receive,
            max_topic_alias: self.max_topic_alias,
            max_qos: self.max_qos,
            disconnect_timeout: self.disconnect_timeout,
            time: Timer::new(Millis::ONE_SEC),
            _t: marker::PhantomData,
        }
    }
}

fn handshake_service_factory<St, C>(
    factory: C,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
    max_qos: Option<QoS>,
    handshake_timeout: Seconds,
    pool: Rc<MqttSinkPool>,
) -> impl ServiceFactory<
    Config = (),
    Request = IoBoxed,
    Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds),
    Error = MqttError<C::Error>,
    InitError = C::InitError,
>
where
    C: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>>,
    C::Error: fmt::Debug,
{
    ntex::service::apply(
        Timeout::new(Millis::from(handshake_timeout)),
        ntex::service::fn_factory(move || {
            let pool = pool.clone();

            let fut = factory.new_service(());
            async move {
                let service = fut.await?;
                let pool = pool.clone();
                let service = Rc::new(service.map_err(MqttError::Service));
                Ok::<_, C::InitError>(ntex::service::apply_fn(service, move |io, service| {
                    handshake(
                        io,
                        service.clone(),
                        max_size,
                        max_receive,
                        max_topic_alias,
                        max_qos,
                        pool.clone(),
                    )
                }))
            }
        }),
    )
    .map_err(|e| match e {
        TimeoutError::Service(e) => e,
        TimeoutError::Timeout => MqttError::HandshakeTimeout,
    })
}

#[allow(clippy::too_many_arguments)]
async fn handshake<S, St, E>(
    io: IoBoxed,
    service: S,
    max_size: u32,
    mut max_receive: u16,
    mut max_topic_alias: u16,
    max_qos: Option<QoS>,
    pool: Rc<MqttSinkPool>,
) -> Result<(IoBoxed, Rc<MqttShared>, Session<St>, Seconds), S::Error>
where
    S: Service<Request = Handshake, Response = HandshakeAck<St>, Error = MqttError<E>>,
{
    log::trace!("Starting mqtt v5 handshake");

    let shared = Rc::new(MqttShared::new(io.get_ref(), mqtt::Codec::default(), 0, pool));

    // set max inbound (decoder) packet size
    shared.codec.set_max_inbound_size(max_size);

    // read first packet
    let packet = io
        .next(&shared.codec)
        .await
        .ok_or_else(|| {
            log::trace!("Server mqtt is disconnected during handshake");
            MqttError::Disconnected
        })?
        .map_err(|err| {
            log::trace!("Error is received during mqtt handshake: {:?}", err);
            MqttError::from(err)
        })?;

    match packet {
        mqtt::Packet::Connect(connect) => {
            // set max outbound (encoder) packet size
            if let Some(size) = connect.max_packet_size {
                shared.codec.set_max_outbound_size(size.get());
            }
            shared.cap.set(connect.receive_max.map(|v| v.get()).unwrap_or(16) as usize);

            let keep_alive = connect.keep_alive;

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
                .await?;

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
                        && (keep_alive > ack.keepalive as u16)
                    {
                        ack.packet.server_keepalive_sec = Some(ack.keepalive as u16);
                    }

                    ack.io
                        .send(mqtt::Packet::ConnectAck(Box::new(ack.packet)), &shared.codec)
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

                    if !ack.io.is_closed()
                        && ack
                            .io
                            .encode(
                                mqtt::Packet::ConnectAck(Box::new(ack.packet)),
                                &ack.shared.codec,
                            )
                            .is_ok()
                    {
                        let _ = ack.io.shutdown().await;
                    }
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

pub(crate) struct ServerSelector<St, C, T, F, R> {
    connect: C,
    handler: Rc<T>,
    time: Timer,
    check: Rc<F>,
    max_size: u32,
    max_receive: u16,
    max_qos: Option<QoS>,
    disconnect_timeout: Seconds,
    max_topic_alias: u16,
    _t: marker::PhantomData<(St, R)>,
}

impl<St, C, T, F, R> ServiceFactory for ServerSelector<St, C, T, F, R>
where
    F: Fn(&Handshake) -> R + 'static,
    R: Future<Output = Result<bool, C::Error>>,
    C: ServiceFactory<Config = (), Request = Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = Session<St>,
            Request = DispatchItem<Rc<MqttShared>>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        > + 'static,
{
    type Config = ();
    type Request = SelectItem;
    type Response = Either<SelectItem, ()>;
    type Error = MqttError<C::Error>;
    type InitError = C::InitError;
    type Service = ServerSelectorImpl<St, C::Service, T, F, R>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Service, Self::InitError>>>>;

    fn new_service(&self, _: ()) -> Self::Future {
        let fut = self.connect.new_service(());
        let handler = self.handler.clone();
        let time = self.time.clone();
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
                time,
                check,
                max_size,
                max_receive,
                max_qos,
                max_topic_alias,
                disconnect_timeout,
                connect: Rc::new(fut.await?),
                _t: marker::PhantomData,
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
    time: Timer,
    _t: marker::PhantomData<(St, R)>,
}

impl<St, C, T, F, R> Service for ServerSelectorImpl<St, C, T, F, R>
where
    F: Fn(&Handshake) -> R + 'static,
    R: Future<Output = Result<bool, C::Error>>,
    C: Service<Request = Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = Session<St>,
            Request = DispatchItem<Rc<MqttShared>>,
            Response = Option<mqtt::Packet>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        > + 'static,
{
    type Request = SelectItem;
    type Response = Either<SelectItem, ()>;
    type Error = MqttError<C::Error>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connect.poll_ready(cx).map_err(MqttError::Service)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        self.connect.poll_shutdown(cx, is_error)
    }

    #[inline]
    fn call(&self, req: Self::Request) -> Self::Future {
        log::trace!("Start connection handshake");

        let check = self.check.clone();
        let connect = self.connect.clone();
        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let time = self.time.clone();
        let max_qos = self.max_qos;
        let max_size = self.max_size;
        let mut max_receive = self.max_receive;
        let mut max_topic_alias = self.max_topic_alias;

        Box::pin(async move {
            let (mut hnd, mut delay) = req;

            let result = if let Some(ref mut delay) = delay {
                let fut = (&*check)(&hnd);
                match crate::utils::select(fut, delay).await {
                    Either::Left(res) => res,
                    Either::Right(_) => return Err(MqttError::HandshakeTimeout),
                }
            } else {
                (&*check)(&hnd).await
            };

            if !result.map_err(MqttError::Service)? {
                Ok(Either::Left((hnd, delay)))
            } else {
                // set max outbound (encoder) packet size
                if let Some(size) = hnd.packet().max_packet_size {
                    hnd.shared.codec.set_max_outbound_size(size.get());
                }
                hnd.shared
                    .cap
                    .set(hnd.packet().receive_max.map(|v| v.get()).unwrap_or(16) as usize);

                let keep_alive = hnd.packet().keep_alive;
                hnd.max_size = max_size;
                hnd.max_receive = max_receive;
                hnd.max_topic_alias = max_topic_alias;

                // authenticate mqtt connection
                let mut ack = if let Some(ref mut delay) = delay {
                    let fut = connect.call(hnd);
                    match crate::utils::select(fut, delay).await {
                        Either::Left(res) => res.map_err(|e| {
                            log::trace!("Connection handshake failed: {:?}", e);
                            MqttError::Service(e)
                        })?,
                        Either::Right(_) => return Err(MqttError::HandshakeTimeout),
                    }
                } else {
                    connect.call(hnd).await.map_err(|e| {
                        log::trace!("Connection handshake failed: {:?}", e);
                        MqttError::Service(e)
                    })?
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
                            && (keep_alive > ack.keepalive as u16)
                        {
                            ack.packet.server_keepalive_sec = Some(ack.keepalive as u16);
                        }

                        ack.io
                            .send(mqtt::Packet::ConnectAck(Box::new(ack.packet)), &shared.codec)
                            .await?;

                        let session = Session::new_v5(
                            session,
                            MqttSink::new(shared.clone()),
                            max_receive,
                            max_topic_alias,
                        );
                        let handler = handler.new_service(session).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Dispatcher::new(ack.io, shared, handler, time)
                            .keepalive_timeout(Seconds(ack.keepalive))
                            .disconnect_timeout(timeout)
                            .await?;
                        Ok(Either::Right(()))
                    }
                    None => {
                        log::trace!("Failed to complete handshake: {:#?}", ack.packet);

                        if !ack.io.is_closed()
                            && ack
                                .io
                                .encode(
                                    mqtt::Packet::ConnectAck(Box::new(ack.packet)),
                                    &ack.shared.codec,
                                )
                                .is_ok()
                        {
                            let _ = ack.io.shutdown().await;
                        }
                        Err(MqttError::Disconnected)
                    }
                }
            }
        })
    }
}
