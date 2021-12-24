use std::task::{Context, Poll};
use std::{fmt, future::Future, marker::PhantomData, pin::Pin, rc::Rc};

use ntex::codec::{Decoder, Encoder};
use ntex::io::{seal, DispatchItem, Filter, Io, IoBoxed, IoRef, Timer};
use ntex::service::{apply_fn_factory, IntoServiceFactory, Service, ServiceFactory};
use ntex::time::{Millis, Seconds, Sleep};
use ntex::util::{timeout::Timeout, timeout::TimeoutError, Either, PoolId, Ready};

use crate::error::{MqttError, ProtocolError};
use crate::io::Dispatcher;
use crate::service::{FramedService, FramedService2};

use super::control::{ControlMessage, ControlResult};
use super::default::{DefaultControlService, DefaultPublishService};
use super::handshake::{Handshake, HandshakeAck};
use super::selector::SelectItem;
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, dispatcher::factory, MqttSink, Publish, Session};

/// Mqtt v3.1.1 Server
pub struct MqttServer<St, C, Cn, P> {
    handshake: C,
    control: Cn,
    publish: P,
    max_size: u32,
    inflight: usize,
    handshake_timeout: Seconds,
    disconnect_timeout: Seconds,
    pub(super) pool: Rc<MqttSinkPool>,
    _t: PhantomData<St>,
}

impl<St, C>
    MqttServer<St, C, DefaultControlService<St, C::Error>, DefaultPublishService<St, C::Error>>
where
    St: 'static,
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    C::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<C, Handshake>,
    {
        MqttServer {
            handshake: handshake.into_factory(),
            control: DefaultControlService::default(),
            publish: DefaultPublishService::default(),
            max_size: 0,
            inflight: 16,
            handshake_timeout: Seconds::ZERO,
            disconnect_timeout: Seconds(3),
            pool: Default::default(),
            _t: PhantomData,
        }
    }
}

impl<St, C, Cn, P> MqttServer<St, C, Cn, P>
where
    St: 'static,
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>> + 'static,
    Cn: ServiceFactory<ControlMessage<C::Error>, Session<St>, Response = ControlResult>
        + 'static,
    P: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
    C::Error: From<Cn::Error>
        + From<Cn::InitError>
        + From<P::Error>
        + From<P::InitError>
        + fmt::Debug,
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

    /// Number of in-flight concurrent messages.
    ///
    /// By default in-flight is set to 16 messages
    pub fn inflight(mut self, val: usize) -> Self {
        self.inflight = val;
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
            publish: self.publish,
            control: service.into_factory(),
            max_size: self.max_size,
            inflight: self.inflight,
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
        Srv: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError> + fmt::Debug,
    {
        MqttServer {
            handshake: self.handshake,
            publish: publish.into_factory(),
            control: self.control,
            max_size: self.max_size,
            inflight: self.inflight,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
            pool: self.pool,
            _t: PhantomData,
        }
    }

    /// Finish server configuration and create mqtt server factory
    pub fn finish<F>(
        self,
    ) -> impl ServiceFactory<Io<F>, Response = (), Error = MqttError<C::Error>>
    where
        F: Filter,
    {
        let handshake = self.handshake;
        let publish = self
            .publish
            .into_factory()
            .map_err(|e| e.into())
            .map_init_err(|e| MqttError::Service(e.into()));
        let control =
            self.control.map_err(|e| e.into()).map_init_err(|e| MqttError::Service(e.into()));

        seal(FramedService::new(
            handshake_service_factory(
                handshake,
                self.max_size,
                self.handshake_timeout,
                self.pool.clone(),
            ),
            factory(publish, control, self.inflight),
            self.disconnect_timeout,
        ))
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn inner_finish(
        self,
    ) -> impl ServiceFactory<
        (IoBoxed, Option<Sleep>),
        Response = (),
        Error = MqttError<C::Error>,
        InitError = C::InitError,
    > {
        let handshake = self.handshake;
        let publish = self
            .publish
            .into_factory()
            .map_err(|e| e.into())
            .map_init_err(|e| MqttError::Service(e.into()));
        let control =
            self.control.map_err(|e| e.into()).map_init_err(|e| MqttError::Service(e.into()));

        FramedService2::new(
            handshake_service_factory(
                handshake,
                self.max_size,
                self.handshake_timeout,
                self.pool.clone(),
            ),
            factory(publish, control, self.inflight),
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
        let publish =
            self.publish.map_err(|e| e.into()).map_init_err(|e| MqttError::Service(e.into()));
        let control =
            self.control.map_err(|e| e.into()).map_init_err(|e| MqttError::Service(e.into()));

        ServerSelector {
            check: Rc::new(check),
            connect: self.handshake,
            handler: Rc::new(factory(publish, control, self.inflight)),
            max_size: self.max_size,
            disconnect_timeout: self.disconnect_timeout,
            time: Timer::new(Millis::ONE_SEC),
            _t: PhantomData,
        }
    }
}

fn handshake_service_factory<St, C>(
    factory: C,
    max_size: u32,
    handshake_timeout: Seconds,
    pool: Rc<MqttSinkPool>,
) -> impl ServiceFactory<
    IoBoxed,
    Response = (IoBoxed, Rc<MqttShared>, Session<St>, Seconds),
    Error = MqttError<C::Error>,
    InitError = C::InitError,
>
where
    C: ServiceFactory<Handshake, Response = HandshakeAck<St>>,
    C::Error: fmt::Debug,
{
    ntex::service::apply(
        Timeout::new(Millis::from(handshake_timeout)),
        ntex::service::fn_factory(move || {
            let pool = pool.clone();
            let fut = factory.new_service(());
            async move {
                let service = fut.await?;
                let service = Rc::new(service.map_err(MqttError::Service));
                Ok::<_, C::InitError>(ntex::service::apply_fn(service, move |conn, service| {
                    handshake(conn, service.clone(), max_size, pool.clone())
                }))
            }
        }),
    )
    .map_err(|e| match e {
        TimeoutError::Service(e) => e,
        TimeoutError::Timeout => MqttError::HandshakeTimeout,
    })
}

async fn handshake<S, St, E>(
    io: IoBoxed,
    service: S,
    max_size: u32,
    pool: Rc<MqttSinkPool>,
) -> Result<(IoBoxed, Rc<MqttShared>, Session<St>, Seconds), S::Error>
where
    S: Service<Handshake, Response = HandshakeAck<St>, Error = MqttError<E>>,
{
    log::trace!("Starting mqtt handshake");

    let shared = Rc::new(MqttShared::new(
        io.get_ref(),
        mqtt::Codec::default().max_size(max_size),
        16,
        pool,
    ));

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
            // authenticate mqtt connection
            let ack = service.call(Handshake::new(connect, io, shared)).await?;

            match ack.session {
                Some(session) => {
                    let pkt = mqtt::Packet::ConnectAck {
                        session_present: ack.session_present,
                        return_code: mqtt::ConnectAckReason::ConnectionAccepted,
                    };

                    log::trace!("Sending success handshake ack: {:#?}", pkt);

                    ack.io.send(&ack.shared.codec, pkt).await?;
                    Ok((
                        ack.io,
                        ack.shared.clone(),
                        Session::new(session, MqttSink::new(ack.shared)),
                        ack.keepalive,
                    ))
                }
                None => {
                    let pkt = mqtt::Packet::ConnectAck {
                        session_present: false,
                        return_code: ack.return_code,
                    };

                    log::trace!("Sending failed handshake ack: {:#?}", pkt);
                    ack.io.send(&ack.shared.codec, pkt).await?;

                    Err(MqttError::Disconnected(None))
                }
            }
        }
        packet => {
            log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {:?}", packet);
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
    disconnect_timeout: Seconds,
    time: Timer,
    check: Rc<F>,
    max_size: u32,
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
    type Future = Pin<Box<dyn Future<Output = Result<Self::Service, Self::InitError>>>>;

    fn new_service(&self, _: ()) -> Self::Future {
        let fut = self.connect.new_service(());
        let handler = self.handler.clone();
        let disconnect_timeout = self.disconnect_timeout;
        let time = self.time.clone();
        let check = self.check.clone();
        let max_size = self.max_size;

        // create connect service and then create service impl
        Box::pin(async move {
            Ok(ServerSelectorImpl {
                handler,
                disconnect_timeout,
                time,
                check,
                max_size,
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
    disconnect_timeout: Seconds,
    time: Timer,
    max_size: u32,
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
    fn call(&self, req: SelectItem) -> Self::Future {
        log::trace!("Start connection handshake");

        let check = self.check.clone();
        let connect = self.connect.clone();
        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let time = self.time.clone();
        let max_size = self.max_size;

        Box::pin(async move {
            let (hnd, mut delay) = req;

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
                // authenticate mqtt connection
                let ack = if let Some(ref mut delay) = delay {
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
                        let pkt = mqtt::Packet::ConnectAck {
                            session_present: ack.session_present,
                            return_code: mqtt::ConnectAckReason::ConnectionAccepted,
                        };
                        log::trace!(
                            "Connection handshake succeeded, sending handshake ack: {:#?}",
                            pkt
                        );

                        ack.shared.codec.set_max_size(max_size);
                        ack.io.send(&ack.shared.codec, pkt).await.map_err(MqttError::from)?;

                        let session = Session::new(session, MqttSink::new(ack.shared.clone()));
                        let handler = handler.new_service(session).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Dispatcher::new(ack.io, ack.shared, handler, time)
                            .keepalive_timeout(ack.keepalive)
                            .disconnect_timeout(timeout)
                            .await?;
                        Ok(Either::Right(()))
                    }
                    None => {
                        let pkt = mqtt::Packet::ConnectAck {
                            session_present: false,
                            return_code: ack.return_code,
                        };

                        log::trace!("Sending failed handshake ack: {:#?}", pkt);
                        ack.io.send(&ack.shared.codec, pkt).await?;

                        Err(MqttError::Disconnected(None))
                    }
                }
            }
        })
    }
}
