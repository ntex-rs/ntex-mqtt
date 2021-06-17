use std::task::{Context, Poll};
use std::{fmt, future::Future, marker::PhantomData, pin::Pin, rc::Rc, time::Duration};

use ntex::codec::{AsyncRead, AsyncWrite, Decoder, Encoder};
use ntex::rt::time::Sleep;
use ntex::service::{apply_fn_factory, IntoServiceFactory, Service, ServiceFactory};
use ntex::util::{timeout::Timeout, timeout::TimeoutError, Either, Ready};

use crate::error::{MqttError, ProtocolError};
use crate::io::{DispatchItem, Dispatcher, State, Timer};
// use super::io::{DispatchItem, Dispatcher, State, Timer};
use crate::service::{FramedService, FramedService2};

use super::control::{ControlMessage, ControlResult};
use super::default::{DefaultControlService, DefaultPublishService};
use super::handshake::{Handshake, HandshakeAck};
use super::selector::SelectItem;
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, dispatcher::factory, MqttSink, Publish, Session};

/// Mqtt v3.1.1 Server
pub struct MqttServer<Io, St, C: ServiceFactory, Cn: ServiceFactory, P: ServiceFactory> {
    handshake: C,
    control: Cn,
    publish: P,
    max_size: u32,
    inflight: usize,
    handshake_timeout: u16,
    disconnect_timeout: u16,
    pub(super) pool: Rc<MqttSinkPool>,
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
    C: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>
        + 'static,
    C::Error: fmt::Debug,
{
    /// Create server factory and provide handshake service
    pub fn new<F>(handshake: F) -> Self
    where
        F: IntoServiceFactory<C>,
    {
        MqttServer {
            handshake: handshake.into_factory(),
            control: DefaultControlService::default(),
            publish: DefaultPublishService::default(),
            max_size: 0,
            inflight: 16,
            handshake_timeout: 0,
            disconnect_timeout: 3000,
            pool: Default::default(),
            _t: PhantomData,
        }
    }
}

impl<Io, St, C, Cn, P> MqttServer<Io, St, C, Cn, P>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    St: 'static,
    C: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>
        + 'static,
    <C::Service as Service>::Future: 'static,
    Cn: ServiceFactory<Config = Session<St>, Request = ControlMessage, Response = ControlResult>
        + 'static,
    <Cn::Service as Service>::Future: 'static,
    P: ServiceFactory<Config = Session<St>, Request = Publish, Response = ()> + 'static,
    P::Service: 'static,
    P::Future: 'static,
    P::Error: 'static,
    P::InitError: 'static,
    <P::Service as Service>::Future: 'static,
    <P::Service as Service>::Error: 'static,

    C::Error: From<Cn::Error>
        + From<Cn::InitError>
        + From<P::Error>
        + From<P::InitError>
        + fmt::Debug,
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

    /// Number of in-flight concurrent messages.
    ///
    /// By default in-flight is set to 16 messages
    pub fn inflight(mut self, val: usize) -> Self {
        self.inflight = val;
        self
    }

    /// Service to handle control packets
    ///
    /// All control packets are processed sequentially, max buffered
    /// control packets is 16.
    pub fn control<F, Srv>(self, service: F) -> MqttServer<Io, St, C, Srv, P>
    where
        F: IntoServiceFactory<Srv>,
        Srv: ServiceFactory<
                Config = Session<St>,
                Request = ControlMessage,
                Response = ControlResult,
            > + 'static,
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
    pub fn publish<F, Srv>(self, publish: F) -> MqttServer<Io, St, C, Cn, Srv>
    where
        F: IntoServiceFactory<Srv> + 'static,
        Srv: ServiceFactory<Config = Session<St>, Request = Publish, Response = ()> + 'static,
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
    pub fn finish(
        self,
    ) -> impl ServiceFactory<Config = (), Request = Io, Response = (), Error = MqttError<C::Error>>
    {
        let handshake = self.handshake;
        let publish = self
            .publish
            .into_factory()
            .map_err(|e| MqttError::Service(e.into()))
            .map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .control
            .map_err(|e| MqttError::Service(e.into()))
            .map_init_err(|e| MqttError::Service(e.into()));

        ntex::unit_config(FramedService::new(
            handshake_service_factory(
                handshake,
                self.max_size,
                self.handshake_timeout,
                self.pool,
            ),
            apply_fn_factory(
                factory(publish, control, self.inflight),
                |req: DispatchItem<Rc<MqttShared>>, srv| match req {
                    DispatchItem::Item(req) => Either::Left(srv.call(req)),
                    DispatchItem::KeepAliveTimeout => Either::Right(Ready::Err(
                        MqttError::Protocol(ProtocolError::KeepAliveTimeout),
                    )),
                    DispatchItem::EncoderError(e) => {
                        Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Encode(e))))
                    }
                    DispatchItem::DecoderError(e) => {
                        Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Decode(e))))
                    }
                    DispatchItem::IoError(e) => {
                        Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Io(e))))
                    }
                    DispatchItem::WBackPressureEnabled
                    | DispatchItem::WBackPressureDisabled => Either::Right(Ready::Ok(None)),
                },
            ),
            self.disconnect_timeout,
        ))
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn inner_finish(
        self,
    ) -> impl ServiceFactory<
        Config = (),
        Request = (Io, State, Option<Pin<Box<Sleep>>>),
        Response = (),
        Error = MqttError<C::Error>,
        InitError = C::InitError,
    > {
        let handshake = self.handshake;
        let publish = self
            .publish
            .into_factory()
            .map_err(|e| MqttError::Service(e.into()))
            .map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .control
            .map_err(|e| MqttError::Service(e.into()))
            .map_init_err(|e| MqttError::Service(e.into()));

        ntex::unit_config(FramedService2::new(
            handshake_service_factory2(
                handshake,
                self.max_size,
                self.handshake_timeout,
                self.pool,
            ),
            apply_fn_factory(
                factory(publish, control, self.inflight),
                |req: DispatchItem<Rc<MqttShared>>, srv| match req {
                    DispatchItem::Item(req) => Either::Left(srv.call(req)),
                    DispatchItem::KeepAliveTimeout => Either::Right(Ready::Err(
                        MqttError::Protocol(ProtocolError::KeepAliveTimeout),
                    )),
                    DispatchItem::EncoderError(e) => {
                        Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Encode(e))))
                    }
                    DispatchItem::DecoderError(e) => {
                        Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Decode(e))))
                    }
                    DispatchItem::IoError(e) => {
                        Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Io(e))))
                    }
                    DispatchItem::WBackPressureEnabled
                    | DispatchItem::WBackPressureDisabled => Either::Right(Ready::Ok(None)),
                },
            ),
            self.disconnect_timeout,
        ))
    }

    /// Set service to handle publish packets and create mqtt server factory
    pub(crate) fn finish_selector<F, R>(
        self,
        check: F,
    ) -> impl ServiceFactory<
        Config = (),
        Request = SelectItem<Io>,
        Response = Either<SelectItem<Io>, ()>,
        Error = MqttError<C::Error>,
        InitError = C::InitError,
    >
    where
        F: Fn(&mqtt::Connect) -> R + 'static,
        R: Future<Output = Result<bool, C::Error>> + 'static,
    {
        let publish = self
            .publish
            .map_err(|e| MqttError::Service(e.into()))
            .map_init_err(|e| MqttError::Service(e.into()));
        let control = self
            .control
            .map_err(|e| MqttError::Service(e.into()))
            .map_init_err(|e| MqttError::Service(e.into()));

        let handler = apply_fn_factory(
            factory(publish, control, self.inflight),
            |req: DispatchItem<Rc<MqttShared>>, srv| match req {
                DispatchItem::Item(req) => Either::Left(srv.call(req)),
                DispatchItem::KeepAliveTimeout => Either::Right(Ready::Err(
                    MqttError::Protocol(ProtocolError::KeepAliveTimeout),
                )),
                DispatchItem::EncoderError(e) => {
                    Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Encode(e))))
                }
                DispatchItem::DecoderError(e) => {
                    Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Decode(e))))
                }
                DispatchItem::IoError(e) => {
                    Either::Right(Ready::Err(MqttError::Protocol(ProtocolError::Io(e))))
                }
                DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => {
                    Either::Right(Ready::Ok(None))
                }
            },
        );

        ServerSelector {
            check: Rc::new(check),
            connect: self.handshake,
            handler: Rc::new(handler),
            max_size: self.max_size,
            disconnect_timeout: self.disconnect_timeout,
            time: Timer::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

fn handshake_service_factory<Io, St, C>(
    factory: C,
    max_size: u32,
    handshake_timeout: u16,
    pool: Rc<MqttSinkPool>,
) -> impl ServiceFactory<
    Config = (),
    Request = Io,
    Response = (Io, State, Rc<MqttShared>, Session<St>, u16),
    Error = MqttError<C::Error>,
>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>,
    C::Error: fmt::Debug,
{
    ntex::apply(
        Timeout::new(Duration::from_millis(handshake_timeout as u64)),
        ntex::fn_factory(move || {
            let pool = pool.clone();
            let fut = factory.new_service(());
            async move {
                let service = fut.await?;
                let pool = pool.clone();
                let service = Rc::new(service.map_err(MqttError::Service));
                Ok::<_, C::InitError>(ntex::apply_fn(service, move |conn: Io, service| {
                    handshake(conn, None, service.clone(), max_size, pool.clone())
                }))
            }
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
    handshake_timeout: u16,
    pool: Rc<MqttSinkPool>,
) -> impl ServiceFactory<
    Config = (),
    Request = (Io, State),
    Response = (Io, State, Rc<MqttShared>, Session<St>, u16),
    Error = MqttError<C::Error>,
    InitError = C::InitError,
>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>,
    C::Error: fmt::Debug,
{
    ntex::apply(
        Timeout::new(Duration::from_millis(handshake_timeout as u64)),
        ntex::fn_factory(move || {
            let pool = pool.clone();
            let fut = factory.new_service(());
            async move {
                let service = fut.await?;
                let pool = pool.clone();
                let service = Rc::new(service.map_err(MqttError::Service));
                Ok(ntex::apply_fn(service, move |(io, state), service| {
                    handshake(io, Some(state), service.clone(), max_size, pool.clone())
                }))
            }
        }),
    )
    .map_err(|e| match e {
        TimeoutError::Service(e) => e,
        TimeoutError::Timeout => MqttError::HandshakeTimeout,
    })
}

async fn handshake<Io, S, St, E>(
    mut io: Io,
    state: Option<State>,
    service: S,
    max_size: u32,
    pool: Rc<MqttSinkPool>,
) -> Result<(Io, State, Rc<MqttShared>, Session<St>, u16), S::Error>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    S: Service<Request = Handshake<Io>, Response = HandshakeAck<Io, St>, Error = MqttError<E>>,
{
    log::trace!("Starting mqtt handshake");

    let state = state.unwrap_or_else(State::new);
    let shared = Rc::new(MqttShared::new(
        state.clone(),
        mqtt::Codec::default().max_size(max_size),
        16,
        pool,
    ));

    // read first packet
    let packet = state
        .next(&mut io, &shared.codec)
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
            // authenticate mqtt connection
            let mut ack = service.call(Handshake::new(connect, io, shared)).await?;

            match ack.session {
                Some(session) => {
                    let pkt = mqtt::Packet::ConnectAck {
                        session_present: ack.session_present,
                        return_code: mqtt::ConnectAckReason::ConnectionAccepted,
                    };

                    log::trace!("Sending success handshake ack: {:#?}", pkt);

                    state.set_buffer_params(ack.read_hw, ack.write_hw, ack.lw);
                    state.send(&mut ack.io, &ack.shared.codec, pkt).await?;

                    Ok((
                        ack.io,
                        ack.shared.state.clone(),
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
                    ack.shared.state.send(&mut ack.io, &ack.shared.codec, pkt).await?;

                    Err(MqttError::Disconnected)
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

type ResponseItem<U> = Option<<U as Encoder>::Item>;

pub(crate) struct ServerSelector<St, C, T, Io, F, R> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer,
    check: Rc<F>,
    max_size: u32,
    _t: PhantomData<(St, Io, R)>,
}

impl<St, C, T, Io, F, R> ServiceFactory for ServerSelector<St, C, T, Io, F, R>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    F: Fn(&mqtt::Connect) -> R + 'static,
    R: Future<Output = Result<bool, C::Error>>,
    C: ServiceFactory<Config = (), Request = Handshake<Io>, Response = HandshakeAck<Io, St>>
        + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = Session<St>,
            Request = DispatchItem<Rc<MqttShared>>,
            Response = ResponseItem<Rc<MqttShared>>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        > + 'static,
{
    type Config = ();
    type Request = SelectItem<Io>;
    type Response = Either<SelectItem<Io>, ()>;
    type Error = MqttError<C::Error>;
    type InitError = C::InitError;
    type Service = ServerSelectorImpl<St, C::Service, T, Io, F, R>;
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

pub(crate) struct ServerSelectorImpl<St, C, T, Io, F, R> {
    check: Rc<F>,
    connect: Rc<C>,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer,
    max_size: u32,
    _t: PhantomData<(St, Io, R)>,
}

impl<St, C, T, Io, F, R> Service for ServerSelectorImpl<St, C, T, Io, F, R>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    F: Fn(&mqtt::Connect) -> R + 'static,
    R: Future<Output = Result<bool, C::Error>>,
    C: Service<Request = Handshake<Io>, Response = HandshakeAck<Io, St>> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = Session<St>,
            Request = DispatchItem<Rc<MqttShared>>,
            Response = ResponseItem<Rc<MqttShared>>,
            Error = MqttError<C::Error>,
            InitError = MqttError<C::Error>,
        > + 'static,
{
    type Request = SelectItem<Io>;
    type Response = Either<SelectItem<Io>, ()>;
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
        let max_size = self.max_size;

        Box::pin(async move {
            let (pkt, io, state, shared, mut delay) = req;

            let result = if let Some(ref mut delay) = delay {
                let fut = (&*check)(&pkt);
                match crate::utils::select(fut, delay).await {
                    Either::Left(res) => res,
                    Either::Right(_) => return Err(MqttError::HandshakeTimeout),
                }
            } else {
                (&*check)(&pkt).await
            };

            if !result.map_err(MqttError::Service)? {
                Ok(Either::Left((pkt, io, state, shared, delay)))
            } else {
                // authenticate mqtt connection
                let mut ack = if let Some(ref mut delay) = delay {
                    let fut = connect.call(Handshake::new(pkt, io, shared));
                    match crate::utils::select(fut, delay).await {
                        Either::Left(res) => res.map_err(|e| {
                            log::trace!("Connection handshake failed: {:?}", e);
                            MqttError::Service(e)
                        })?,
                        Either::Right(_) => return Err(MqttError::HandshakeTimeout),
                    }
                } else {
                    connect.call(Handshake::new(pkt, io, shared)).await.map_err(|e| {
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
                        state.set_buffer_params(ack.read_hw, ack.write_hw, ack.lw);
                        state
                            .send(&mut ack.io, &ack.shared.codec, pkt)
                            .await
                            .map_err(MqttError::from)?;

                        let session = Session::new(session, MqttSink::new(ack.shared.clone()));
                        let handler = handler.new_service(session).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Dispatcher::with(
                            ack.io,
                            ack.shared.state.clone(),
                            ack.shared,
                            handler,
                            time,
                        )
                        .keepalive_timeout(ack.keepalive as u16)
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
                        ack.shared.state.send(&mut ack.io, &ack.shared.codec, pkt).await?;

                        Err(MqttError::Disconnected)
                    }
                }
            }
        })
    }
}
