use std::task::{Context, Poll};
use std::time::Duration;
use std::{fmt, future::Future, marker::PhantomData, pin::Pin, rc::Rc};

use futures::future::{select, Either, FutureExt};
use futures::ready;

use ntex::rt::time::Delay;
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::io::{DispatcherItem, IoDispatcher, IoState, Timer};

type ResponseItem<U> = Option<<U as Encoder>::Item>;

/// Service builder - structure that follows the builder pattern
/// for building instances for framed services.
pub(crate) struct FactoryBuilder<St, C, Io, Codec> {
    connect: C,
    disconnect_timeout: u16,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, Io, Codec> FactoryBuilder<St, C, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Io, Response = (Io, IoState<Codec>, St, u16)>,
    C::Error: fmt::Debug,
    Codec: Decoder + Encoder + 'static,
{
    /// Construct framed handler service factory with specified connect service
    pub(crate) fn new<F>(connect: F) -> FactoryBuilder<St, C, Io, Codec>
    where
        F: IntoServiceFactory<C>,
    {
        FactoryBuilder {
            connect: connect.into_factory(),
            disconnect_timeout: 3000,
            _t: PhantomData,
        }
    }

    /// Set connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub(crate) fn disconnect_timeout(mut self, val: u16) -> Self {
        self.disconnect_timeout = val;
        self
    }

    pub(crate) fn build<F, T, Cfg>(self, service: F) -> FramedService<St, C, T, Io, Codec, Cfg>
    where
        F: IntoServiceFactory<T>,
        T: ServiceFactory<
            Config = St,
            Request = DispatcherItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        >,
    {
        FramedService {
            connect: self.connect,
            handler: Rc::new(service.into_factory()),
            disconnect_timeout: self.disconnect_timeout,
            time: Timer::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

pub(crate) struct FramedService<St, C, T, Io, Codec, Cfg> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer<Codec>,
    _t: PhantomData<(St, Io, Codec, Cfg)>,
}

impl<St, C, T, Io, Codec, Cfg> ServiceFactory for FramedService<St, C, T, Io, Codec, Cfg>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: ServiceFactory<Config = (), Request = Io, Response = (Io, IoState<Codec>, St, u16)>,
    C::Error: fmt::Debug,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatcherItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Config = Cfg;
    type Request = Io;
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl<St, C::Service, T, Io, Codec>;
    type Future = FramedServiceResponse<St, C, T, Io, Codec>;

    fn new_service(&self, _: Cfg) -> Self::Future {
        // create connect service and then create service impl
        FramedServiceResponse {
            fut: self.connect.new_service(()),
            handler: self.handler.clone(),
            disconnect_timeout: self.disconnect_timeout,
            time: self.time.clone(),
        }
    }
}

#[pin_project::pin_project]
pub(crate) struct FramedServiceResponse<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Io, Response = (Io, IoState<Codec>, St, u16)>,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = DispatcherItem<Codec>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
{
    #[pin]
    fut: C::Future,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer<Codec>,
}

impl<St, C, T, Io, Codec> Future for FramedServiceResponse<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Io, Response = (Io, IoState<Codec>, St, u16)>,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = DispatcherItem<Codec>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
{
    type Output = Result<FramedServiceImpl<St, C::Service, T, Io, Codec>, C::InitError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let connect = ready!(this.fut.poll(cx))?;

        Poll::Ready(Ok(FramedServiceImpl {
            connect,
            handler: this.handler.clone(),
            disconnect_timeout: *this.disconnect_timeout,
            time: this.time.clone(),
            _t: PhantomData,
        }))
    }
}

pub(crate) struct FramedServiceImpl<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer<Codec>,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> Service for FramedServiceImpl<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = Io, Response = (Io, IoState<Codec>, St, u16)>,
    C::Error: fmt::Debug,
    C::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatcherItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Request = Io;
    type Response = ();
    type Error = C::Error;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connect.poll_ready(cx)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        self.connect.poll_shutdown(cx, is_error)
    }

    #[inline]
    fn call(&self, req: Io) -> Self::Future {
        log::trace!("Start connection handshake");

        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let handshake = self.connect.call(req);
        let time = self.time.clone();

        Box::pin(async move {
            let (io, st, session, keepalive) = handshake.await.map_err(|e| {
                log::trace!("Connection handshake failed: {:?}", e);
                e
            })?;
            log::trace!("Connection handshake succeeded");

            let handler = handler.new_service(session).await?;
            log::trace!("Connection handler is created, starting dispatcher");

            IoDispatcher::with(io, st, handler, time)
                .keepalive_timeout(keepalive as u16)
                .disconnect_timeout(timeout)
                .await
        })
    }
}

/// Service builder - structure that follows the builder pattern
/// for building instances for framed services.
pub(crate) struct FactoryBuilder2<St, C, Io, Codec> {
    connect: C,
    disconnect_timeout: u16,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, Io, Codec> FactoryBuilder2<St, C, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = (Io, IoState<Codec>),
        Response = (Io, IoState<Codec>, St, u16),
    >,
    C::Error: fmt::Debug,
    Codec: Decoder + Encoder + 'static,
{
    /// Construct framed handler service factory with specified connect service
    pub(crate) fn new<F>(connect: F) -> FactoryBuilder2<St, C, Io, Codec>
    where
        F: IntoServiceFactory<C>,
    {
        FactoryBuilder2 {
            connect: connect.into_factory(),
            disconnect_timeout: 3000,
            _t: PhantomData,
        }
    }

    /// Set connection disconnect timeout in milliseconds.
    pub(crate) fn disconnect_timeout(mut self, val: u16) -> Self {
        self.disconnect_timeout = val;
        self
    }

    pub(crate) fn build<F, T, Cfg>(self, service: F) -> FramedService2<St, C, T, Io, Codec, Cfg>
    where
        F: IntoServiceFactory<T>,
        T: ServiceFactory<
            Config = St,
            Request = DispatcherItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        >,
    {
        FramedService2 {
            connect: self.connect,
            handler: Rc::new(service.into_factory()),
            disconnect_timeout: self.disconnect_timeout,
            time: Timer::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

pub(crate) struct FramedService2<St, C, T, Io, Codec, Cfg> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer<Codec>,
    _t: PhantomData<(St, Io, Codec, Cfg)>,
}

impl<St, C, T, Io, Codec, Cfg> ServiceFactory for FramedService2<St, C, T, Io, Codec, Cfg>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: ServiceFactory<
        Config = (),
        Request = (Io, IoState<Codec>),
        Response = (Io, IoState<Codec>, St, u16),
    >,
    C::Error: fmt::Debug,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatcherItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Config = Cfg;
    type Request = (Io, IoState<Codec>, Option<Delay>);
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl2<St, C::Service, T, Io, Codec>;
    type Future = FramedServiceResponse2<St, C, T, Io, Codec>;

    fn new_service(&self, _: Cfg) -> Self::Future {
        // create connect service and then create service impl
        FramedServiceResponse2 {
            fut: self.connect.new_service(()),
            handler: self.handler.clone(),
            disconnect_timeout: self.disconnect_timeout,
            time: self.time.clone(),
        }
    }
}

#[pin_project::pin_project]
pub(crate) struct FramedServiceResponse2<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = (Io, IoState<Codec>),
        Response = (Io, IoState<Codec>, St, u16),
    >,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = DispatcherItem<Codec>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
{
    #[pin]
    fut: C::Future,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer<Codec>,
}

impl<St, C, T, Io, Codec> Future for FramedServiceResponse2<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = (Io, IoState<Codec>),
        Response = (Io, IoState<Codec>, St, u16),
    >,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = DispatcherItem<Codec>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
{
    type Output = Result<FramedServiceImpl2<St, C::Service, T, Io, Codec>, C::InitError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let connect = ready!(this.fut.poll(cx))?;

        Poll::Ready(Ok(FramedServiceImpl2 {
            connect,
            handler: this.handler.clone(),
            disconnect_timeout: *this.disconnect_timeout,
            time: this.time.clone(),
            _t: PhantomData,
        }))
    }
}

pub(crate) struct FramedServiceImpl2<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer<Codec>,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> Service for FramedServiceImpl2<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = (Io, IoState<Codec>), Response = (Io, IoState<Codec>, St, u16)>,
    C::Error: fmt::Debug,
    C::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatcherItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Request = (Io, IoState<Codec>, Option<Delay>);
    type Response = ();
    type Error = C::Error;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connect.poll_ready(cx)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        self.connect.poll_shutdown(cx, is_error)
    }

    #[inline]
    fn call(&self, (req, state, delay): (Io, IoState<Codec>, Option<Delay>)) -> Self::Future {
        log::trace!("Start connection handshake");

        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let handshake = self.connect.call((req, state));
        let time = self.time.clone();

        Box::pin(async move {
            let (io, state, ka, handler) = if let Some(delay) = delay {
                let res = select(
                    delay,
                    async {
                        let (io, state, st, ka) = handshake.await.map_err(|e| {
                            log::trace!("Connection handshake failed: {:?}", e);
                            e
                        })?;
                        log::trace!("Connection handshake succeeded");

                        let handler = handler.new_service(st).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Ok::<_, C::Error>((io, state, ka, handler))
                    }
                    .boxed_local(),
                )
                .await;

                match res {
                    Either::Left(_) => {
                        log::warn!("Handshake timed out");
                        return Ok(());
                    }
                    Either::Right(item) => item.0?,
                }
            } else {
                let (io, state, st, ka) = handshake.await.map_err(|e| {
                    log::trace!("Connection handshake failed: {:?}", e);
                    e
                })?;
                log::trace!("Connection handshake succeeded");

                let handler = handler.new_service(st).await?;
                log::trace!("Connection handler is created, starting dispatcher");
                (io, state, ka, handler)
            };

            IoDispatcher::with(io, state, handler, time)
                .keepalive_timeout(ka as u16)
                .disconnect_timeout(timeout)
                .await
        })
    }
}
