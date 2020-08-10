use std::task::{Context, Poll};
use std::time::Duration;
use std::{fmt, future::Future, marker::PhantomData, pin::Pin, rc::Rc};

use futures::future::{select, Either, FutureExt};
use futures::{ready, Stream};

use ntex::rt::time::Delay;
use ntex::service::{IntoService, IntoServiceFactory, Service, ServiceFactory};
use ntex::util::time::LowResTimeService;
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder, Framed};

use super::framed::{Dispatcher, DispatcherError};
use super::handshake::{Handshake, HandshakeResult};

type RequestItem<U> = <U as Decoder>::Item;
type ResponseItem<U> = Option<<U as Encoder>::Item>;

/// Service builder - structure that follows the builder pattern
/// for building instances for framed services.
pub(crate) struct Builder<St, C, Io, Codec, Out> {
    connect: C,
    _t: PhantomData<(St, Io, Codec, Out)>,
}

impl<St, C, Io, Codec, Out> Builder<St, C, Io, Codec, Out>
where
    C: Service<Request = Handshake<Io, Codec>, Response = HandshakeResult<Io, St, Codec, Out>>,
    C::Error: fmt::Debug,
    Io: AsyncRead + AsyncWrite + Unpin,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    /// Construct framed handler service with specified connect service
    pub(crate) fn new<F>(connect: F) -> Builder<St, C, Io, Codec, Out>
    where
        F: IntoService<C>,
    {
        Builder { connect: connect.into_service(), _t: PhantomData }
    }

    /// Provide stream items handler service and construct service factory.
    pub(crate) fn build<F, T>(self, service: F) -> FramedServiceImpl<St, C, T, Io, Codec, Out>
    where
        F: IntoServiceFactory<T>,
        T: ServiceFactory<
            Config = St,
            Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        >,
    {
        FramedServiceImpl {
            connect: self.connect,
            handler: Rc::new(service.into_factory()),
            disconnect_timeout: 3000,
            time: LowResTimeService::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

/// Service builder - structure that follows the builder pattern
/// for building instances for framed services.
pub(crate) struct FactoryBuilder<St, C, Io, Codec, Out> {
    connect: C,
    disconnect_timeout: usize,
    _t: PhantomData<(St, Io, Codec, Out)>,
}

impl<St, C, Io, Codec, Out> FactoryBuilder<St, C, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = Handshake<Io, Codec>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    Codec: Decoder + Encoder,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    /// Construct framed handler service factory with specified connect service
    pub(crate) fn new<F>(connect: F) -> FactoryBuilder<St, C, Io, Codec, Out>
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
    pub(crate) fn disconnect_timeout(mut self, val: usize) -> Self {
        self.disconnect_timeout = val;
        self
    }

    pub(crate) fn build<F, T, Cfg>(
        self,
        service: F,
    ) -> FramedService<St, C, T, Io, Codec, Out, Cfg>
    where
        F: IntoServiceFactory<T>,
        T: ServiceFactory<
            Config = St,
            Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        >,
    {
        FramedService {
            connect: self.connect,
            handler: Rc::new(service.into_factory()),
            disconnect_timeout: self.disconnect_timeout,
            time: LowResTimeService::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

pub(crate) struct FramedService<St, C, T, Io, Codec, Out, Cfg> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: usize,
    time: LowResTimeService,
    _t: PhantomData<(St, Io, Codec, Out, Cfg)>,
}

impl<St, C, T, Io, Codec, Out, Cfg> ServiceFactory
    for FramedService<St, C, T, Io, Codec, Out, Cfg>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = Handshake<Io, Codec>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    type Config = Cfg;
    type Request = Io;
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl<St, C::Service, T, Io, Codec, Out>;
    type Future = FramedServiceResponse<St, C, T, Io, Codec, Out>;

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
pub(crate) struct FramedServiceResponse<St, C, T, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = Handshake<Io, Codec>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    #[pin]
    fut: C::Future,
    handler: Rc<T>,
    disconnect_timeout: usize,
    time: LowResTimeService,
}

impl<St, C, T, Io, Codec, Out> Future for FramedServiceResponse<St, C, T, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = Handshake<Io, Codec>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    type Output = Result<FramedServiceImpl<St, C::Service, T, Io, Codec, Out>, C::InitError>;

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

pub(crate) struct FramedServiceImpl<St, C, T, Io, Codec, Out> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: usize,
    time: LowResTimeService,
    _t: PhantomData<(St, Io, Codec, Out)>,
}

impl<St, C, T, Io, Codec, Out> Service for FramedServiceImpl<St, C, T, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: Service<Request = Handshake<Io, Codec>, Response = HandshakeResult<Io, St, Codec, Out>>,
    C::Error: fmt::Debug,
    C::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
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
        let handshake = self.connect.call(Handshake::new(req));
        let time = self.time.clone();

        Box::pin(async move {
            let result = handshake.await.map_err(|e| {
                log::trace!("Connection handshake failed: {:?}", e);
                e
            })?;
            log::trace!("Connection handshake succeeded");

            let handler = handler.new_service(result.state).await?;
            log::trace!("Connection handler is created, starting dispatcher");

            Dispatcher::with(result.framed, result.out, handler, time)
                .keepalive_timeout(result.keepalive)
                .disconnect_timeout(timeout as u64)
                .await
        })
    }
}

/// Service builder - structure that follows the builder pattern
/// for building instances for framed services.
pub(crate) struct FactoryBuilder2<St, C, Io, Codec, Out> {
    connect: C,
    disconnect_timeout: usize,
    _t: PhantomData<(St, Io, Codec, Out)>,
}

impl<St, C, Io, Codec, Out> FactoryBuilder2<St, C, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = HandshakeResult<Io, (), Codec, Out>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    Codec: Decoder + Encoder,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    /// Construct framed handler service factory with specified connect service
    pub(crate) fn new<F>(connect: F) -> FactoryBuilder2<St, C, Io, Codec, Out>
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
    pub(crate) fn disconnect_timeout(mut self, val: usize) -> Self {
        self.disconnect_timeout = val;
        self
    }

    pub(crate) fn build<F, T, Cfg>(
        self,
        service: F,
    ) -> FramedService2<St, C, T, Io, Codec, Out, Cfg>
    where
        F: IntoServiceFactory<T>,
        T: ServiceFactory<
            Config = St,
            Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        >,
    {
        FramedService2 {
            connect: self.connect,
            handler: Rc::new(service.into_factory()),
            disconnect_timeout: self.disconnect_timeout,
            time: LowResTimeService::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

pub(crate) struct FramedService2<St, C, T, Io, Codec, Out, Cfg> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: usize,
    time: LowResTimeService,
    _t: PhantomData<(St, Io, Codec, Out, Cfg)>,
}

impl<St, C, T, Io, Codec, Out, Cfg> ServiceFactory
    for FramedService2<St, C, T, Io, Codec, Out, Cfg>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = HandshakeResult<Io, (), Codec, Out>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    type Config = Cfg;
    type Request = (Framed<Io, Codec>, Option<Delay>);
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl2<St, C::Service, T, Io, Codec, Out>;
    type Future = FramedServiceResponse2<St, C, T, Io, Codec, Out>;

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
pub(crate) struct FramedServiceResponse2<St, C, T, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = HandshakeResult<Io, (), Codec, Out>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    #[pin]
    fut: C::Future,
    handler: Rc<T>,
    disconnect_timeout: usize,
    time: LowResTimeService,
}

impl<St, C, T, Io, Codec, Out> Future for FramedServiceResponse2<St, C, T, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<
        Config = (),
        Request = HandshakeResult<Io, (), Codec, Out>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    T: ServiceFactory<
        Config = St,
        Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
        Response = ResponseItem<Codec>,
        Error = C::Error,
        InitError = C::Error,
    >,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    type Output = Result<FramedServiceImpl2<St, C::Service, T, Io, Codec, Out>, C::InitError>;

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

pub(crate) struct FramedServiceImpl2<St, C, T, Io, Codec, Out> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: usize,
    time: LowResTimeService,
    _t: PhantomData<(St, Io, Codec, Out)>,
}

impl<St, C, T, Io, Codec, Out> Service for FramedServiceImpl2<St, C, T, Io, Codec, Out>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: Service<
        Request = HandshakeResult<Io, (), Codec, Out>,
        Response = HandshakeResult<Io, St, Codec, Out>,
    >,
    C::Error: fmt::Debug,
    C::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = Result<RequestItem<Codec>, DispatcherError<Codec>>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder,
    <Codec as Encoder>::Item: 'static,
    Out: Stream<Item = <Codec as Encoder>::Item> + Unpin,
{
    type Request = (Framed<Io, Codec>, Option<Delay>);
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
    fn call(&self, (req, delay): (Framed<Io, Codec>, Option<Delay>)) -> Self::Future {
        log::trace!("Start connection handshake");

        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let handshake = self.connect.call(Handshake::with_codec(req));
        let time = self.time.clone();

        Box::pin(async move {
            let (framed, out, ka, handler) = if let Some(delay) = delay {
                let res = select(
                    delay,
                    async {
                        let result = handshake.await.map_err(|e| {
                            log::trace!("Connection handshake failed: {:?}", e);
                            e
                        })?;
                        log::trace!("Connection handshake succeeded");

                        let handler = handler.new_service(result.state).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Ok::<_, C::Error>((
                            result.framed,
                            result.out,
                            result.keepalive,
                            handler,
                        ))
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
                let result = handshake.await.map_err(|e| {
                    log::trace!("Connection handshake failed: {:?}", e);
                    e
                })?;
                log::trace!("Connection handshake succeeded");

                let handler = handler.new_service(result.state).await?;
                log::trace!("Connection handler is created, starting dispatcher");
                (result.framed, result.out, result.keepalive, handler)
            };

            Dispatcher::with(framed, out, handler, time)
                .keepalive_timeout(ka)
                .disconnect_timeout(timeout as u64)
                .await
        })
    }
}
