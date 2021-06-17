use std::task::{Context, Poll};
use std::{fmt, future::Future, marker::PhantomData, pin::Pin, rc::Rc, time::Duration};

use ntex::codec::{AsyncRead, AsyncWrite, Decoder, Encoder};
use ntex::rt::time::Sleep;
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::util::{select, Either};

use super::io::{DispatchItem, Dispatcher, State, Timer};

type ResponseItem<U> = Option<<U as Encoder>::Item>;

pub(crate) struct FramedService<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> FramedService<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: ServiceFactory<Config = (), Request = Io, Response = (Io, State, Codec, St, u16)>,
    C::Error: fmt::Debug,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
{
    pub(crate) fn new(connect: C, service: T, disconnect_timeout: u16) -> Self {
        FramedService {
            connect,
            disconnect_timeout,
            handler: Rc::new(service),
            time: Timer::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

impl<St, C, T, Io, Codec> ServiceFactory for FramedService<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: ServiceFactory<Config = (), Request = Io, Response = (Io, State, Codec, St, u16)>,
    C::Error: fmt::Debug,
    C::Future: 'static,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Config = ();
    type Request = Io;
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl<St, C::Service, T, Io, Codec>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Service, Self::InitError>>>>;

    fn new_service(&self, _: ()) -> Self::Future {
        let fut = self.connect.new_service(());
        let handler = self.handler.clone();
        let disconnect_timeout = self.disconnect_timeout;
        let time = self.time.clone();

        // create connect service and then create service impl
        Box::pin(async move {
            Ok(FramedServiceImpl {
                handler,
                disconnect_timeout,
                time,
                connect: fut.await?,
                _t: PhantomData,
            })
        })
    }
}

pub(crate) struct FramedServiceImpl<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> Service for FramedServiceImpl<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = Io, Response = (Io, State, Codec, St, u16)>,
    C::Error: fmt::Debug,
    C::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + Clone + 'static,
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
            let (io, st, codec, session, keepalive) = handshake.await.map_err(|e| {
                log::trace!("Connection handshake failed: {:?}", e);
                e
            })?;
            log::trace!("Connection handshake succeeded");

            let handler = handler.new_service(session).await?;
            log::trace!("Connection handler is created, starting dispatcher");

            Dispatcher::with(io, st, codec, handler, time)
                .keepalive_timeout(keepalive as u16)
                .disconnect_timeout(timeout)
                .await
        })
    }
}

pub(crate) struct FramedService2<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> FramedService2<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: ServiceFactory<
        Config = (),
        Request = (Io, State),
        Response = (Io, State, Codec, St, u16),
    >,
    C::Error: fmt::Debug,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
{
    pub(crate) fn new(connect: C, service: T, disconnect_timeout: u16) -> Self {
        FramedService2 {
            connect,
            disconnect_timeout,
            handler: Rc::new(service),
            time: Timer::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

impl<St, C, T, Io, Codec> ServiceFactory for FramedService2<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: ServiceFactory<
        Config = (),
        Request = (Io, State),
        Response = (Io, State, Codec, St, u16),
    >,
    C::Error: fmt::Debug,
    C::Future: 'static,
    <C::Service as Service>::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Config = ();
    type Request = (Io, State, Option<Pin<Box<Sleep>>>);
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl2<St, C::Service, T, Io, Codec>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Service, Self::InitError>>>>;

    fn new_service(&self, _: ()) -> Self::Future {
        let fut = self.connect.new_service(());
        let handler = self.handler.clone();
        let disconnect_timeout = self.disconnect_timeout;
        let time = self.time.clone();

        // create connect service and then create service impl
        Box::pin(async move {
            Ok(FramedServiceImpl2 {
                handler,
                disconnect_timeout,
                time,
                connect: fut.await?,
                _t: PhantomData,
            })
        })
    }
}

pub(crate) struct FramedServiceImpl2<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: u16,
    time: Timer,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> Service for FramedServiceImpl2<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = (Io, State), Response = (Io, State, Codec, St, u16)>,
    C::Error: fmt::Debug,
    C::Future: 'static,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    <T::Service as Service>::Error: 'static,
    <T::Service as Service>::Future: 'static,
    Codec: Decoder + Encoder + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Request = (Io, State, Option<Pin<Box<Sleep>>>);
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
    fn call(&self, (req, state, delay): (Io, State, Option<Pin<Box<Sleep>>>)) -> Self::Future {
        log::trace!("Start connection handshake");

        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let handshake = self.connect.call((req, state));
        let time = self.time.clone();

        Box::pin(async move {
            let (io, state, codec, ka, handler) = if let Some(delay) = delay {
                let res = select(
                    delay,
                    Box::pin(async {
                        let (io, state, codec, st, ka) = handshake.await.map_err(|e| {
                            log::trace!("Connection handshake failed: {:?}", e);
                            e
                        })?;
                        log::trace!("Connection handshake succeeded");

                        let handler = handler.new_service(st).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Ok::<_, C::Error>((io, state, codec, ka, handler))
                    }),
                )
                .await;

                match res {
                    Either::Left(_) => {
                        log::warn!("Handshake timed out");
                        return Ok(());
                    }
                    Either::Right(item) => item?,
                }
            } else {
                let (io, state, codec, st, ka) = handshake.await.map_err(|e| {
                    log::trace!("Connection handshake failed: {:?}", e);
                    e
                })?;
                log::trace!("Connection handshake succeeded");

                let handler = handler.new_service(st).await?;
                log::trace!("Connection handler is created, starting dispatcher");
                (io, state, codec, ka, handler)
            };

            Dispatcher::with(io, state, codec, handler, time)
                .keepalive_timeout(ka as u16)
                .disconnect_timeout(timeout)
                .await
        })
    }
}
