use std::task::{Context, Poll};
use std::{fmt, future::Future, marker::PhantomData, pin::Pin, rc::Rc};

use ntex::codec::{AsyncRead, AsyncWrite, Decoder, Encoder};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::time::{Millis, Seconds, Sleep};
use ntex::util::{select, Either, Pool};

use super::io::{DispatchItem, Dispatcher, State, Timer};

type ResponseItem<U> = Option<<U as Encoder>::Item>;

pub(crate) struct FramedService<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: Seconds,
    time: Timer,
    pool: Pool,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> FramedService<St, C, T, Io, Codec> {
    pub(crate) fn new(connect: C, service: T, pool: Pool, disconnect_timeout: Seconds) -> Self {
        FramedService {
            pool,
            connect,
            disconnect_timeout,
            handler: Rc::new(service),
            time: Timer::new(Millis::ONE_SEC),
            _t: PhantomData,
        }
    }
}

impl<St, C, T, Io, Codec> ServiceFactory for FramedService<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: ServiceFactory<Config = (), Request = Io, Response = (Io, State, Codec, St, Seconds)>,
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
        let pool = self.pool.clone();

        // create connect service and then create service impl
        Box::pin(async move {
            Ok(FramedServiceImpl {
                handler,
                disconnect_timeout,
                pool,
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
    disconnect_timeout: Seconds,
    pool: Pool,
    time: Timer,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> Service for FramedServiceImpl<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = Io, Response = (Io, State, Codec, St, Seconds)>,
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
        let ready1 = self.connect.poll_ready(cx)?.is_ready();
        let ready2 = self.pool.poll_ready(cx).is_ready();

        if ready1 && ready2 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
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
                .keepalive_timeout(keepalive)
                .disconnect_timeout(timeout)
                .await
        })
    }
}

pub(crate) struct FramedService2<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: Seconds,
    pool: Pool,
    time: Timer,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> FramedService2<St, C, T, Io, Codec> {
    pub(crate) fn new(connect: C, service: T, pool: Pool, disconnect_timeout: Seconds) -> Self {
        FramedService2 {
            connect,
            pool,
            disconnect_timeout,
            handler: Rc::new(service),
            time: Timer::new(Millis::ONE_SEC),
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
        Response = (Io, State, Codec, St, Seconds),
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
    type Request = (Io, State, Option<Sleep>);
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
        let pool = self.pool.clone();

        // create connect service and then create service impl
        Box::pin(async move {
            Ok(FramedServiceImpl2 {
                handler,
                disconnect_timeout,
                time,
                pool,
                connect: fut.await?,
                _t: PhantomData,
            })
        })
    }
}

pub(crate) struct FramedServiceImpl2<St, C, T, Io, Codec> {
    connect: C,
    handler: Rc<T>,
    pool: Pool,
    disconnect_timeout: Seconds,
    time: Timer,
    _t: PhantomData<(St, Io, Codec)>,
}

impl<St, C, T, Io, Codec> Service for FramedServiceImpl2<St, C, T, Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = (Io, State), Response = (Io, State, Codec, St, Seconds)>,
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
    type Request = (Io, State, Option<Sleep>);
    type Response = ();
    type Error = C::Error;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ready1 = self.connect.poll_ready(cx)?.is_ready();
        let ready2 = self.pool.poll_ready(cx).is_ready();

        if ready1 && ready2 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        self.connect.poll_shutdown(cx, is_error)
    }

    #[inline]
    fn call(&self, (req, state, delay): (Io, State, Option<Sleep>)) -> Self::Future {
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
                .keepalive_timeout(ka)
                .disconnect_timeout(timeout)
                .await
        })
    }
}
