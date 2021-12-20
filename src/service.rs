use std::task::{Context, Poll};
use std::{fmt, future::Future, marker::PhantomData, pin::Pin, rc::Rc};

use ntex::codec::{Decoder, Encoder};
use ntex::io::{DispatchItem, IoBoxed, Timer};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::time::{Millis, Seconds, Sleep};
use ntex::util::{select, Either, Pool};

use crate::io::Dispatcher;

type ResponseItem<U> = Option<<U as Encoder>::Item>;

pub(crate) struct FramedService<St, C, T, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: Seconds,
    time: Timer,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, Codec> FramedService<St, C, T, Codec> {
    pub(crate) fn new(connect: C, service: T, disconnect_timeout: Seconds) -> Self {
        FramedService {
            connect,
            disconnect_timeout,
            handler: Rc::new(service),
            time: Timer::new(Millis::ONE_SEC),
            _t: PhantomData,
        }
    }
}

impl<St, C, T, Codec> ServiceFactory for FramedService<St, C, T, Codec>
where
    C: ServiceFactory<Config = (), Request = IoBoxed, Response = (IoBoxed, Codec, St, Seconds)>
        + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
{
    type Config = ();
    type Request = IoBoxed;
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl<St, C::Service, T, Codec>;
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

pub(crate) struct FramedServiceImpl<St, C, T, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: Seconds,
    time: Timer,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, Codec> Service for FramedServiceImpl<St, C, T, Codec>
where
    C: Service<Request = IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Request = IoBoxed;
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
    fn call(&self, req: IoBoxed) -> Self::Future {
        log::trace!("Start connection handshake");

        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let handshake = self.connect.call(req);
        let time = self.time.clone();

        Box::pin(async move {
            let (io, codec, session, keepalive) = handshake.await.map_err(|e| {
                log::trace!("Connection handshake failed: {:?}", e);
                e
            })?;
            log::trace!("Connection handshake succeeded");

            let handler = handler.new_service(session).await?;
            log::trace!("Connection handler is created, starting dispatcher");

            Dispatcher::new(io, codec, handler, time)
                .keepalive_timeout(keepalive)
                .disconnect_timeout(timeout)
                .await
        })
    }
}

pub(crate) struct FramedService2<St, C, T, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: Seconds,
    time: Timer,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, Codec> FramedService2<St, C, T, Codec> {
    pub(crate) fn new(connect: C, service: T, disconnect_timeout: Seconds) -> Self {
        FramedService2 {
            connect,
            disconnect_timeout,
            handler: Rc::new(service),
            time: Timer::new(Millis::ONE_SEC),
            _t: PhantomData,
        }
    }
}

impl<St, C, T, Codec> ServiceFactory for FramedService2<St, C, T, Codec>
where
    C: ServiceFactory<Config = (), Request = IoBoxed, Response = (IoBoxed, Codec, St, Seconds)>
        + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Config = ();
    type Request = (IoBoxed, Option<Sleep>);
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = FramedServiceImpl2<St, C::Service, T, Codec>;
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

pub(crate) struct FramedServiceImpl2<St, C, T, Codec> {
    connect: C,
    handler: Rc<T>,
    disconnect_timeout: Seconds,
    time: Timer,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, Codec> Service for FramedServiceImpl2<St, C, T, Codec>
where
    C: Service<Request = IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            Config = St,
            Request = DispatchItem<Codec>,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Request = (IoBoxed, Option<Sleep>);
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
    fn call(&self, (req, delay): Self::Request) -> Self::Future {
        log::trace!("Start connection handshake");

        let handler = self.handler.clone();
        let timeout = self.disconnect_timeout;
        let handshake = self.connect.call(req);
        let time = self.time.clone();

        Box::pin(async move {
            let (io, codec, ka, handler) = if let Some(delay) = delay {
                let res = select(
                    delay,
                    Box::pin(async {
                        let (io, codec, st, ka) = handshake.await.map_err(|e| {
                            log::trace!("Connection handshake failed: {:?}", e);
                            e
                        })?;
                        log::trace!("Connection handshake succeeded");

                        let handler = handler.new_service(st).await?;
                        log::trace!("Connection handler is created, starting dispatcher");

                        Ok::<_, C::Error>((io, codec, ka, handler))
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
                let (io, codec, st, ka) = handshake.await.map_err(|e| {
                    log::trace!("Connection handshake failed: {:?}", e);
                    e
                })?;
                log::trace!("Connection handshake succeeded");

                let handler = handler.new_service(st).await?;
                log::trace!("Connection handler is created, starting dispatcher");
                (io, codec, ka, handler)
            };

            Dispatcher::new(io, codec, handler, time)
                .keepalive_timeout(ka)
                .disconnect_timeout(timeout)
                .await
        })
    }
}
