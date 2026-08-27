use std::{error::Error, fmt, marker::PhantomData};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::pipeline::Pipeline;
use ntex_service::{Ctx, Middleware, Service, ServiceFactory, cfg::SharedCfg};

use crate::error::{DecodeError, DispatcherError, EncodeError, MqttError};
use crate::{HandshakePipeline, control::Control, io::Dispatcher};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

pub struct MqttServer<St, AppSt, Codec, Err, E, T, M, C> {
    handshake: HandshakePipeline<St, AppSt, Codec, MqttError<Err>>,
    handler: T,
    middleware: M,
    control: C,
    _t: PhantomData<E>,
}

impl<St, AppSt, Codec, Err, E, T, M, C> fmt::Debug
    for MqttServer<St, AppSt, Codec, Err, E, T, M, C>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<St, AppSt, Codec, Err, E, T, M, C> MqttServer<St, AppSt, Codec, Err, E, T, M, C> {
    pub(crate) fn new(
        handshake: HandshakePipeline<St, AppSt, Codec, MqttError<Err>>,
        service: T,
        mw: M,
        control: C,
    ) -> Self {
        MqttServer {
            handshake,
            handler: service,
            middleware: mw,
            control,
            _t: PhantomData,
        }
    }
}

impl<St, AppSt, Codec, Err, E, T, M, C> Service<St, IoBoxed>
    for MqttServer<St, AppSt, Codec, Err, E, T, M, C>
where
    St: Clone + 'static,
    AppSt: Clone + 'static,
    Err: 'static,
    E: 'static,
    T: ServiceFactory<
            (),
            Request<Codec>,
            (SharedCfg, AppSt),
            Res = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, AppSt)>,
    M::Service:
        Service<(), Request<Codec>, Res = Response<Codec>, Error = DispatcherError<E>> + 'static,
    C: ServiceFactory<
            (),
            Control<E>,
            AppSt,
            Res = Response<Codec>,
            Error = MqttError<Err>,
            InitError = Box<dyn Error>,
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Res = ();
    type Error = MqttError<Err>;

    async fn call(&self, req: IoBoxed, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let tag = req.tag();

        let (io, codec, session, keepalive) = self.handshake.call(req, ctx.st()).await?;
        log::trace!("{tag}: Connection handshake succeeded");

        let control = self
            .control
            .create(&session)
            .await
            .map_err(MqttError::HandlerInit)?;
        let handler = self
            .handler
            .create(&(io.shared(), session.clone()))
            .await
            .map_err(MqttError::HandlerInit)?;
        let hnd = self.middleware.create(handler, &(io.shared(), session));
        log::trace!("{tag}: Connection handler is created, starting dispatcher");

        Dispatcher::new(io, codec, Pipeline::new(hnd), Pipeline::new(control))
            .keepalive_timeout(keepalive)
            .await
    }

    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        self.handshake.ready(ctx.st()).await
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.handshake.shutdown(ctx.st()).await;
    }
}

impl<F, St, AppSt, Codec, Err, E, T, M, C> Service<St, Io<F>>
    for MqttServer<St, AppSt, Codec, Err, E, T, M, C>
where
    F: Filter,
    St: Clone + 'static,
    AppSt: Clone + 'static,
    Err: 'static,
    E: 'static,
    T: ServiceFactory<
            (),
            Request<Codec>,
            (SharedCfg, AppSt),
            Res = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, AppSt)>,
    M::Service:
        Service<(), Request<Codec>, Res = Response<Codec>, Error = DispatcherError<E>> + 'static,
    C: ServiceFactory<
            (),
            Control<E>,
            AppSt,
            Res = Response<Codec>,
            Error = MqttError<Err>,
            InitError = Box<dyn Error>,
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Res = ();
    type Error = MqttError<Err>;

    #[inline]
    async fn call(&self, io: Io<F>, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        ctx.call::<_, IoBoxed>(self, IoBoxed::from(io)).await
    }

    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        self.handshake.ready(ctx.st()).await
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.handshake.shutdown(ctx.st()).await;
    }
}
