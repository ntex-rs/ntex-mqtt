use std::{error::Error, fmt, marker::PhantomData};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::pipeline::{Pipeline, PipelineFactory};
use ntex_service::{Ctx, Middleware, Service, ServiceFactory};

use crate::error::{DecodeError, DispatcherError, EncodeError, MqttError};
use crate::{Connection, HandshakePipeline, Session, control::Control, io::Dispatcher};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

type ControlPipeline<St, AppSt, Codec, Cfg, Err, E> = PipelineFactory<
    Session<Cfg, AppSt>,
    Control<E>,
    Response<Codec>,
    MqttError<Err>,
    Connection<Cfg, St, AppSt>,
    Box<dyn Error>,
>;

pub struct MqttServer<St, AppSt, Codec: Encoder, Cfg, Err, E, T, M> {
    handshake: HandshakePipeline<St, AppSt, Codec, Cfg, MqttError<Err>>,
    handler: T,
    middleware: M,
    control: ControlPipeline<St, AppSt, Codec, Cfg, Err, E>,
    _t: PhantomData<(E, Cfg)>,
}

impl<St, AppSt, Codec: Encoder, Cfg, Err, E, T, M> fmt::Debug
    for MqttServer<St, AppSt, Codec, Cfg, Err, E, T, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<St, AppSt, Codec: Encoder, Cfg, Err, E, T, M> MqttServer<St, AppSt, Codec, Cfg, Err, E, T, M> {
    pub(crate) fn new(
        handshake: HandshakePipeline<St, AppSt, Codec, Cfg, MqttError<Err>>,
        service: T,
        mw: M,
        control: ControlPipeline<St, AppSt, Codec, Cfg, Err, E>,
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

impl<St, AppSt, Codec, Cfg, Err, E, T, M> MqttServer<St, AppSt, Codec, Cfg, Err, E, T, M>
where
    St: Clone + 'static,
    AppSt: 'static,
    Cfg: 'static,
    Err: 'static,
    E: 'static,
    T: ServiceFactory<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Connection<Cfg, St, AppSt>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        > + 'static,
    M: Middleware<T::Service, Session<Cfg, AppSt>, Connection<Cfg, St, AppSt>>,
    M::Service: Service<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    pub fn build(
        self,
    ) -> impl Service<St, IoBoxed, Res = (), Error = MqttError<Err>>
    + use<St, AppSt, Codec, Cfg, Err, E, T, M> {
        self
    }
}

impl<St, AppSt, Codec, Cfg, Err, E, T, M> Service<St, IoBoxed>
    for MqttServer<St, AppSt, Codec, Cfg, Err, E, T, M>
where
    St: Clone + 'static,
    AppSt: 'static,
    Cfg: 'static,
    Err: 'static,
    E: 'static,
    T: ServiceFactory<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Connection<Cfg, St, AppSt>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        > + 'static,
    M: Middleware<T::Service, Session<Cfg, AppSt>, Connection<Cfg, St, AppSt>>,
    M::Service: Service<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Res = ();
    type Error = MqttError<Err>;

    async fn call(&self, req: IoBoxed, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let tag = req.tag();

        let (io, codec, session, con, keepalive) = self.handshake.call(req, ctx.st()).await?;
        log::trace!("{tag}: Connection handshake succeeded");

        let control = self
            .control
            .create(&con, &session)
            .await
            .map_err(MqttError::HandlerInit)?;
        let handler = self
            .handler
            .create(&con)
            .await
            .map_err(MqttError::HandlerInit)?;
        let hnd = self.middleware.create(handler, &con);
        log::trace!("{tag}: Connection handler is created, starting dispatcher");

        Dispatcher::new(io, codec, Pipeline::with(session.clone(), hnd), control)
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

impl<F, St, AppSt, Codec, Cfg, Err, E, T, M> Service<St, Io<F>>
    for MqttServer<St, AppSt, Codec, Cfg, Err, E, T, M>
where
    F: Filter,
    St: Clone + 'static,
    AppSt: 'static,
    Cfg: 'static,
    Err: 'static,
    E: 'static,
    T: ServiceFactory<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Connection<Cfg, St, AppSt>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Box<dyn Error>,
        > + 'static,
    M: Middleware<T::Service, Session<Cfg, AppSt>, Connection<Cfg, St, AppSt>>,
    M::Service: Service<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
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
