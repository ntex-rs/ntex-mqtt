use std::{fmt, marker::PhantomData};

use ntex_codec::{Decoder, Encoder};
use ntex_error::ErrorInfo;
use ntex_io::IoBoxed;
use ntex_service::pipeline::{Pipeline, PipelineFactory};
use ntex_service::{Ctx, Middleware, RequestState, Service, ServiceFactory};

use crate::error::{DecodeError, DispatcherError, EncodeError, MqttError};
use crate::{ConnectPipeline, Session, control::Control, io::Dispatcher};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

type ControlPipeline<AppSt, Codec, Cfg, Err, E> =
    PipelineFactory<Session<Cfg, AppSt>, Control<E>, Response<Codec>, MqttError<Err>, ErrorInfo>;

pub struct MqttServer<St, Im, AppSt, Codec: Encoder, Cfg, Err, E, T, M> {
    connect: ConnectPipeline<St, Im, AppSt, Codec, Cfg, MqttError<Err>>,
    handler: T,
    middleware: M,
    control: ControlPipeline<AppSt, Codec, Cfg, Err, E>,
    _t: PhantomData<(E, Cfg)>,
}

impl<St, Im, AppSt, Codec, Cfg, Err, E, T, M> fmt::Debug
    for MqttServer<St, Im, AppSt, Codec, Cfg, Err, E, T, M>
where
    Codec: Encoder,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<St, Im, AppSt, Codec, Cfg, Err, E, T, M> MqttServer<St, Im, AppSt, Codec, Cfg, Err, E, T, M>
where
    St: 'static,
    Im: 'static,
    AppSt: 'static,
    Cfg: 'static,
    Err: 'static,
    E: 'static,
    Codec: Encoder,
{
    pub(crate) fn new(
        connect: ConnectPipeline<St, Im, AppSt, Codec, Cfg, MqttError<Err>>,
        handler: T,
        middleware: M,
        control: ControlPipeline<AppSt, Codec, Cfg, Err, E>,
    ) -> Self {
        MqttServer {
            connect,
            handler,
            middleware,
            control,
            _t: PhantomData,
        }
    }
}

impl<St, Im, AppSt, Codec, Cfg, Err, E, T, M, Req> Service<St, Req>
    for MqttServer<St, Im, AppSt, Codec, Cfg, Err, E, T, M>
where
    St: 'static,
    Im: 'static,
    AppSt: 'static,
    Cfg: 'static,
    Err: 'static,
    E: 'static,
    T: ServiceFactory<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = ErrorInfo,
        > + 'static,
    M: Middleware<T::Service, Session<Cfg, AppSt>>,
    M::Service: Service<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
    Req: RequestState<IoBoxed, State = Im>,
{
    type Res = ();
    type Error = MqttError<Err>;

    async fn call(&self, req: Req, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let (st, io) = req.unpack();
        let tag = io.tag();

        let (io, codec, session, keepalive) = self.connect.call((io, st), ctx.st()).await?;
        log::trace!("{tag}: Connection handshake succeeded");

        let control = self
            .control
            .create(session.clone())
            .await
            .map_err(MqttError::HandlerInit)?;
        let handler = self
            .handler
            .create(&session)
            .await
            .map_err(MqttError::HandlerInit)?;
        let hnd = self.middleware.create(&session, handler);
        log::trace!("{tag}: Connection handler is created, starting dispatcher");

        Dispatcher::new(io, codec, Pipeline::new(session, hnd), control)
            .keepalive_timeout(keepalive)
            .await
    }

    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        self.connect.ready(ctx.st()).await
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.connect.shutdown(ctx.st()).await;
    }
}
