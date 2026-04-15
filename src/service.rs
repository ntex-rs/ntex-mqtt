use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::{Middleware, Service, ServiceCtx, ServiceFactory, cfg::SharedCfg};
use ntex_util::time::Seconds;

use crate::error::{DecodeError, DispatcherError, EncodeError, MqttError};
use crate::{control::Control, io::Dispatcher};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

pub struct MqttServer<St, E, Err, H, T, M, C, Codec> {
    handshake: H,
    handler: Rc<T>,
    middleware: Rc<M>,
    control: Rc<C>,
    _t: PhantomData<(St, E, Err, Codec)>,
}

impl<St, E, Err, H, T, M, C, Codec> fmt::Debug for MqttServer<St, E, Err, H, T, M, C, Codec> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<St, E, Err, H, T, M, C, Codec> MqttServer<St, E, Err, H, T, M, C, Codec> {
    pub(crate) fn new(handshake: H, service: T, mw: M, control: C) -> Self {
        MqttServer {
            handshake,
            handler: Rc::new(service),
            middleware: Rc::new(mw),
            control: Rc::new(control),
            _t: PhantomData,
        }
    }
}

impl<St, E, Err, H, T, M, C, Codec> MqttServer<St, E, Err, H, T, M, C, Codec>
where
    H: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (IoBoxed, Codec, St, Seconds),
            Error = MqttError<Err>,
        >,
{
    async fn create_service(
        &self,
        cfg: SharedCfg,
    ) -> Result<MqttHandler<St, E, Err, H::Service, T, M, C, Codec>, H::InitError> {
        let handshake = self.handshake.create(cfg.clone()).await?;

        // create connect service and then create service impl
        Ok(MqttHandler {
            cfg,
            handshake,
            handler: self.handler.clone(),
            middleware: self.middleware.clone(),
            control: self.control.clone(),
            _t: PhantomData,
        })
    }
}

impl<St, E, Err, H, T, M, C, Codec> ServiceFactory<IoBoxed, SharedCfg>
    for MqttServer<St, E, Err, H, T, M, C, Codec>
where
    St: Clone + 'static,
    E: 'static,
    H: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (IoBoxed, Codec, St, Seconds),
            Error = MqttError<Err>,
        > + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Err,
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<Request<Codec>, Response = Response<Codec>, Error = DispatcherError<E>>
        + 'static,
    C: ServiceFactory<Control<E>, St, Response = Response<Codec>, Error = Err, InitError = Err>
        + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type InitError = H::InitError;
    type Service = MqttHandler<St, E, Err, H::Service, T, M, C, Codec>;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }
}

impl<F, St, E, Err, H, T, M, C, Codec> ServiceFactory<Io<F>, SharedCfg>
    for MqttServer<St, E, Err, H, T, M, C, Codec>
where
    F: Filter,
    St: Clone + 'static,
    E: 'static,
    H: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (IoBoxed, Codec, St, Seconds),
            Error = MqttError<Err>,
        > + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Err,
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<Request<Codec>, Response = Response<Codec>, Error = DispatcherError<E>>
        + 'static,
    C: ServiceFactory<Control<E>, St, Response = Response<Codec>, Error = Err, InitError = Err>
        + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type InitError = H::InitError;
    type Service = MqttHandler<St, E, Err, H::Service, T, M, C, Codec>;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }
}

pub struct MqttHandler<St, E, Err, H, T, M, C, Codec> {
    handshake: H,
    handler: Rc<T>,
    middleware: Rc<M>,
    control: Rc<C>,
    cfg: SharedCfg,
    _t: PhantomData<(St, E, Err, Codec)>,
}

impl<St, E, Err, H, T, M, C, Codec> fmt::Debug for MqttHandler<St, E, Err, H, T, M, C, Codec> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttHandler").finish()
    }
}

impl<St, E, Err, H, T, M, C, Codec> Service<IoBoxed>
    for MqttHandler<St, E, Err, H, T, M, C, Codec>
where
    St: Clone + 'static,
    E: 'static,
    H: Service<IoBoxed, Response = (IoBoxed, Codec, St, Seconds), Error = MqttError<Err>>
        + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Err,
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<Request<Codec>, Response = Response<Codec>, Error = DispatcherError<E>>
        + 'static,
    C: ServiceFactory<Control<E>, St, Response = Response<Codec>, Error = Err, InitError = Err>
        + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = MqttError<Err>;

    ntex_service::forward_ready!(handshake);
    ntex_service::forward_poll!(handshake);
    ntex_service::forward_shutdown!(handshake);

    async fn call(&self, req: IoBoxed, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let tag = req.tag();
        let handshake = ctx.call(&self.handshake, req).await;

        let (io, codec, session, keepalive) = handshake.map_err(|e| {
            // log::trace!("{tag}: Connection handshake failed: {e:?}");
            e
        })?;
        log::trace!("{tag}: Connection handshake succeeded");

        let control = self.control.create(session.clone()).await.map_err(MqttError::Service)?;
        let handler = self
            .handler
            .create((self.cfg.clone(), session.clone()))
            .await
            .map_err(MqttError::Service)?;
        log::trace!("{tag}: Connection handler is created, starting dispatcher");

        Dispatcher::new(
            io,
            codec,
            self.middleware.create(handler, (self.cfg.clone(), session)),
            control,
        )
        .keepalive_timeout(keepalive)
        .await
        .map_err(MqttError::Service)
    }
}

impl<F, St, E, Err, H, T, M, C, Codec> Service<Io<F>>
    for MqttHandler<St, E, Err, H, T, M, C, Codec>
where
    F: Filter,
    St: Clone + 'static,
    E: 'static,
    H: Service<IoBoxed, Response = (IoBoxed, Codec, St, Seconds), Error = MqttError<Err>>
        + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Err,
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<Request<Codec>, Response = Response<Codec>, Error = DispatcherError<E>>
        + 'static,
    C: ServiceFactory<Control<E>, St, Response = Response<Codec>, Error = Err, InitError = Err>
        + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = MqttError<Err>;

    ntex_service::forward_ready!(handshake);
    ntex_service::forward_poll!(handshake);
    ntex_service::forward_shutdown!(handshake);

    #[inline]
    async fn call(&self, io: Io<F>, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io), ctx).await
    }
}
