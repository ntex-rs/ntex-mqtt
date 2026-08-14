use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::{Middleware, Pipeline, Service, ServiceCtx, ServiceFactory, cfg::SharedCfg};
use ntex_util::time::Seconds;

use crate::error::{DecodeError, DispatcherError, EncodeError};
use crate::{control::Control, io::Dispatcher};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

pub struct MqttServer<St, E, H, T, M, C, Codec> {
    handshake: H,
    handler: Rc<T>,
    middleware: Rc<M>,
    control: Rc<C>,
    _t: PhantomData<(St, E, Codec)>,
}

impl<St, E, H, T, M, C, Codec> fmt::Debug for MqttServer<St, E, H, T, M, C, Codec> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<St, E, H, T, M, C, Codec> MqttServer<St, E, H, T, M, C, Codec> {
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

impl<St, E, H, T, M, C, Codec> MqttServer<St, E, H, T, M, C, Codec>
where
    H: ServiceFactory<IoBoxed, SharedCfg, Response = (IoBoxed, Codec, St, Seconds), Data = ()>,
{
    async fn create_service(
        &self,
        cfg: SharedCfg,
    ) -> Result<MqttHandler<St, E, H::Service, T, M, C, Codec>, H::InitError> {
        let handshake_data = self.handshake.map_data(&cfg, &()).await?;
        let handshake = self.handshake.create(cfg.clone()).await?;

        // create connect service and then create service impl
        Ok(MqttHandler {
            cfg,
            handshake: Pipeline::new(handshake, handshake_data),
            handler: self.handler.clone(),
            middleware: self.middleware.clone(),
            control: self.control.clone(),
            _t: PhantomData,
        })
    }
}

impl<St, E, H, T, M, C, Codec> ServiceFactory<IoBoxed, SharedCfg>
    for MqttServer<St, E, H, T, M, C, Codec>
where
    St: Clone + 'static,
    E: 'static,
    H: ServiceFactory<IoBoxed, SharedCfg, Response = (IoBoxed, Codec, St, Seconds), Data = ()>
        + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = H::Error,
            Data = (),
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<
            Request<Codec>,
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            Data = <T::Service as Service<Request<Codec>>>::Data,
        > + 'static,
    C: ServiceFactory<
            Control<E>,
            St,
            Response = Response<Codec>,
            Error = H::Error,
            InitError = H::Error,
            Data = (),
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = H::Error;
    type InitError = H::InitError;
    type Service = MqttHandler<St, E, H::Service, T, M, C, Codec>;
    type Data = ();

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }

    async fn map_data(
        &self,
        _cfg: &SharedCfg,
        _data: &Self::Data,
    ) -> Result<<Self::Service as Service<IoBoxed>>::Data, Self::InitError> {
        Ok(())
    }
}

impl<F, St, E, H, T, M, C, Codec> ServiceFactory<Io<F>, SharedCfg>
    for MqttServer<St, E, H, T, M, C, Codec>
where
    F: Filter,
    St: Clone + 'static,
    E: 'static,
    H: ServiceFactory<IoBoxed, SharedCfg, Response = (IoBoxed, Codec, St, Seconds), Data = ()>
        + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = H::Error,
            Data = (),
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<
            Request<Codec>,
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            Data = <T::Service as Service<Request<Codec>>>::Data,
        > + 'static,
    C: ServiceFactory<
            Control<E>,
            St,
            Response = Response<Codec>,
            Error = H::Error,
            InitError = H::Error,
            Data = (),
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = H::Error;
    type InitError = H::InitError;
    type Service = MqttHandler<St, E, H::Service, T, M, C, Codec>;
    type Data = ();

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }

    async fn map_data(
        &self,
        _cfg: &SharedCfg,
        _data: &Self::Data,
    ) -> Result<<Self::Service as Service<Io<F>>>::Data, Self::InitError> {
        Ok(())
    }
}

pub struct MqttHandler<St, E, H, T, M, C, Codec>
where
    H: Service<IoBoxed>,
{
    handshake: Pipeline<H, H::Data>,
    handler: Rc<T>,
    middleware: Rc<M>,
    control: Rc<C>,
    cfg: SharedCfg,
    _t: PhantomData<(St, E, Codec)>,
}

impl<St, E, H, T, M, C, Codec> fmt::Debug for MqttHandler<St, E, H, T, M, C, Codec>
where
    H: Service<IoBoxed>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttHandler").finish()
    }
}

impl<St, E, H, T, M, C, Codec> Service<IoBoxed> for MqttHandler<St, E, H, T, M, C, Codec>
where
    St: Clone + 'static,
    E: 'static,
    H: Service<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = H::Error,
            Data = (),
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<
            Request<Codec>,
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            Data = <T::Service as Service<Request<Codec>>>::Data,
        > + 'static,
    C: ServiceFactory<
            Control<E>,
            St,
            Response = Response<Codec>,
            Error = H::Error,
            InitError = H::Error,
            Data = (),
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = H::Error;
    type Data = ();

    async fn ready(&self, _: &Self::Data, _: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        self.handshake.ready().await
    }

    fn poll(&self, _: &Self::Data, cx: &mut std::task::Context<'_>) -> Result<(), Self::Error> {
        self.handshake.poll(cx)
    }

    async fn shutdown(&self, _: &Self::Data) {
        self.handshake.shutdown().await;
    }

    async fn call(
        &self,
        req: IoBoxed,
        _data: &Self::Data,
        _ctx: ServiceCtx<'_, Self>,
    ) -> Result<(), Self::Error> {
        let tag = req.tag();
        let handshake = self.handshake.call(req).await;

        let (io, codec, session, keepalive) = handshake?;
        log::trace!("{tag}: Connection handshake succeeded");

        let control_data = self.control.map_data(&session, &()).await?;
        let control = self.control.create(session.clone()).await?;
        let handler_cfg = (self.cfg.clone(), session.clone());
        let handler_data = self.handler.map_data(&handler_cfg, &()).await?;
        let handler = self.handler.create(handler_cfg.clone()).await?;
        log::trace!("{tag}: Connection handler is created, starting dispatcher");

        Dispatcher::new(
            io,
            codec,
            self.middleware.create(handler, handler_cfg),
            handler_data,
            control,
            control_data,
        )
        .keepalive_timeout(keepalive)
        .await
    }
}

impl<F, St, E, H, T, M, C, Codec> Service<Io<F>> for MqttHandler<St, E, H, T, M, C, Codec>
where
    F: Filter,
    St: Clone + 'static,
    E: 'static,
    H: Service<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    T: ServiceFactory<
            Request<Codec>,
            (SharedCfg, St),
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = H::Error,
            Data = (),
        > + 'static,
    M: Middleware<T::Service, (SharedCfg, St)>,
    M::Service: Service<
            Request<Codec>,
            Response = Response<Codec>,
            Error = DispatcherError<E>,
            Data = <T::Service as Service<Request<Codec>>>::Data,
        > + 'static,
    C: ServiceFactory<
            Control<E>,
            St,
            Response = Response<Codec>,
            Error = H::Error,
            InitError = H::Error,
            Data = (),
        > + 'static,
    Codec: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
{
    type Response = ();
    type Error = H::Error;
    type Data = ();

    async fn ready(&self, _: &Self::Data, _: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        self.handshake.ready().await
    }

    fn poll(&self, _: &Self::Data, cx: &mut std::task::Context<'_>) -> Result<(), Self::Error> {
        self.handshake.poll(cx)
    }

    async fn shutdown(&self, _: &Self::Data) {
        self.handshake.shutdown().await;
    }

    #[inline]
    async fn call(
        &self,
        io: Io<F>,
        data: &Self::Data,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<(), Self::Error> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io), data, ctx).await
    }
}
