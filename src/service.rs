use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{DispatchItem, DispatcherConfig, Filter, Io, IoBoxed};
use ntex_service::{Middleware, Service, ServiceCtx, ServiceFactory};
use ntex_util::time::Seconds;

use crate::io::Dispatcher;

type ResponseItem<U> = Option<<U as Encoder>::Item>;

pub struct MqttServer<St, C, T, M, Codec> {
    connect: C,
    handler: Rc<T>,
    middleware: Rc<M>,
    config: DispatcherConfig,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, M, Codec> MqttServer<St, C, T, M, Codec> {
    pub(crate) fn new(connect: C, service: T, mw: M, config: DispatcherConfig) -> Self {
        MqttServer {
            connect,
            config,
            handler: Rc::new(service),
            middleware: Rc::new(mw),
            _t: PhantomData,
        }
    }
}

impl<St, C, T, M, Codec> MqttServer<St, C, T, M, Codec>
where
    C: ServiceFactory<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)>,
{
    async fn create_service(
        &self,
    ) -> Result<MqttHandler<St, C::Service, T, M, Codec>, C::InitError> {
        // create connect service and then create service impl
        Ok(MqttHandler {
            config: self.config.clone(),
            handler: self.handler.clone(),
            connect: self.connect.create(()).await?,
            middleware: self.middleware.clone(),
            _t: PhantomData,
        })
    }
}

impl<St, C, T, M, Codec> ServiceFactory<IoBoxed> for MqttServer<St, C, T, M, Codec>
where
    St: 'static,
    C: ServiceFactory<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Codec>,
            St,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    M: Middleware<T::Service>,
    M::Service: Service<DispatchItem<Codec>, Response = ResponseItem<Codec>, Error = C::Error>
        + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = MqttHandler<St, C::Service, T, M, Codec>;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.create_service().await
    }
}

impl<F, St, C, T, M, Codec> ServiceFactory<Io<F>> for MqttServer<St, C, T, M, Codec>
where
    F: Filter,
    St: 'static,
    C: ServiceFactory<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Codec>,
            St,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    M: Middleware<T::Service>,
    M::Service: Service<DispatchItem<Codec>, Response = ResponseItem<Codec>, Error = C::Error>
        + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = MqttHandler<St, C::Service, T, M, Codec>;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.create_service().await
    }
}

pub struct MqttHandler<St, C, T, M, Codec> {
    connect: C,
    handler: Rc<T>,
    middleware: Rc<M>,
    config: DispatcherConfig,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, M, Codec> Service<IoBoxed> for MqttHandler<St, C, T, M, Codec>
where
    St: 'static,
    C: Service<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Codec>,
            St,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    M: Middleware<T::Service>,
    M::Service: Service<DispatchItem<Codec>, Response = ResponseItem<Codec>, Error = C::Error>
        + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;

    ntex_service::forward_ready!(connect);
    ntex_service::forward_shutdown!(connect);

    async fn call(&self, req: IoBoxed, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let tag = req.tag();
        let handshake = ctx.call(&self.connect, req).await;

        let (io, codec, session, keepalive) = handshake.map_err(|e| {
            log::trace!("{}: Connection handshake failed: {:?}", tag, e);
            e
        })?;
        log::trace!("{}: Connection handshake succeeded", tag);

        let handler = self.handler.create(session).await?;
        log::trace!("{}: Connection handler is created, starting dispatcher", tag);

        Dispatcher::new(io, codec, self.middleware.create(handler), &self.config)
            .keepalive_timeout(keepalive)
            .await
    }
}

impl<F, St, C, T, M, Codec> Service<Io<F>> for MqttHandler<St, C, T, M, Codec>
where
    F: Filter,
    St: 'static,
    C: Service<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)> + 'static,
    C::Error: fmt::Debug,
    T: ServiceFactory<
            DispatchItem<Codec>,
            St,
            Response = ResponseItem<Codec>,
            Error = C::Error,
            InitError = C::Error,
        > + 'static,
    M: Middleware<T::Service>,
    M::Service: Service<DispatchItem<Codec>, Response = ResponseItem<Codec>, Error = C::Error>
        + 'static,
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;

    ntex_service::forward_ready!(connect);
    ntex_service::forward_shutdown!(connect);

    #[inline]
    async fn call(&self, io: Io<F>, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io), ctx).await
    }
}
