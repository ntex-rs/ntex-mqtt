use std::{fmt, marker::PhantomData, rc::Rc};

use ntex::codec::{Decoder, Encoder};
use ntex::io::{DispatchItem, DispatcherConfig, Filter, Io, IoBoxed};
use ntex::service::{Service, ServiceCtx, ServiceFactory};
use ntex::time::{Deadline, Seconds};
use ntex::util::{select, Either};

use crate::io::Dispatcher;

type ResponseItem<U> = Option<<U as Encoder>::Item>;

pub struct MqttServer<St, C, T, Codec> {
    connect: C,
    handler: Rc<T>,
    config: DispatcherConfig,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, Codec> MqttServer<St, C, T, Codec> {
    pub(crate) fn new(connect: C, service: T, config: DispatcherConfig) -> Self {
        MqttServer { connect, config, handler: Rc::new(service), _t: PhantomData }
    }
}

impl<St, C, T, Codec> MqttServer<St, C, T, Codec>
where
    C: ServiceFactory<IoBoxed, Response = (IoBoxed, Codec, St, Seconds)>,
{
    async fn create_service(
        &self,
    ) -> Result<MqttHandler<St, C::Service, T, Codec>, C::InitError> {
        // create connect service and then create service impl
        Ok(MqttHandler {
            config: self.config.clone(),
            handler: self.handler.clone(),
            connect: self.connect.create(()).await?,
            _t: PhantomData,
        })
    }
}

impl<St, C, T, Codec> ServiceFactory<IoBoxed> for MqttServer<St, C, T, Codec>
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
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = MqttHandler<St, C::Service, T, Codec>;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.create_service().await
    }
}

impl<F, St, C, T, Codec> ServiceFactory<Io<F>> for MqttServer<St, C, T, Codec>
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
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = MqttHandler<St, C::Service, T, Codec>;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.create_service().await
    }
}

impl<St, C, T, Codec> ServiceFactory<(IoBoxed, Deadline)> for MqttServer<St, C, T, Codec>
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
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;
    type InitError = C::InitError;
    type Service = MqttHandler<St, C::Service, T, Codec>;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.create_service().await
    }
}

pub struct MqttHandler<St, C, T, Codec> {
    connect: C,
    handler: Rc<T>,
    config: DispatcherConfig,
    _t: PhantomData<(St, Codec)>,
}

impl<St, C, T, Codec> Service<IoBoxed> for MqttHandler<St, C, T, Codec>
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
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;

    ntex::forward_poll_ready!(connect);
    ntex::forward_poll_shutdown!(connect);

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

        Dispatcher::new(io, codec, handler, &self.config).keepalive_timeout(keepalive).await
    }
}

impl<F, St, C, T, Codec> Service<Io<F>> for MqttHandler<St, C, T, Codec>
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
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;

    ntex::forward_poll_ready!(connect);
    ntex::forward_poll_shutdown!(connect);

    #[inline]
    async fn call(&self, io: Io<F>, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io), ctx).await
    }
}

impl<St, C, T, Codec> Service<(IoBoxed, Deadline)> for MqttHandler<St, C, T, Codec>
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
    Codec: Decoder + Encoder + Clone + 'static,
{
    type Response = ();
    type Error = C::Error;

    ntex::forward_poll_ready!(connect);
    ntex::forward_poll_shutdown!(connect);

    async fn call(
        &self,
        (io, delay): (IoBoxed, Deadline),
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<(), Self::Error> {
        let tag = io.tag();
        let (io, codec, ka, handler) = {
            let res = select(
                delay,
                Box::pin(async {
                    let (io, codec, st, ka) =
                        ctx.call(&self.connect, io).await.map_err(|e| {
                            log::trace!("{}: Connection handshake failed: {:?}", tag, e);
                            e
                        })?;
                    log::trace!("{}: Connection handshake succeeded", tag);

                    let handler = self.handler.create(st).await?;
                    log::trace!("{}: Connection handler is created, starting dispatcher", tag);

                    Ok::<_, C::Error>((io, codec, ka, handler))
                }),
            )
            .await;

            match res {
                Either::Left(_) => {
                    log::warn!("{}: Handshake timed out", tag);
                    return Ok(());
                }
                Either::Right(item) => item?,
            }
        };

        Dispatcher::new(io, codec, handler, &self.config).keepalive_timeout(ka).await
    }
}
