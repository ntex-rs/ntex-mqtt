use std::{error::Error, fmt, marker::PhantomData, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::{Ctx, Middleware, Service, ServiceFactory, cfg::SharedCfg};

use crate::error::{DecodeError, DispatcherError, EncodeError, MqttError};
use crate::{HandshakePipeline, control::Control, io::Dispatcher};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

pub struct MqttServer<AppSt, Codec, Err, E, T, M, C> {
    handshake: HandshakePipeline<AppSt, Codec, MqttError<Err>>,
    handler: Rc<T>,
    middleware: Rc<M>,
    control: Rc<C>,
    _t: PhantomData<E>,
}

impl<AppSt, Codec, Err, E, T, M, C> fmt::Debug for MqttServer<AppSt, Codec, Err, E, T, M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<AppSt, Codec, Err, E, T, M, C> MqttServer<AppSt, Codec, Err, E, T, M, C> {
    pub(crate) fn new(
        handshake: HandshakePipeline<AppSt, Codec, MqttError<Err>>,
        service: T,
        mw: M,
        control: C,
    ) -> Self {
        MqttServer {
            handshake,
            handler: Rc::new(service),
            middleware: Rc::new(mw),
            control: Rc::new(control),
            _t: PhantomData,
        }
    }
}

impl<AppSt, Codec, Err, E, T, M, C> Service<(), IoBoxed>
    for MqttServer<AppSt, Codec, Err, E, T, M, C>
where
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

    async fn call(&self, req: IoBoxed, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        let tag = req.tag();

        let (io, codec, session, keepalive) = self.handshake.call(req).await?;
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

        Dispatcher::new(io, codec, hnd, control)
            .keepalive_timeout(keepalive)
            .await
    }

    ntex_service::forward_pl_ready!((), handshake);
    ntex_service::forward_pl_shutdown!((), handshake);
}

impl<F, AppSt, Codec, Err, E, T, M, C> Service<(), Io<F>>
    for MqttServer<AppSt, Codec, Err, E, T, M, C>
where
    F: Filter,
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
    async fn call(&self, io: Io<F>, ctx: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        Service::<(), IoBoxed>::call(self, IoBoxed::from(io), ctx).await
    }

    ntex_service::forward_pl_ready!((), handshake);
    ntex_service::forward_pl_shutdown!((), handshake);
}
