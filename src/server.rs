use std::{fmt, io, marker, task::Context};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::{Middleware, Service, ServiceCtx, ServiceFactory, cfg::Cfg, cfg::SharedCfg};
use ntex_util::future::{Either, join, select};
use ntex_util::time::{Deadline, Seconds};

use crate::error::{DecodeError, DispatcherError, EncodeError, HandshakeError, MqttError};
use crate::version::{ProtocolVersion, VersionCodec};
use crate::{MqttServiceConfig, control::Control, service};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

/// Mqtt Server
pub struct MqttServer<V3, V5, Err, InitErr> {
    svc_v3: V3,
    svc_v5: V5,
    _t: marker::PhantomData<(Err, InitErr)>,
}

impl<V3, V5, Err, InitErr> fmt::Debug for MqttServer<V3, V5, Err, InitErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<Err, InitErr>
    MqttServer<
        DefaultProtocolServer<Err, InitErr>,
        DefaultProtocolServer<Err, InitErr>,
        Err,
        InitErr,
    >
{
    /// Create mqtt server
    pub fn new() -> Self {
        MqttServer {
            svc_v3: DefaultProtocolServer::new(ProtocolVersion::MQTT3),
            svc_v5: DefaultProtocolServer::new(ProtocolVersion::MQTT5),
            _t: marker::PhantomData,
        }
    }
}

impl<Err, InitErr> Default
    for MqttServer<
        DefaultProtocolServer<Err, InitErr>,
        DefaultProtocolServer<Err, InitErr>,
        Err,
        InitErr,
    >
{
    fn default() -> Self {
        MqttServer::new()
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    Err: fmt::Debug,
    V3: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>,
    V5: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>,
{
    /// Service to handle v3 protocol
    pub fn v3<St, E, H, T, M, C, Codec>(
        self,
        service: service::MqttServer<St, E, Err, H, T, M, C, Codec>,
    ) -> MqttServer<
        impl ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        >,
        V5,
        Err,
        InitErr,
    >
    where
        St: Clone + 'static,
        E: 'static,
        H: ServiceFactory<
                IoBoxed,
                SharedCfg,
                Response = (IoBoxed, Codec, St, Seconds),
                Error = MqttError<Err>,
                InitError = InitErr,
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
        C: ServiceFactory<
                Control<E>,
                St,
                Response = Response<Codec>,
                Error = Err,
                InitError = Err,
            > + 'static,
        Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError> + Clone + 'static,
    {
        MqttServer { svc_v3: service, svc_v5: self.svc_v5, _t: marker::PhantomData }
    }

    /// Service to handle v5 protocol
    pub fn v5<St, E, H, T, M, C, Codec>(
        self,
        service: service::MqttServer<St, E, Err, H, T, M, C, Codec>,
    ) -> MqttServer<
        V3,
        impl ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        >,
        Err,
        InitErr,
    >
    where
        St: Clone + 'static,
        E: fmt::Debug + 'static,
        H: ServiceFactory<
                IoBoxed,
                SharedCfg,
                Response = (IoBoxed, Codec, St, Seconds),
                Error = MqttError<Err>,
                InitError = InitErr,
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
        C: ServiceFactory<
                Control<E>,
                St,
                Response = Response<Codec>,
                Error = Err,
                InitError = Err,
            > + 'static,
        Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError> + Clone + 'static,
    {
        MqttServer { svc_v3: self.svc_v3, svc_v5: service, _t: marker::PhantomData }
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>,
    V5: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>,
{
    async fn create_service(
        &self,
        cfg: SharedCfg,
    ) -> Result<MqttServerImpl<V3::Service, V5::Service, Err>, InitErr> {
        let (v3, v5) =
            join(self.svc_v3.create(cfg.clone()), self.svc_v5.create(cfg.clone())).await;
        let v3 = v3?;
        let v5 = v5?;
        Ok(MqttServerImpl { handlers: (v3, v5), cfg: cfg.get(), _t: marker::PhantomData })
    }
}

impl<V3, V5, Err, InitErr> ServiceFactory<IoBoxed, SharedCfg>
    for MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>
        + 'static,
    V5: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>
        + 'static,
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }
}

impl<F, V3, V5, Err, InitErr> ServiceFactory<Io<F>, SharedCfg>
    for MqttServer<V3, V5, Err, InitErr>
where
    F: Filter,
    V3: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>
        + 'static,
    V5: ServiceFactory<IoBoxed, SharedCfg, Response = (), Error = Err, InitError = InitErr>
        + 'static,
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }
}

/// Mqtt Server
pub struct MqttServerImpl<V3, V5, Err> {
    handlers: (V3, V5),
    cfg: Cfg<MqttServiceConfig>,
    _t: marker::PhantomData<Err>,
}

impl<V3, V5, Err> fmt::Debug for MqttServerImpl<V3, V5, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServerImpl").finish()
    }
}

impl<V3, V5, Err> Service<IoBoxed> for MqttServerImpl<V3, V5, Err>
where
    V3: Service<IoBoxed, Response = (), Error = Err>,
    V5: Service<IoBoxed, Response = (), Error = Err>,
{
    type Response = ();
    type Error = MqttError<Err>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (ready1, ready2) =
            join(ctx.ready(&self.handlers.0), ctx.ready(&self.handlers.1)).await;
        ready1.map_err(MqttError::Service)?;
        ready2.map_err(MqttError::Service)
    }

    #[inline]
    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        self.handlers.0.poll(cx).map_err(MqttError::Service)?;
        self.handlers.1.poll(cx).map_err(MqttError::Service)
    }

    #[inline]
    async fn shutdown(&self) {
        self.handlers.0.shutdown().await;
        self.handlers.1.shutdown().await;
    }

    #[inline]
    async fn call(
        &self,
        io: IoBoxed,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        // try to read Version, buffer may already contain info
        let res = io
            .decode(&VersionCodec)
            .map_err(|e| MqttError::Handshake(HandshakeError::Protocol(e.into())))?;
        if let Some(ver) = res {
            match ver {
                ProtocolVersion::MQTT3 => {
                    ctx.call(&self.handlers.0, io).await.map_err(MqttError::Service)
                }
                ProtocolVersion::MQTT5 => {
                    ctx.call(&self.handlers.1, io).await.map_err(MqttError::Service)
                }
            }
        } else {
            let fut = async {
                match io.recv(&VersionCodec).await {
                    Ok(ver) => Ok(ver),
                    Err(Either::Left(e)) => {
                        Err(MqttError::Handshake(HandshakeError::Protocol(e.into())))
                    }
                    Err(Either::Right(e)) => {
                        Err(MqttError::Handshake(HandshakeError::Disconnected(Some(e))))
                    }
                }
            };

            match select(&mut Deadline::new(self.cfg.protocol_version_timeout), fut).await {
                Either::Left(()) => Err(MqttError::Handshake(HandshakeError::Timeout)),
                Either::Right(Ok(Some(ver))) => match ver {
                    ProtocolVersion::MQTT3 => {
                        ctx.call(&self.handlers.0, io).await.map_err(MqttError::Service)
                    }
                    ProtocolVersion::MQTT5 => {
                        ctx.call(&self.handlers.1, io).await.map_err(MqttError::Service)
                    }
                },
                Either::Right(Ok(None)) => {
                    Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                }
                Either::Right(Err(e)) => Err(e),
            }
        }
    }
}

impl<F, V3, V5, Err> Service<Io<F>> for MqttServerImpl<V3, V5, Err>
where
    F: Filter,
    V3: Service<IoBoxed, Response = (), Error = Err>,
    V5: Service<IoBoxed, Response = (), Error = Err>,
{
    type Response = ();
    type Error = MqttError<Err>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        Service::<IoBoxed>::ready(self, ctx).await
    }

    #[inline]
    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        Service::<IoBoxed>::poll(self, cx)
    }

    #[inline]
    async fn shutdown(&self) {
        Service::<IoBoxed>::shutdown(self).await;
    }

    #[inline]
    async fn call(
        &self,
        io: Io<F>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io), ctx).await
    }
}

pub struct DefaultProtocolServer<Err, InitErr> {
    ver: ProtocolVersion,
    _t: marker::PhantomData<(Err, InitErr)>,
}

impl<Err, InitErr> fmt::Debug for DefaultProtocolServer<Err, InitErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultProtocolServer").field("ver", &self.ver).finish()
    }
}

impl<Err, InitErr> DefaultProtocolServer<Err, InitErr> {
    fn new(ver: ProtocolVersion) -> Self {
        Self { ver, _t: marker::PhantomData }
    }
}

impl<Err, InitErr> ServiceFactory<IoBoxed, SharedCfg> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;
    type Service = DefaultProtocolServer<Err, InitErr>;
    type InitError = InitErr;

    async fn create(&self, _: SharedCfg) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultProtocolServer { ver: self.ver, _t: marker::PhantomData })
    }
}

impl<Err, InitErr> Service<IoBoxed> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;

    async fn call(
        &self,
        _: IoBoxed,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        Err(MqttError::Handshake(HandshakeError::Disconnected(Some(io::Error::other(
            format!("Protocol is not supported: {:?}", self.ver),
        )))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug() {
        // Use the default constructor which fills in all type params automatically
        let server = <MqttServer<_, _, (), ()>>::default();
        assert!(format!("{server:?}").contains("MqttServer"));
    }
}
