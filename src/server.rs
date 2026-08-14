use std::{fmt, io, marker, task::Context};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::{
    Middleware, Pipeline, Service, ServiceCtx, ServiceFactory, cfg::Cfg, cfg::SharedCfg,
};
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
    V3: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        >,
    V5: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        >,
{
    /// Service to handle v3 protocol
    pub fn v3<St, E, H, T, M, C, Codec>(
        self,
        service: service::MqttServer<St, E, H, T, M, C, Codec>,
    ) -> MqttServer<
        impl ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
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
                Data = (),
            > + 'static,
        T: ServiceFactory<
                Request<Codec>,
                (SharedCfg, St),
                Response = Response<Codec>,
                Error = DispatcherError<E>,
                InitError = MqttError<Err>,
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
                Error = MqttError<Err>,
                InitError = MqttError<Err>,
                Data = (),
            > + 'static,
        Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError> + Clone + 'static,
    {
        MqttServer { svc_v3: service, svc_v5: self.svc_v5, _t: marker::PhantomData }
    }

    /// Service to handle v5 protocol
    pub fn v5<St, E, H, T, M, C, Codec>(
        self,
        service: service::MqttServer<St, E, H, T, M, C, Codec>,
    ) -> MqttServer<
        V3,
        impl ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        >,
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
                Data = (),
            > + 'static,
        T: ServiceFactory<
                Request<Codec>,
                (SharedCfg, St),
                Response = Response<Codec>,
                Error = DispatcherError<E>,
                InitError = MqttError<Err>,
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
                Error = MqttError<Err>,
                InitError = MqttError<Err>,
                Data = (),
            > + 'static,
        Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError> + Clone + 'static,
    {
        MqttServer { svc_v3: self.svc_v3, svc_v5: service, _t: marker::PhantomData }
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        >,
    V5: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        >,
{
    async fn create_service(
        &self,
        cfg: SharedCfg,
    ) -> Result<MqttServerImpl<V3::Service, V5::Service, Err>, InitErr> {
        let (v3_data, v5_data) =
            join(self.svc_v3.map_data(&cfg, &()), self.svc_v5.map_data(&cfg, &())).await;
        let (v3, v5) =
            join(self.svc_v3.create(cfg.clone()), self.svc_v5.create(cfg.clone())).await;
        let v3_data = v3_data?;
        let v5_data = v5_data?;
        let v3 = v3?;
        let v5 = v5?;
        Ok(MqttServerImpl {
            handlers: (Pipeline::new(v3, v3_data), Pipeline::new(v5, v5_data)),
            cfg: cfg.get(),
            _t: marker::PhantomData,
        })
    }
}

impl<V3, V5, Err, InitErr> ServiceFactory<IoBoxed, SharedCfg>
    for MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        > + 'static,
    V5: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        > + 'static,
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;
    type Data = ();

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }

    async fn map_data(&self, _: &SharedCfg, _: &Self::Data) -> Result<(), Self::InitError> {
        Ok(())
    }
}

impl<F, V3, V5, Err, InitErr> ServiceFactory<Io<F>, SharedCfg>
    for MqttServer<V3, V5, Err, InitErr>
where
    F: Filter,
    V3: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        > + 'static,
    V5: ServiceFactory<
            IoBoxed,
            SharedCfg,
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
            Data = (),
        > + 'static,
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;
    type Data = ();

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        self.create_service(cfg).await
    }

    async fn map_data(&self, _: &SharedCfg, _: &Self::Data) -> Result<(), Self::InitError> {
        Ok(())
    }
}

/// Mqtt Server
pub struct MqttServerImpl<V3, V5, Err>
where
    V3: Service<IoBoxed>,
    V5: Service<IoBoxed>,
{
    handlers: (Pipeline<V3, V3::Data>, Pipeline<V5, V5::Data>),
    cfg: Cfg<MqttServiceConfig>,
    _t: marker::PhantomData<Err>,
}

impl<V3, V5, Err> fmt::Debug for MqttServerImpl<V3, V5, Err>
where
    V3: Service<IoBoxed>,
    V5: Service<IoBoxed>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServerImpl").finish()
    }
}

impl<V3, V5, Err> Service<IoBoxed> for MqttServerImpl<V3, V5, Err>
where
    V3: Service<IoBoxed, Response = (), Error = MqttError<Err>>,
    V5: Service<IoBoxed, Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Data = ();

    #[inline]
    async fn ready(&self, _: &Self::Data, _: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (ready1, ready2) = join(self.handlers.0.ready(), self.handlers.1.ready()).await;
        ready1?;
        ready2
    }

    #[inline]
    fn poll(&self, _: &Self::Data, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        self.handlers.0.poll(cx)?;
        self.handlers.1.poll(cx)
    }

    #[inline]
    async fn shutdown(&self, _: &Self::Data) {
        self.handlers.0.shutdown().await;
        self.handlers.1.shutdown().await;
    }

    #[inline]
    async fn call(
        &self,
        io: IoBoxed,
        _: &Self::Data,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        // try to read Version, buffer may already contain info
        let res = io
            .decode(&VersionCodec)
            .map_err(|e| MqttError::Handshake(HandshakeError::Protocol(e.into())))?;
        if let Some(ver) = res {
            match ver {
                ProtocolVersion::MQTT3 => self.handlers.0.call(io).await,
                ProtocolVersion::MQTT5 => self.handlers.1.call(io).await,
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
                    ProtocolVersion::MQTT3 => self.handlers.0.call(io).await,
                    ProtocolVersion::MQTT5 => self.handlers.1.call(io).await,
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
    V3: Service<IoBoxed, Response = (), Error = MqttError<Err>>,
    V5: Service<IoBoxed, Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Data = ();

    #[inline]
    async fn ready(
        &self,
        data: &Self::Data,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<(), Self::Error> {
        Service::<IoBoxed>::ready(self, data, ctx).await
    }

    #[inline]
    fn poll(&self, data: &Self::Data, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        Service::<IoBoxed>::poll(self, data, cx)
    }

    #[inline]
    async fn shutdown(&self, data: &Self::Data) {
        Service::<IoBoxed>::shutdown(self, data).await;
    }

    #[inline]
    async fn call(
        &self,
        io: Io<F>,
        data: &Self::Data,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io), data, ctx).await
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
    type Data = ();

    async fn create(&self, _: SharedCfg) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultProtocolServer { ver: self.ver, _t: marker::PhantomData })
    }

    async fn map_data(&self, _: &SharedCfg, _: &Self::Data) -> Result<(), Self::InitError> {
        Ok(())
    }
}

impl<Err, InitErr> Service<IoBoxed> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;
    type Data = ();

    async fn call(
        &self,
        _: IoBoxed,
        _: &Self::Data,
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
