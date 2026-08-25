use std::{error::Error, fmt, io, marker};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::cfg::{Configuration, SharedCfg};
use ntex_service::{Ctx, Middleware, Service, ServiceFactory};
use ntex_util::future::{Either, join, select};
use ntex_util::time::Deadline;

use crate::error::{DecodeError, DispatcherError, EncodeError, HandshakeError, MqttError};
use crate::version::{ProtocolVersion, VersionCodec};
use crate::{MqttServiceConfig, control::Control, service};

type Request<U> = <U as Decoder>::Item;
type Response<U> = Option<<U as Encoder>::Item>;

/// Mqtt Server
pub struct MqttServer<V3, V5, Err> {
    v3: V3,
    v5: V5,
    _t: marker::PhantomData<Err>,
}

impl<V3, V5, Err> fmt::Debug for MqttServer<V3, V5, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<Err> MqttServer<DefaultProtocolServer<Err>, DefaultProtocolServer<Err>, Err> {
    /// Create mqtt server
    pub fn new() -> Self {
        MqttServer {
            v3: DefaultProtocolServer::new(ProtocolVersion::MQTT3),
            v5: DefaultProtocolServer::new(ProtocolVersion::MQTT5),
            _t: marker::PhantomData,
        }
    }
}

impl<Err> Default for MqttServer<DefaultProtocolServer<Err>, DefaultProtocolServer<Err>, Err> {
    fn default() -> Self {
        MqttServer::new()
    }
}

impl<V3, V5, Err> MqttServer<V3, V5, Err>
where
    V3: Service<(), IoBoxed, Res = (), Error = MqttError<Err>>,
    V5: Service<(), IoBoxed, Res = (), Error = MqttError<Err>>,
{
    /// Service to handle v3 protocol
    pub fn v3<AppSt, Codec, E, T, M, C>(
        self,
        service: service::MqttServer<AppSt, Codec, Err, E, T, M, C>,
    ) -> MqttServer<service::MqttServer<AppSt, Codec, Err, E, T, M, C>, V5, Err>
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
        M::Service: Service<(), Request<Codec>, Res = Response<Codec>, Error = DispatcherError<E>>
            + 'static,
        C: ServiceFactory<
                (),
                Control<E>,
                AppSt,
                Res = Response<Codec>,
                Error = MqttError<Err>,
                InitError = Box<dyn Error>,
            > + 'static,
        Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError> + Clone + 'static,
    {
        MqttServer {
            v3: service,
            v5: self.v5,
            _t: marker::PhantomData,
        }
    }

    /// Service to handle v5 protocol
    pub fn v5<AppSt, Codec, E, T, M, C>(
        self,
        service: service::MqttServer<AppSt, Codec, Err, E, T, M, C>,
    ) -> MqttServer<V3, service::MqttServer<AppSt, Codec, Err, E, T, M, C>, Err>
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
        M::Service: Service<(), Request<Codec>, Res = Response<Codec>, Error = DispatcherError<E>>
            + 'static,
        C: ServiceFactory<
                (),
                Control<E>,
                AppSt,
                Res = Response<Codec>,
                Error = MqttError<Err>,
                InitError = Box<dyn Error>,
            > + 'static,
        Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError> + Clone + 'static,
    {
        MqttServer {
            v3: self.v3,
            v5: service,
            _t: marker::PhantomData,
        }
    }
}

impl<V3, V5, Err> Service<(), IoBoxed> for MqttServer<V3, V5, Err>
where
    V3: Service<(), IoBoxed, Res = (), Error = MqttError<Err>>,
    V5: Service<(), IoBoxed, Res = (), Error = MqttError<Err>>,
{
    type Res = ();
    type Error = MqttError<Err>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        let (ready1, ready2) = join(ctx.ready(&self.v3), ctx.ready(&self.v5)).await;
        ready1?;
        ready2
    }

    #[inline]
    async fn call(&self, io: IoBoxed, ctx: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
        // try to read Version, buffer may already contain info
        let res = io
            .decode(&VersionCodec)
            .map_err(|e| MqttError::Handshake(HandshakeError::Protocol(e.into())))?;
        if let Some(ver) = res {
            match ver {
                ProtocolVersion::MQTT3 => ctx.call(&self.v3, io).await,
                ProtocolVersion::MQTT5 => ctx.call(&self.v5, io).await,
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

            let cfg = io.cfg().ctx().get::<MqttServiceConfig>();

            match select(&mut Deadline::new(cfg.protocol_version_timeout), fut).await {
                Either::Left(()) => Err(MqttError::Handshake(HandshakeError::Timeout)),
                Either::Right(Ok(Some(ver))) => match ver {
                    ProtocolVersion::MQTT3 => ctx.call(&self.v3, io).await,
                    ProtocolVersion::MQTT5 => ctx.call(&self.v5, io).await,
                },
                Either::Right(Ok(None)) => {
                    Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                }
                Either::Right(Err(e)) => Err(e),
            }
        }
    }

    #[inline]
    async fn shutdown(&self, ctx: Ctx<'_, Self, ()>) {
        ctx.shutdown(&self.v3).await;
        ctx.shutdown(&self.v5).await;
    }
}

impl<F, V3, V5, Err> Service<(), Io<F>> for MqttServer<V3, V5, Err>
where
    F: Filter,
    V3: Service<(), IoBoxed, Res = (), Error = MqttError<Err>>,
    V5: Service<(), IoBoxed, Res = (), Error = MqttError<Err>>,
{
    type Res = ();
    type Error = MqttError<Err>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        Service::<(), IoBoxed>::ready(self, ctx).await
    }

    #[inline]
    async fn shutdown(&self, ctx: Ctx<'_, Self, ()>) {
        ctx.shutdown(&self.v3).await;
        ctx.shutdown(&self.v5).await;
    }

    #[inline]
    async fn call(&self, io: Io<F>, ctx: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
        Service::<(), IoBoxed>::call(self, IoBoxed::from(io), ctx).await
    }
}

pub struct DefaultProtocolServer<Err> {
    ver: ProtocolVersion,
    _t: marker::PhantomData<Err>,
}

impl<Err> fmt::Debug for DefaultProtocolServer<Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultProtocolServer")
            .field("ver", &self.ver)
            .finish()
    }
}

impl<Err> DefaultProtocolServer<Err> {
    fn new(ver: ProtocolVersion) -> Self {
        Self {
            ver,
            _t: marker::PhantomData,
        }
    }
}

impl<Err> Service<(), IoBoxed> for DefaultProtocolServer<Err> {
    type Res = ();
    type Error = MqttError<Err>;

    async fn call(&self, _: IoBoxed, _: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
        Err(MqttError::Handshake(HandshakeError::Disconnected(Some(
            io::Error::other(format!("Protocol is not supported: {:?}", self.ver)),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug() {
        // Use the default constructor which fills in all type params automatically
        let server = <MqttServer<_, _, ()>>::default();
        assert!(format!("{server:?}").contains("MqttServer"));
    }
}
