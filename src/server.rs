use std::{fmt, io, marker};

use ntex_io::IoBoxed;
use ntex_service::state::{DefaultState, RequestState};
use ntex_service::{Ctx, IntoService, Service, cfg::Configuration};
use ntex_util::future::{Either, join, select};
use ntex_util::time::Deadline;

use crate::MqttServiceConfig;
use crate::error::{HandshakeError, MqttError};
use crate::version::{ProtocolVersion, VersionCodec};

/// Mqtt Server
pub struct MqttServer<St, Rst, V3, V5, Err> {
    rst: Rst,
    v3: V3,
    v5: V5,
    _t: marker::PhantomData<(St, Err)>,
}

impl<St, Rst, V3, V5, Err> fmt::Debug for MqttServer<St, Rst, V3, V5, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<St, Err>
    MqttServer<St, DefaultState<IoBoxed>, DefaultProtoSrv<Err>, DefaultProtoSrv<Err>, Err>
{
    /// Create mqtt server
    pub fn new() -> Self {
        MqttServer {
            rst: DefaultState::new(),
            v3: DefaultProtoSrv::new(ProtocolVersion::MQTT3),
            v5: DefaultProtoSrv::new(ProtocolVersion::MQTT5),
            _t: marker::PhantomData,
        }
    }
}

impl<St, Rst, Err> MqttServer<St, Rst, DefaultProtoSrv<Err>, DefaultProtoSrv<Err>, Err> {
    /// Create mqtt server
    pub fn with(rst: Rst) -> Self {
        MqttServer {
            rst,
            v3: DefaultProtoSrv::new(ProtocolVersion::MQTT3),
            v5: DefaultProtoSrv::new(ProtocolVersion::MQTT5),
            _t: marker::PhantomData,
        }
    }
}

impl<Err> Default
    for MqttServer<(), DefaultState<IoBoxed>, DefaultProtoSrv<Err>, DefaultProtoSrv<Err>, Err>
{
    fn default() -> Self {
        MqttServer::new()
    }
}

impl<St, Rst, V3, V5, Err> MqttServer<St, Rst, V3, V5, Err>
where
    Rst: RequestState<Res = IoBoxed>,
    V3: Service<St, (IoBoxed, Rst::State), Res = (), Error = MqttError<Err>>,
    V5: Service<St, (IoBoxed, Rst::State), Res = (), Error = MqttError<Err>>,
{
    /// Service to handle v3 protocol
    pub fn v3<S>(self, service: S) -> MqttServer<St, Rst, S, V5, Err>
    where
        S: Service<St, (IoBoxed, Rst::State), Res = (), Error = MqttError<Err>>,
    {
        MqttServer {
            v3: service.into_service(),
            v5: self.v5,
            rst: self.rst,
            _t: marker::PhantomData,
        }
    }

    /// Service to handle v5 protocol
    pub fn v5<S>(self, service: S) -> MqttServer<St, Rst, V3, S, Err>
    where
        S: Service<St, (IoBoxed, Rst::State), Res = (), Error = MqttError<Err>>,
    {
        MqttServer {
            v3: self.v3,
            v5: service.into_service(),
            rst: self.rst,
            _t: marker::PhantomData,
        }
    }
}

impl<St, Rst, V3, V5, Err> Service<St, Rst::Req> for MqttServer<St, Rst, V3, V5, Err>
where
    Rst: RequestState<Res = IoBoxed>,
    V3: Service<St, (IoBoxed, Rst::State), Res = (), Error = MqttError<Err>>,
    V5: Service<St, (IoBoxed, Rst::State), Res = (), Error = MqttError<Err>>,
{
    type Res = ();
    type Error = MqttError<Err>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let (ready1, ready2) = join(ctx.ready(&self.v3), ctx.ready(&self.v5)).await;
        ready1?;
        ready2
    }

    #[inline]
    async fn call(&self, req: Rst::Req, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        let (io, st) = self.rst.map(req);

        // try to read Version, buffer may already contain info
        let res = io
            .decode(&VersionCodec)
            .map_err(|e| MqttError::Handshake(HandshakeError::Protocol(e.into())))?;
        if let Some(ver) = res {
            match ver {
                ProtocolVersion::MQTT3 => ctx.call(&self.v3, (io, st)).await,
                ProtocolVersion::MQTT5 => ctx.call(&self.v5, (io, st)).await,
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
                    ProtocolVersion::MQTT3 => ctx.call(&self.v3, (io, st)).await,
                    ProtocolVersion::MQTT5 => ctx.call(&self.v5, (io, st)).await,
                },
                Either::Right(Ok(None)) => {
                    Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
                }
                Either::Right(Err(e)) => Err(e),
            }
        }
    }

    #[inline]
    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        ctx.shutdown(&self.v3).await;
        ctx.shutdown(&self.v5).await;
    }
}

// impl<F, St, Rst, V3, V5, Err> Service<St, Io<F>> for MqttServer<St, Rst, V3, V5, Err>
// where
//     F: Filter,
//     Rst: RequestState<Res = Io<F>>,
//     V3: Service<St, IoBoxed, Res = (), Error = MqttError<Err>>,
//     V5: Service<St, IoBoxed, Res = (), Error = MqttError<Err>>,
// {
//     type Res = ();
//     type Error = MqttError<Err>;

//     #[inline]
//     async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
//         ctx.ready::<_, IoBoxed>(self).await
//     }

//     #[inline]
//     async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
//         ctx.shutdown(&self.v3).await;
//         ctx.shutdown(&self.v5).await;
//     }

//     #[inline]
//     async fn call(&self, io: Io<F>, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
//         ctx.call::<_, IoBoxed>(self, IoBoxed::from(io)).await
//     }
// }

pub struct DefaultProtoSrv<Err> {
    ver: ProtocolVersion,
    _t: marker::PhantomData<Err>,
}

impl<Err> fmt::Debug for DefaultProtoSrv<Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultProtoSrv")
            .field("ver", &self.ver)
            .finish()
    }
}

impl<Err> DefaultProtoSrv<Err> {
    fn new(ver: ProtocolVersion) -> Self {
        Self {
            ver,
            _t: marker::PhantomData,
        }
    }
}

impl<St, Req, Err> Service<St, Req> for DefaultProtoSrv<Err> {
    type Res = ();
    type Error = MqttError<Err>;

    async fn call(&self, _: Req, _: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
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
        let server = <MqttServer<(), _, _, ()>>::default();
        assert!(format!("{server:?}").contains("MqttServer"));
    }
}
