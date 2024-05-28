use std::{fmt, io, marker};

use ntex_io::{Filter, Io, IoBoxed};
use ntex_service::{Service, ServiceCtx, ServiceFactory};
use ntex_util::future::{join, select, Either};
use ntex_util::time::{Deadline, Millis, Seconds};

use crate::version::{ProtocolVersion, VersionCodec};
use crate::{error::HandshakeError, error::MqttError, v3, v5};

/// Mqtt Server
pub struct MqttServer<V3, V5, Err, InitErr> {
    v3: V3,
    v5: V5,
    connect_timeout: Millis,
    _t: marker::PhantomData<(Err, InitErr)>,
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
            v3: DefaultProtocolServer::new(ProtocolVersion::MQTT3),
            v5: DefaultProtocolServer::new(ProtocolVersion::MQTT5),
            connect_timeout: Millis(5_000),
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

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr> {
    /// Set client timeout reading protocol version.
    ///
    /// Defines a timeout for reading protocol version. If a client does not transmit
    /// version of the protocol within this time, the connection is terminated with
    /// Mqtt::Handshake(HandshakeError::Timeout) error.
    ///
    /// By default, timeuot is 5 seconds.
    pub fn protocol_version_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout.into();
        self
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>,
    V5: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>,
{
    /// Service to handle v3 protocol
    pub fn v3<St, C, Cn, P>(
        self,
        service: v3::MqttServer<St, C, Cn, P>,
    ) -> MqttServer<
        impl ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>,
        V5,
        Err,
        InitErr,
    >
    where
        St: 'static,
        C: ServiceFactory<
                v3::Handshake,
                Response = v3::HandshakeAck<St>,
                Error = Err,
                InitError = InitErr,
            > + 'static,
        Cn: ServiceFactory<v3::Control<Err>, v3::Session<St>, Response = v3::ControlAck>
            + 'static,
        P: ServiceFactory<v3::Publish, v3::Session<St>, Response = ()> + 'static,
        C::Error: From<Cn::Error>
            + From<Cn::InitError>
            + From<P::Error>
            + From<P::InitError>
            + fmt::Debug,
    {
        MqttServer {
            v3: service.finish(),
            v5: self.v5,
            connect_timeout: self.connect_timeout,
            _t: marker::PhantomData,
        }
    }

    /// Service to handle v5 protocol
    pub fn v5<St, C, Cn, P>(
        self,
        service: v5::MqttServer<St, C, Cn, P>,
    ) -> MqttServer<
        V3,
        impl ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>,
        Err,
        InitErr,
    >
    where
        St: 'static,
        C: ServiceFactory<
                v5::Handshake,
                Response = v5::HandshakeAck<St>,
                Error = Err,
                InitError = InitErr,
            > + 'static,
        Cn: ServiceFactory<v5::Control<Err>, v5::Session<St>, Response = v5::ControlAck>
            + 'static,
        P: ServiceFactory<v5::Publish, v5::Session<St>, Response = v5::PublishAck> + 'static,
        P::Error: fmt::Debug,
        C::Error: From<Cn::Error>
            + From<Cn::InitError>
            + From<P::Error>
            + From<P::InitError>
            + fmt::Debug,
        v5::PublishAck: TryFrom<P::Error, Error = C::Error>,
    {
        MqttServer {
            v3: self.v3,
            v5: service.finish(),
            connect_timeout: self.connect_timeout,
            _t: marker::PhantomData,
        }
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>,
    V5: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>,
{
    async fn create_service(
        &self,
    ) -> Result<MqttServerImpl<V3::Service, V5::Service, Err>, InitErr> {
        let (v3, v5) = join(self.v3.create(()), self.v5.create(())).await;
        let v3 = v3?;
        let v5 = v5?;
        Ok(MqttServerImpl {
            handlers: (v3, v5),
            connect_timeout: self.connect_timeout,
            _t: marker::PhantomData,
        })
    }
}

impl<V3, V5, Err, InitErr> ServiceFactory<IoBoxed> for MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>
        + 'static,
    V5: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>
        + 'static,
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.create_service().await
    }
}

impl<F, V3, V5, Err, InitErr> ServiceFactory<Io<F>> for MqttServer<V3, V5, Err, InitErr>
where
    F: Filter,
    V3: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>
        + 'static,
    V5: ServiceFactory<IoBoxed, Response = (), Error = MqttError<Err>, InitError = InitErr>
        + 'static,
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        self.create_service().await
    }
}

/// Mqtt Server
pub struct MqttServerImpl<V3, V5, Err> {
    handlers: (V3, V5),
    connect_timeout: Millis,
    _t: marker::PhantomData<Err>,
}

impl<V3, V5, Err> Service<IoBoxed> for MqttServerImpl<V3, V5, Err>
where
    V3: Service<IoBoxed, Response = (), Error = MqttError<Err>>,
    V5: Service<IoBoxed, Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (ready1, ready2) =
            join(ctx.ready(&self.handlers.0), ctx.ready(&self.handlers.1)).await;
        ready1?;
        ready2
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
                ProtocolVersion::MQTT3 => ctx.call(&self.handlers.0, io).await,
                ProtocolVersion::MQTT5 => ctx.call(&self.handlers.1, io).await,
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

            match select(&mut Deadline::new(self.connect_timeout), fut).await {
                Either::Left(_) => Err(MqttError::Handshake(HandshakeError::Timeout)),
                Either::Right(Ok(Some(ver))) => match ver {
                    ProtocolVersion::MQTT3 => ctx.call(&self.handlers.0, io).await,
                    ProtocolVersion::MQTT5 => ctx.call(&self.handlers.1, io).await,
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

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        Service::<IoBoxed>::ready(self, ctx).await
    }

    #[inline]
    async fn shutdown(&self) {
        Service::<IoBoxed>::shutdown(self).await
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

impl<Err, InitErr> DefaultProtocolServer<Err, InitErr> {
    fn new(ver: ProtocolVersion) -> Self {
        Self { ver, _t: marker::PhantomData }
    }
}

impl<Err, InitErr> ServiceFactory<IoBoxed> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;
    type Service = DefaultProtocolServer<Err, InitErr>;
    type InitError = InitErr;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
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
        Err(MqttError::Handshake(HandshakeError::Disconnected(Some(io::Error::new(
            io::ErrorKind::Other,
            format!("Protocol is not supported: {:?}", self.ver),
        )))))
    }
}
