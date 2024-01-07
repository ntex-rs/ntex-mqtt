use std::{convert::TryFrom, fmt, io, marker, task::Context, task::Poll};

use ntex::io::{Filter, Io, IoBoxed};
use ntex::service::{Service, ServiceCtx, ServiceFactory};
use ntex::time::{Deadline, Millis, Seconds};
use ntex::util::{join, select, Either};

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
    /// Create mqtt protocol selector server
    pub fn new() -> Self {
        MqttServer {
            v3: DefaultProtocolServer::new(ProtocolVersion::MQTT3),
            v5: DefaultProtocolServer::new(ProtocolVersion::MQTT5),
            connect_timeout: Millis(10000),
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
    /// Set client timeout for first `Connect` frame.
    ///
    /// Defines a timeout for reading `Connect` frame. If a client does not transmit
    /// the entire frame within this time, the connection is terminated with
    /// Mqtt::Handshake(HandshakeError::Timeout) error.
    ///
    /// By default, connect timeuot is 10 seconds.
    pub fn connect_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout.into();
        self
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<
        (IoBoxed, Deadline),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        (IoBoxed, Deadline),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
{
    /// Service to handle v3 protocol
    pub fn v3<St, C, Cn, P>(
        self,
        service: v3::MqttServer<St, C, Cn, P>,
    ) -> MqttServer<
        impl ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        >,
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
        Cn: ServiceFactory<
                v3::ControlMessage<Err>,
                v3::Session<St>,
                Response = v3::ControlResult,
            > + 'static,
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

    /// Service to handle v3 protocol
    pub fn v3_variants(
        self,
        service: v3::Selector<Err, InitErr>,
    ) -> MqttServer<
        impl ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        >,
        V5,
        Err,
        InitErr,
    >
    where
        Err: 'static,
        InitErr: 'static,
    {
        MqttServer {
            v3: service,
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
        impl ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        >,
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
        Cn: ServiceFactory<
                v5::ControlMessage<Err>,
                v5::Session<St>,
                Response = v5::ControlResult,
            > + 'static,
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

    /// Service to handle v5 protocol
    pub fn v5_variants<St, C, Cn, P>(
        self,
        service: v5::Selector<Err, InitErr>,
    ) -> MqttServer<
        V3,
        impl ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        >,
        Err,
        InitErr,
    >
    where
        Err: 'static,
        InitErr: 'static,
    {
        MqttServer {
            v3: self.v3,
            v5: service,
            connect_timeout: self.connect_timeout,
            _t: marker::PhantomData,
        }
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<
        (IoBoxed, Deadline),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        (IoBoxed, Deadline),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
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
    V3: ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        > + 'static,
    V5: ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        > + 'static,
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
    V3: ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        > + 'static,
    V5: ServiceFactory<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
            InitError = InitErr,
        > + 'static,
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
    V3: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
    V5: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ready1 = self.handlers.0.poll_ready(cx)?.is_ready();
        let ready2 = self.handlers.1.poll_ready(cx)?.is_ready();

        if ready1 && ready2 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let ready1 = self.handlers.0.poll_shutdown(cx).is_ready();
        let ready2 = self.handlers.1.poll_shutdown(cx).is_ready();

        if ready1 && ready2 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    #[inline]
    async fn call(
        &self,
        io: IoBoxed,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        let mut deadline = Deadline::new(self.connect_timeout);

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

        match select(&mut deadline, fut).await {
            Either::Left(_) => Err(MqttError::Handshake(HandshakeError::Timeout)),
            Either::Right(Ok(Some(ver))) => match ver {
                ProtocolVersion::MQTT3 => ctx.call(&self.handlers.0, (io, deadline)).await,
                ProtocolVersion::MQTT5 => ctx.call(&self.handlers.1, (io, deadline)).await,
            },
            Either::Right(Ok(None)) => {
                Err(MqttError::Handshake(HandshakeError::Disconnected(None)))
            }
            Either::Right(Err(e)) => Err(e),
        }
    }
}

impl<F, V3, V5, Err> Service<Io<F>> for MqttServerImpl<V3, V5, Err>
where
    F: Filter,
    V3: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
    V5: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::<IoBoxed>::poll_ready(self, cx)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        Service::<IoBoxed>::poll_shutdown(self, cx)
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

impl<Err, InitErr> ServiceFactory<(IoBoxed, Deadline)> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;
    type Service = DefaultProtocolServer<Err, InitErr>;
    type InitError = InitErr;

    async fn create(&self, _: ()) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultProtocolServer { ver: self.ver, _t: marker::PhantomData })
    }
}

impl<Err, InitErr> Service<(IoBoxed, Deadline)> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;

    async fn call(
        &self,
        _: (IoBoxed, Deadline),
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        Err(MqttError::Handshake(HandshakeError::Disconnected(Some(io::Error::new(
            io::ErrorKind::Other,
            format!("Protocol is not supported: {:?}", self.ver),
        )))))
    }
}
