use std::task::{Context, Poll};
use std::{convert::TryFrom, fmt, future::Future, io, marker, pin::Pin};

use ntex::io::{Filter, Io, IoBoxed, RecvError};
use ntex::service::{Service, ServiceFactory};
use ntex::time::{Deadline, Millis, Seconds};
use ntex::util::{join, ready, BoxFuture, Ready};

use crate::version::{ProtocolVersion, VersionCodec};
use crate::{error::MqttError, v3, v5};

/// Mqtt Server
pub struct MqttServer<V3, V5, Err, InitErr> {
    v3: V3,
    v5: V5,
    handshake_timeout: Millis,
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
            handshake_timeout: Millis(10000),
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
    /// Set handshake timeout.
    ///
    /// Handshake includes `connect` packet.
    /// By default handshake timeuot is 10 seconds.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout.into();
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
            handshake_timeout: self.handshake_timeout,
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
            handshake_timeout: self.handshake_timeout,
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
            handshake_timeout: self.handshake_timeout,
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
            handshake_timeout: self.handshake_timeout,
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
            handshake_timeout: self.handshake_timeout,
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
    type Future<'f> =
        BoxFuture<'f, Result<MqttServerImpl<V3::Service, V5::Service, Err>, InitErr>>;

    #[inline]
    fn create(&self, _: ()) -> Self::Future<'_> {
        Box::pin(self.create_service())
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
    type Future<'f> =
        BoxFuture<'f, Result<MqttServerImpl<V3::Service, V5::Service, Err>, InitErr>>;

    #[inline]
    fn create(&self, _: ()) -> Self::Future<'_> {
        Box::pin(self.create_service())
    }
}

/// Mqtt Server
pub struct MqttServerImpl<V3, V5, Err> {
    handlers: (V3, V5),
    handshake_timeout: Millis,
    _t: marker::PhantomData<Err>,
}

impl<V3, V5, Err> Service<IoBoxed> for MqttServerImpl<V3, V5, Err>
where
    V3: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
    V5: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Future<'f> = MqttServerImplResponse<'f, V3, V5, Err> where Self: 'f;

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
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        let ready1 = self.handlers.0.poll_shutdown(cx, is_error).is_ready();
        let ready2 = self.handlers.1.poll_shutdown(cx, is_error).is_ready();

        if ready1 && ready2 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn call(&self, req: IoBoxed) -> Self::Future<'_> {
        MqttServerImplResponse {
            state: MqttServerImplState::Version {
                item: Some((
                    req,
                    VersionCodec,
                    Deadline::new(self.handshake_timeout),
                )),
            },
            handlers: &self.handlers,
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
    type Future<'f> = MqttServerImplResponse<'f, V3, V5, Err> where Self: 'f;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::<IoBoxed>::poll_ready(self, cx)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        Service::<IoBoxed>::poll_shutdown(self, cx, is_error)
    }

    #[inline]
    fn call(&self, io: Io<F>) -> Self::Future<'_> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io))
    }
}

pin_project_lite::pin_project! {
    pub struct MqttServerImplResponse<'f, V3, V5, Err>
    where
        V3: Service<
            (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
    >,
        V3: 'f,
        V5: Service<
        (IoBoxed, Deadline),
            Response = (),
            Error = MqttError<Err>,
    >,
        V5: 'f,
    {
        #[pin]
        state: MqttServerImplState<'f, V3, V5>,
        handlers: &'f (V3, V5),
    }
}

pin_project_lite::pin_project! {
    #[project = MqttServerImplStateProject]
        pub(crate) enum MqttServerImplState<'f, V3: Service<(IoBoxed, Deadline)>, V5: Service<(IoBoxed, Deadline)>>
        where V3: 'f, V5: 'f
    {
        V3 { #[pin] fut: V3::Future<'f> },
        V5 { #[pin] fut: V5::Future<'f> },
        Version { item: Option<(IoBoxed, VersionCodec, Deadline)> },
    }
}

impl<'f, V3, V5, Err> Future for MqttServerImplResponse<'f, V3, V5, Err>
where
    V3: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
    V5: Service<(IoBoxed, Deadline), Response = (), Error = MqttError<Err>>,
{
    type Output = Result<(), MqttError<Err>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();

            match this.state.project() {
                MqttServerImplStateProject::V3 { fut } => return fut.poll(cx),
                MqttServerImplStateProject::V5 { fut } => return fut.poll(cx),
                MqttServerImplStateProject::Version { ref mut item } => {
                    match item.as_mut().unwrap().2.poll_elapsed(cx) {
                        Poll::Pending => (),
                        Poll::Ready(_) => return Poll::Ready(Err(MqttError::HandshakeTimeout)),
                    }

                    let st = item.as_mut().unwrap();
                    return match ready!(st.0.poll_recv(&st.1, cx)) {
                        Ok(ver) => {
                            let (io, _, delay) = item.take().unwrap();
                            this = self.as_mut().project();
                            match ver {
                                ProtocolVersion::MQTT3 => {
                                    this.state.set(MqttServerImplState::V3 {
                                        fut: this.handlers.0.call((io, delay)),
                                    })
                                }
                                ProtocolVersion::MQTT5 => {
                                    this.state.set(MqttServerImplState::V5 {
                                        fut: this.handlers.1.call((io, delay)),
                                    })
                                }
                            }
                            continue;
                        }
                        Err(RecvError::KeepAlive | RecvError::Stop) => {
                            unreachable!()
                        }
                        Err(RecvError::WriteBackpressure) => {
                            ready!(st.0.poll_flush(cx, false))
                                .map_err(|e| MqttError::Disconnected(Some(e)))?;
                            continue;
                        }
                        Err(RecvError::Decoder(err)) => {
                            Poll::Ready(Err(MqttError::Protocol(err.into())))
                        }
                        Err(RecvError::PeerGone(err)) => {
                            Poll::Ready(Err(MqttError::Disconnected(err)))
                        }
                    };
                }
            }
        }
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
    type Future<'f> = Ready<Self::Service, Self::InitError> where Self: 'f;

    fn create(&self, _: ()) -> Self::Future<'_> {
        Ready::Ok(DefaultProtocolServer { ver: self.ver, _t: marker::PhantomData })
    }
}

impl<Err, InitErr> Service<(IoBoxed, Deadline)> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;
    type Future<'f> = Ready<Self::Response, Self::Error> where Self: 'f;

    fn call(&self, _: (IoBoxed, Deadline)) -> Self::Future<'_> {
        Ready::Err(MqttError::Disconnected(Some(io::Error::new(
            io::ErrorKind::Other,
            format!("Protocol is not supported: {:?}", self.ver),
        ))))
    }
}
