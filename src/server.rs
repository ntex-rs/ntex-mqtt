use std::task::{Context, Poll};
use std::{convert::TryFrom, fmt, future::Future, io, marker, pin::Pin, rc::Rc, time};

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::service::{Service, ServiceFactory};
use ntex::time::{sleep, Seconds, Sleep};
use ntex::util::{join, Pool, PoolId, PoolRef, Ready};

use crate::error::{MqttError, ProtocolError};
use crate::io::State;
use crate::version::{ProtocolVersion, VersionCodec};
use crate::{v3, v5};

/// Mqtt Server
pub struct MqttServer<Io, V3, V5, Err, InitErr> {
    v3: V3,
    v5: V5,
    handshake_timeout: Seconds,
    pool: Pool,
    _t: marker::PhantomData<(Io, Err, InitErr)>,
}

impl<Io, Err, InitErr>
    MqttServer<
        Io,
        DefaultProtocolServer<Io, Err, InitErr>,
        DefaultProtocolServer<Io, Err, InitErr>,
        Err,
        InitErr,
    >
{
    /// Create mqtt protocol selector server
    pub fn new() -> Self {
        MqttServer {
            v3: DefaultProtocolServer::new(ProtocolVersion::MQTT3),
            v5: DefaultProtocolServer::new(ProtocolVersion::MQTT5),
            pool: PoolId::P5.pool(),
            handshake_timeout: Seconds::ZERO,
            _t: marker::PhantomData,
        }
    }
}

impl<Io, Err, InitErr> Default
    for MqttServer<
        Io,
        DefaultProtocolServer<Io, Err, InitErr>,
        DefaultProtocolServer<Io, Err, InitErr>,
        Err,
        InitErr,
    >
{
    fn default() -> Self {
        MqttServer::new()
    }
}

impl<Io, V3, V5, Err, InitErr> MqttServer<Io, V3, V5, Err, InitErr> {
    /// Set handshake timeout.
    ///
    /// Handshake includes `connect` packet.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set memory pool.
    ///
    /// Use specified memory pool for memory allocations. By default P5
    /// memory pool is used.
    pub fn memory_pool(mut self, id: PoolId) -> Self {
        self.pool = id.pool();
        self
    }
}

impl<Io, V3, V5, Err, InitErr> MqttServer<Io, V3, V5, Err, InitErr>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: ServiceFactory<
        Config = (),
        Request = (Io, State, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        Config = (),
        Request = (Io, State, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
{
    /// Service to handle v3 protocol
    pub fn v3<St, C, Cn, P>(
        self,
        service: v3::MqttServer<Io, St, C, Cn, P>,
    ) -> MqttServer<
        Io,
        impl ServiceFactory<
            Config = (),
            Request = (Io, State, Option<Sleep>),
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
                Config = (),
                Request = v3::Handshake<Io>,
                Response = v3::HandshakeAck<Io, St>,
                Error = Err,
                InitError = InitErr,
            > + 'static,
        Cn: ServiceFactory<
                Config = v3::Session<St>,
                Request = v3::ControlMessage<C::Error>,
                Response = v3::ControlResult,
            > + 'static,
        P: ServiceFactory<Config = v3::Session<St>, Request = v3::Publish, Response = ()>
            + 'static,
        C::Error: From<Cn::Error>
            + From<Cn::InitError>
            + From<P::Error>
            + From<P::InitError>
            + fmt::Debug,
    {
        MqttServer {
            v3: service.inner_finish(),
            v5: self.v5,
            pool: self.pool,
            handshake_timeout: self.handshake_timeout,
            _t: marker::PhantomData,
        }
    }

    /// Service to handle v3 protocol
    pub fn v3_variants(
        self,
        service: v3::Selector<Io, Err, InitErr>,
    ) -> MqttServer<
        Io,
        impl ServiceFactory<
            Config = (),
            Request = (Io, State, Option<Sleep>),
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
            v3: service.finish_server(),
            v5: self.v5,
            pool: self.pool,
            handshake_timeout: self.handshake_timeout,
            _t: marker::PhantomData,
        }
    }

    /// Service to handle v5 protocol
    pub fn v5<St, C, Cn, P>(
        self,
        service: v5::MqttServer<Io, St, C, Cn, P>,
    ) -> MqttServer<
        Io,
        V3,
        impl ServiceFactory<
            Config = (),
            Request = (Io, State, Option<Sleep>),
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
                Config = (),
                Request = v5::Handshake<Io>,
                Response = v5::HandshakeAck<Io, St>,
                Error = Err,
                InitError = InitErr,
            > + 'static,
        Cn: ServiceFactory<
                Config = v5::Session<St>,
                Request = v5::ControlMessage<C::Error>,
                Response = v5::ControlResult,
            > + 'static,
        P: ServiceFactory<
                Config = v5::Session<St>,
                Request = v5::Publish,
                Response = v5::PublishAck,
            > + 'static,
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
            v5: service.inner_finish(),
            pool: self.pool,
            handshake_timeout: self.handshake_timeout,
            _t: marker::PhantomData,
        }
    }

    /// Service to handle v5 protocol
    pub fn v5_variants<St, C, Cn, P>(
        self,
        service: v5::Selector<Io, Err, InitErr>,
    ) -> MqttServer<
        Io,
        V3,
        impl ServiceFactory<
            Config = (),
            Request = (Io, State, Option<Sleep>),
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
            v5: service.finish_server(),
            pool: self.pool,
            handshake_timeout: self.handshake_timeout,
            _t: marker::PhantomData,
        }
    }
}

impl<Io, V3, V5, Err, InitErr> ServiceFactory for MqttServer<Io, V3, V5, Err, InitErr>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: ServiceFactory<
        Config = (),
        Request = (Io, State, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        Config = (),
        Request = (Io, State, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V3::Future: 'static,
    V5::Future: 'static,
{
    type Config = ();
    type Request = Io;
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<Io, V3::Service, V5::Service, Err>;
    type InitError = InitErr;
    type Future = Pin<
        Box<
            dyn Future<
                Output = Result<MqttServerImpl<Io, V3::Service, V5::Service, Err>, InitErr>,
            >,
        >,
    >;

    fn new_service(&self, _: ()) -> Self::Future {
        let pool = self.pool.clone();
        let handshake_timeout = self.handshake_timeout;
        let fut = join(self.v3.new_service(()), self.v5.new_service(()));
        Box::pin(async move {
            let (v3, v5) = fut.await;
            let v3 = v3?;
            let v5 = v5?;
            Ok(MqttServerImpl {
                handlers: Rc::new((v3, v5)),
                pool,
                handshake_timeout,
                _t: marker::PhantomData,
            })
        })
    }
}

/// Mqtt Server
pub struct MqttServerImpl<Io, V3, V5, Err> {
    handlers: Rc<(V3, V5)>,
    handshake_timeout: Seconds,
    pool: Pool,
    _t: marker::PhantomData<(Io, Err)>,
}

impl<Io, V3, V5, Err> Service for MqttServerImpl<Io, V3, V5, Err>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: Service<Request = (Io, State, Option<Sleep>), Response = (), Error = MqttError<Err>>,
    V5: Service<Request = (Io, State, Option<Sleep>), Response = (), Error = MqttError<Err>>,
{
    type Request = Io;
    type Response = ();
    type Error = MqttError<Err>;
    type Future = MqttServerImplResponse<Io, V3, V5, Err>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ready1 = self.handlers.0.poll_ready(cx)?.is_ready();
        let ready2 = self.handlers.1.poll_ready(cx)?.is_ready();
        let ready3 = self.pool.poll_ready(cx).is_ready();

        if ready1 && ready2 && ready3 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        let ready1 = self.handlers.0.poll_shutdown(cx, is_error).is_ready();
        let ready2 = self.handlers.1.poll_shutdown(cx, is_error).is_ready();

        if ready1 && ready2 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn call(&self, req: Io) -> Self::Future {
        let pool = self.pool.pool_ref();
        let delay = self.handshake_timeout.map(sleep);

        MqttServerImplResponse {
            state: MqttServerImplState::Version {
                item: Some((
                    req,
                    State::with_memory_pool(pool),
                    VersionCodec,
                    self.handlers.clone(),
                    delay,
                )),
            },
        }
    }
}

pin_project_lite::pin_project! {
    pub struct MqttServerImplResponse<Io, V3, V5, Err>
    where
        V3: Service<
            Request = (Io, State, Option<Sleep>),
            Response = (),
            Error = MqttError<Err>,
        >,
        V5: Service<
            Request = (Io, State, Option<Sleep>),
            Response = (),
            Error = MqttError<Err>,
        >,
    {
        #[pin]
        state: MqttServerImplState<Io, V3, V5>,
    }
}

pin_project_lite::pin_project! {
    #[project = MqttServerImplStateProject]
    pub(crate) enum MqttServerImplState<Io, V3: Service, V5: Service> {
        V3 { #[pin] fut: V3::Future },
        V5 { #[pin] fut: V5::Future },
        Version { item: Option<(Io, State, VersionCodec, Rc<(V3, V5)>, Option<Sleep>)> },
    }
}

impl<Io, V3, V5, Err> Future for MqttServerImplResponse<Io, V3, V5, Err>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: Service<Request = (Io, State, Option<Sleep>), Response = (), Error = MqttError<Err>>,
    V5: Service<Request = (Io, State, Option<Sleep>), Response = (), Error = MqttError<Err>>,
{
    type Output = Result<(), MqttError<Err>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        loop {
            match this.state.project() {
                MqttServerImplStateProject::V3 { fut } => return fut.poll(cx),
                MqttServerImplStateProject::V5 { fut } => return fut.poll(cx),
                MqttServerImplStateProject::Version { ref mut item } => {
                    if let Some(ref mut delay) = item.as_mut().unwrap().4 {
                        match Pin::new(delay).poll(cx) {
                            Poll::Pending => (),
                            Poll::Ready(_) => {
                                return Poll::Ready(Err(MqttError::HandshakeTimeout))
                            }
                        }
                    };

                    let st = item.as_mut().unwrap();

                    match st.1.poll_next(&mut st.0, &st.2, cx) {
                        Poll::Ready(Ok(Some(ver))) => {
                            let (io, state, _, handlers, delay) = item.take().unwrap();
                            this = self.as_mut().project();
                            match ver {
                                ProtocolVersion::MQTT3 => {
                                    this.state.set(MqttServerImplState::V3 {
                                        fut: handlers.0.call((io, state, delay)),
                                    })
                                }
                                ProtocolVersion::MQTT5 => {
                                    this.state.set(MqttServerImplState::V5 {
                                        fut: handlers.1.call((io, state, delay)),
                                    })
                                }
                            }
                            continue;
                        }
                        Poll::Ready(Ok(None)) => {
                            return Poll::Ready(Err(MqttError::Disconnected))
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(MqttError::from(err))),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

pub struct DefaultProtocolServer<Io, Err, InitErr> {
    ver: ProtocolVersion,
    _t: marker::PhantomData<(Io, Err, InitErr)>,
}

impl<Io, Err, InitErr> DefaultProtocolServer<Io, Err, InitErr> {
    fn new(ver: ProtocolVersion) -> Self {
        Self { ver, _t: marker::PhantomData }
    }
}

impl<Io, Err, InitErr> ServiceFactory for DefaultProtocolServer<Io, Err, InitErr> {
    type Config = ();
    type Request = (Io, State, Option<Sleep>);
    type Response = ();
    type Error = MqttError<Err>;
    type Service = DefaultProtocolServer<Io, Err, InitErr>;
    type InitError = InitErr;
    type Future = Ready<Self::Service, Self::InitError>;

    fn new_service(&self, _: ()) -> Self::Future {
        Ready::Ok(DefaultProtocolServer { ver: self.ver, _t: marker::PhantomData })
    }
}

impl<Io, Err, InitErr> Service for DefaultProtocolServer<Io, Err, InitErr> {
    type Request = (Io, State, Option<Sleep>);
    type Response = ();
    type Error = MqttError<Err>;
    type Future = Ready<Self::Response, Self::Error>;

    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&self, _: Self::Request) -> Self::Future {
        Ready::Err(MqttError::Protocol(ProtocolError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("Protocol is not supported: {:?}", self.ver),
        ))))
    }
}
