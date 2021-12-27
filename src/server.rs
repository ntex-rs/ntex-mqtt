use std::task::{Context, Poll};
use std::{convert::TryFrom, fmt, future::Future, io, marker, pin::Pin, rc::Rc, time};

use ntex::io::{Filter, Io, IoBoxed, RecvError};
use ntex::service::{Service, ServiceFactory};
use ntex::time::{sleep, Seconds, Sleep};
use ntex::util::{join, ready, Pool, PoolId, PoolRef, Ready};

use crate::error::{MqttError, ProtocolError};
use crate::version::{ProtocolVersion, VersionCodec};
use crate::{v3, v5};

/// Mqtt Server
pub struct MqttServer<V3, V5, Err, InitErr> {
    v3: V3,
    v5: V5,
    handshake_timeout: Seconds,
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
            handshake_timeout: Seconds::ZERO,
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
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout;
        self
    }
}

impl<V3, V5, Err, InitErr> MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<
        (IoBoxed, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        (IoBoxed, Option<Sleep>),
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
            (IoBoxed, Option<Sleep>),
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
            v3: service.inner_finish(),
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
            (IoBoxed, Option<Sleep>),
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
            (IoBoxed, Option<Sleep>),
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
            v5: service.inner_finish(),
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
            (IoBoxed, Option<Sleep>),
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
            handshake_timeout: self.handshake_timeout,
            _t: marker::PhantomData,
        }
    }
}

impl<V3, V5, Err, InitErr> ServiceFactory<IoBoxed> for MqttServer<V3, V5, Err, InitErr>
where
    V3: ServiceFactory<
        (IoBoxed, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        (IoBoxed, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V3::Future: 'static,
    V5::Future: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;
    type Future = Pin<
        Box<
            dyn Future<Output = Result<MqttServerImpl<V3::Service, V5::Service, Err>, InitErr>>,
        >,
    >;

    fn new_service(&self, _: ()) -> Self::Future {
        let handshake_timeout = self.handshake_timeout;
        let fut = join(self.v3.new_service(()), self.v5.new_service(()));
        Box::pin(async move {
            let (v3, v5) = fut.await;
            let v3 = v3?;
            let v5 = v5?;
            Ok(MqttServerImpl {
                handlers: Rc::new((v3, v5)),
                handshake_timeout,
                _t: marker::PhantomData,
            })
        })
    }
}

impl<F, V3, V5, Err, InitErr> ServiceFactory<Io<F>> for MqttServer<V3, V5, Err, InitErr>
where
    F: Filter,
    V3: ServiceFactory<
        (IoBoxed, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        (IoBoxed, Option<Sleep>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V3::Future: 'static,
    V5::Future: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = MqttServerImpl<V3::Service, V5::Service, Err>;
    type InitError = InitErr;
    type Future = Pin<
        Box<
            dyn Future<Output = Result<MqttServerImpl<V3::Service, V5::Service, Err>, InitErr>>,
        >,
    >;

    fn new_service(&self, _: ()) -> Self::Future {
        let handshake_timeout = self.handshake_timeout;
        let fut = join(self.v3.new_service(()), self.v5.new_service(()));
        Box::pin(async move {
            let (v3, v5) = fut.await;
            let v3 = v3?;
            let v5 = v5?;
            Ok(MqttServerImpl {
                handlers: Rc::new((v3, v5)),
                handshake_timeout,
                _t: marker::PhantomData,
            })
        })
    }
}

/// Mqtt Server
pub struct MqttServerImpl<V3, V5, Err> {
    handlers: Rc<(V3, V5)>,
    handshake_timeout: Seconds,
    _t: marker::PhantomData<Err>,
}

impl<V3, V5, Err> Service<IoBoxed> for MqttServerImpl<V3, V5, Err>
where
    V3: Service<(IoBoxed, Option<Sleep>), Response = (), Error = MqttError<Err>>,
    V5: Service<(IoBoxed, Option<Sleep>), Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Future = MqttServerImplResponse<V3, V5, Err>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ready1 = self.handlers.0.poll_ready(cx)?.is_ready();
        let ready2 = self.handlers.1.poll_ready(cx)?.is_ready();

        if ready1 && ready2 {
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

    fn call(&self, req: IoBoxed) -> Self::Future {
        let delay = self.handshake_timeout.map(sleep);

        MqttServerImplResponse {
            state: MqttServerImplState::Version {
                item: Some((req, VersionCodec, self.handlers.clone(), delay)),
            },
        }
    }
}

impl<F, V3, V5, Err> Service<Io<F>> for MqttServerImpl<V3, V5, Err>
where
    F: Filter,
    V3: Service<(IoBoxed, Option<Sleep>), Response = (), Error = MqttError<Err>>,
    V5: Service<(IoBoxed, Option<Sleep>), Response = (), Error = MqttError<Err>>,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Future = MqttServerImplResponse<V3, V5, Err>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ready1 = self.handlers.0.poll_ready(cx)?.is_ready();
        let ready2 = self.handlers.1.poll_ready(cx)?.is_ready();

        if ready1 && ready2 {
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

    fn call(&self, req: Io<F>) -> Self::Future {
        let delay = self.handshake_timeout.map(sleep);

        MqttServerImplResponse {
            state: MqttServerImplState::Version {
                item: Some((IoBoxed::from(req), VersionCodec, self.handlers.clone(), delay)),
            },
        }
    }
}

pin_project_lite::pin_project! {
    pub struct MqttServerImplResponse<V3, V5, Err>
    where
        V3: Service<
            (IoBoxed, Option<Sleep>),
            Response = (),
            Error = MqttError<Err>,
        >,
        V5: Service<
        (IoBoxed, Option<Sleep>),
            Response = (),
            Error = MqttError<Err>,
        >,
    {
        #[pin]
        state: MqttServerImplState<V3, V5>,
    }
}

pin_project_lite::pin_project! {
    #[project = MqttServerImplStateProject]
    pub(crate) enum MqttServerImplState<V3: Service<(IoBoxed, Option<Sleep>)>, V5: Service<(IoBoxed, Option<Sleep>)>> {
        V3 { #[pin] fut: V3::Future },
        V5 { #[pin] fut: V5::Future },
        Version { item: Option<(IoBoxed, VersionCodec, Rc<(V3, V5)>, Option<Sleep>)> },
    }
}

impl<V3, V5, Err> Future for MqttServerImplResponse<V3, V5, Err>
where
    V3: Service<(IoBoxed, Option<Sleep>), Response = (), Error = MqttError<Err>>,
    V5: Service<(IoBoxed, Option<Sleep>), Response = (), Error = MqttError<Err>>,
{
    type Output = Result<(), MqttError<Err>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();

            match this.state.project() {
                MqttServerImplStateProject::V3 { fut } => return fut.poll(cx),
                MqttServerImplStateProject::V5 { fut } => return fut.poll(cx),
                MqttServerImplStateProject::Version { ref mut item } => {
                    if let Some(ref mut delay) = item.as_mut().unwrap().3 {
                        match Pin::new(delay).poll(cx) {
                            Poll::Pending => (),
                            Poll::Ready(_) => {
                                return Poll::Ready(Err(MqttError::HandshakeTimeout))
                            }
                        }
                    };

                    let st = item.as_mut().unwrap();

                    return match ready!(st.0.poll_recv(&st.1, cx)) {
                        Ok(ver) => {
                            let (io, _, handlers, delay) = item.take().unwrap();
                            this = self.as_mut().project();
                            match ver {
                                ProtocolVersion::MQTT3 => {
                                    this.state.set(MqttServerImplState::V3 {
                                        fut: handlers.0.call((io, delay)),
                                    })
                                }
                                ProtocolVersion::MQTT5 => {
                                    this.state.set(MqttServerImplState::V5 {
                                        fut: handlers.1.call((io, delay)),
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

impl<Err, InitErr> ServiceFactory<(IoBoxed, Option<Sleep>)>
    for DefaultProtocolServer<Err, InitErr>
{
    type Response = ();
    type Error = MqttError<Err>;
    type Service = DefaultProtocolServer<Err, InitErr>;
    type InitError = InitErr;
    type Future = Ready<Self::Service, Self::InitError>;

    fn new_service(&self, _: ()) -> Self::Future {
        Ready::Ok(DefaultProtocolServer { ver: self.ver, _t: marker::PhantomData })
    }
}

impl<Err, InitErr> Service<(IoBoxed, Option<Sleep>)> for DefaultProtocolServer<Err, InitErr> {
    type Response = ();
    type Error = MqttError<Err>;
    type Future = Ready<Self::Response, Self::Error>;

    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&self, _: (IoBoxed, Option<Sleep>)) -> Self::Future {
        Ready::Err(MqttError::Disconnected(Some(io::Error::new(
            io::ErrorKind::Other,
            format!("Protocol is not supported: {:?}", self.ver),
        ))))
    }
}
