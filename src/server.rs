use std::convert::TryFrom;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::{fmt, io, time};

use futures::future::{err, join, ok, Future, FutureExt, LocalBoxFuture, Ready};
use ntex::rt::time::{delay_for, Delay};
use ntex::service::{Service, ServiceFactory};
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use crate::error::{MqttError, ProtocolError};
use crate::io::IoState;
use crate::version::{ProtocolVersion, VersionCodec};
use crate::{v3, v5};

/// Mqtt Server
pub struct MqttServer<Io, V3, V5, Err, InitErr> {
    v3: V3,
    v5: V5,
    handshake_timeout: usize,
    _t: PhantomData<(Io, Err, InitErr)>,
}

impl<Io, Err, InitErr>
    MqttServer<
        Io,
        DefaultProtocolServer<Io, Err, InitErr, v3::codec::Codec>,
        DefaultProtocolServer<Io, Err, InitErr, v5::codec::Codec>,
        Err,
        InitErr,
    >
{
    /// Create mqtt protocol selector server
    pub fn new() -> Self {
        MqttServer {
            v3: DefaultProtocolServer::new(ProtocolVersion::MQTT3),
            v5: DefaultProtocolServer::new(ProtocolVersion::MQTT5),
            handshake_timeout: 0,
            _t: PhantomData,
        }
    }
}

impl<Io, Err, InitErr> Default
    for MqttServer<
        Io,
        DefaultProtocolServer<Io, Err, InitErr, v3::codec::Codec>,
        DefaultProtocolServer<Io, Err, InitErr, v5::codec::Codec>,
        Err,
        InitErr,
    >
{
    fn default() -> Self {
        MqttServer::new()
    }
}

impl<Io, V3, V5, Err, InitErr> MqttServer<Io, V3, V5, Err, InitErr> {
    /// Set handshake timeout in millis.
    ///
    /// Handshake includes `connect` packet.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: usize) -> Self {
        self.handshake_timeout = timeout;
        self
    }
}

impl<Io, V3, V5, Err, InitErr> MqttServer<Io, V3, V5, Err, InitErr>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: ServiceFactory<
        Config = (),
        Request = (Io, IoState<v3::codec::Codec>, Option<Delay>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        Config = (),
        Request = (Io, IoState<v5::codec::Codec>, Option<Delay>),
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
            Request = (Io, IoState<v3::codec::Codec>, Option<Delay>),
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
                Request = v3::ControlMessage,
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
            handshake_timeout: self.handshake_timeout,
            _t: PhantomData,
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
            Request = (Io, IoState<v5::codec::Codec>, Option<Delay>),
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
            handshake_timeout: self.handshake_timeout,
            _t: PhantomData,
        }
    }
}

impl<Io, V3, V5, Err, InitErr> ServiceFactory for MqttServer<Io, V3, V5, Err, InitErr>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: ServiceFactory<
        Config = (),
        Request = (Io, IoState<v3::codec::Codec>, Option<Delay>),
        Response = (),
        Error = MqttError<Err>,
        InitError = InitErr,
    >,
    V5: ServiceFactory<
        Config = (),
        Request = (Io, IoState<v5::codec::Codec>, Option<Delay>),
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
    type Future = LocalBoxFuture<
        'static,
        Result<MqttServerImpl<Io, V3::Service, V5::Service, Err>, InitErr>,
    >;

    fn new_service(&self, _: ()) -> Self::Future {
        let handshake_timeout = self.handshake_timeout;

        join(self.v3.new_service(()), self.v5.new_service(()))
            .map(move |(v3, v5)| {
                let v3 = v3?;
                let v5 = v5?;
                Ok(MqttServerImpl {
                    handlers: Rc::new((v3, v5)),
                    handshake_timeout,
                    _t: PhantomData,
                })
            })
            .boxed_local()
    }
}

/// Mqtt Server
pub struct MqttServerImpl<Io, V3, V5, Err> {
    handlers: Rc<(V3, V5)>,
    handshake_timeout: usize,
    _t: PhantomData<(Io, Err)>,
}

impl<Io, V3, V5, Err> Service for MqttServerImpl<Io, V3, V5, Err>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: Service<
        Request = (Io, IoState<v3::codec::Codec>, Option<Delay>),
        Response = (),
        Error = MqttError<Err>,
    >,
    V5: Service<
        Request = (Io, IoState<v5::codec::Codec>, Option<Delay>),
        Response = (),
        Error = MqttError<Err>,
    >,
{
    type Request = Io;
    type Response = ();
    type Error = MqttError<Err>;
    type Future = MqttServerImplResponse<Io, V3, V5, Err>;

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

    fn call(&self, req: Io) -> Self::Future {
        let delay = if self.handshake_timeout > 0 {
            Some(delay_for(time::Duration::from_secs(self.handshake_timeout as u64)))
        } else {
            None
        };

        MqttServerImplResponse {
            state: MqttServerImplState::Version(Some((
                req,
                IoState::new(VersionCodec),
                self.handlers.clone(),
                delay,
            ))),
        }
    }
}

pin_project_lite::pin_project! {
    pub struct MqttServerImplResponse<Io, V3, V5, Err>
    where
        V3: Service<
            Request = (Io, IoState<v3::codec::Codec>, Option<Delay>),
            Response = (),
            Error = MqttError<Err>,
        >,
        V5: Service<
            Request = (Io, IoState<v5::codec::Codec>, Option<Delay>),
            Response = (),
            Error = MqttError<Err>,
        >,
    {
        #[pin]
        state: MqttServerImplState<Io, V3, V5>,
    }
}

#[pin_project::pin_project(project = MqttServerImplStateProject)]
pub(crate) enum MqttServerImplState<Io, V3: Service, V5: Service> {
    V3(#[pin] V3::Future),
    V5(#[pin] V5::Future),
    Version(Option<(Io, IoState<VersionCodec>, Rc<(V3, V5)>, Option<Delay>)>),
}

impl<Io, V3, V5, Err> Future for MqttServerImplResponse<Io, V3, V5, Err>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    V3: Service<
        Request = (Io, IoState<v3::codec::Codec>, Option<Delay>),
        Response = (),
        Error = MqttError<Err>,
    >,
    V5: Service<
        Request = (Io, IoState<v5::codec::Codec>, Option<Delay>),
        Response = (),
        Error = MqttError<Err>,
    >,
{
    type Output = Result<(), MqttError<Err>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        loop {
            match this.state.project() {
                MqttServerImplStateProject::V3(fut) => return fut.poll(cx),
                MqttServerImplStateProject::V5(fut) => return fut.poll(cx),
                MqttServerImplStateProject::Version(ref mut item) => {
                    if let Some(ref mut delay) = item.as_mut().unwrap().3 {
                        match Pin::new(delay).poll(cx) {
                            Poll::Pending => (),
                            Poll::Ready(_) => {
                                return Poll::Ready(Err(MqttError::HandshakeTimeout))
                            }
                        }
                    };

                    let st = item.as_mut().unwrap();

                    match futures::ready!(st.1.poll_next(&mut st.0, cx)) {
                        Ok(Some(ver)) => {
                            let (io, state, handlers, delay) = item.take().unwrap();
                            this = self.as_mut().project();
                            match ver {
                                ProtocolVersion::MQTT3 => {
                                    let state = state.map_codec(|_| v3::codec::Codec::new());
                                    this.state.set(MqttServerImplState::V3(
                                        handlers.0.call((io, state, delay)),
                                    ))
                                }
                                ProtocolVersion::MQTT5 => {
                                    let state = state.map_codec(|_| v5::codec::Codec::new());
                                    this.state.set(MqttServerImplState::V5(
                                        handlers.1.call((io, state, delay)),
                                    ))
                                }
                            }
                            continue;
                        }
                        Ok(None) => return Poll::Ready(Err(MqttError::Disconnected)),
                        Err(err) => return Poll::Ready(Err(MqttError::from(err))),
                    }
                }
            }
        }
    }
}

pub struct DefaultProtocolServer<Io, Err, InitErr, Codec> {
    ver: ProtocolVersion,
    _t: PhantomData<(Io, Err, InitErr, Codec)>,
}

impl<Io, Err, InitErr, Codec> DefaultProtocolServer<Io, Err, InitErr, Codec> {
    fn new(ver: ProtocolVersion) -> Self {
        Self { ver, _t: PhantomData }
    }
}

impl<Io, Err, InitErr, Codec> ServiceFactory for DefaultProtocolServer<Io, Err, InitErr, Codec>
where
    Codec: Encoder + Decoder,
{
    type Config = ();
    type Request = (Io, IoState<Codec>, Option<Delay>);
    type Response = ();
    type Error = MqttError<Err>;
    type Service = DefaultProtocolServer<Io, Err, InitErr, Codec>;
    type InitError = InitErr;
    type Future = Ready<Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: ()) -> Self::Future {
        ok(DefaultProtocolServer { ver: self.ver, _t: PhantomData })
    }
}

impl<Io, Err, InitErr, Codec> Service for DefaultProtocolServer<Io, Err, InitErr, Codec>
where
    Codec: Encoder + Decoder,
{
    type Request = (Io, IoState<Codec>, Option<Delay>);
    type Response = ();
    type Error = MqttError<Err>;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&self, _: Self::Request) -> Self::Future {
        err(MqttError::Protocol(ProtocolError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("Protocol is not supported: {:?}", self.ver),
        ))))
    }
}
