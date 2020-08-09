use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::{Sink, SinkExt, Stream, StreamExt};
use ntex::channel::mpsc;
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::service::{boxed, IntoService, IntoServiceFactory, Service, ServiceFactory};

use crate::error::{DecodeError, EncodeError, MqttError};
use crate::framed::DispatcherError;
use crate::handshake::{Handshake, HandshakeResult};
use crate::service::Builder;

use super::control::{ControlPacket, ControlResult};
use super::default::DefaultControlService;
use super::dispatcher::factory;
use super::publish::Publish;
use super::sink::MqttSink;
use super::{codec as mqtt, Session};

/// Mqtt client
#[derive(Clone)]
pub struct Client<Io, St> {
    client_id: ByteString,
    clean_session: bool,
    keep_alive: u16,
    last_will: Option<mqtt::LastWill>,
    username: Option<ByteString>,
    password: Option<Bytes>,
    inflight: usize,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St> Client<Io, St>
where
    St: 'static,
{
    /// Create new client and provide client id
    pub fn new(client_id: ByteString) -> Self {
        Client {
            client_id,
            clean_session: true,
            keep_alive: 30,
            last_will: None,
            username: None,
            password: None,
            inflight: 15,
            _t: PhantomData,
        }
    }

    /// The handling of the Session state.
    pub fn clean_session(mut self, val: bool) -> Self {
        self.clean_session = val;
        self
    }

    /// A time interval measured in seconds.
    ///
    /// keep-alive is set to 30 seconds by default.
    pub fn keep_alive(mut self, val: u16) -> Self {
        self.keep_alive = val;
        self
    }

    /// Will Message be stored on the Server and associated with the Network Connection.
    ///
    /// by default last will value is not set
    pub fn last_will(mut self, val: mqtt::LastWill) -> Self {
        self.last_will = Some(val);
        self
    }

    /// Username can be used by the Server for authentication and authorization.
    pub fn username(mut self, val: ByteString) -> Self {
        self.username = Some(val);
        self
    }

    /// Password can be used by the Server for authentication and authorization.
    pub fn password(mut self, val: Bytes) -> Self {
        self.password = Some(val);
        self
    }

    /// Number of in-flight concurrent messages.
    ///
    /// in-flight is set to 15 messages
    pub fn inflight(mut self, val: usize) -> Self {
        self.inflight = val;
        self
    }

    /// Set state service
    ///
    /// State service verifies connect ack packet and construct connection state.
    pub fn state<C, F>(self, state: F) -> ServiceBuilder<Io, St, C>
    where
        F: IntoService<C>,
        Io: AsyncRead + AsyncWrite + Unpin,
        C: Service<Request = ConnectAck<Io>, Response = ConnectAckResult<Io, St>>,
        C::Error: fmt::Debug + 'static,
    {
        ServiceBuilder {
            state: Rc::new(state.into_service()),
            packet: mqtt::Connect {
                client_id: self.client_id,
                clean_session: self.clean_session,
                keep_alive: self.keep_alive,
                last_will: self.last_will,
                username: self.username,
                password: self.password,
            },
            control: boxed::factory(
                DefaultControlService::default()
                    .map_init_err(|_: C::Error| unreachable!())
                    .map_err(|_| unreachable!()),
            ),
            keep_alive: self.keep_alive.into(),
            inflight: self.inflight,
            _t: PhantomData,
        }
    }
}

pub struct ServiceBuilder<Io, St, C: Service> {
    state: Rc<C>,
    packet: mqtt::Connect,
    keep_alive: u64,
    inflight: usize,
    control: boxed::BoxServiceFactory<
        Session<St>,
        ControlPacket,
        ControlResult,
        MqttError<C::Error>,
        MqttError<C::Error>,
    >,

    _t: PhantomData<(Io, St, C)>,
}

impl<Io, St, C> ServiceBuilder<Io, St, C>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = ConnectAck<Io>, Response = ConnectAckResult<Io, St>> + 'static,
    C::Error: fmt::Debug + 'static,
{
    pub fn finish<F, T>(
        self,
        service: F,
    ) -> impl Service<Request = Io, Response = (), Error = MqttError<C::Error>>
    where
        F: IntoServiceFactory<T>,
        T: ServiceFactory<
                Config = Session<St>,
                Request = Publish,
                Response = (),
                Error = C::Error,
                InitError = C::Error,
            > + 'static,
    {
        Builder::new(ConnectService {
            connect: self.state,
            packet: self.packet,
            keep_alive: self.keep_alive,
            inflight: self.inflight,
            _t: PhantomData,
        })
        .build(factory(
            service.into_factory().map_err(MqttError::Service).map_init_err(MqttError::Service),
            self.control,
        ))
        .map_err(|e| match e {
            DispatcherError::Service(e) => e,
            DispatcherError::Encoder(e) => MqttError::Encode(e),
            DispatcherError::Decoder(e) => MqttError::Decode(e),
        })
    }
}

struct ConnectService<Io, St, C> {
    connect: Rc<C>,
    packet: mqtt::Connect,
    keep_alive: u64,
    inflight: usize,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St, C> Service for ConnectService<Io, St, C>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = ConnectAck<Io>, Response = ConnectAckResult<Io, St>> + 'static,
    C::Error: fmt::Debug + 'static,
{
    type Request = Handshake<Io, mqtt::Codec>;
    type Response = HandshakeResult<Io, Session<St>, mqtt::Codec, mpsc::Receiver<mqtt::Packet>>;
    type Error = MqttError<C::Error>;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connect.as_ref().poll_ready(cx).map_err(MqttError::Service)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        self.connect.as_ref().poll_shutdown(cx, is_error)
    }

    fn call(&self, req: Self::Request) -> Self::Future {
        let srv = self.connect.clone();
        let packet = self.packet.clone();
        let keep_alive = Duration::from_secs(self.keep_alive as u64);
        let inflight = self.inflight;

        // send Connect packet
        async move {
            let mut framed = req.codec(mqtt::Codec::new());
            framed.send(mqtt::Packet::Connect(packet)).await.map_err(MqttError::Encode)?;

            let packet = framed
                .next()
                .await
                .ok_or_else(|| {
                    log::trace!("Client mqtt is disconnected during handshake");
                    MqttError::Disconnected
                })
                .and_then(|res| res.map_err(MqttError::Decode))?;

            match packet {
                mqtt::Packet::ConnectAck { session_present, return_code } => {
                    let (tx, rx) = mpsc::channel();
                    let sink = MqttSink::new(tx);
                    let ack = ConnectAck {
                        sink,
                        session_present,
                        return_code,
                        keep_alive,
                        inflight,
                        io: framed,
                    };
                    Ok(srv
                        .as_ref()
                        .call(ack)
                        .await
                        .map_err(MqttError::Service)
                        .map(move |ack| ack.io.out(rx).state(ack.state))?)
                }
                p => Err(MqttError::Unexpected(p.packet_type(), "Expected CONNECT-ACK packet")),
            }
        }
        .boxed_local()
    }
}

pub struct ConnectAck<Io> {
    io: HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
    sink: MqttSink,
    session_present: bool,
    return_code: mqtt::ConnectAckReason,
    keep_alive: Duration,
    inflight: usize,
}

impl<Io> ConnectAck<Io> {
    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.session_present
    }

    #[inline]
    /// Connect return code
    pub fn return_code(&self) -> mqtt::ConnectAckReason {
        self.return_code
    }

    #[inline]
    /// Mqtt client sink object
    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }

    #[inline]
    /// Set connection state and create result object
    pub fn state<St>(self, state: St) -> ConnectAckResult<Io, St> {
        ConnectAckResult {
            io: self.io,
            state: Session::new(state, self.sink, self.keep_alive, self.inflight),
        }
    }
}

impl<Io> Stream for ConnectAck<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin + Unpin,
{
    type Item = Result<mqtt::Packet, DecodeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.io).poll_next(cx)
    }
}

impl<Io> Sink<mqtt::Packet> for ConnectAck<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    type Error = EncodeError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: mqtt::Packet) -> Result<(), Self::Error> {
        Pin::new(&mut self.io).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_close(cx)
    }
}

pin_project_lite::pin_project! {
    pub struct ConnectAckResult<Io, St> {
        state: Session<St>,
        io: HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
    }
}

impl<Io, St> Stream for ConnectAckResult<Io, St>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<mqtt::Packet, DecodeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.io).poll_next(cx)
    }
}

impl<Io, St> Sink<mqtt::Packet> for ConnectAckResult<Io, St>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    type Error = EncodeError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: mqtt::Packet) -> Result<(), Self::Error> {
        Pin::new(&mut self.io).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_close(cx)
    }
}
