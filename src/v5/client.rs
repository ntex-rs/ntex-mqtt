use std::task::{Context, Poll};
use std::time::Duration;
use std::{fmt, io, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use either::Either;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::{Sink, SinkExt, Stream, StreamExt};
use ntex::channel::mpsc;
use ntex::service::{boxed, IntoService, IntoServiceFactory, Service, ServiceFactory};
use ntex_codec::{AsyncRead, AsyncWrite};

use crate::error::{DecodeError, EncodeError, MqttError, ProtocolError};
use crate::handshake::{Handshake, HandshakeResult};
use crate::service::Builder;

use super::control::{ControlPacket, ControlResult};
use super::default::DefaultControlService;
use super::dispatcher::factory;
use super::publish::{Publish, PublishAck};
use super::sink::MqttSink;
use super::{codec, Session};

/// Mqtt client
#[derive(Clone)]
pub struct Client<Io, St> {
    keep_alive: u16,
    max_receive: u16,
    connect: codec::Connect,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St> Client<Io, St>
where
    St: 'static,
{
    /// Create new client and provide client id
    pub fn new(client_id: ByteString) -> Self {
        Client {
            keep_alive: 30,
            max_receive: 16,
            connect: codec::Connect { client_id, ..Default::default() },
            _t: PhantomData,
        }
    }

    /// The handling of the Session state.
    pub fn clean_start(mut self) -> Self {
        self.connect.clean_start = true;
        self
    }

    /// A time interval measured in seconds.
    ///
    /// keep-alive is set to 30 seconds by default.
    pub fn keep_alive(mut self, val: u16) -> Self {
        self.connect.keep_alive = val;
        self
    }

    /// Will Message be stored on the Server and associated with the Network Connection.
    ///
    /// by default last will value is not set
    pub fn last_will(mut self, val: codec::LastWill) -> Self {
        self.connect.last_will = Some(val);
        self
    }

    /// Username can be used by the Server for authentication and authorization.
    pub fn username(mut self, val: ByteString) -> Self {
        self.connect.username = Some(val);
        self
    }

    /// Password can be used by the Server for authentication and authorization.
    pub fn password(mut self, val: Bytes) -> Self {
        self.connect.password = Some(val);
        self
    }

    /// Set `receive max`
    ///
    /// Number of in-flight publish packets. By default receive max is set to 15 packets.
    /// To disable timeout set value to 0.
    pub fn receive_max(mut self, val: u16) -> Self {
        self.max_receive = val;
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
            packet: self.connect.clone(),
            state: Rc::new(state.into_service()),
            control: boxed::factory(
                DefaultControlService::default()
                    .map_init_err(|_: C::Error| unreachable!())
                    .map_err(|_| unreachable!()),
            ),
            keep_alive: self.keep_alive.into(),
            max_receive: self.max_receive,
            _t: PhantomData,
        }
    }
}

pub struct ServiceBuilder<Io, St, C: Service> {
    state: Rc<C>,
    packet: codec::Connect,
    keep_alive: u64,
    max_receive: u16,
    control: boxed::BoxServiceFactory<
        Session<St>,
        ControlPacket<C::Error>,
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
                Response = PublishAck,
                Error = C::Error,
                InitError = C::Error,
            > + 'static,
    {
        Builder::new(ConnectService {
            connect: self.state,
            packet: self.packet,
            keep_alive: self.keep_alive,
            max_receive: self.max_receive,
            _t: PhantomData,
        })
        .build(factory(
            service.into_factory()
                .map_err(<C::Error>::from)
                .map_init_err(MqttError::Service),
            self.control,
            0,
            16,
        ))
    }
}

struct ConnectService<Io, St, C> {
    connect: Rc<C>,
    packet: codec::Connect,
    keep_alive: u64,
    max_receive: u16,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St, C> Service for ConnectService<Io, St, C>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Service<Request = ConnectAck<Io>, Response = ConnectAckResult<Io, St>> + 'static,
    C::Error: fmt::Debug + 'static,
{
    type Request = Handshake<Io, codec::Codec>;
    type Response =
        HandshakeResult<Io, Session<St>, codec::Codec, mpsc::Receiver<codec::Packet>>;
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
        let max_receive = self.max_receive;
        if max_receive > 0 {
            packet.receive_max = Some(NonZeroU16::new(max_receive).unwrap())
        }

        // send Connect packet
        async move {
            let mut framed = req.codec(codec::Codec::new());
            framed.set_keepalive_timeout(keep_alive);
            framed.send(codec::Packet::Connect(packet)).await.map_err(MqttError::from)?;

            let packet = framed
                .next()
                .await
                .ok_or_else(|| {
                    log::trace!("Client mqtt is disconnected during handshake");
                    MqttError::Disconnected
                })
                .and_then(|res| res.map_err(From::from))?;

            match packet {
                codec::Packet::ConnectAck(packet) => {
                    let (tx, rx) = mpsc::channel();
                    let sink = MqttSink::new(
                        tx,
                        packet.receive_max.map(|v| v.get()).unwrap_or(16) as usize,
                    );
                    let ack = ConnectAck { sink, packet, io: framed };
                    Ok(srv
                        .as_ref()
                        .call(ack)
                        .await
                        .map_err(MqttError::Service)
                        .map(move |ack| ack.io.out(rx).state(ack.state))?)
                }
                p => Err(MqttError::Protocol(ProtocolError::Unexpected(
                    p.packet_type(),
                    "Expected CONNECT-ACK packet",
                ))),
            }
        }
        .boxed_local()
    }
}

pub struct ConnectAck<Io> {
    io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
    sink: MqttSink,
    packet: codec::ConnectAck,
}

impl<Io> ConnectAck<Io> {
    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.packet.session_present
    }

    #[inline]
    /// Connect return code
    pub fn reason_code(&self) -> codec::ConnectAckReason {
        self.packet.reason_code
    }

    #[inline]
    /// Mqtt client sink object
    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }

    #[inline]
    /// Set connection state and create result object
    pub fn state<St>(self, state: St) -> ConnectAckResult<Io, St> {
        ConnectAckResult { io: self.io, state: Session::new(state, self.sink) }
    }
}

impl<Io> Stream for ConnectAck<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin + Unpin,
{
    type Item = Result<codec::Packet, Either<DecodeError, io::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.io).poll_next(cx)
    }
}

impl<Io> Sink<codec::Packet> for ConnectAck<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    type Error = Either<EncodeError, io::Error>;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: codec::Packet) -> Result<(), Self::Error> {
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
        io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
    }
}

impl<Io, St> Stream for ConnectAckResult<Io, St>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<codec::Packet, Either<DecodeError, io::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.io).poll_next(cx)
    }
}

impl<Io, St> Sink<codec::Packet> for ConnectAckResult<Io, St>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    type Error = Either<EncodeError, io::Error>;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: codec::Packet) -> Result<(), Self::Error> {
        Pin::new(&mut self.io).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.io).poll_close(cx)
    }
}
