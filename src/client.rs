use std::marker::PhantomData;
use std::rc::Rc;

use actix_codec::{AsyncRead, AsyncWrite};
use actix_ioframe as ioframe;
use actix_service::{boxed, IntoNewService, IntoService, NewService, Service, ServiceExt};
use bytes::Bytes;
use futures::future::{err, Either};
use futures::{Future, Poll, Sink, Stream};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::default::{SubsNotImplemented, UnsubsNotImplemented};
use crate::dispatcher::{dispatcher, MqttState};
use crate::error::MqttError;
use crate::publish::Publish;
use crate::sink::MqttSink;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};
use crate::State;

/// Mqtt client
#[derive(Clone)]
pub struct Client<Io, St> {
    client_id: string::String<Bytes>,
    clean_session: bool,
    protocol: mqtt::Protocol,
    keep_alive: u16,
    last_will: Option<mqtt::LastWill>,
    username: Option<string::String<Bytes>>,
    password: Option<Bytes>,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St> Client<Io, St>
where
    St: 'static,
{
    /// Create new client and provide client id
    pub fn new(client_id: string::String<Bytes>) -> Self {
        Client {
            client_id,
            clean_session: true,
            protocol: mqtt::Protocol::default(),
            keep_alive: 30,
            last_will: None,
            username: None,
            password: None,
            _t: PhantomData,
        }
    }

    /// Mqtt protocol version
    pub fn protocol(mut self, val: mqtt::Protocol) -> Self {
        self.protocol = val;
        self
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
    pub fn username(mut self, val: string::String<Bytes>) -> Self {
        self.username = Some(val);
        self
    }

    /// Password can be used by the Server for authentication and authorization.
    pub fn password(mut self, val: Bytes) -> Self {
        self.password = Some(val);
        self
    }

    /// Set state service
    ///
    /// State service verifies connect ack packet and construct connection state.
    pub fn state<C, F>(self, state: F) -> ServiceBuilder<Io, St, C>
    where
        F: IntoService<C>,
        Io: AsyncRead + AsyncWrite,
        C: Service<Request = ConnectAck<Io>, Response = ConnectAckResult<Io, St>>,
        C::Error: 'static,
    {
        ServiceBuilder {
            state: Cell::new(state.into_service()),
            packet: mqtt::Connect {
                client_id: self.client_id,
                clean_session: self.clean_session,
                protocol: self.protocol,
                keep_alive: self.keep_alive,
                last_will: self.last_will,
                username: self.username,
                password: self.password,
            },
            subscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            unsubscribe: Rc::new(boxed::new_service(UnsubsNotImplemented::default())),
            disconnect: None,
            _t: PhantomData,
        }
    }
}

pub struct ServiceBuilder<Io, St, C: Service> {
    state: Cell<C>,
    packet: mqtt::Connect,
    subscribe: Rc<
        boxed::BoxedNewService<
            St,
            Subscribe<St>,
            SubscribeResult,
            MqttError<C::Error>,
            MqttError<C::Error>,
        >,
    >,
    unsubscribe: Rc<
        boxed::BoxedNewService<
            St,
            Unsubscribe<St>,
            (),
            MqttError<C::Error>,
            MqttError<C::Error>,
        >,
    >,
    disconnect: Option<Cell<boxed::BoxedService<State<St>, (), MqttError<C::Error>>>>,

    _t: PhantomData<(Io, St, C)>,
}

impl<Io, St, C> ServiceBuilder<Io, St, C>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = ConnectAck<Io>, Response = ConnectAckResult<Io, St>> + 'static,
    C::Error: 'static,
{
    /// Service to execute for subscribe packet
    pub fn subscribe<F, Srv>(mut self, subscribe: F) -> Self
    where
        F: IntoNewService<Srv>,
        Srv: NewService<
                Config = St,
                Request = Subscribe<St>,
                Response = SubscribeResult,
                InitError = C::Error,
                Error = C::Error,
            > + 'static,
        Srv::Service: 'static,
    {
        self.subscribe = Rc::new(boxed::new_service(
            subscribe
                .into_new_service()
                .map_err(|e| MqttError::Service(e))
                .map_init_err(|e| MqttError::Service(e)),
        ));
        self
    }

    /// Service to execute for unsubscribe packet
    pub fn unsubscribe<F, Srv>(mut self, unsubscribe: F) -> Self
    where
        F: IntoNewService<Srv>,
        Srv: NewService<
                Config = St,
                Request = Unsubscribe<St>,
                Response = (),
                InitError = C::Error,
                Error = C::Error,
            > + 'static,
        Srv::Service: 'static,
    {
        self.unsubscribe = Rc::new(boxed::new_service(
            unsubscribe
                .into_new_service()
                .map_err(|e| MqttError::Service(e))
                .map_init_err(|e| MqttError::Service(e)),
        ));
        self
    }

    /// Service to execute on disconnect
    pub fn disconnect<UF, U>(mut self, srv: UF) -> Self
    where
        UF: IntoService<U>,
        U: Service<Request = State<St>, Response = (), Error = C::Error> + 'static,
    {
        self.disconnect = Some(Cell::new(boxed::service(
            srv.into_service().map_err(|e| MqttError::Service(e)),
        )));
        self
    }

    pub fn finish<F, T>(
        self,
        service: F,
    ) -> impl Service<Request = Io, Response = (), Error = MqttError<C::Error>>
    where
        F: IntoNewService<T>,
        T: NewService<
                Config = St,
                Request = Publish<St>,
                Response = (),
                Error = C::Error,
                InitError = C::Error,
            > + 'static,
    {
        ioframe::Builder::new()
            .service(ConnectService {
                connect: self.state,
                packet: self.packet,
                _t: PhantomData,
            })
            .finish(dispatcher(
                service
                    .into_new_service()
                    .map_err(|e| MqttError::Service(e))
                    .map_init_err(|e| MqttError::Service(e)),
                self.subscribe,
                self.unsubscribe,
            ))
            .map_err(|e| match e {
                ioframe::ServiceError::Service(e) => e,
                ioframe::ServiceError::Encoder(e) => MqttError::Protocol(e),
                ioframe::ServiceError::Decoder(e) => MqttError::Protocol(e),
            })
    }
}

struct ConnectService<Io, St, C> {
    connect: Cell<C>,
    packet: mqtt::Connect,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St, C> Service for ConnectService<Io, St, C>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = ConnectAck<Io>, Response = ConnectAckResult<Io, St>> + 'static,
    C::Error: 'static,
{
    type Request = ioframe::Connect<Io>;
    type Response = ioframe::ConnectResult<Io, MqttState<St>, mqtt::Codec>;
    type Error = MqttError<C::Error>;
    type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.connect
            .get_mut()
            .poll_ready()
            .map_err(|e| MqttError::Service(e))
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        let mut srv = self.connect.clone();

        // send Connect packet
        Box::new(
            req.codec(mqtt::Codec::new())
                .send(mqtt::Packet::Connect(self.packet.clone()))
                .map_err(|e| MqttError::Protocol(e))
                .and_then(|framed| {
                    framed
                        .into_future()
                        .map_err(|(e, _)| MqttError::Protocol(e))
                })
                .and_then(move |(packet, framed)| match packet {
                    Some(mqtt::Packet::ConnectAck {
                        session_present,
                        return_code,
                    }) => {
                        let sink = MqttSink::new(framed.sink().clone());
                        let ack = ConnectAck {
                            sink,
                            session_present,
                            return_code,
                            io: framed,
                        };
                        Either::A(
                            srv.get_mut()
                                .call(ack)
                                .map_err(|e| MqttError::Service(e))
                                .map(|ack| ack.io.state(ack.state)),
                        )
                    }
                    Some(p) => {
                        Either::B(err(MqttError::Unexpected(p, "Expected CONNECT-ACK packet")))
                    }
                    None => Either::B(err(MqttError::Disconnected)),
                }),
        )
    }
}

pub struct ConnectAck<Io> {
    io: ioframe::ConnectResult<Io, (), mqtt::Codec>,
    sink: MqttSink,
    session_present: bool,
    return_code: mqtt::ConnectCode,
}

impl<Io> ConnectAck<Io> {
    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.session_present
    }

    #[inline]
    /// Connect return code
    pub fn return_code(&self) -> mqtt::ConnectCode {
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
            state: MqttState::new(state, self.sink),
        }
    }
}

impl<Io> futures::Stream for ConnectAck<Io>
where
    Io: AsyncRead + AsyncWrite,
{
    type Item = mqtt::Packet;
    type Error = mqtt::ParseError;

    fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
        self.io.poll()
    }
}

impl<Io> futures::Sink for ConnectAck<Io>
where
    Io: AsyncRead + AsyncWrite,
{
    type SinkItem = mqtt::Packet;
    type SinkError = mqtt::ParseError;

    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> futures::StartSend<Self::SinkItem, Self::SinkError> {
        self.io.start_send(item)
    }

    fn poll_complete(&mut self) -> futures::Poll<(), Self::SinkError> {
        self.io.poll_complete()
    }

    fn close(&mut self) -> futures::Poll<(), Self::SinkError> {
        self.io.close()
    }
}

pub struct ConnectAckResult<Io, St> {
    state: MqttState<St>,
    io: ioframe::ConnectResult<Io, (), mqtt::Codec>,
}

impl<Io, St> futures::Stream for ConnectAckResult<Io, St>
where
    Io: AsyncRead + AsyncWrite,
{
    type Item = mqtt::Packet;
    type Error = mqtt::ParseError;

    fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
        self.io.poll()
    }
}

impl<Io, St> futures::Sink for ConnectAckResult<Io, St>
where
    Io: AsyncRead + AsyncWrite,
{
    type SinkItem = mqtt::Packet;
    type SinkError = mqtt::ParseError;

    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> futures::StartSend<Self::SinkItem, Self::SinkError> {
        self.io.start_send(item)
    }

    fn poll_complete(&mut self) -> futures::Poll<(), Self::SinkError> {
        self.io.poll_complete()
    }

    fn close(&mut self) -> futures::Poll<(), Self::SinkError> {
        self.io.close()
    }
}
