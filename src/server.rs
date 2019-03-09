use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use actix_codec::{AsyncRead, AsyncWrite, Decoder, Encoder, Framed};
use actix_server_config::ServerConfig;
use actix_service::boxed;
use actix_service::{
    IntoConfigurableNewService, IntoNewService, NewService, Service, ServiceExt,
};
use actix_utils::framed::{FramedTransport, FramedTransportError};
use actix_utils::inflight::InFlightService;
use actix_utils::keepalive::KeepAliveService;
use actix_utils::order::{InOrder, InOrderError};
use actix_utils::time::LowResTimeService;
use futures::future::{err, ok, Either, FutureResult};
use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::connect::{Connect, ConnectAck};
use crate::dispatcher::ServerDispatcher;
use crate::error::{MqttConnectError, MqttError, MqttPublishError};
use crate::publish::Publish;
use crate::request::IntoRequest;

/// Mqtt Server service
pub struct MqttServer<Req, Io, S, C: NewService, P, E, I> {
    connect: Rc<C>,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    time: LowResTimeService,
    _t: PhantomData<(Req, Io, S, I)>,
}

impl<Req, Io, S, C, I> MqttServer<Req, Io, S, C, NotImplemented<S, ()>, (), I>
where
    S: 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug + 'static,
{
    /// Create server factory
    pub fn new<F>(connect: F) -> MqttServer<Req, Io, S, C, NotImplemented<S, ()>, (), I>
    where
        F: IntoNewService<C>,
    {
        MqttServer {
            connect: Rc::new(connect.into_new_service()),
            publish: Rc::new(NotImplemented::default()),
            subscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            unsubscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            time: LowResTimeService::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }

    /// Set publish service
    pub fn publish<F, P1, E>(self, publish: F) -> MqttServer<Req, Io, S, C, P1, E, I>
    where
        E: fmt::Debug + 'static,
        F: IntoConfigurableNewService<P1, S>,
        P1: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>
            + 'static,
    {
        MqttServer {
            connect: self.connect,
            publish: Rc::new(publish.into_new_service()),
            subscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            unsubscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            time: self.time,
            _t: PhantomData,
        }
    }
}

impl<Req, Io, S, C, P, E, I> MqttServer<Req, Io, S, C, P, E, I>
where
    C: NewService<Request = Connect<I>>,
    C::Error: fmt::Debug + 'static,
    E: fmt::Debug + 'static,
    S: 'static,
{
    /// Set subscribe service
    pub fn subscribe<F, Srv>(mut self, subscribe: F) -> Self
    where
        F: IntoConfigurableNewService<Srv, S>,
        Srv: NewService<
                S,
                Request = mqtt::Packet,
                Response = (),
                Error = E,
                InitError = C::Error,
            > + 'static,
        Srv::Service: 'static,
    {
        self.subscribe = Rc::new(boxed::new_service(subscribe.into_new_service()));
        self
    }

    /// Set unsubscribe service
    pub fn unsubscribe<F, Srv>(mut self, unsubscribe: F) -> Self
    where
        F: IntoConfigurableNewService<Srv, S>,
        Srv: NewService<
                S,
                Request = mqtt::Packet,
                Response = (),
                Error = E,
                InitError = C::Error,
            > + 'static,
        Srv::Service: 'static,
    {
        self.unsubscribe = Rc::new(boxed::new_service(unsubscribe.into_new_service()));
        self
    }
}

impl<Req, Io, S, C, P, E, I> MqttServer<Req, Io, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug + 'static,
    E: fmt::Debug + 'static,
    S: 'static,
    I: 'static,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>
        + 'static,
{
    pub fn framed<U>(
        self,
    ) -> impl NewService<
        ServerConfig,
        Request = Req,
        Response = (),
        Error = MqttError<C::Error, E>,
        InitError = C::InitError,
    >
    where
        Req: IntoRequest<Io, U, I>,
        U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
            + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>
            + 'static,
    {
        FramedMqttServer {
            connect: self.connect,
            publish: self.publish,
            subscribe: self.subscribe,
            unsubscribe: self.unsubscribe,
            time: self.time,
            _t: PhantomData,
        }
    }
}

impl<Req, Io, S, C, P, E, I> Clone for MqttServer<Req, Io, S, C, P, E, I>
where
    C: NewService<Request = Connect<I>>,
{
    fn clone(&self) -> Self {
        MqttServer {
            connect: self.connect.clone(),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }
    }
}

impl<Req, Io, S, C, P, E, I> NewService<ServerConfig> for MqttServer<Req, Io, S, C, P, E, I>
where
    S: 'static,
    I: 'static,
    E: fmt::Debug + 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>
        + 'static,
    Req: IntoRequest<Io, mqtt::Codec, I>,
{
    type Request = Req;
    type Response = ();
    type Error = MqttError<C::Error, P::Error>;
    type Service = Server<Req, Io, S, C::Service, P, E, I>;
    type InitError = C::InitError;
    type Future = MqttServerFactory<Req, Io, S, C, P, E, I>;

    fn new_service(&self, _: &ServerConfig) -> Self::Future {
        MqttServerFactory {
            fut: self.connect.new_service(&()),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }
    }
}

pub struct MqttServerFactory<Req, Io, S, C: NewService, P, E, I> {
    fut: C::Future,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    time: LowResTimeService,
    _t: PhantomData<(Req, Io, S, E, I)>,
}

impl<Req, Io, S, C, P, E, I> Future for MqttServerFactory<Req, Io, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    S: 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>,
{
    type Item = Server<Req, Io, S, C::Service, P, E, I>;
    type Error = C::InitError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(Server(
            Cell::new(ServerInner {
                connect: try_ready!(self.fut.poll()),
                publish: self.publish.clone(),
                subscribe: self.subscribe.clone(),
                unsubscribe: self.unsubscribe.clone(),
                time: self.time.clone(),
                _t: PhantomData,
            }),
            PhantomData,
        )))
    }
}

/// Mqtt Server
pub struct Server<Req, Io, S, C: Service, P, E, I>(
    Cell<ServerInner<Io, mqtt::Codec, S, C, P, E, I>>,
    PhantomData<Req>,
);

pub(crate) struct ServerInner<Io, U, S, C: Service, P, E, I> {
    connect: C,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    time: LowResTimeService,
    _t: PhantomData<(Io, S, E, U, I)>,
}

impl<Io, U, S, C, P, E, I> ServerInner<Io, U, S, C, P, E, I>
where
    S: 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>,
    E: fmt::Debug + 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug + 'static,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>
        + 'static,
{
    fn dispatch(
        &mut self,
        packet: Option<mqtt::Packet>,
        framed: Framed<Io, U>,
        param: I,
        inner: Cell<ServerInner<Io, U, S, C, P, E, I>>,
    ) -> impl Future<Item = (), Error = MqttError<C::Error, E>> {
        match packet {
            Some(mqtt::Packet::Connect(connect)) => Either::A(
                // authenticate mqtt connection
                self.connect
                    .call(Connect::new(connect, param))
                    .map_err(|e| MqttConnectError::Service(e))
                    .and_then(move |result| match result.into_inner() {
                        either::Either::Left((session, session_present)) => Either::A(
                            framed
                                .send(mqtt::Packet::ConnectAck {
                                    session_present,
                                    return_code: mqtt::ConnectCode::ConnectionAccepted,
                                })
                                .map_err(|e| MqttConnectError::Protocol(e.into()))
                                .map(move |framed| (session, framed, inner)),
                        ),
                        either::Either::Right(code) => Either::B(
                            framed
                                .send(mqtt::Packet::ConnectAck {
                                    session_present: false,
                                    return_code: code,
                                })
                                .map_err(|e| MqttConnectError::Protocol(e.into()))
                                .and_then(|_| err(MqttConnectError::Disconnected)),
                        ),
                    }),
            ),
            Some(packet) => {
                log::info!(
                    "MQTT-3.1.0-1: Expected CONNECT packet, received {}",
                    packet.packet_type()
                );
                Either::B(err(MqttConnectError::UnexpectedPacket(packet)))
            }
            None => {
                log::trace!("mqtt client disconnected",);
                Either::B(err(MqttConnectError::Disconnected))
            }
        }
        .from_err()
        .and_then(|(session, framed, mut inner)| {
            let time = inner.time.clone();
            let inner = inner.get_mut();

            // construct publish, subscribe, unsubscribe services
            inner
                .publish
                .new_service(&session)
                .join3(
                    inner.subscribe.new_service(&session),
                    inner.unsubscribe.new_service(&session),
                )
                .map_err(|e| MqttError::from(MqttConnectError::Service(e)))
                .and_then(move |(publish, subscribe, unsubscribe)| {
                    // mqtt dispatcher pipeline
                    let service =
                            // keep-alive connection
                            KeepAliveService::new(
                                Duration::from_secs(3600), time, || MqttPublishError::KeepAliveTimeout
                            )
                            .and_then(
                                // limit number of in-flight messages
                                InFlightService::new(
                                    15,
                                    // mqtt spec requires ack ordering, so enforce response ordering
                                    InOrder::service(
                                        ServerDispatcher::new(
                                            Cell::new(session),
                                            publish,
                                            subscribe,
                                            unsubscribe))
                                        .map_err(|e| match e {
                                            InOrderError::Service(e) => e,
                                            InOrderError::Disconnected => MqttPublishError::InternalError,
                                        })
                                    ));

                    FramedTransport::new(framed, service)
                        .map_err(|e| match e {
                            FramedTransportError::Service(e) => e,
                            FramedTransportError::Decoder(e) => MqttPublishError::Protocol(e.into()),
                            FramedTransportError::Encoder(e) => MqttPublishError::Protocol(e.into()),
                        })
                        .map_err(MqttError::from)
                })
        })
    }
}

impl<Req, Io, S, C, P, E, I> Service for Server<Req, Io, S, C, P, E, I>
where
    S: 'static,
    I: 'static,
    E: fmt::Debug + 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>
        + 'static,
    Req: IntoRequest<Io, mqtt::Codec, I>,
{
    type Request = Req;
    type Response = ();
    type Error = MqttError<C::Error, P::Error>;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.0
            .get_mut()
            .connect
            .poll_ready()
            .map_err(|e| MqttConnectError::Service(e).into())
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let (framed, param) = req.into_request().into_parts();
        let mut inner = self.0.clone();

        Box::new(
            framed
                .into_future()
                .map_err(|(e, _)| MqttError::from(MqttConnectError::Protocol(e.into())))
                .and_then(move |(packet, framed)| {
                    let inner2 = inner.clone();
                    inner.get_mut().dispatch(packet, framed, param, inner2)
                }),
        )
    }
}

/// Mqtt Server service
struct FramedMqttServer<Req, Io, U, S, C: NewService, P, E, I> {
    connect: Rc<C>,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    time: LowResTimeService,
    _t: PhantomData<(Req, Io, U, S, I)>,
}

impl<Req, Io, U, S, C, P, E, I> NewService<ServerConfig>
    for FramedMqttServer<Req, Io, U, S, C, P, E, I>
where
    S: 'static,
    I: 'static,
    E: fmt::Debug + 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>
        + 'static,
    Req: IntoRequest<Io, U, I>,
{
    type Request = Req;
    type Response = ();
    type Error = MqttError<C::Error, P::Error>;
    type Service = FramedServer<Req, Io, U, S, C::Service, P, E, I>;
    type InitError = C::InitError;
    type Future = FramedMqttServerFactory<Req, Io, U, S, C, P, E, I>;

    fn new_service(&self, _: &ServerConfig) -> Self::Future {
        FramedMqttServerFactory {
            fut: self.connect.new_service(&()),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }
    }
}

struct FramedMqttServerFactory<Req, Io, U, S, C: NewService, P, E, I> {
    fut: C::Future,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, C::Error>>,
    time: LowResTimeService,
    _t: PhantomData<(Req, Io, U, S, E, I)>,
}

impl<Req, Io, U, S, C, P, E, I> Future for FramedMqttServerFactory<Req, Io, U, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>,
    S: 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>,
{
    type Item = FramedServer<Req, Io, U, S, C::Service, P, E, I>;
    type Error = C::InitError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(FramedServer(
            Cell::new(ServerInner {
                connect: try_ready!(self.fut.poll()),
                publish: self.publish.clone(),
                subscribe: self.subscribe.clone(),
                unsubscribe: self.unsubscribe.clone(),
                time: self.time.clone(),
                _t: PhantomData,
            }),
            PhantomData,
        )))
    }
}

/// Mqtt Server
pub struct FramedServer<Req, Io, U, S, C: Service, P, E, I>(
    Cell<ServerInner<Io, U, S, C, P, E, I>>,
    PhantomData<Req>,
);

impl<Req, Io, U, S, C, P, E, I> Service for FramedServer<Req, Io, U, S, C, P, E, I>
where
    S: 'static,
    I: 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + 'static,
    E: fmt::Debug + 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = Connect<I>, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    P: NewService<S, Request = Publish<S>, Response = (), Error = E, InitError = C::Error>
        + 'static,
    Req: IntoRequest<Io, U, I>,
{
    type Request = Req;
    type Response = ();
    type Error = MqttError<C::Error, P::Error>;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.0
            .get_mut()
            .connect
            .poll_ready()
            .map_err(|e| MqttConnectError::Service(e).into())
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let (framed, param) = req.into_request().into_parts();
        let mut inner = self.0.clone();

        Box::new(
            framed
                .into_future()
                .map_err(|(e, _)| MqttError::from(MqttConnectError::Protocol(e.into())))
                .and_then(move |(packet, framed)| {
                    let inner2 = inner.clone();
                    inner.get_mut().dispatch(packet, framed, param, inner2)
                }),
        )
    }
}

/// Not implemented publish service
pub struct NotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for NotImplemented<S, E> {
    fn default() -> Self {
        NotImplemented(PhantomData)
    }
}

impl<S, E> NewService<S> for NotImplemented<S, E> {
    type Request = Publish<S>;
    type Response = ();
    type Error = E;
    type InitError = ();
    type Service = NotImplemented<S, E>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        ok(NotImplemented(PhantomData))
    }
}

impl<S, E> Service for NotImplemented<S, E> {
    type Request = Publish<S>;
    type Response = ();
    type Error = E;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: Publish<S>) -> Self::Future {
        log::warn!("MQTT Publish is not implemented");
        ok(())
    }
}

/// Not implemented subscribe service
pub struct SubsNotImplemented<S, E1, E2>(PhantomData<(S, E1, E2)>);

impl<S, E1, E2> Default for SubsNotImplemented<S, E1, E2> {
    fn default() -> Self {
        SubsNotImplemented(PhantomData)
    }
}

impl<S, E1, E2> NewService<S> for SubsNotImplemented<S, E1, E2> {
    type Request = mqtt::Packet;
    type Response = ();
    type Error = E1;
    type InitError = E2;
    type Service = SubsNotImplemented<S, E1, E2>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        ok(SubsNotImplemented(PhantomData))
    }
}

impl<S, E1, E2> Service for SubsNotImplemented<S, E1, E2> {
    type Request = mqtt::Packet;
    type Response = ();
    type Error = E1;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: mqtt::Packet) -> Self::Future {
        log::warn!("MQTT Subscribe is not implemented");
        ok(())
    }
}
