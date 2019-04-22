use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use actix_codec::{AsyncRead, AsyncWrite, Decoder, Encoder, Framed};
use actix_server_config::{Io as ServerIo, ServerConfig};
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
use futures::unsync::mpsc;
use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::connect::{Connect, ConnectAck};
use crate::dispatcher::ServerDispatcher;
use crate::error::MqttError;
use crate::publish::Publish;
use crate::sink::MqttSink;
use crate::subs::{Subscribe, SubscribeResult};

/// Mqtt Server service
pub struct MqttServer<Io, S, C: NewService, P, E, I> {
    connect: Rc<C>,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, Subscribe<S>, SubscribeResult, E, E>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, E>>,
    time: LowResTimeService,
    on_close: Option<Rc<Fn(&mut S)>>,
    _t: PhantomData<(Io, S, I)>,
}

impl<Io, S, C, E, I> MqttServer<Io, S, C, NotImplemented<S, E>, E, I>
where
    S: 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    E: From<C::InitError> + 'static,
{
    /// Create server factory
    pub fn new<F>(connect: F) -> MqttServer<Io, S, C, NotImplemented<S, E>, E, I>
    where
        F: IntoNewService<C>,
    {
        MqttServer {
            connect: Rc::new(connect.into_new_service()),
            publish: Rc::new(NotImplemented::default()),
            subscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            unsubscribe: Rc::new(boxed::new_service(UnsubsNotImplemented::default())),
            time: LowResTimeService::with(Duration::from_secs(1)),
            on_close: None,
            _t: PhantomData,
        }
    }

    /// Set publish service
    pub fn publish<F, P1>(self, publish: F) -> MqttServer<Io, S, C, P1, E, I>
    where
        F: IntoConfigurableNewService<P1, S>,
        P1: NewService<S, Request = Publish<S>, Response = ()> + 'static,
        E: From<P1::Error> + From<P1::InitError> + 'static,
    {
        MqttServer {
            connect: self.connect,
            publish: Rc::new(publish.into_new_service()),
            subscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            unsubscribe: Rc::new(boxed::new_service(UnsubsNotImplemented::default())),
            time: self.time,
            on_close: self.on_close,
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P, E, I> MqttServer<Io, S, C, P, E, I>
where
    C: NewService,
    S: 'static,
{
    /// Set subscribe service
    pub fn subscribe<F, Srv>(mut self, subscribe: F) -> Self
    where
        F: IntoConfigurableNewService<Srv, S>,
        Srv: NewService<S, Request = Subscribe<S>, Response = SubscribeResult> + 'static,
        Srv::Service: 'static,
        E: From<Srv::Error> + From<Srv::InitError> + 'static,
    {
        self.subscribe = Rc::new(boxed::new_service(
            subscribe
                .into_new_service()
                .map_err(|e| e.into())
                .map_init_err(|e| e.into()),
        ));
        self
    }

    /// Set unsubscribe service
    pub fn unsubscribe<F, Srv>(mut self, unsubscribe: F) -> Self
    where
        F: IntoConfigurableNewService<Srv, S>,
        Srv: NewService<S, Request = mqtt::Packet, Response = ()> + 'static,
        Srv::Service: 'static,
        E: From<Srv::Error> + From<Srv::InitError> + 'static,
    {
        self.unsubscribe = Rc::new(boxed::new_service(
            unsubscribe
                .into_new_service()
                .map_err(|e| e.into())
                .map_init_err(|e| e.into()),
        ));
        self
    }

    pub fn on_close<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut S) + 'static,
    {
        self.on_close = Some(Rc::new(f));
        self
    }
}

impl<Io, S, C, P, E, I> MqttServer<Io, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    E: From<C::InitError> + From<P::Error> + From<P::InitError> + 'static,
    S: 'static,
    I: 'static,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
{
    pub fn framed<U>(
        self,
    ) -> impl NewService<
        ServerConfig,
        Request = (Framed<Io, U>, I),
        Response = (),
        Error = MqttError<E>,
        InitError = MqttError<E>,
    >
    where
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
            on_close: self.on_close,
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P, E, I> Clone for MqttServer<Io, S, C, P, E, I>
where
    C: NewService,
{
    fn clone(&self) -> Self {
        MqttServer {
            connect: self.connect.clone(),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            on_close: self.on_close.clone(),
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P, E, I> NewService<ServerConfig> for MqttServer<Io, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    E: From<C::InitError> + From<P::Error> + From<P::InitError> + 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
    S: 'static,
    I: 'static,
{
    type Request = ServerIo<Io, I>;
    type Response = ();
    type Error = MqttError<E>;
    type Service = Server<Io, S, C::Service, P, E, I>;
    type InitError = MqttError<E>;
    type Future = MqttServerFactory<Io, S, C, P, E, I>;

    fn new_service(&self, _: &ServerConfig) -> Self::Future {
        MqttServerFactory {
            fut: self.connect.new_service(&()),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            on_close: self.on_close.clone(),
            _t: PhantomData,
        }
    }
}

pub struct MqttServerFactory<Io, S, C: NewService, P, E, I> {
    fut: C::Future,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, Subscribe<S>, SubscribeResult, E, E>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, E>>,
    time: LowResTimeService,
    on_close: Option<Rc<Fn(&mut S)>>,
    _t: PhantomData<(Io, S, E, I)>,
}

impl<Io, S, C, P, E, I> Future for MqttServerFactory<Io, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<S, Request = Publish<S>, Response = ()>,
    E: From<C::InitError> + From<P::Error> + From<P::InitError> + 'static,
{
    type Item = Server<Io, S, C::Service, P, E, I>;
    type Error = MqttError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(Server(Cell::new(ServerInner {
            connect: try_ready!(self.fut.poll().map_err(|e| MqttError::Service(e.into()))),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            on_close: self.on_close.clone(),
            _t: PhantomData,
        }))))
    }
}

/// Mqtt Server
pub struct Server<Io, S, C: Service, P, E, I>(
    Cell<ServerInner<Io, mqtt::Codec, S, C, P, E, I>>,
);

pub(crate) struct ServerInner<Io, U, S, C: Service, P, E, I> {
    connect: C,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, Subscribe<S>, SubscribeResult, E, E>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, E>>,
    time: LowResTimeService,
    on_close: Option<Rc<Fn(&mut S)>>,
    _t: PhantomData<(Io, S, E, U, I)>,
}

impl<Io, U, S, C, P, E, I> ServerInner<Io, U, S, C, P, E, I>
where
    S: 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
    E: From<P::Error> + From<P::InitError> + 'static,
{
    fn dispatch(
        &mut self,
        packet: Option<mqtt::Packet>,
        framed: Framed<Io, U>,
        param: I,
        inner: Cell<ServerInner<Io, U, S, C, P, E, I>>,
    ) -> impl Future<Item = (), Error = MqttError<E>> {
        let on_close = self.on_close.clone();

        match packet {
            Some(mqtt::Packet::Connect(connect)) => {
                let (tx, rx) = mpsc::unbounded();
                Either::A(
                    // authenticate mqtt connection
                    self.connect
                        .call(Connect::new(connect, param, MqttSink::new(tx)))
                        .map_err(|e| MqttError::Service(e.into()))
                        .and_then(move |result| match result.into_inner() {
                            either::Either::Left((session, session_present)) => Either::A(
                                framed
                                    .send(mqtt::Packet::ConnectAck {
                                        session_present,
                                        return_code: mqtt::ConnectCode::ConnectionAccepted,
                                    })
                                    .map_err(|e| MqttError::Protocol(e.into()))
                                    .map(move |framed| (session, framed, inner, rx)),
                            ),
                            either::Either::Right(code) => Either::B(
                                framed
                                    .send(mqtt::Packet::ConnectAck {
                                        session_present: false,
                                        return_code: code,
                                    })
                                    .map_err(|e| MqttError::Protocol(e.into()))
                                    .and_then(|_| err(MqttError::Disconnected)),
                            ),
                        }),
                )
            },
            Some(packet) => {
                log::info!(
                    "MQTT-3.1.0-1: Expected CONNECT packet, received {}",
                    packet.packet_type()
                );
                Either::B(err(MqttError::UnexpectedPacket(packet)))
            }
            None => {
                log::trace!("mqtt client disconnected",);
                Either::B(err(MqttError::Disconnected))
            }
        }
        .from_err()
        .and_then(|(session, framed, mut inner, rx)| {
            let time = inner.time.clone();
            let inner = inner.get_mut();

            // construct publish, subscribe, unsubscribe services
            inner
                .publish
                .new_service(&session)
                .map_err(|e| e.into())
                .join3(
                    inner.subscribe.new_service(&session),
                    inner.unsubscribe.new_service(&session),
                )
                .map_err(|e| MqttError::Service(e.into()))
                .and_then(move |(publish, subscribe, unsubscribe)| {
                    let mut session = Cell::new(session);

                    // mqtt dispatcher pipeline
                    let service =
                            // keep-alive connection
                            KeepAliveService::new(
                                Duration::from_secs(3600), time, || MqttError::KeepAliveTimeout
                            )
                            .and_then(
                                // limit number of in-flight messages
                                InFlightService::new(
                                    15,
                                    // mqtt spec requires ack ordering, so enforce response ordering
                                    InOrder::service(
                                        ServerDispatcher::new(
                                            session.clone(),
                                            publish.map_err(|e| e.into()),
                                            subscribe,
                                            unsubscribe))
                                        .map_err(|e| match e {
                                            InOrderError::Service(e) => e,
                                            InOrderError::Disconnected => MqttError::InternalError,
                                        })
                                    ));

                    FramedTransport::new(framed, service)
                        .set_receiver(rx)
                        .map_err(|e| match e {
                            FramedTransportError::Service(e) => e,
                            FramedTransportError::Decoder(e) => MqttError::Protocol(e.into()),
                            FramedTransportError::Encoder(e) => MqttError::Protocol(e.into()),
                        })
                        .map_err(MqttError::from)
                        .then(move |res| {
                            if let Some(on_close) = on_close {
                                (*on_close)(session.get_mut());
                            }
                            res
                          })
                })
        })
    }
}

impl<Io, S, C, P, E, I> Service for Server<Io, S, C, P, E, I>
where
    S: 'static,
    I: 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
    E: From<P::Error> + From<P::InitError> + 'static,
{
    type Request = ServerIo<Io, I>;
    type Response = ();
    type Error = MqttError<E>;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.0
            .get_mut()
            .connect
            .poll_ready()
            .map_err(|e| MqttError::Service(e.into()))
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        let (io, params, _) = req.into_parts();
        let framed = Framed::new(io, mqtt::Codec::new());
        let mut inner = self.0.clone();

        Box::new(
            framed
                .into_future()
                .map_err(|(e, _)| MqttError::Protocol(e.into()))
                .and_then(move |(packet, framed)| {
                    let inner2 = inner.clone();
                    inner.get_mut().dispatch(packet, framed, params, inner2)
                }),
        )
    }
}

/// Mqtt Server service
struct FramedMqttServer<Io, U, S, C: NewService, P, E, I> {
    connect: Rc<C>,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, Subscribe<S>, SubscribeResult, E, E>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, E>>,
    time: LowResTimeService,
    on_close: Option<Rc<Fn(&mut S)>>,
    _t: PhantomData<(Io, U, S, I)>,
}

impl<Io, U, S, C, P, E, I> NewService<ServerConfig> for FramedMqttServer<Io, U, S, C, P, E, I>
where
    S: 'static,
    I: 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
    E: From<C::InitError> + From<P::Error> + From<P::InitError> + 'static,
{
    type Request = (Framed<Io, U>, I);
    type Response = ();
    type Error = MqttError<E>;
    type Service = FramedServer<Io, U, S, C::Service, P, E, I>;
    type InitError = MqttError<E>;
    type Future = FramedMqttServerFactory<Io, U, S, C, P, E, I>;

    fn new_service(&self, _: &ServerConfig) -> Self::Future {
        FramedMqttServerFactory {
            fut: self.connect.new_service(&()),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            on_close: self.on_close.clone(),
            _t: PhantomData,
        }
    }
}

struct FramedMqttServerFactory<Io, U, S, C: NewService, P, E, I> {
    fut: C::Future,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, Subscribe<S>, SubscribeResult, E, E>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, mqtt::Packet, (), E, E>>,
    time: LowResTimeService,
    on_close: Option<Rc<Fn(&mut S)>>,
    _t: PhantomData<(Io, U, S, E, I)>,
}

impl<Io, U, S, C, P, E, I> Future for FramedMqttServerFactory<Io, U, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>,
    C: NewService<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<S, Request = Publish<S>, Response = ()>,
    E: From<C::InitError> + From<P::Error> + From<P::InitError> + 'static,
{
    type Item = FramedServer<Io, U, S, C::Service, P, E, I>;
    type Error = MqttError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(FramedServer(Cell::new(ServerInner {
            connect: try_ready!(self.fut.poll().map_err(|e| MqttError::Service(e.into()))),
            publish: self.publish.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            time: self.time.clone(),
            on_close: self.on_close.clone(),
            _t: PhantomData,
        }))))
    }
}

/// Mqtt Server
pub struct FramedServer<Io, U, S, C: Service, P, E, I>(Cell<ServerInner<Io, U, S, C, P, E, I>>);

impl<Io, U, S, C, P, E, I> Service for FramedServer<Io, U, S, C, P, E, I>
where
    S: 'static,
    I: 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
    E: From<P::Error> + From<P::InitError> + 'static,
{
    type Request = (Framed<Io, U>, I);
    type Response = ();
    type Error = MqttError<E>;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.0
            .get_mut()
            .connect
            .poll_ready()
            .map_err(|e| MqttError::Service(e.into()))
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        let mut inner = self.0.clone();
        let (framed, params) = req;

        Box::new(
            framed
                .into_future()
                .map_err(|(e, _)| MqttError::Protocol(e.into()))
                .and_then(move |(packet, framed)| {
                    let inner2 = inner.clone();
                    inner.get_mut().dispatch(packet, framed, params, inner2)
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
    type InitError = E;
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
pub struct SubsNotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for SubsNotImplemented<S, E> {
    fn default() -> Self {
        SubsNotImplemented(PhantomData)
    }
}

impl<S, E> NewService<S> for SubsNotImplemented<S, E> {
    type Request = Subscribe<S>;
    type Response = SubscribeResult;
    type Error = E;
    type InitError = E;
    type Service = SubsNotImplemented<S, E>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        ok(SubsNotImplemented(PhantomData))
    }
}

impl<S, E> Service for SubsNotImplemented<S, E> {
    type Request = Subscribe<S>;
    type Response = SubscribeResult;
    type Error = E;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, subs: Subscribe<S>) -> Self::Future {
        log::warn!("MQTT Subscribe is not implemented");
        ok(subs.finish())
    }
}

/// Not implemented subscribe service
pub struct UnsubsNotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for UnsubsNotImplemented<S, E> {
    fn default() -> Self {
        UnsubsNotImplemented(PhantomData)
    }
}

impl<S, E> NewService<S> for UnsubsNotImplemented<S, E> {
    type Request = mqtt::Packet;
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = UnsubsNotImplemented<S, E>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        ok(UnsubsNotImplemented(PhantomData))
    }
}

impl<S, E> Service for UnsubsNotImplemented<S, E> {
    type Request = mqtt::Packet;
    type Response = ();
    type Error = E;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: mqtt::Packet) -> Self::Future {
        log::warn!("MQTT Unsubscribe is not implemented");
        ok(())
    }
}
