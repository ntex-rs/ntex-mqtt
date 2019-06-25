use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use actix_codec::{AsyncRead, AsyncWrite, Decoder, Encoder, Framed};
use actix_server_config::{Io as ServerIo, ServerConfig};
use actix_service::boxed;
use actix_service::{IntoNewService, IntoService, NewService, Service, ServiceExt};
use actix_utils::framed::{FramedTransport, FramedTransportError};
use actix_utils::inflight::InFlightService;
use actix_utils::keepalive::KeepAliveService;
use actix_utils::order::{InOrder, InOrderError};
use actix_utils::time::LowResTimeService;
use futures::future::{err, ok, result, Either, FutureResult};
use futures::unsync::mpsc;
use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::connect::{Connect, ConnectAck};
use crate::dispatcher::ServerDispatcher;
use crate::error::MqttError;
use crate::publish::Publish;
use crate::session::Session;
use crate::sink::MqttSink;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};

/// Mqtt Server
pub struct MqttServer<Io, S, C: NewService, P, E, I> {
    connect: Rc<C>,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, Subscribe<S>, SubscribeResult, E, E>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, Unsubscribe<S>, (), E, E>>,
    disconnect: Option<Cell<boxed::BoxedService<Session<S>, (), E>>>,
    time: LowResTimeService,
    _t: PhantomData<(Io, S, I)>,
}

impl<Io, S, C, E, I> MqttServer<Io, S, C, NotImplemented<S, E>, E, I>
where
    S: 'static,
    C: NewService<Config = (), Request = Connect<I>, Response = ConnectAck<S>, Error = E>
        + 'static,
    E: From<C::InitError> + 'static,
{
    /// Create server factory and provide connect service
    pub fn new<F>(connect: F) -> MqttServer<Io, S, C, NotImplemented<S, E>, E, I>
    where
        F: IntoNewService<C>,
    {
        MqttServer {
            connect: Rc::new(connect.into_new_service()),
            publish: Rc::new(NotImplemented::default()),
            subscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            unsubscribe: Rc::new(boxed::new_service(UnsubsNotImplemented::default())),
            disconnect: None,
            time: LowResTimeService::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }

    /// Service to execute for publish packet
    pub fn publish<F, P1>(self, publish: F) -> MqttServer<Io, S, C, P1, E, I>
    where
        F: IntoNewService<P1>,
        P1: NewService<Config = S, Request = Publish<S>, Response = ()> + 'static,
        E: From<P1::Error> + From<P1::InitError> + 'static,
    {
        MqttServer {
            connect: self.connect,
            publish: Rc::new(publish.into_new_service()),
            subscribe: Rc::new(boxed::new_service(SubsNotImplemented::default())),
            unsubscribe: Rc::new(boxed::new_service(UnsubsNotImplemented::default())),
            disconnect: self.disconnect,
            time: self.time,
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P, E, I> MqttServer<Io, S, C, P, E, I>
where
    C: NewService,
    S: 'static,
{
    /// Service to execute for subscribe packet
    pub fn subscribe<F, Srv>(mut self, subscribe: F) -> Self
    where
        F: IntoNewService<Srv>,
        Srv: NewService<Config = S, Request = Subscribe<S>, Response = SubscribeResult>
            + 'static,
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

    /// Service to execute for unsubscribe packet
    pub fn unsubscribe<F, Srv>(mut self, unsubscribe: F) -> Self
    where
        F: IntoNewService<Srv>,
        Srv: NewService<Config = S, Request = Unsubscribe<S>, Response = ()> + 'static,
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

    /// Service to execute on disconnect
    pub fn disconnect<UF, U>(mut self, srv: UF) -> Self
    where
        UF: IntoService<U>,
        U: Service<Request = Session<S>, Response = ()> + 'static,
        E: From<U::Error> + 'static,
    {
        self.disconnect = Some(Cell::new(boxed::service(
            srv.into_service().map_err(|e| e.into()),
        )));
        self
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
            disconnect: self.disconnect.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P, E, I> NewService for MqttServer<Io, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    E: From<C::InitError> + From<P::Error> + From<P::InitError> + 'static,
    C: NewService<Config = (), Request = Connect<I>, Response = ConnectAck<S>, Error = E>
        + 'static,
    P: NewService<Config = S, Request = Publish<S>, Response = ()> + 'static,
    S: 'static,
    I: 'static,
{
    type Config = ServerConfig;
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
            disconnect: self.disconnect.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }
    }
}

pub struct MqttServerFactory<Io, S, C: NewService, P, E, I> {
    fut: C::Future,
    publish: Rc<P>,
    subscribe: Rc<boxed::BoxedNewService<S, Subscribe<S>, SubscribeResult, E, E>>,
    unsubscribe: Rc<boxed::BoxedNewService<S, Unsubscribe<S>, (), E, E>>,
    disconnect: Option<Cell<boxed::BoxedService<Session<S>, (), E>>>,
    time: LowResTimeService,
    _t: PhantomData<(Io, S, E, I)>,
}

impl<Io, S, C, P, E, I> Future for MqttServerFactory<Io, S, C, P, E, I>
where
    Io: AsyncRead + AsyncWrite + 'static,
    C: NewService<Config = (), Request = Connect<I>, Response = ConnectAck<S>, Error = E>
        + 'static,
    P: NewService<Config = S, Request = Publish<S>, Response = ()>,
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
            disconnect: self.disconnect.clone(),
            time: self.time.clone(),
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
    unsubscribe: Rc<boxed::BoxedNewService<S, Unsubscribe<S>, (), E, E>>,
    disconnect: Option<Cell<boxed::BoxedService<Session<S>, (), E>>>,
    time: LowResTimeService,
    _t: PhantomData<(Io, S, E, U, I)>,
}

impl<Io, U, S, C, P, E, I> ServerInner<Io, U, S, C, P, E, I>
where
    S: 'static,
    U: Decoder<Item = mqtt::Packet, Error = mqtt::ParseError>
        + Encoder<Item = mqtt::Packet, Error = mqtt::ParseError>,
    Io: AsyncRead + AsyncWrite + 'static,
    C: Service<Request = Connect<I>, Response = ConnectAck<S>, Error = E> + 'static,
    P: NewService<Config = S, Request = Publish<S>, Response = ()> + 'static,
    E: From<P::Error> + From<P::InitError> + 'static,
{
    fn dispatch(
        &mut self,
        packet: Option<mqtt::Packet>,
        framed: Framed<Io, U>,
        param: I,
        inner: Cell<ServerInner<Io, U, S, C, P, E, I>>,
    ) -> impl Future<Item = (), Error = MqttError<E>> {
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
                Either::B(err(MqttError::ExpectedConnect(packet)))
            }
            None => {
                log::trace!("mqtt client disconnected",);
                Either::B(err(MqttError::Disconnected))
            }
        }
        .from_err()
        .and_then(|(session, framed, mut inner, rx)| {
            let time = inner.time.clone();
            let mut inner2 = inner.clone();
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
                    let session = Cell::new(session);

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
                                            InOrderError::Disconnected => MqttError::Disconnected,
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
                            if let Some(ref mut disconnect) = inner2.get_mut().disconnect {
                                Either::A(disconnect.get_mut().call(Session::new(session)).map_err(|e| MqttError::Service(e)))
                            } else {
                                Either::B(result(res))
                            }
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
    P: NewService<Config = S, Request = Publish<S>, Response = ()> + 'static,
    E: From<P::Error> + From<P::InitError> + 'static,
{
    type Request = ServerIo<Io, I>;
    type Response = ();
    type Error = MqttError<E>;
    type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let inner = self.0.get_mut();

        let not_ready1 = inner
            .connect
            .poll_ready()
            .map_err(|e| MqttError::Service(e.into()))?
            .is_not_ready();

        let not_ready2 = if let Some(ref mut srv) = inner.disconnect {
            srv.get_mut()
                .poll_ready()
                .map_err(|e| MqttError::Service(e.into()))?
                .is_not_ready()
        } else {
            false
        };

        if not_ready1 || not_ready2 {
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(()))
        }
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

/// Not implemented publish service
pub struct NotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for NotImplemented<S, E> {
    fn default() -> Self {
        NotImplemented(PhantomData)
    }
}

impl<S, E> NewService for NotImplemented<S, E> {
    type Config = S;
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

impl<S, E> NewService for SubsNotImplemented<S, E> {
    type Config = S;
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
        ok(subs.into_result())
    }
}

/// Not implemented subscribe service
pub struct UnsubsNotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for UnsubsNotImplemented<S, E> {
    fn default() -> Self {
        UnsubsNotImplemented(PhantomData)
    }
}

impl<S, E> NewService for UnsubsNotImplemented<S, E> {
    type Config = S;
    type Request = Unsubscribe<S>;
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
    type Request = Unsubscribe<S>;
    type Response = ();
    type Error = E;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: Unsubscribe<S>) -> Self::Future {
        log::warn!("MQTT Unsubscribe is not implemented");
        ok(())
    }
}
