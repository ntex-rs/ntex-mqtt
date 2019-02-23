use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::boxed::BoxedService;
use actix_service::{
    Apply, IntoConfigurableNewService, IntoNewService, NewService, Service, ServiceExt,
    Transform,
};
use actix_utils::framed::{FramedTransport, FramedTransportError};
use actix_utils::inflight::InFlightService;
use actix_utils::keepalive::KeepAliveService;
use actix_utils::order::{InOrder, InOrderError};
use actix_utils::time::LowResTimeService;
use futures::future::{err, ok, Either, FutureResult};
use futures::{Async, Future, Poll, Sink, Stream};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::connect::ConnectAck;
use crate::error::{MqttConnectError, MqttError, MqttPublishError};
use crate::publish::Publish;

/// Mqtt Server service
pub struct MqttServer<Io, S, C, P> {
    connect: Rc<C>,
    publish: Rc<P>,
    time: LowResTimeService,
    _t: PhantomData<(Io, S)>,
}

impl<Io, S, C> MqttServer<Io, S, C, NotImplemented<S>>
where
    S: 'static,
    C: NewService<Request = mqtt::Connect, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    C::InitError: fmt::Debug,
{
    /// Create server factory
    pub fn new<F>(connect: F) -> MqttServer<Io, S, C, NotImplemented<S>>
    where
        F: IntoNewService<C>,
    {
        MqttServer {
            connect: Rc::new(connect.into_new_service()),
            publish: Rc::new(NotImplemented::default()),
            time: LowResTimeService::with(Duration::from_secs(1)),
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P> MqttServer<Io, S, C, P> {
    /// Set publish service
    pub fn publish<F, P1>(self, publish: F) -> MqttServer<Io, S, C, P1>
    where
        F: IntoConfigurableNewService<P1, S>,
        P1: NewService<S, Request = Publish<S>, Response = ()> + 'static,
        P1::Error: fmt::Debug,
        P1::InitError: fmt::Debug,
    {
        MqttServer {
            connect: self.connect,
            publish: Rc::new(publish.into_new_service()),
            time: self.time,
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P> Clone for MqttServer<Io, S, C, P> {
    fn clone(&self) -> Self {
        MqttServer {
            connect: self.connect.clone(),
            publish: self.publish.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }
    }
}

impl<Io, S, C, P> NewService<()> for MqttServer<Io, S, C, P>
where
    Io: AsyncRead + AsyncWrite + 'static,
    S: 'static,
    C: NewService<Request = mqtt::Connect, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    C::InitError: fmt::Debug,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
    P::Error: fmt::Debug,
    P::InitError: fmt::Debug,
{
    type Request = Io;
    type Response = ();
    type Error = MqttError<C::Error, P::Error>;
    type Service = Server<Io, S, C::Service, P>;
    type InitError = ();
    type Future = MqttServerFactory<Io, S, C, P>;

    fn new_service(&self, _: &()) -> Self::Future {
        MqttServerFactory {
            fut: self.connect.new_service(&()),
            publish: self.publish.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }
    }
}

pub struct MqttServerFactory<Io, S, C: NewService, P> {
    fut: C::Future,
    publish: Rc<P>,
    time: LowResTimeService,
    _t: PhantomData<(Io, S)>,
}

impl<Io, S, C, P> Future for MqttServerFactory<Io, S, C, P>
where
    Io: AsyncRead + AsyncWrite + 'static,
    S: 'static,
    C: NewService<Request = mqtt::Connect, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    C::InitError: fmt::Debug,
    P: NewService<S, Request = Publish<S>, Response = ()>,
    P::Error: fmt::Debug,
    P::InitError: fmt::Debug,
{
    type Item = Server<Io, S, C::Service, P>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let connect = futures::try_ready!(self
            .fut
            .poll()
            .map_err(|e| log::error!("Connect service construction error: {:?}", e)));
        Ok(Async::Ready(Server(Cell::new(ServerInner {
            connect,
            publish: self.publish.clone(),
            time: self.time.clone(),
            _t: PhantomData,
        }))))
    }
}

/// Mqtt Server
pub struct Server<Io, S, C, P>(Cell<ServerInner<Io, S, C, P>>);

pub(crate) struct ServerInner<Io, S, C, P> {
    connect: C,
    publish: Rc<P>,
    time: LowResTimeService,
    _t: PhantomData<(Io, S)>,
}

impl<Io, S, C, P> Service for Server<Io, S, C, P>
where
    Io: AsyncRead + AsyncWrite + 'static,
    S: 'static,
    C: Service<Request = mqtt::Connect, Response = ConnectAck<S>> + 'static,
    C::Error: fmt::Debug,
    P: NewService<S, Request = Publish<S>, Response = ()> + 'static,
    P::Error: fmt::Debug,
    P::InitError: fmt::Debug,
{
    type Request = Io;
    type Response = ();
    type Error = MqttError<C::Error, P::Error>;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.0.get_mut().connect.poll_ready().map_err(|e| {
            log::error!("Connect service readiness error: {:?}", e);
            MqttConnectError::Service(e).into()
        })
    }

    fn call(&mut self, req: Io) -> Self::Future {
        let mut inner = self.0.clone();

        let fut = Framed::new(req, mqtt::Codec::new())
            .into_future()
            .map_err(|(e, _)| MqttConnectError::Protocol(e.into()))
            .and_then(move |(packet, framed)| match packet {
                Some(mqtt::Packet::Connect(connect)) => Either::A(
                    inner.get_mut().connect.call(connect)
                        .map_err(|e| MqttConnectError::Service(e))
                        .and_then(move |result| match result.into_inner() {
                            either::Either::Left((session, session_present)) => {
                                Either::A(
                                    framed.send(
                                        mqtt::Packet::ConnectAck{
                                            session_present,
                                            return_code: mqtt::ConnectCode::ConnectionAccepted} )
                                        .map_err(|e| MqttConnectError::Protocol(e.into()))
                                        .map(move|framed| (session, framed, inner)))
                            },
                            either::Either::Right(code) => {
                                Either::B(framed.send(mqtt::Packet::ConnectAck{session_present: false, return_code: code})
                                          .map_err(|e| MqttConnectError::Protocol(e.into()))
                                          .and_then(|_| err(MqttConnectError::Disconnected))
                                )
                            }
                        })
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
            })
            .from_err()
            .and_then(|(session, framed, mut inner)| {
                let time = inner.time.clone();
                inner
                    .get_mut()
                    .publish
                    .new_service(&session)
                    .map_err(|e| {
                        log::error!("Can not construct publish service: {:?}", e);
                        MqttPublishError::ConstructError
                    })
                    .and_then(move |publish| {
                        // mqtt dispatcher service
                        let service =
                        // keep-alive
                            KeepAliveService::new(
                                Duration::from_secs(3600), time, || MqttPublishError::KeepAliveTimeout
                            )
                            .apply(
                                // limit number of in-flight messages
                                InFlightService::new(15),
                                // enforce response ordering
                                Apply::new(
                                    InOrder::service().map_err(|e| match e {
                                        InOrderError::Service(e) => e,
                                        InOrderError::Disconnected => MqttPublishError::InternalError,
                                    }),
                                    ServerDispatcher::new(Cell::new(session), publish)
                                ));

                        FramedTransport::new(framed, service).map_err(|e| match e {
                            FramedTransportError::Service(e) => e,
                            FramedTransportError::Decoder(e) => MqttPublishError::Protocol(e.into()),
                            FramedTransportError::Encoder(e) => MqttPublishError::Protocol(e.into()),
                        })
                    })
                    .map_err(MqttError::from)
            });
        Box::new(fut)
    }
}

/// PUBLIS/SUBSCRIBER/UNSUBSCRIBER packets dispatcher
struct ServerDispatcher<S, T> {
    session: Cell<S>,
    publish: T,
}

impl<S, T> ServerDispatcher<S, T>
where
    T: Service<Request = Publish<S>, Response = ()> + 'static,
    T::Error: fmt::Debug,
{
    fn new(session: Cell<S>, publish: T) -> Self {
        Self { session, publish }
    }
}

impl<S, T> Service for ServerDispatcher<S, T>
where
    T: Service<Request = Publish<S>, Response = ()> + 'static,
    T::Error: fmt::Debug,
{
    type Request = mqtt::Packet;
    type Response = mqtt::Packet;
    type Error = MqttPublishError<T::Error>;
    type Future = Either<FutureResult<Self::Response, Self::Error>, PublishResponse<T>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.publish
            .poll_ready()
            .map_err(|e| MqttPublishError::Service(e))
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        match req {
            mqtt::Packet::PingRequest => Either::A(ok(mqtt::Packet::PingResponse)),
            mqtt::Packet::Disconnect => Either::A(ok(mqtt::Packet::Empty)),
            mqtt::Packet::Publish(publish) => {
                let packet_id = publish.packet_id;
                Either::B(PublishResponse {
                    fut: self
                        .publish
                        .call(Publish::new(self.session.clone(), publish)),
                    packet_id,
                })
            }
            mqtt::Packet::Subscribe {
                packet_id,
                topic_filters,
            } => Either::A(ok(mqtt::Packet::SubscribeAck {
                packet_id,
                status: topic_filters
                    .into_iter()
                    .map(|t| {
                        mqtt::SubscribeReturnCode::Success(if t.1 == mqtt::QoS::AtMostOnce {
                            t.1
                        } else {
                            mqtt::QoS::AtLeastOnce
                        })
                    })
                    .collect(),
            })),
            _ => Either::A(ok(mqtt::Packet::Empty)),
        }
    }
}

/// Publish service response future
pub struct PublishResponse<T: Service> {
    fut: T::Future,
    packet_id: Option<u16>,
}

impl<T> Future for PublishResponse<T>
where
    T: Service<Response = ()> + 'static,
    T::Error: fmt::Debug,
{
    type Item = mqtt::Packet;
    type Error = MqttPublishError<T::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        futures::try_ready!(self.fut.poll().map_err(|e| MqttPublishError::Service(e)));
        if let Some(packet_id) = self.packet_id {
            Ok(Async::Ready(mqtt::Packet::PublishAck { packet_id }))
        } else {
            Ok(Async::Ready(mqtt::Packet::Empty))
        }
    }
}

/// Not implemented publish service
pub struct NotImplemented<S>(PhantomData<S>);

impl<S> Default for NotImplemented<S> {
    fn default() -> Self {
        NotImplemented(PhantomData)
    }
}

impl<S> Clone for NotImplemented<S> {
    fn clone(&self) -> Self {
        NotImplemented(PhantomData)
    }
}

impl<S> NewService<S> for NotImplemented<S> {
    type Request = Publish<S>;
    type Response = ();
    type Error = &'static str;
    type InitError = &'static str;
    type Service = BoxedService<Publish<S>, (), &'static str>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        err("not implemented")
    }
}
