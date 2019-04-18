use actix_service::{boxed, Service};
use futures::future::{ok, Either, FutureResult};
use futures::{try_ready, Async, Future, Poll};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::error::MqttPublishError;
use crate::publish::Publish;
use crate::subs::Subscribe;

/// PUBLIS/SUBSCRIBER/UNSUBSCRIBER packets dispatcher
pub(crate) struct ServerDispatcher<S, T: Service> {
    session: Cell<S>,
    publish: T,
    subscribe: boxed::BoxedService<Subscribe<S>, Vec<mqtt::SubscribeReturnCode>, T::Error>,
    unsubscribe: boxed::BoxedService<mqtt::Packet, (), T::Error>,
}

impl<S, T> ServerDispatcher<S, T>
where
    T: Service<Request = Publish<S>, Response = ()>,
{
    pub(crate) fn new(
        session: Cell<S>,
        publish: T,
        subscribe: boxed::BoxedService<Subscribe<S>, Vec<mqtt::SubscribeReturnCode>, T::Error>,
        unsubscribe: boxed::BoxedService<mqtt::Packet, (), T::Error>,
    ) -> Self {
        Self {
            session,
            publish,
            subscribe,
            unsubscribe,
        }
    }
}

impl<S, T> Service for ServerDispatcher<S, T>
where
    T: Service<Request = Publish<S>, Response = ()>,
    T::Error: std::fmt::Debug,
{
    type Request = mqtt::Packet;
    type Response = mqtt::Packet;
    type Error = MqttPublishError<T::Error>;
    type Future = Either<
        FutureResult<mqtt::Packet, MqttPublishError<T::Error>>,
        Either<SubscribeResponse<T::Error>, PublishResponse<T::Future>>,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.publish
            .poll_ready()
            .map_err(|e| MqttPublishError::Service(e))
    }

    fn call(&mut self, req: mqtt::Packet) -> Self::Future {
        println!("PKT: {:#?}", req);
        match req {
            mqtt::Packet::PingRequest => Either::A(ok(mqtt::Packet::PingResponse)),
            mqtt::Packet::Disconnect => Either::A(ok(mqtt::Packet::Empty)),
            mqtt::Packet::Publish(publish) => {
                let packet_id = publish.packet_id;
                Either::B(Either::B(PublishResponse {
                    fut: self
                        .publish
                        .call(Publish::new(self.session.clone(), publish)),
                    packet_id,
                }))
            }
            mqtt::Packet::Subscribe {
                packet_id,
                topic_filters,
            } => Either::B(Either::A(SubscribeResponse {
                packet_id,
                fut: self
                    .subscribe
                    .call(Subscribe::new(self.session.clone(), topic_filters)),
            })),
            _ => Either::A(ok(mqtt::Packet::Empty)),
        }
    }
}

/// Publish service response future
pub struct PublishResponse<T> {
    fut: T,
    packet_id: Option<u16>,
}

impl<T> Future for PublishResponse<T>
where
    T: Future<Item = ()>,
    T::Error: std::fmt::Debug,
{
    type Item = mqtt::Packet;
    type Error = MqttPublishError<T::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.fut.poll().map_err(|e| MqttPublishError::Service(e)));
        if let Some(packet_id) = self.packet_id {
            Ok(Async::Ready(mqtt::Packet::PublishAck { packet_id }))
        } else {
            // Do not ack if QoS::AtMostOnce
            Ok(Async::Ready(mqtt::Packet::Empty))
        }
    }
}

type BoxedServiceResponse<Res, Err> =
    Either<FutureResult<Res, Err>, Box<Future<Item = Res, Error = Err>>>;

/// Subscribe service response future
pub struct SubscribeResponse<E> {
    fut: BoxedServiceResponse<Vec<mqtt::SubscribeReturnCode>, E>,
    packet_id: u16,
}

impl<E> Future for SubscribeResponse<E>
where
    E: std::fmt::Debug,
{
    type Item = mqtt::Packet;
    type Error = MqttPublishError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let status = try_ready!(self.fut.poll().map_err(|e| MqttPublishError::Service(e)));
        Ok(Async::Ready(mqtt::Packet::SubscribeAck {
            status,
            packet_id: self.packet_id,
        }))
    }
}
