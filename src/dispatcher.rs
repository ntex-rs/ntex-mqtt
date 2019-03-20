use actix_service::{boxed, Service};
use futures::future::{ok, Either, FutureResult};
use futures::{Async, Future, Poll};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::error::MqttPublishError;
use crate::publish::Publish;

/// PUBLIS/SUBSCRIBER/UNSUBSCRIBER packets dispatcher
pub(crate) struct ServerDispatcher<S, T: Service> {
    session: Cell<S>,
    publish: T,
    _subscribe: boxed::BoxedService<mqtt::Packet, (), T::Error>,
    _unsubscribe: boxed::BoxedService<mqtt::Packet, (), T::Error>,
}

impl<S, T> ServerDispatcher<S, T>
where
    T: Service<Request = Publish<S>, Response = ()>,
{
    pub(crate) fn new(
        session: Cell<S>,
        publish: T,
        _subscribe: boxed::BoxedService<mqtt::Packet, (), T::Error>,
        _unsubscribe: boxed::BoxedService<mqtt::Packet, (), T::Error>,
    ) -> Self {
        Self {
            session,
            publish,
            _subscribe,
            _unsubscribe,
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
    type Future = Either<FutureResult<Self::Response, Self::Error>, PublishResponse<T::Future>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.publish
            .poll_ready()
            .map_err(|e| MqttPublishError::Service(e))
    }

    fn call(&mut self, req: mqtt::Packet) -> Self::Future {
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
        futures::try_ready!(self.fut.poll().map_err(|e| MqttPublishError::Service(e)));
        if let Some(packet_id) = self.packet_id {
            Ok(Async::Ready(mqtt::Packet::PublishAck { packet_id }))
        } else {
            // Do not ack if QoS::AtMostOnce
            Ok(Async::Ready(mqtt::Packet::Empty))
        }
    }
}
