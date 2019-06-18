use actix_service::{boxed, Service};
use futures::future::{ok, Either, FutureResult};
use futures::{try_ready, Async, Future, Poll};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::error::MqttError;
use crate::publish::Publish;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};

/// PUBLIS/SUBSCRIBER/UNSUBSCRIBER packets dispatcher
pub(crate) struct ServerDispatcher<S, T: Service> {
    session: Cell<S>,
    publish: T,
    subscribe: boxed::BoxedService<Subscribe<S>, SubscribeResult, T::Error>,
    unsubscribe: boxed::BoxedService<Unsubscribe<S>, (), T::Error>,
}

impl<S, T> ServerDispatcher<S, T>
where
    T: Service<Request = Publish<S>, Response = ()>,
{
    pub(crate) fn new(
        session: Cell<S>,
        publish: T,
        subscribe: boxed::BoxedService<Subscribe<S>, SubscribeResult, T::Error>,
        unsubscribe: boxed::BoxedService<Unsubscribe<S>, (), T::Error>,
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
    T::Error: 'static,
{
    type Request = mqtt::Packet;
    type Response = mqtt::Packet;
    type Error = MqttError<T::Error>;
    type Future = Either<
        Either<
            FutureResult<mqtt::Packet, MqttError<T::Error>>,
            Box<dyn Future<Item = mqtt::Packet, Error = MqttError<T::Error>>>,
        >,
        PublishResponse<T::Future>,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let res1 = self
            .publish
            .poll_ready()
            .map_err(|e| MqttError::Service(e))?;
        let res2 = self
            .subscribe
            .poll_ready()
            .map_err(|e| MqttError::Service(e))?;
        let res3 = self
            .unsubscribe
            .poll_ready()
            .map_err(|e| MqttError::Service(e))?;

        if res1.is_not_ready() || res2.is_not_ready() || res3.is_not_ready() {
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(()))
        }
    }

    fn call(&mut self, req: mqtt::Packet) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", req);
        match req {
            mqtt::Packet::PingRequest => Either::A(Either::A(ok(mqtt::Packet::PingResponse))),
            mqtt::Packet::Disconnect => Either::A(Either::A(ok(mqtt::Packet::Empty))),
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
            } => Either::A(Either::B(Box::new(SubscribeResponse {
                packet_id,
                fut: self
                    .subscribe
                    .call(Subscribe::new(self.session.clone(), topic_filters)),
            }))),
            mqtt::Packet::Unsubscribe {
                packet_id,
                topic_filters,
            } => Either::A(Either::B(Box::new(
                self.unsubscribe
                    .call(Unsubscribe::new(self.session.clone(), topic_filters))
                    .map_err(|e| MqttError::Service(e))
                    .map(move |_| mqtt::Packet::UnsubscribeAck { packet_id }),
            ))),
            _ => Either::A(Either::A(ok(mqtt::Packet::Empty))),
        }
    }
}

/// Publish service response future
pub(crate) struct PublishResponse<T> {
    fut: T,
    packet_id: Option<u16>,
}

impl<T> Future for PublishResponse<T>
where
    T: Future<Item = ()>,
{
    type Item = mqtt::Packet;
    type Error = MqttError<T::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.fut.poll().map_err(|e| MqttError::Service(e)));
        if let Some(packet_id) = self.packet_id {
            Ok(Async::Ready(mqtt::Packet::PublishAck { packet_id }))
        } else {
            // Do not ack if QoS::AtMostOnce
            Ok(Async::Ready(mqtt::Packet::Empty))
        }
    }
}

type BoxedServiceResponse<Res, Err> =
    Either<FutureResult<Res, Err>, Box<dyn Future<Item = Res, Error = Err>>>;

/// Subscribe service response future
pub(crate) struct SubscribeResponse<E> {
    fut: BoxedServiceResponse<SubscribeResult, E>,
    packet_id: u16,
}

impl<E> Future for SubscribeResponse<E> {
    type Item = mqtt::Packet;
    type Error = MqttError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let res = try_ready!(self.fut.poll().map_err(|e| MqttError::Service(e)));
        Ok(Async::Ready(mqtt::Packet::SubscribeAck {
            status: res.codes,
            packet_id: self.packet_id,
        }))
    }
}
