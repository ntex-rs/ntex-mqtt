use std::rc::Rc;
use std::time::Duration;

use actix_ioframe as ioframe;
use actix_service::{boxed, new_service_cfg, NewService, Service, ServiceExt};
use actix_utils::inflight::InFlightService;
use actix_utils::keepalive::KeepAliveService;
use actix_utils::order::{InOrder, InOrderError};
use actix_utils::time::LowResTimeService;
use futures::future::{ok, Either, FutureResult};
use futures::{try_ready, Async, Future, Poll};
use mqtt_codec as mqtt;

use crate::error::MqttError;
use crate::publish2::Publish;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};

// dispatcher factory
pub fn dispatcher<St, T, E>(
    publish: T,
    subscribe: Rc<
        boxed::BoxedNewService<St, Subscribe<St>, SubscribeResult, MqttError<E>, MqttError<E>>,
    >,
    unsubscribe: Rc<
        boxed::BoxedNewService<St, Unsubscribe<St>, (), MqttError<E>, MqttError<E>>,
    >,
) -> impl NewService<
    Config = St,
    Request = ioframe::Item<St, mqtt::Codec>,
    Response = Option<mqtt::Packet>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    E: 'static,
    St: 'static,
    T: NewService<
        Config = St,
        Request = Publish<St>,
        Response = (),
        Error = MqttError<E>,
        InitError = MqttError<E>,
    >,
    T::Service: 'static,
{
    let time = LowResTimeService::with(Duration::from_secs(1));

    new_service_cfg(move |cfg: &St| {
        let time = time.clone();

        // create services
        publish
            .new_service(cfg)
            .join3(subscribe.new_service(cfg), unsubscribe.new_service(cfg))
            .map(move |(publish, subscribe, unsubscribe)| {
                // mqtt dispatcher
                Dispatcher::new(
                    // keep-alive connection
                    KeepAliveService::new(Duration::from_secs(3600), time, || {
                        MqttError::KeepAliveTimeout
                    })
                    .and_then(
                        // limit number of in-flight messages
                        InFlightService::new(
                            15,
                            // mqtt spec requires ack ordering, so enforce response ordering
                            InOrder::service(publish).map_err(|e| match e {
                                InOrderError::Service(e) => e,
                                InOrderError::Disconnected => MqttError::Disconnected,
                            }),
                        ),
                    ),
                    subscribe,
                    unsubscribe,
                )
            })
    })
}

/// PUBLIS/SUBSCRIBER/UNSUBSCRIBER packets dispatcher
pub(crate) struct Dispatcher<St, T: Service> {
    publish: T,
    subscribe: boxed::BoxedService<Subscribe<St>, SubscribeResult, T::Error>,
    unsubscribe: boxed::BoxedService<Unsubscribe<St>, (), T::Error>,
}

impl<St, T> Dispatcher<St, T>
where
    T: Service<Request = Publish<St>, Response = ()>,
{
    pub(crate) fn new(
        publish: T,
        subscribe: boxed::BoxedService<Subscribe<St>, SubscribeResult, T::Error>,
        unsubscribe: boxed::BoxedService<Unsubscribe<St>, (), T::Error>,
    ) -> Self {
        Self {
            publish,
            subscribe,
            unsubscribe,
        }
    }
}

impl<St, T> Service for Dispatcher<St, T>
where
    T: Service<Request = Publish<St>, Response = ()>,
    T::Error: 'static,
{
    type Request = ioframe::Item<St, mqtt::Codec>;
    type Response = Option<mqtt::Packet>;
    type Error = T::Error;
    type Future = Either<
        Either<
            FutureResult<Self::Response, T::Error>,
            Box<dyn Future<Item = Self::Response, Error = T::Error>>,
        >,
        PublishResponse<T::Future>,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let res1 = self.publish.poll_ready()?;
        let res2 = self.subscribe.poll_ready()?;
        let res3 = self.unsubscribe.poll_ready()?;

        if res1.is_not_ready() || res2.is_not_ready() || res3.is_not_ready() {
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(()))
        }
    }

    fn call(&mut self, req: ioframe::Item<St, mqtt::Codec>) -> Self::Future {
        let (state, _sink, packet) = req.into_parts();

        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            mqtt::Packet::PingRequest => {
                Either::A(Either::A(ok(Some(mqtt::Packet::PingResponse))))
            }
            mqtt::Packet::Disconnect => Either::A(Either::A(ok(Some(mqtt::Packet::Empty)))),
            mqtt::Packet::Publish(publish) => {
                let packet_id = publish.packet_id;
                Either::B(PublishResponse {
                    fut: self.publish.call(Publish::new(state, publish)),
                    packet_id,
                })
            }
            // mqtt::Packet::Subscribe {
            //     packet_id,
            //     topic_filters,
            // } => Either::A(Either::B(Box::new(SubscribeResponse {
            //     packet_id,
            //     fut: self
            //         .subscribe
            //         .call(Subscribe::new(self.session.clone(), topic_filters)),
            // }))),
            // mqtt::Packet::Unsubscribe {
            //     packet_id,
            //     topic_filters,
            // } => Either::A(Either::B(Box::new(
            //     self.unsubscribe
            //         .call(Unsubscribe::new(self.session.clone(), topic_filters))
            //         .map_err(|e| MqttError::Service(e))
            //         .map(move |_| mqtt::Packet::UnsubscribeAck { packet_id }),
            // ))),
            _ => Either::A(Either::A(ok(Some(mqtt::Packet::Empty)))),
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
    type Item = Option<mqtt::Packet>;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.fut.poll());
        if let Some(packet_id) = self.packet_id {
            Ok(Async::Ready(Some(mqtt::Packet::PublishAck { packet_id })))
        } else {
            Ok(Async::Ready(None))
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
