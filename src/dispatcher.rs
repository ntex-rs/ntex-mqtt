use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::{err, join3, ok, Either, FutureExt, LocalBoxFuture, Ready};
use futures::ready;
use fxhash::FxHashSet;
use ntex::service::{boxed, fn_factory_with_config, pipeline, Service, ServiceFactory};
use ntex::util::inflight::InFlightService;
use ntex::util::keepalive::KeepAliveService;
use ntex::util::order::{InOrder, InOrderError};
use ntex::util::time::LowResTimeService;

use crate::codec3 as mqtt;
use crate::error::MqttError;
use crate::publish::Publish;
use crate::session::Session;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, E>(
    publish: T,
    subscribe: boxed::BoxServiceFactory<
        Session<St>,
        Subscribe,
        SubscribeResult,
        MqttError<E>,
        MqttError<E>,
    >,
    unsubscribe: boxed::BoxServiceFactory<
        Session<St>,
        Unsubscribe,
        (),
        MqttError<E>,
        MqttError<E>,
    >,
    disconnect: Option<Rc<dyn Fn(&Session<St>, bool)>>,
) -> impl ServiceFactory<
    Config = Session<St>,
    Request = mqtt::Packet,
    Response = Option<mqtt::Packet>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    E: 'static,
    St: 'static,
    T: ServiceFactory<
            Config = Session<St>,
            Request = Publish,
            Response = (),
            Error = MqttError<E>,
            InitError = MqttError<E>,
        > + 'static,
{
    let time = LowResTimeService::with(Duration::from_secs(1));

    fn_factory_with_config(move |cfg: Session<St>| {
        let time = time.clone();
        let disconnect = disconnect.clone();
        let (timeout, inflight) = cfg.params();

        // create services
        let fut = join3(
            publish.new_service(cfg.clone()),
            subscribe.new_service(cfg.clone()),
            unsubscribe.new_service(cfg.clone()),
        );

        async move {
            let (publish, subscribe, unsubscribe) = fut.await;

            // mqtt dispatcher
            Ok(Dispatcher::<_, _, E>::new(
                cfg,
                // keep-alive connection
                pipeline(KeepAliveService::new(timeout, time, || {
                    MqttError::KeepAliveTimeout
                }))
                .and_then(
                    // limit number of in-flight messages
                    InFlightService::new(
                        inflight,
                        // mqtt spec requires ack ordering, so enforce response ordering
                        InOrder::service(publish?).map_err(|e| match e {
                            InOrderError::Service(e) => e,
                            InOrderError::Disconnected => MqttError::Disconnected,
                        }),
                    ),
                ),
                subscribe?,
                unsubscribe?,
                disconnect,
            ))
        }
    })
}

/// PUBLIS/SUBSCRIBER/UNSUBSCRIBER packets dispatcher
pub(crate) struct Dispatcher<St, T: Service<Error = MqttError<E>>, E> {
    session: Session<St>,
    publish: T,
    subscribe: boxed::BoxService<Subscribe, SubscribeResult, T::Error>,
    unsubscribe: boxed::BoxService<Unsubscribe, (), T::Error>,
    disconnect: RefCell<Option<Rc<dyn Fn(&Session<St>, bool)>>>,
    inflight: Rc<RefCell<FxHashSet<NonZeroU16>>>,
}

impl<St, T, E> Dispatcher<St, T, E>
where
    T: Service<Request = Publish, Response = (), Error = MqttError<E>>,
{
    pub(crate) fn new(
        session: Session<St>,
        publish: T,
        subscribe: boxed::BoxService<Subscribe, SubscribeResult, T::Error>,
        unsubscribe: boxed::BoxService<Unsubscribe, (), T::Error>,
        disconnect: Option<Rc<dyn Fn(&Session<St>, bool)>>,
    ) -> Self {
        Self {
            session,
            publish,
            subscribe,
            unsubscribe,
            disconnect: RefCell::new(disconnect),
            inflight: Rc::new(RefCell::new(FxHashSet::default())),
        }
    }
}

impl<St, T, E> Service for Dispatcher<St, T, E>
where
    T: Service<Request = Publish, Response = (), Error = MqttError<E>>,
    E: 'static,
{
    type Request = mqtt::Packet;
    type Response = Option<mqtt::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        Either<
            Ready<Result<Self::Response, MqttError<E>>>,
            LocalBoxFuture<'static, Result<Self::Response, MqttError<E>>>,
        >,
        PublishResponse<T::Future, MqttError<E>>,
    >;

    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx)?;
        let res2 = self.subscribe.poll_ready(cx)?;
        let res3 = self.unsubscribe.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() || res3.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, _: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if let Some(disconnect) = self.disconnect.borrow_mut().take() {
            disconnect(&self.session, is_error);
        }
        Poll::Ready(())
    }

    fn call(&self, packet: mqtt::Packet) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            mqtt::Packet::PingRequest => {
                Either::Left(Either::Left(ok(Some(mqtt::Packet::PingResponse))))
            }
            mqtt::Packet::Disconnect => Either::Left(Either::Left(ok(None))),
            mqtt::Packet::Publish(publish) => {
                let inflight = self.inflight.clone();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inflight.borrow_mut().insert(pid) {
                        return Either::Left(Either::Left(err(MqttError::DuplicatedPacketId)));
                    }
                }
                Either::Right(PublishResponse {
                    packet_id,
                    inflight,
                    fut: self.publish.call(Publish::new(publish)),
                    _t: PhantomData,
                })
            }
            mqtt::Packet::PublishAck { packet_id } => {
                self.session.sink().complete_publish_qos1(packet_id);
                Either::Left(Either::Left(ok(None)))
            }
            mqtt::Packet::Subscribe {
                packet_id,
                topic_filters,
            } => Either::Left(Either::Right(
                SubscribeResponse {
                    packet_id,
                    fut: self.subscribe.call(Subscribe::new(topic_filters)),
                }
                .boxed_local(),
            )),
            mqtt::Packet::Unsubscribe {
                packet_id,
                topic_filters,
            } => Either::Left(Either::Right(
                self.unsubscribe
                    .call(Unsubscribe::new(topic_filters))
                    .map(move |_| Ok(Some(mqtt::Packet::UnsubscribeAck { packet_id })))
                    .boxed_local(),
            )),
            _ => Either::Left(Either::Left(ok(None))),
        }
    }
}

/// Publish service response future
#[pin_project::pin_project]
pub(crate) struct PublishResponse<T, E> {
    #[pin]
    fut: T,
    packet_id: Option<NonZeroU16>,
    inflight: Rc<RefCell<FxHashSet<NonZeroU16>>>,
    _t: PhantomData<E>,
}

impl<T, E> Future for PublishResponse<T, E>
where
    T: Future<Output = Result<(), E>>,
{
    type Output = Result<Option<mqtt::Packet>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();

        ready!(this.fut.poll(cx))?;
        if let Some(packet_id) = this.packet_id {
            this.inflight.borrow_mut().remove(&packet_id);
            Poll::Ready(Ok(Some(mqtt::Packet::PublishAck {
                packet_id: *packet_id,
            })))
        } else {
            Poll::Ready(Ok(None))
        }
    }
}

/// Subscribe service response future
pub(crate) struct SubscribeResponse<E> {
    fut: LocalBoxFuture<'static, Result<SubscribeResult, E>>,
    packet_id: NonZeroU16,
}

impl<E> Future for SubscribeResponse<E> {
    type Output = Result<Option<mqtt::Packet>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let res = ready!(Pin::new(&mut self.fut).poll(cx))?;
        Poll::Ready(Ok(Some(mqtt::Packet::SubscribeAck {
            status: res.codes,
            packet_id: self.packet_id,
        })))
    }
}
