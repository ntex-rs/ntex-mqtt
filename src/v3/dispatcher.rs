use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::{err, join, ok, Either, FutureExt, Ready};
use futures::ready;
use fxhash::FxHashSet;
use ntex::service::{fn_factory_with_config, pipeline, Service, ServiceFactory};
use ntex::util::buffer::BufferService;
use ntex::util::inflight::InFlightService;
use ntex::util::keepalive::KeepAliveService;
use ntex::util::order::{InOrder, InOrderError};
use ntex::util::time::LowResTimeService;

use crate::error::MqttError;

use super::control::{ControlPacket, ControlResult, ControlResultKind, Subscribe, Unsubscribe};
use super::publish::Publish;
use super::{codec, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
) -> impl ServiceFactory<
    Config = Session<St>,
    Request = codec::Packet,
    Response = Option<codec::Packet>,
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
    C: ServiceFactory<
            Config = Session<St>,
            Request = ControlPacket,
            Response = ControlResult,
            Error = MqttError<E>,
            InitError = MqttError<E>,
        > + 'static,
{
    let time = LowResTimeService::with(Duration::from_secs(1));

    fn_factory_with_config(move |cfg: Session<St>| {
        let time = time.clone();
        let (timeout, inflight) = cfg.params();

        // create services
        let fut = join(
            publish.new_service(cfg.clone()),
            control.new_service(cfg.clone()),
        );

        async move {
            let (publish, control) = fut.await;

            // mqtt dispatcher
            Ok(Dispatcher::<_, _, _, E>::new(
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
                BufferService::new(
                    16,
                    || MqttError::Disconnected,
                    // limit number of in-flight messages
                    InFlightService::new(1, control?),
                ),
            ))
        }
    })
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T: Service<Error = MqttError<E>>, C, E> {
    session: Session<St>,
    publish: T,
    control: C,
    inflight: Rc<RefCell<FxHashSet<NonZeroU16>>>,
    shutdown: Cell<bool>,
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    T: Service<Request = Publish, Response = (), Error = MqttError<E>>,
    C: Service<Request = ControlPacket, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(session: Session<St>, publish: T, control: C) -> Self {
        Self {
            session,
            publish,
            control,
            inflight: Rc::new(RefCell::new(FxHashSet::default())),
            shutdown: Cell::new(false),
        }
    }
}

impl<St, T, C, E> Service for Dispatcher<St, T, C, E>
where
    T: Service<Request = Publish, Response = (), Error = MqttError<E>>,
    C: Service<Request = ControlPacket, Response = ControlResult, Error = MqttError<E>>,
    C::Future: 'static,
    E: 'static,
{
    type Request = codec::Packet;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T::Future, MqttError<E>>,
        Either<Ready<Result<Self::Response, MqttError<E>>>, ControlResponse<C::Future, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx)?;
        let res2 = self.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, _: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if !self.shutdown.get() {
            self.shutdown.set(true);
            ntex::rt::spawn(
                self.control
                    .call(ControlPacket::closed(is_error))
                    .map(|_| ()),
            );
        }
        Poll::Ready(())
    }

    fn call(&self, packet: codec::Packet) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            codec::Packet::Publish(publish) => {
                let inflight = self.inflight.clone();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inflight.borrow_mut().insert(pid) {
                        return Either::Right(Either::Left(err(MqttError::DuplicatedPacketId)));
                    }
                }
                Either::Left(PublishResponse {
                    packet_id,
                    inflight,
                    fut: self.publish.call(Publish::new(publish)),
                    _t: PhantomData,
                })
            }
            codec::Packet::PublishAck { packet_id } => {
                self.session.sink().complete_publish_qos1(packet_id);
                Either::Right(Either::Left(ok(None)))
            }
            codec::Packet::PingRequest => Either::Right(Either::Right(ControlResponse {
                fut: self.control.call(ControlPacket::ping()),
            })),
            codec::Packet::Disconnect => Either::Right(Either::Right(ControlResponse {
                fut: self.control.call(ControlPacket::disconnect()),
            })),
            codec::Packet::Subscribe {
                packet_id,
                topic_filters,
            } => Either::Right(Either::Right(ControlResponse {
                fut: self.control.call(ControlPacket::Subscribe(Subscribe::new(
                    packet_id,
                    topic_filters,
                ))),
            })),
            codec::Packet::Unsubscribe {
                packet_id,
                topic_filters,
            } => Either::Right(Either::Right(ControlResponse {
                fut: self
                    .control
                    .call(ControlPacket::Unsubscribe(Unsubscribe::new(
                        packet_id,
                        topic_filters,
                    ))),
            })),
            _ => Either::Right(Either::Left(ok(None))),
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
    type Output = Result<Option<codec::Packet>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();

        ready!(this.fut.poll(cx))?;
        if let Some(packet_id) = this.packet_id {
            this.inflight.borrow_mut().remove(&packet_id);
            Poll::Ready(Ok(Some(codec::Packet::PublishAck {
                packet_id: *packet_id,
            })))
        } else {
            Poll::Ready(Ok(None))
        }
    }
}

/// Control service response future
#[pin_project::pin_project]
pub(crate) struct ControlResponse<T, E>
where
    T: Future<Output = Result<ControlResult, MqttError<E>>>,
{
    #[pin]
    fut: T,
}

impl<T, E> Future for ControlResponse<T, E>
where
    T: Future<Output = Result<ControlResult, MqttError<E>>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let packet = match ready!(self.project().fut.poll(cx))?.result {
            ControlResultKind::Ping => Some(codec::Packet::PingResponse),
            ControlResultKind::Subscribe(res) => Some(codec::Packet::SubscribeAck {
                status: res.codes,
                packet_id: res.packet_id,
            }),
            ControlResultKind::Unsubscribe(res) => Some(codec::Packet::UnsubscribeAck {
                packet_id: res.packet_id,
            }),
            ControlResultKind::Disconnect => None,
            ControlResultKind::Closed => None,
        };

        Poll::Ready(Ok(packet))
    }
}
