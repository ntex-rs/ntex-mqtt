use std::cell::{Cell, RefCell};
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use futures::future::{err, ok, Either, FutureExt, Ready};
use ntex::service::Service;
use ntex::util::inflight::InFlightService;

use crate::v3::{
    codec, control::ControlResultKind, publish::Publish, sink::Ack, sink::MqttSink,
};
use crate::{error::MqttError, error::ProtocolError, types::packet_type, AHashSet};

use super::control::{ControlMessage, ControlResult};

/// mqtt3 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: MqttSink,
    inflight: usize,
    publish: T,
    control: C,
) -> impl Service<Request = codec::Packet, Response = Option<codec::Packet>, Error = MqttError<E>>
where
    E: 'static,
    T: Service<Request = Publish, Response = either::Either<(), Publish>, Error = MqttError<E>>
        + 'static,
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>
        + 'static,
{
    // limit number of in-flight messages
    InFlightService::new(inflight, Dispatcher::<_, _, E>::new(sink, publish, control))
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T: Service<Error = MqttError<E>>, C, E> {
    sink: MqttSink,
    publish: T,
    shutdown: Cell<bool>,
    inner: Rc<Inner<C>>,
}

struct Inner<C> {
    control: C,
    sink: MqttSink,
    inflight: RefCell<AHashSet<NonZeroU16>>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<Request = Publish, Response = either::Either<(), Publish>, Error = MqttError<E>>,
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(sink: MqttSink, publish: T, control: C) -> Self {
        Self {
            publish,
            sink: sink.clone(),
            shutdown: Cell::new(false),
            inner: Rc::new(Inner {
                sink,
                control,
                inflight: RefCell::new(AHashSet::default()),
            }),
        }
    }
}

impl<T, C, E> Service for Dispatcher<T, C, E>
where
    T: Service<Request = Publish, Response = either::Either<(), Publish>, Error = MqttError<E>>,
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>,
    C::Future: 'static,
    E: 'static,
{
    type Request = codec::Packet;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T, C, E>,
        Either<Ready<Result<Self::Response, MqttError<E>>>, ControlResponse<C, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx)?;
        let res2 = self.inner.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, _: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if !self.shutdown.get() {
            self.inner.sink.close();
            self.shutdown.set(true);
            ntex::rt::spawn(
                self.inner.control.call(ControlMessage::closed(is_error)).map(|_| ()),
            );
        }
        Poll::Ready(())
    }

    fn call(&self, packet: codec::Packet) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            codec::Packet::Publish(publish) => {
                let inner = self.inner.clone();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inner.inflight.borrow_mut().insert(pid) {
                        log::trace!("Duplicated packet id for publish packet: {:?}", pid);
                        return Either::Right(Either::Left(err(MqttError::V3ProtocolError)));
                    }
                }
                Either::Left(PublishResponse {
                    packet_id,
                    inner,
                    fut: self.publish.call(Publish::new(publish)),
                    fut_c: None,
                    _t: PhantomData,
                })
            }
            codec::Packet::PublishAck { packet_id } => {
                if let Err(e) = self.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Either::Right(Either::Left(err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            codec::Packet::PingRequest => {
                Either::Right(Either::Left(ok(Some(codec::Packet::PingResponse))))
            }
            codec::Packet::Disconnect => Either::Right(Either::Right(ControlResponse::new(
                self.inner.control.call(ControlMessage::dis()),
                &self.inner,
            ))),
            codec::Packet::SubscribeAck { packet_id, status } => {
                if let Err(e) = self.sink.pkt_ack(Ack::Subscribe { packet_id, status }) {
                    Either::Right(Either::Left(err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            codec::Packet::UnsubscribeAck { packet_id } => {
                if let Err(e) = self.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Either::Right(Either::Left(err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            codec::Packet::Subscribe { .. } => {
                Either::Right(Either::Left(err(ProtocolError::Unexpected(
                    packet_type::SUBSCRIBE,
                    "Subscribe packet is not supported",
                )
                .into())))
            }
            codec::Packet::Unsubscribe { .. } => {
                Either::Right(Either::Left(err(ProtocolError::Unexpected(
                    packet_type::UNSUBSCRIBE,
                    "Unsubscribe packet is not supported",
                )
                .into())))
            }
            _ => Either::Right(Either::Left(ok(None))),
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<T: Service, C: Service, E> {
        #[pin]
        fut: T::Future,
        #[pin]
        fut_c: Option<ControlResponse<C, E>>,
        packet_id: Option<NonZeroU16>,
        inner: Rc<Inner<C>>,
        _t: PhantomData<E>,
    }
}

impl<T, C, E> Future for PublishResponse<T, C, E>
where
    T: Service<Request = Publish, Response = either::Either<(), Publish>, Error = MqttError<E>>,
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(fut) = self.as_mut().project().fut_c.as_pin_mut() {
            return fut.poll(cx);
        }

        let mut this = self.as_mut().project();
        let res = futures::ready!(this.fut.poll(cx))?;
        match res {
            either::Either::Left(_) => {
                log::trace!("Publish result for packet {:?} is ready", this.packet_id);

                if let Some(packet_id) = this.packet_id {
                    this.inner.inflight.borrow_mut().remove(&packet_id);
                    Poll::Ready(Ok(Some(codec::Packet::PublishAck { packet_id: *packet_id })))
                } else {
                    Poll::Ready(Ok(None))
                }
            }
            either::Either::Right(pkt) => {
                this.fut_c.set(Some(ControlResponse::new(
                    this.inner.control.call(ControlMessage::publish(pkt.into_inner())),
                    &*this.inner,
                )));
                self.poll(cx)
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Control service response future
    pub(crate) struct ControlResponse<C: Service, E> {
        #[pin]
        fut: C::Future,
        inner: Rc<Inner<C>>,
        _t: PhantomData<E>,
    }
}

impl<C, E> ControlResponse<C, E>
where
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(fut: C::Future, inner: &Rc<Inner<C>>) -> Self {
        Self { fut, inner: inner.clone(), _t: PhantomData }
    }
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let packet = match futures::ready!(this.fut.poll(cx))?.result {
            ControlResultKind::Ping => Some(codec::Packet::PingResponse),
            ControlResultKind::PublishAck(id) => {
                this.inner.inflight.borrow_mut().remove(&id);
                Some(codec::Packet::PublishAck { packet_id: id })
            }
            ControlResultKind::Subscribe(_) => unreachable!(),
            ControlResultKind::Unsubscribe(_) => unreachable!(),
            ControlResultKind::Disconnect => {
                this.inner.sink.close();
                Some(codec::Packet::Disconnect)
            }
            ControlResultKind::Closed | ControlResultKind::Nothing => None,
        };

        Poll::Ready(Ok(packet))
    }
}
