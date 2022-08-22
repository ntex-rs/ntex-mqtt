use std::cell::RefCell;
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use ntex::io::DispatchItem;
use ntex::service::Service;
use ntex::util::{inflight::InFlightService, Either, HashSet, Ready};

use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{codec, control::ControlResultKind, publish::Publish, sink::MqttSink};
use crate::{error::MqttError, error::ProtocolError, types::packet_type};

use super::control::{ControlMessage, ControlResult};

/// mqtt3 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: MqttSink,
    inflight: usize,
    publish: T,
    control: C,
) -> impl Service<DispatchItem<Rc<MqttShared>>, Response = Option<codec::Packet>, Error = MqttError<E>>
where
    E: 'static,
    T: Service<Publish, Response = Either<(), Publish>, Error = E> + 'static,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = E> + 'static,
{
    // limit number of in-flight messages
    InFlightService::new(
        inflight,
        Dispatcher::new(sink, publish, control.map_err(MqttError::Service)),
    )
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<ControlMessage<E>>, E> {
    sink: MqttSink,
    publish: T,
    shutdown: RefCell<Option<Pin<Box<C::Future>>>>,
    inner: Rc<Inner<C>>,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: C,
    sink: MqttSink,
    inflight: RefCell<HashSet<NonZeroU16>>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(sink: MqttSink, publish: T, control: C) -> Self {
        Self {
            publish,
            sink: sink.clone(),
            shutdown: RefCell::new(None),
            inner: Rc::new(Inner { sink, control, inflight: RefCell::new(HashSet::default()) }),
            _t: PhantomData,
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>> + 'static,
    E: 'static,
{
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T, C, E>,
        Either<Ready<Self::Response, MqttError<E>>, ControlResponse<C, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(MqttError::Service)?;
        let res2 = self.inner.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        let mut shutdown = self.shutdown.borrow_mut();
        if !shutdown.is_some() {
            self.inner.sink.close();
            *shutdown =
                Some(Box::pin(self.inner.control.call(ControlMessage::closed(is_error))));
        }

        let res0 = shutdown.as_mut().expect("guard above").as_mut().poll(cx);
        let res1 = self.publish.poll_shutdown(cx, is_error);
        let res2 = self.inner.control.poll_shutdown(cx, is_error);
        if res0.is_pending() || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn call(&self, packet: DispatchItem<Rc<MqttShared>>) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            DispatchItem::Item(codec::Packet::Publish(publish)) => {
                let inner = self.inner.clone();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inner.inflight.borrow_mut().insert(pid) {
                        log::trace!("Duplicated packet id for publish packet: {:?}", pid);
                        return Either::Right(Either::Left(Ready::Err(
                            MqttError::ServerError("Duplicated packet id for publish packet"),
                        )));
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
            DispatchItem::Item(codec::Packet::PublishAck { packet_id }) => {
                if let Err(e) = self.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Either::Right(Either::Left(Ready::Err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(codec::Packet::PingRequest) => {
                Either::Right(Either::Left(Ready::Ok(Some(codec::Packet::PingResponse))))
            }
            DispatchItem::Item(codec::Packet::Disconnect) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::dis(), &self.inner),
            )),
            DispatchItem::Item(codec::Packet::SubscribeAck { packet_id, status }) => {
                if let Err(e) = self.sink.pkt_ack(Ack::Subscribe { packet_id, status }) {
                    Either::Right(Either::Left(Ready::Err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(codec::Packet::UnsubscribeAck { packet_id }) => {
                if let Err(e) = self.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Either::Right(Either::Left(Ready::Err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(codec::Packet::Subscribe { .. }) => {
                Either::Right(Either::Left(Ready::Err(
                    ProtocolError::Unexpected(
                        packet_type::SUBSCRIBE,
                        "Subscribe packet is not supported",
                    )
                    .into(),
                )))
            }
            DispatchItem::Item(codec::Packet::Unsubscribe { .. }) => {
                Either::Right(Either::Left(Ready::Err(
                    ProtocolError::Unexpected(
                        packet_type::UNSUBSCRIBE,
                        "Unsubscribe packet is not supported",
                    )
                    .into(),
                )))
            }
            DispatchItem::Item(pkt) => {
                log::debug!("Unsupported packet: {:?}", pkt);
                Either::Right(Either::Left(Ready::Ok(None)))
            }
            DispatchItem::EncoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Encode(err)),
                    &self.inner,
                )))
            }
            DispatchItem::DecoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Decode(err)),
                    &self.inner,
                )))
            }
            DispatchItem::Disconnect(err) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::peer_gone(err), &self.inner),
            )),
            DispatchItem::KeepAliveTimeout => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::KeepAliveTimeout),
                    &self.inner,
                )))
            }
            DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => {
                Either::Right(Either::Left(Ready::Ok(None)))
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<T: Service<Publish>, C: Service<ControlMessage<E>>, E> {
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
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(fut) = self.as_mut().project().fut_c.as_pin_mut() {
            return fut.poll(cx);
        }

        let mut this = self.as_mut().project();
        let res = match this.fut.poll(cx) {
            Poll::Ready(Ok(item)) => item,
            Poll::Ready(Err(e)) => {
                this.fut_c
                    .set(Some(ControlResponse::new(ControlMessage::error(e), &*this.inner)));
                return self.poll(cx);
            }
            Poll::Pending => return Poll::Pending,
        };

        match res {
            Either::Left(_) => {
                log::trace!("Publish result for packet {:?} is ready", this.packet_id);

                if let Some(packet_id) = this.packet_id {
                    this.inner.inflight.borrow_mut().remove(packet_id);
                    Poll::Ready(Ok(Some(codec::Packet::PublishAck { packet_id: *packet_id })))
                } else {
                    Poll::Ready(Ok(None))
                }
            }
            Either::Right(pkt) => {
                this.fut_c.set(Some(ControlResponse::new(
                    ControlMessage::publish(pkt.into_inner()),
                    &*this.inner,
                )));
                self.poll(cx)
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Control service response future
    pub(crate) struct ControlResponse<C: Service<ControlMessage<E>>, E> {
        #[pin]
        fut: C::Future,
        inner: Rc<Inner<C>>,
        _t: PhantomData<E>,
    }
}

impl<C, E> ControlResponse<C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(msg: ControlMessage<E>, inner: &Rc<Inner<C>>) -> Self {
        Self { fut: inner.control.call(msg), inner: inner.clone(), _t: PhantomData }
    }
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let packet = match this.fut.poll(cx)? {
            Poll::Ready(item) => match item.result {
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
            },
            Poll::Pending => return Poll::Pending,
        };

        Poll::Ready(Ok(packet))
    }
}
