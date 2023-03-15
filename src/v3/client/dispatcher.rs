use std::cell::RefCell;
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use ntex::io::DispatchItem;
use ntex::service::Service;
use ntex::util::{inflight::InFlightService, BoxFuture, Either, HashSet, Ready};

use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{codec, control::ControlResultKind, publish::Publish};
use crate::{error::MqttError, error::ProtocolError};

use super::control::{ControlMessage, ControlResult};

/// mqtt3 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: Rc<MqttShared>,
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
    publish: T,
    shutdown: RefCell<Option<BoxFuture<'static, ()>>>,
    inner: Rc<Inner<C>>,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    inflight: RefCell<HashSet<NonZeroU16>>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(sink: Rc<MqttShared>, publish: T, control: C) -> Self {
        Self {
            publish,
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
    type Future<'f> = Either<
        PublishResponse<'f, T, C, E>,
        Either<Ready<Self::Response, MqttError<E>>, ControlResponse<'f, C, E>>,
    > where Self: 'f;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(MqttError::Service)?;
        let res2 = self.inner.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut shutdown = self.shutdown.borrow_mut();
        if !shutdown.is_some() {
            self.inner.sink.close();
            let inner = self.inner.clone();
            *shutdown = Some(Box::pin(async move {
                let _ = inner.control.call(ControlMessage::closed()).await;
            }));
        }

        let res0 = shutdown.as_mut().expect("guard above").as_mut().poll(cx);
        let res1 = self.publish.poll_shutdown(cx);
        let res2 = self.inner.control.poll_shutdown(cx);
        if res0.is_pending() || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn call(&self, packet: DispatchItem<Rc<MqttShared>>) -> Self::Future<'_> {
        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            DispatchItem::Item(codec::Packet::Publish(publish)) => {
                let inner = self.inner.as_ref();
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
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Either::Right(Either::Left(Ready::Err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(codec::Packet::SubscribeAck { packet_id, status }) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Subscribe { packet_id, status }) {
                    Either::Right(Either::Left(Ready::Err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(codec::Packet::UnsubscribeAck { packet_id }) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Either::Right(Either::Left(Ready::Err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(
                pkt @ (codec::Packet::PingRequest
                | codec::Packet::Disconnect
                | codec::Packet::Subscribe { .. }
                | codec::Packet::Unsubscribe { .. }),
            ) => Either::Right(Either::Left(Ready::Err(
                ProtocolError::unexpected_packet(
                    pkt.packet_type(),
                    "Packet of the type is not expected from server",
                )
                .into(),
            ))),
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
            DispatchItem::WBackPressureEnabled => {
                self.inner.sink.enable_wr_backpressure();
                Either::Right(Either::Left(Ready::Ok(None)))
            }
            DispatchItem::WBackPressureDisabled => {
                self.inner.sink.disable_wr_backpressure();
                Either::Right(Either::Left(Ready::Ok(None)))
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<'f, T: Service<Publish>, C: Service<ControlMessage<E>>, E>
    where T: 'f
    {
        #[pin]
        fut: T::Future<'f>,
        #[pin]
        fut_c: Option<ControlResponse<'f, C, E>>,
        packet_id: Option<NonZeroU16>,
        inner: &'f Inner<C>,
        _t: PhantomData<E>,
    }
}

impl<'f, T, C, E> Future for PublishResponse<'f, T, C, E>
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
                    .set(Some(ControlResponse::new(ControlMessage::error(e), *this.inner)));
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
                    *this.inner,
                )));
                self.poll(cx)
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Control service response future
    pub(crate) struct ControlResponse<'f, C: Service<ControlMessage<E>>, E>
    where C: 'f, E: 'f
    {
        #[pin]
        fut: C::Future<'f>,
        inner: &'f Inner<C>,
        _t: PhantomData<E>,
    }
}

impl<'f, C, E> ControlResponse<'f, C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(msg: ControlMessage<E>, inner: &'f Inner<C>) -> Self {
        Self { fut: inner.control.call(msg), inner, _t: PhantomData }
    }
}

impl<'f, C, E> Future for ControlResponse<'f, C, E>
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
                    None
                }
                ControlResultKind::Closed | ControlResultKind::Nothing => None,
            },
            Poll::Pending => return Poll::Pending,
        };

        Poll::Ready(Ok(packet))
    }
}

#[cfg(test)]
mod tests {
    use ntex::{io::Io, service::fn_service, testing::IoTest, util::lazy};
    use std::rc::Rc;

    use super::*;
    use crate::v3::{codec::Codec, MqttSink};

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0);
        let codec = Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Default::default()));

        let disp = Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| Ready::Ok(Either::Left(()))),
            fn_service(|_| Ready::Ok(ControlResult { result: ControlResultKind::Nothing })),
        );

        let sink = MqttSink::new(shared.clone());
        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());
        assert!(shared.wait_readiness().is_none());

        disp.call(DispatchItem::WBackPressureEnabled).await.unwrap();
        assert!(!sink.is_ready());
        let rx = shared.wait_readiness();
        let rx2 = shared.wait_readiness().unwrap();
        assert!(rx.is_some());

        let rx = rx.unwrap();
        disp.call(DispatchItem::WBackPressureDisabled).await.unwrap();
        assert!(lazy(|cx| rx.poll_recv(cx).is_ready()).await);
        assert!(!lazy(|cx| rx2.poll_recv(cx).is_ready()).await);
    }
}
