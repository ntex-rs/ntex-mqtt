use std::task::{Context, Poll};
use std::{cell::RefCell, marker::PhantomData, num::NonZeroU16, rc::Rc};

use ntex::io::DispatchItem;
use ntex::service::{Pipeline, Service, ServiceCtx};
use ntex::util::{inflight::InFlightService, BoxFuture, Either, HashSet};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{codec, control::ControlResultKind, publish::Publish};

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
                let _ = Pipeline::new(&inner.control).call(ControlMessage::closed()).await;
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

    async fn call(
        &self,
        packet: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            DispatchItem::Item((codec::Packet::Publish(publish), size)) => {
                let inner = self.inner.as_ref();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inner.inflight.borrow_mut().insert(pid) {
                        log::trace!("Duplicated packet id for publish packet: {:?}", pid);
                        return Err(MqttError::Handshake(
                            HandshakeError::Protocol(
                                ProtocolError::generic_violation("PUBLISH received with packet id that is already in use [MQTT-2.2.1-3]"))
                        ));
                    }
                }
                publish_fn(&self.publish, Publish::new(publish, size), packet_id, inner, ctx)
                    .await
            }
            DispatchItem::Item((codec::Packet::PublishAck { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Err(MqttError::Handshake(HandshakeError::Protocol(e)))
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item((codec::Packet::SubscribeAck { packet_id, status }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Subscribe { packet_id, status }) {
                    Err(MqttError::Handshake(HandshakeError::Protocol(e)))
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item((codec::Packet::UnsubscribeAck { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Err(MqttError::Handshake(HandshakeError::Protocol(e)))
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item((
                pkt @ (codec::Packet::PingRequest
                | codec::Packet::Disconnect
                | codec::Packet::Subscribe { .. }
                | codec::Packet::Unsubscribe { .. }),
                _,
            )) => Err(HandshakeError::Protocol(ProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            ))
            .into()),
            DispatchItem::Item((pkt, _)) => {
                log::debug!("Unsupported packet: {:?}", pkt);
                Ok(None)
            }
            DispatchItem::EncoderError(err) => {
                control(
                    ControlMessage::proto_error(ProtocolError::Encode(err)),
                    &self.inner,
                    ctx,
                )
                .await
            }
            DispatchItem::DecoderError(err) => {
                control(
                    ControlMessage::proto_error(ProtocolError::Decode(err)),
                    &self.inner,
                    ctx,
                )
                .await
            }
            DispatchItem::Disconnect(err) => {
                control(ControlMessage::peer_gone(err), &self.inner, ctx).await
            }
            DispatchItem::KeepAliveTimeout => {
                control(
                    ControlMessage::proto_error(ProtocolError::KeepAliveTimeout),
                    &self.inner,
                    ctx,
                )
                .await
            }
            DispatchItem::ReadTimeout => {
                control(
                    ControlMessage::proto_error(ProtocolError::ReadTimeout),
                    &self.inner,
                    ctx,
                )
                .await
            }
            DispatchItem::WBackPressureEnabled => {
                self.inner.sink.enable_wr_backpressure();
                Ok(None)
            }
            DispatchItem::WBackPressureDisabled => {
                self.inner.sink.disable_wr_backpressure();
                Ok(None)
            }
        }
    }
}

async fn publish_fn<'f, T, C, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: Option<NonZeroU16>,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<codec::Packet>, MqttError<E>>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    let res = match ctx.call(svc, pkt).await {
        Ok(item) => item,
        Err(e) => {
            return control(ControlMessage::error(e), inner, ctx).await;
        }
    };

    match res {
        Either::Left(_) => {
            log::trace!("Publish result for packet {:?} is ready", packet_id);

            if let Some(packet_id) = packet_id {
                inner.inflight.borrow_mut().remove(&packet_id);
                Ok(Some(codec::Packet::PublishAck { packet_id }))
            } else {
                Ok(None)
            }
        }
        Either::Right(pkt) => {
            control(ControlMessage::publish(pkt.into_inner()), inner, ctx).await
        }
    }
}

async fn control<'f, T, C, E>(
    msg: ControlMessage<E>,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<codec::Packet>, MqttError<E>>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    let packet = match ctx.call(&inner.control, msg).await?.result {
        ControlResultKind::Ping => Some(codec::Packet::PingResponse),
        ControlResultKind::PublishAck(id) => {
            inner.inflight.borrow_mut().remove(&id);
            Some(codec::Packet::PublishAck { packet_id: id })
        }
        ControlResultKind::Subscribe(_) => unreachable!(),
        ControlResultKind::Unsubscribe(_) => unreachable!(),
        ControlResultKind::Disconnect => {
            inner.sink.close();
            None
        }
        ControlResultKind::Closed | ControlResultKind::Nothing => None,
    };

    Ok(packet)
}

#[cfg(test)]
mod tests {
    use ntex::time::{sleep, Seconds};
    use ntex::util::{lazy, ByteString, Bytes, Ready};
    use ntex::{io::Io, service::fn_service, testing::IoTest};
    use std::{future::Future, pin::Pin};

    use super::*;
    use crate::v3::{codec::Codec, MqttSink, QoS};

    #[ntex::test]
    async fn test_dup_packet_id() {
        let io = Io::new(IoTest::create().0);
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Default::default()));

        let disp = Pipeline::new(Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| async {
                sleep(Seconds(10)).await;
                Ok(Either::Left(()))
            }),
            fn_service(|_| Ready::Ok(ControlResult { result: ControlResultKind::Nothing })),
        ));

        let mut f: Pin<Box<dyn Future<Output = Result<_, _>>>> =
            Box::pin(disp.call(DispatchItem::Item((
                codec::Packet::Publish(codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtLeastOnce,
                    topic: ByteString::new(),
                    packet_id: NonZeroU16::new(1),
                    payload: Bytes::new(),
                }),
                999,
            ))));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        let f = Box::pin(disp.call(DispatchItem::Item((
            codec::Packet::Publish(codec::Publish {
                dup: false,
                retain: false,
                qos: QoS::AtLeastOnce,
                topic: ByteString::new(),
                packet_id: NonZeroU16::new(1),
                payload: Bytes::new(),
            }),
            999,
        ))));
        let err = f.await.err().unwrap();
        match err {
            MqttError::Handshake(HandshakeError::Protocol(msg)) => {
                assert!(format!("{}", msg)
                    .contains("PUBLISH received with packet id that is already in use"))
            }
            _ => panic!(),
        }
    }

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0);
        let codec = Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Default::default()));

        let disp = Pipeline::new(Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| Ready::Ok(Either::Left(()))),
            fn_service(|_| Ready::Ok(ControlResult { result: ControlResultKind::Nothing })),
        ));

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
