use std::{cell::RefCell, marker::PhantomData, num::NonZeroU16, rc::Rc};

use ntex_io::DispatchItem;
use ntex_service::{Pipeline, Service, ServiceCtx};
use ntex_util::future::{join, Either};
use ntex_util::{services::inflight::InFlightService, HashSet};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{codec, control::ControlAckKind, publish::Publish};

use super::control::{Control, ControlAck};

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
    C: Service<Control<E>, Response = ControlAck, Error = E> + 'static,
{
    // limit number of in-flight messages
    InFlightService::new(
        inflight,
        Dispatcher::new(sink, publish, control.map_err(MqttError::Service)),
    )
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<Control<E>>, E> {
    publish: T,
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
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    pub(crate) fn new(sink: Rc<MqttShared>, publish: T, control: C) -> Self {
        Self {
            publish,
            inner: Rc::new(Inner { sink, control, inflight: RefCell::new(HashSet::default()) }),
            _t: PhantomData,
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
    E: 'static,
{
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), ctx.ready(&self.inner.control)).await;
        if let Err(e) = res1 {
            if res2.is_err() {
                Err(MqttError::Readiness(Some(e.into())))
            } else {
                match ctx.call_nowait(&self.inner.control, Control::error(e.into())).await {
                    Ok(_) => Err(MqttError::Readiness(None)),
                    Err(err) => Err(err),
                }
            }
        } else {
            res2
        }
    }

    async fn shutdown(&self) {
        self.inner.sink.close();
        let _ = Pipeline::new(&self.inner.control).call(Control::closed()).await;

        self.publish.shutdown().await;
        self.inner.control.shutdown().await
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
                control(Control::proto_error(ProtocolError::Encode(err)), &self.inner, ctx)
                    .await
            }
            DispatchItem::DecoderError(err) => {
                control(Control::proto_error(ProtocolError::Decode(err)), &self.inner, ctx)
                    .await
            }
            DispatchItem::Disconnect(err) => {
                control(Control::peer_gone(err), &self.inner, ctx).await
            }
            DispatchItem::KeepAliveTimeout => {
                control(Control::proto_error(ProtocolError::KeepAliveTimeout), &self.inner, ctx)
                    .await
            }
            DispatchItem::ReadTimeout => {
                control(Control::proto_error(ProtocolError::ReadTimeout), &self.inner, ctx)
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
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let res = match ctx.call(svc, pkt).await {
        Ok(item) => item,
        Err(e) => {
            return control(Control::error(e), inner, ctx).await;
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
        Either::Right(pkt) => control(Control::publish(pkt.into_inner()), inner, ctx).await,
    }
}

async fn control<'f, T, C, E>(
    msg: Control<E>,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<codec::Packet>, MqttError<E>>
where
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let packet = match ctx.call(&inner.control, msg).await?.result {
        ControlAckKind::Ping => Some(codec::Packet::PingResponse),
        ControlAckKind::PublishAck(id) => {
            inner.inflight.borrow_mut().remove(&id);
            Some(codec::Packet::PublishAck { packet_id: id })
        }
        ControlAckKind::Subscribe(_) => unreachable!(),
        ControlAckKind::Unsubscribe(_) => unreachable!(),
        ControlAckKind::Disconnect => {
            inner.sink.close();
            None
        }
        ControlAckKind::Closed | ControlAckKind::Nothing => None,
    };

    Ok(packet)
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use ntex_bytes::{ByteString, Bytes};
    use ntex_io::{testing::IoTest, Io};
    use ntex_service::fn_service;
    use ntex_util::future::{lazy, Ready};
    use ntex_util::time::{sleep, Seconds};

    use super::*;
    use crate::v3::{codec::Codec, MqttSink, QoS};

    #[ntex_macros::rt_test]
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
            fn_service(|_| Ready::Ok(ControlAck { result: ControlAckKind::Nothing })),
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

    #[ntex_macros::rt_test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0);
        let codec = Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Default::default()));

        let disp = Pipeline::new(Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| Ready::Ok(Either::Left(()))),
            fn_service(|_| Ready::Ok(ControlAck { result: ControlAckKind::Nothing })),
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
