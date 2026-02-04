use std::cell::{Cell, RefCell};
use std::{marker::PhantomData, num::NonZeroU16, rc::Rc, task::Context};

use ntex_dispatcher::{Control as DispControl, DispatchItem, Reason};
use ntex_service::{Pipeline, Service, ServiceCtx};
use ntex_util::future::{Either, join};
use ntex_util::{HashSet, services::inflight::InFlightService};

use crate::error::{HandshakeError, MqttError, PayloadError, ProtocolError};
use crate::v3::codec::{self, Decoded, Encoded, Packet};
use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{control::ControlAckKind, publish::Publish};
use crate::{payload::Payload, payload::PayloadStatus, payload::PlSender};

use super::control::{Control, ControlAck};

/// mqtt3 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: Rc<MqttShared>,
    inflight: usize,
    max_buffer_size: usize,
    publish: T,
    control: C,
) -> impl Service<DispatchItem<Rc<MqttShared>>, Response = Option<Encoded>, Error = MqttError<E>>
where
    E: 'static,
    T: Service<Publish, Response = Either<(), Publish>, Error = E> + 'static,
    C: Service<Control<E>, Response = ControlAck, Error = E> + 'static,
{
    // limit number of in-flight messages
    InFlightService::new(
        inflight,
        Dispatcher::new(sink, publish, control.map_err(MqttError::Service), max_buffer_size),
    )
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<Control<E>>, E> {
    publish: T,
    inner: Rc<Inner<C>>,
    max_buffer_size: usize,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: Pipeline<C>,
    sink: Rc<MqttShared>,
    payload: Cell<Option<PlSender>>,
    inflight: RefCell<HashSet<NonZeroU16>>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    pub(crate) fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: C,
        max_buffer_size: usize,
    ) -> Self {
        Self {
            publish,
            max_buffer_size,
            inner: Rc::new(Inner {
                sink,
                payload: Cell::new(None),
                control: Pipeline::new(control),
                inflight: RefCell::new(HashSet::default()),
            }),
            _t: PhantomData,
        }
    }
}

impl<C> Inner<C> {
    fn drop_payload<PErr>(&self, err: &PErr)
    where
        PErr: Clone,
        PayloadError: From<PErr>,
    {
        if let Some(pl) = self.payload.take() {
            pl.set_error(err.clone().into());
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E> + 'static,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
    E: 'static,
{
    type Response = Option<Encoded>;
    type Error = MqttError<E>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), self.inner.control.ready()).await;
        let result = if let Err(e) = res1 {
            if res2.is_err() {
                Err(MqttError::Service(e))
            } else {
                match self.inner.control.call(Control::error(e)).await {
                    Ok(_) => {
                        self.inner.sink.close();
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
        } else {
            res2
        };

        if result.is_ok()
            && let Some(pl) = self.inner.payload.take()
        {
            self.inner.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.close();
            }
        }
        result
    }

    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        if let Err(e) = self.publish.poll(cx) {
            let inner = self.inner.clone();
            ntex_rt::spawn(async move {
                if inner.control.call(Control::error(e)).await.is_ok() {
                    inner.sink.close();
                }
            });
        }
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        self.inner.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.close();
        let _ = self.inner.control.call(Control::shutdown()).await;

        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        packet: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Dispatch packet: {packet:#?}");

        match packet {
            DispatchItem::Item(Decoded::Publish(publish, payload, size)) => {
                let inner = self.inner.as_ref();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id
                    && !inner.inflight.borrow_mut().insert(pid)
                {
                    log::trace!("Duplicated packet id for publish packet: {pid:?}");
                    return Err(MqttError::Handshake(HandshakeError::Protocol(
                        ProtocolError::generic_violation(
                            "PUBLISH received with packet id that is already in use [MQTT-2.2.1-3]",
                        ),
                    )));
                }

                let payload = if publish.payload_size == payload.len() as u32 {
                    Payload::from_bytes(payload)
                } else {
                    let (pl, sender) = Payload::from_stream(payload, self.max_buffer_size);
                    self.inner.payload.set(Some(sender));
                    pl
                };

                publish_fn(
                    &self.publish,
                    Publish::new(publish, payload, size),
                    packet_id,
                    inner,
                    ctx,
                )
                .await
            }
            DispatchItem::Item(Decoded::PayloadChunk(buf, eof)) => {
                let pl = self.inner.payload.take().unwrap();
                pl.feed_data(buf);
                if eof {
                    pl.feed_eof();
                } else {
                    self.inner.payload.set(Some(pl));
                }
                Ok(None)
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishAck { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Err(MqttError::Handshake(HandshakeError::Protocol(e)))
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishReceived { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(packet_id)) {
                    control(Control::proto_error(e), &self.inner).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishComplete { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(packet_id)) {
                    control(Control::proto_error(e), &self.inner).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishRelease { packet_id }, _)) => {
                if self.inner.inflight.borrow().contains(&packet_id) {
                    control(Control::pubrel(packet_id), &self.inner).await
                } else {
                    log::warn!("Unknown packet-id in PublishRelease packet");
                    self.inner.sink.close();
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(
                Packet::SubscribeAck { packet_id, status },
                _,
            )) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Subscribe { packet_id, status }) {
                    Err(MqttError::Handshake(HandshakeError::Protocol(e)))
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::UnsubscribeAck { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Err(MqttError::Handshake(HandshakeError::Protocol(e)))
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(
                pkt @ (Packet::PingRequest
                | Packet::Disconnect
                | Packet::Subscribe { .. }
                | Packet::Unsubscribe { .. }),
                _,
            )) => Err(HandshakeError::Protocol(ProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            ))
            .into()),
            DispatchItem::Item(Decoded::Packet(pkt, _)) => {
                log::debug!("Unsupported packet: {pkt:?}");
                Ok(None)
            }
            DispatchItem::Stop(Reason::Encoder(err)) => {
                let err = ProtocolError::Encode(err);
                self.inner.drop_payload(&err);
                control(Control::proto_error(err), &self.inner).await
            }
            DispatchItem::Stop(Reason::Decoder(err)) => {
                let err = ProtocolError::Decode(err);
                self.inner.drop_payload(&err);
                control(Control::proto_error(err), &self.inner).await
            }
            DispatchItem::Stop(Reason::Io(err)) => {
                self.inner.drop_payload(&PayloadError::Disconnected);
                control(Control::peer_gone(err), &self.inner).await
            }
            DispatchItem::Stop(Reason::KeepAliveTimeout) => {
                self.inner.drop_payload(&ProtocolError::KeepAliveTimeout);
                control(Control::proto_error(ProtocolError::KeepAliveTimeout), &self.inner)
                    .await
            }
            DispatchItem::Stop(Reason::ReadTimeout) => {
                self.inner.drop_payload(&ProtocolError::ReadTimeout);
                control(Control::proto_error(ProtocolError::ReadTimeout), &self.inner).await
            }
            DispatchItem::Control(DispControl::WBackPressureEnabled) => {
                self.inner.sink.enable_wr_backpressure();
                Ok(None)
            }
            DispatchItem::Control(DispControl::WBackPressureDisabled) => {
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
) -> Result<Option<Encoded>, MqttError<E>>
where
    T: Service<Publish, Response = Either<(), Publish>, Error = E>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let res = match ctx.call(svc, pkt).await {
        Ok(item) => item,
        Err(e) => {
            return control(Control::error(e), inner).await;
        }
    };

    match res {
        Either::Left(()) => {
            log::trace!("Publish result for packet {packet_id:?} is ready");

            if let Some(packet_id) = packet_id {
                inner.inflight.borrow_mut().remove(&packet_id);
                Ok(Some(Encoded::Packet(Packet::PublishAck { packet_id })))
            } else {
                Ok(None)
            }
        }
        Either::Right(pkt) => {
            let (pkt, payload, size) = pkt.into_inner();
            control(Control::publish(pkt, payload, size), inner).await
        }
    }
}

async fn control<C, E>(
    msg: Control<E>,
    inner: &Inner<C>,
) -> Result<Option<Encoded>, MqttError<E>>
where
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let packet = match inner
        .control
        .call(msg)
        .await
        .inspect_err(|_| {
            inner.drop_payload(&PayloadError::Service);
            inner.sink.close();
        })?
        .result
    {
        ControlAckKind::Ping => Some(Encoded::Packet(codec::Packet::PingResponse)),
        ControlAckKind::PublishAck(id) => {
            inner.inflight.borrow_mut().remove(&id);
            Some(Encoded::Packet(codec::Packet::PublishAck { packet_id: id }))
        }
        ControlAckKind::PublishRelease(id) => {
            inner.inflight.borrow_mut().remove(&id);
            Some(Encoded::Packet(Packet::PublishComplete { packet_id: id }))
        }
        ControlAckKind::Subscribe(_) | ControlAckKind::Unsubscribe(_) => unreachable!(),
        ControlAckKind::Disconnect => {
            inner.drop_payload(&PayloadError::Service);
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
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{cfg::SharedCfg, fn_service};
    use ntex_util::future::{Ready, lazy};
    use ntex_util::time::{Seconds, sleep};

    use super::*;
    use crate::v3::{MqttSink, QoS, codec::Codec, codec::Decoded};

    #[ntex::test]
    async fn test_dup_packet_id() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));

        let disp = Pipeline::new(Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| async {
                sleep(Seconds(10)).await;
                Ok(Either::Left(()))
            }),
            fn_service(|_| Ready::Ok(ControlAck { result: ControlAckKind::Nothing })),
            32 * 1024,
        ));

        let mut f: Pin<Box<dyn Future<Output = Result<_, _>>>> =
            Box::pin(disp.call(DispatchItem::Item(Decoded::Publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtLeastOnce,
                    topic: ByteString::new(),
                    packet_id: NonZeroU16::new(1),
                    payload_size: 0,
                },
                Bytes::new(),
                999,
            ))));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        let f = Box::pin(disp.call(DispatchItem::Item(Decoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: QoS::AtLeastOnce,
                topic: ByteString::new(),
                packet_id: NonZeroU16::new(1),
                payload_size: 0,
            },
            Bytes::new(),
            999,
        ))));
        let err = f.await.err().unwrap();
        match err {
            MqttError::Handshake(HandshakeError::Protocol(msg)) => {
                assert!(
                    format!("{msg}")
                        .contains("PUBLISH received with packet id that is already in use")
                );
            }
            _ => panic!(),
        }
    }

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));

        let disp = Pipeline::new(Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| Ready::Ok(Either::Left(()))),
            fn_service(|_| Ready::Ok(ControlAck { result: ControlAckKind::Nothing })),
            32 * 1024,
        ));

        let sink = MqttSink::new(shared.clone());
        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());
        assert!(shared.wait_readiness().is_none());

        disp.call(DispatchItem::Control(DispControl::WBackPressureEnabled)).await.unwrap();
        assert!(!sink.is_ready());
        let rx = shared.wait_readiness();
        let rx2 = shared.wait_readiness().unwrap();
        assert!(rx.is_some());

        let rx = rx.unwrap();
        disp.call(DispatchItem::Control(DispControl::WBackPressureDisabled)).await.unwrap();
        assert!(lazy(|cx| rx.poll_recv(cx).is_ready()).await);
        assert!(!lazy(|cx| rx2.poll_recv(cx).is_ready()).await);
    }
}
