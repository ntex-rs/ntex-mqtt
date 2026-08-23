use std::{cell::Cell, cell::RefCell, num::NonZeroU16, rc::Rc};

use ntex_service::{Ctx, Pipeline, Service};
use ntex_util::future::{Either, join};
use ntex_util::{HashSet, services::inflight::InFlightService};

use crate::error::{DispatcherError, PayloadError, ProtocolError, SpecViolation};
use crate::v3::codec::{self, Decoded, Encoded, Packet};
use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{control::ProtocolMessageKind, publish::Publish};
use crate::{payload::Payload, payload::PayloadStatus, payload::PlSender};

use super::control::{ProtocolMessage, ProtocolMessageAck};

/// mqtt3 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: Rc<MqttShared>,
    inflight: usize,
    max_buffer_size: usize,
    publish: T,
    control: C,
) -> impl Service<(), Decoded, Res = Option<Encoded>, Error = DispatcherError<E>>
where
    E: 'static,
    T: Service<(), Publish, Res = Either<(), Publish>, Error = E> + 'static,
    C: Service<(), ProtocolMessage, Res = ProtocolMessageAck, Error = E> + 'static,
{
    // limit number of in-flight messages
    InFlightService::new(
        inflight,
        Dispatcher::new(
            sink,
            publish,
            control.map_err(DispatcherError::Service),
            max_buffer_size,
        ),
    )
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, E> {
    publish: T,
    inner: Rc<Inner<E>>,
    max_buffer_size: usize,
}

struct Inner<E> {
    control: Pipeline<ProtocolMessage, ProtocolMessageAck, DispatcherError<E>>,
    sink: Rc<MqttShared>,
    payload: Cell<Option<PlSender>>,
    inflight: RefCell<HashSet<NonZeroU16>>,
}

impl<T, E> Dispatcher<T, E>
where
    T: Service<(), Publish, Res = Either<(), Publish>, Error = E>,
    E: 'static,
{
    pub(crate) fn new<C>(
        sink: Rc<MqttShared>,
        publish: T,
        control: C,
        max_buffer_size: usize,
    ) -> Self
    where
        C: Service<(), ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>
            + 'static,
    {
        Self {
            publish,
            max_buffer_size,
            inner: Rc::new(Inner {
                sink,
                payload: Cell::new(None),
                control: Pipeline::new(control),
                inflight: RefCell::new(HashSet::default()),
            }),
        }
    }
}

impl<E> Inner<E> {
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

impl<T, E> Service<(), Decoded> for Dispatcher<T, E>
where
    T: Service<(), Publish, Res = Either<(), Publish>, Error = E> + 'static,
    E: 'static,
{
    type Res = Option<Encoded>;
    type Error = DispatcherError<E>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), self.inner.control.ready()).await;
        if (res1.is_err() || res2.is_err())
            && let Some(pl) = self.inner.payload.take()
        {
            self.inner.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }

        res1.map_err(DispatcherError::Service)?;
        res2?;
        Ok(())
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, ()>) {
        self.inner.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.close();
        self.inner.control.shutdown().await;

        ctx.shutdown(&self.publish).await;
    }

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        packet: Decoded,
        ctx: Ctx<'_, Self, ()>,
    ) -> Result<Self::Res, Self::Error> {
        log::trace!("Dispatch packet: {packet:#?}");

        match packet {
            Decoded::Publish(publish, payload, size) => {
                let inner = self.inner.as_ref();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id
                    && !inner.inflight.borrow_mut().insert(pid)
                {
                    log::trace!("Duplicated packet id for publish packet: {pid:?}");
                    return Err(SpecViolation::PacketId_2_2_1_3_Pub.into());
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
            Decoded::PayloadChunk(buf, eof) => {
                let pl = self.inner.payload.take().unwrap();
                pl.feed_data(buf);
                if eof {
                    pl.feed_eof();
                } else {
                    self.inner.payload.set(Some(pl));
                }
                Ok(None)
            }
            Decoded::Packet(Packet::PublishAck { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishReceived { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishComplete { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishRelease { packet_id }, _) => {
                if self.inner.inflight.borrow().contains(&packet_id) {
                    self.inner.control(ProtocolMessage::pubrel(packet_id)).await
                } else {
                    log::warn!("Unknown packet-id in PublishRelease packet");
                    self.inner.sink.close();
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::SubscribeAck { packet_id, status }, _) => {
                if let Err(e) = self
                    .inner
                    .sink
                    .pkt_ack(Ack::Subscribe { packet_id, status })
                {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::UnsubscribeAck { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(
                pkt @ (Packet::PingRequest
                | Packet::Disconnect
                | Packet::Subscribe { .. }
                | Packet::Unsubscribe { .. }),
                _,
            ) => Err(ProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            )
            .into()),
            Decoded::Packet(pkt, _) => {
                log::debug!("Unsupported packet: {pkt:?}");
                Ok(None)
            }
        }
    }
}

async fn publish_fn<'f, T, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: Option<NonZeroU16>,
    inner: &'f Inner<E>,
    ctx: Ctx<'f, Dispatcher<T, E>>,
) -> Result<Option<Encoded>, DispatcherError<E>>
where
    E: 'static,
    T: Service<(), Publish, Res = Either<(), Publish>, Error = E>,
{
    let res = ctx.call(svc, pkt).await.map_err(DispatcherError::Service)?;
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
            inner
                .control(ProtocolMessage::publish(pkt, payload, size))
                .await
        }
    }
}

impl<E: 'static> Inner<E> {
    async fn control(&self, msg: ProtocolMessage) -> Result<Option<Encoded>, DispatcherError<E>> {
        let packet = match self
            .control
            .call(msg)
            .await
            .inspect_err(|_| {
                self.drop_payload(&PayloadError::Service);
                self.sink.close();
            })?
            .result
        {
            ProtocolMessageKind::Ping => Some(Encoded::Packet(codec::Packet::PingResponse)),
            ProtocolMessageKind::PublishAck(id) => {
                self.inflight.borrow_mut().remove(&id);
                Some(Encoded::Packet(codec::Packet::PublishAck { packet_id: id }))
            }
            ProtocolMessageKind::PublishRelease(id) => {
                self.inflight.borrow_mut().remove(&id);
                Some(Encoded::Packet(Packet::PublishComplete { packet_id: id }))
            }
            ProtocolMessageKind::Subscribe(_) | ProtocolMessageKind::Unsubscribe(_) => {
                unreachable!()
            }
            ProtocolMessageKind::Disconnect => {
                self.drop_payload(&PayloadError::Service);
                self.sink.close();
                None
            }
            ProtocolMessageKind::Nothing => None,
        };

        Ok(packet)
    }
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
    use crate::v3::{QoS, codec::Decoded};

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
            fn_service(|_| {
                Ready::Ok(ProtocolMessageAck {
                    result: ProtocolMessageKind::Nothing,
                })
            }),
            32 * 1024,
        ));

        let mut f: Pin<Box<dyn Future<Output = Result<_, _>>>> =
            Box::pin(disp.call(Decoded::Publish(
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
            )));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        let f = Box::pin(disp.call(Decoded::Publish(
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
        )));
        let err = f.await.err().unwrap();
        match err {
            DispatcherError::Protocol(msg) => {
                assert!(
                    format!("{msg}")
                        .contains("PUBLISH received with packet id that is already in use")
                );
            }
            DispatcherError::Service(()) => panic!(),
        }
    }
}
