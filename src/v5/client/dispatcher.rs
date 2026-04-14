use std::cell::{Cell, RefCell};
use std::{marker::PhantomData, num::NonZero, num::NonZeroU16, rc::Rc, task::Context};

use ntex_bytes::ByteString;
use ntex_service::{Pipeline, Service, ServiceCtx, cfg::Cfg};
use ntex_util::{HashMap, HashSet, future::Either, future::join};

use crate::error::{
    DispatcherError, HandshakeError, MqttError, PayloadError, ProtocolError, SpecViolation,
};
use crate::payload::{Payload, PayloadStatus, PlSender};
use crate::v5::codec::{Decoded, DisconnectReasonCode, Encoded, Packet};
use crate::v5::shared::{Ack, MqttShared};
use crate::v5::{codec, control::Pkt, publish::Publish, publish::PublishAck};
use crate::{MqttServiceConfig, types::packet_type};

use super::control::{ProtocolMessage, ProtocolMessageAck};

/// mqtt5 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: Rc<MqttShared>,
    publish: T,
    control: C,
    max_receive: usize,
    max_topic_alias: u16,
    cfg: Cfg<MqttServiceConfig>,
) -> impl Service<Decoded, Response = Option<Encoded>, Error = DispatcherError<E>>
where
    E: From<T::Error> + 'static,
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E> + 'static,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = E> + 'static,
{
    Dispatcher {
        cfg,
        publish,
        max_receive,
        max_topic_alias,
        inner: Rc::new(Inner {
            sink,
            control: Pipeline::new(control.map_err(DispatcherError::Service)),
            info: RefCell::new(PublishInfo {
                aliases: HashMap::default(),
                inflight: HashSet::default(),
            }),
        }),
        _t: PhantomData,
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C, E> {
    publish: T,
    inner: Rc<Inner<C>>,
    max_receive: usize,
    max_topic_alias: u16,
    cfg: Cfg<MqttServiceConfig>,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: Pipeline<C>,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashSet<NonZeroU16>,
    aliases: HashMap<NonZeroU16, ByteString>,
}

impl<T, C, E> Service<Decoded> for Dispatcher<T, C, E>
where
    E: 'static,
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E> + 'static,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>
        + 'static,
{
    type Response = Option<Encoded>;
    type Error = DispatcherError<E>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), self.inner.control.ready()).await;
        if (res1.is_err() || res2.is_err())
            && let Some(pl) = self.inner.sink.payload.take()
        {
            self.inner.sink.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }

        res1.map_err(|e| DispatcherError::Service(e.into()))?;
        res2?;
        Ok(())
    }

    #[inline]
    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        self.publish.poll(cx).map_err(|e| DispatcherError::Service(e.into()))?;
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        self.inner.sink.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.drop_sink();
        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    #[allow(clippy::too_many_lines, clippy::await_holding_refcell_ref)]
    async fn call(
        &self,
        request: Decoded,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Dispatch packet: {request:#?}");

        match request {
            Decoded::Publish(mut publish, payload, size) => {
                let info = self.inner.as_ref();
                let packet_id = publish.packet_id;

                {
                    let mut inner = info.info.borrow_mut();

                    if let Some(pid) = packet_id {
                        // check for receive maximum
                        if self.max_receive != 0 && inner.inflight.len() >= self.max_receive {
                            log::trace!(
                                "Receive maximum exceeded: max: {} inflight: {}",
                                self.max_receive,
                                inner.inflight.len()
                            );
                            return Err(SpecViolation::Pub_3_3_4_9.into());
                        }

                        // check for duplicated packet id
                        if !inner.inflight.insert(pid) {
                            let _ = self.inner.sink.encode_packet(Packet::PublishAck(
                                codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                },
                            ));
                            return Ok(None);
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        if publish.topic.is_empty() {
                            // lookup topic by provided alias
                            if let Some(aliased_topic) = inner.aliases.get(&alias) {
                                publish.topic = aliased_topic.clone();
                            } else {
                                return Err(ProtocolError::violation(
                                    DisconnectReasonCode::TopicAliasInvalid,
                                    "Unknown topic alias",
                                )
                                .into());
                            }
                        } else {
                            // record new alias
                            match inner.aliases.entry(alias) {
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    if entry.get().as_str() != publish.topic.as_str() {
                                        let mut topic = publish.topic.clone();
                                        topic.trimdown();
                                        entry.insert(topic);
                                    }
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    if alias.get() > self.max_topic_alias {
                                        return Err(SpecViolation::Connect_3_1_2_26.into());
                                    }
                                    let mut topic = publish.topic.clone();
                                    topic.trimdown();
                                    entry.insert(topic);
                                }
                            }
                        }
                    }
                }

                let payload = if publish.payload_size == payload.len() as u32 {
                    Payload::from_bytes(payload)
                } else {
                    let (pl, sender) =
                        Payload::from_stream(payload, self.cfg.max_payload_buffer_size);
                    self.inner.sink.payload.set(Some(sender));
                    pl
                };

                publish_fn(
                    &self.publish,
                    Publish::new(publish, payload, size),
                    packet_id.map_or(0, NonZero::get),
                    size,
                    info,
                    ctx,
                )
                .await
            }
            Decoded::PayloadChunk(buf, eof) => {
                let pl = self.inner.sink.payload.take().unwrap();
                pl.feed_data(buf);
                if eof {
                    pl.feed_eof();
                } else {
                    self.inner.sink.payload.set(Some(pl));
                }
                Ok(None)
            }
            Decoded::Packet(Packet::PublishAck(pkt), ..) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(pkt)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishReceived(pkt), _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(pkt)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishRelease(pkt), size) => {
                if self.inner.info.borrow().inflight.contains(&pkt.packet_id) {
                    self.inner.control(ProtocolMessage::pubrel(pkt, size)).await
                } else {
                    Ok(Some(Encoded::Packet(codec::Packet::PublishComplete(
                        codec::PublishAck2 {
                            packet_id: pkt.packet_id,
                            reason_code: codec::PublishAck2Reason::PacketIdNotFound,
                            properties: codec::UserProperties::default(),
                            reason_string: None,
                        },
                    ))))
                }
            }
            Decoded::Packet(Packet::PublishComplete(pkt), _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(pkt)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::SubscribeAck(packet), ..) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Subscribe(packet)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::UnsubscribeAck(packet), ..) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::Disconnect(pkt), size) => {
                if pkt.session_expiry_interval_secs.is_some() {
                    Err(SpecViolation::Disconnect_3_14_2_21.into())
                } else {
                    // dont send disconnect if we received one and close connection
                    self.inner.sink.is_disconnect_sent();
                    self.inner.sink.close(None);
                    self.inner.control(ProtocolMessage::dis(pkt, size)).await
                }
            }
            Decoded::Packet(Packet::Auth(_), ..) => Err(ProtocolError::unexpected_packet(
                packet_type::AUTH,
                "AUTH packet is not supported at this time",
            )
            .into()),
            Decoded::Packet(Packet::PingResponse, ..) => Ok(None),
            Decoded::Packet(
                pkt @ (Packet::PingRequest | Packet::Subscribe(_) | Packet::Unsubscribe(_)),
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

/// Publish service response future
async fn publish_fn<'f, T, C, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: u16,
    packet_size: u32,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<Encoded>, DispatcherError<E>>
where
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E>,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    let ack = match ctx.call(svc, pkt).await.map_err(|e| DispatcherError::Service(e.into()))? {
        Either::Right(ack) => ack,
        Either::Left(pkt) => {
            let (pkt, payload) = pkt.into_inner();
            return inner
                .control_pkt(ProtocolMessage::publish(pkt, payload, packet_size), packet_id)
                .await;
        }
    };

    if let Some(id) = NonZeroU16::new(packet_id) {
        log::trace!("Sending publish ack for {packet_id:?} id");
        inner.info.borrow_mut().inflight.remove(&id);
        let ack = codec::PublishAck {
            packet_id: id,
            reason_code: ack.reason_code,
            reason_string: ack.reason_string,
            properties: ack.properties,
        };
        Ok(Some(Encoded::Packet(Packet::PublishAck(ack))))
    } else {
        Ok(None)
    }
}

impl<C> Inner<C> {
    async fn control<E>(
        &self,
        pkt: ProtocolMessage,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        self.control_pkt(pkt, 0).await
    }

    async fn control_pkt<E>(
        &self,
        mut pkt: ProtocolMessage,
        packet_id: u16,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        let result = match self.control.call(pkt).await {
            Ok(result) => {
                if let Some(id) = NonZeroU16::new(packet_id) {
                    self.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Err(err) => {
                // do not handle nested error
                self.sink.drop_payload(&PayloadError::Service);
                self.sink.drop_sink();
                return Err(err);
            }
        };

        let response = match result.packet {
            Pkt::Packet(pkt) => Ok(Some(Encoded::Packet(pkt))),
            Pkt::Disconnect(pkt) => {
                if self.sink.is_disconnect_sent() {
                    Ok(None)
                } else {
                    Ok(Some(Encoded::Packet(codec::Packet::from(pkt))))
                }
            }
            Pkt::None => Ok(None),
        };
        if result.disconnect {
            self.sink.drop_payload(&PayloadError::Service);
            self.sink.drop_sink();
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{cfg::SharedCfg, fn_service};
    use ntex_util::future::{Ready, lazy};

    use super::super::MqttSink;
    use super::*;

    #[derive(Debug)]
    struct TestError;

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Rc::default()));
        let sink = MqttSink::new(shared.clone());

        let disp = Pipeline::new(create_dispatcher(
            shared.clone(),
            fn_service(|p: Publish| Ready::Ok::<_, TestError>(Either::Right(p.ack()))),
            fn_service(|_| {
                Ready::Ok::<_, TestError>(ControlAck { packet: Pkt::None, disconnect: false })
            }),
            16,
            16,
            Cfg::default(),
        ));

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
