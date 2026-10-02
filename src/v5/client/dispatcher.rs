use std::{cell::RefCell, marker::PhantomData, num::NonZero, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_service::{Ctx, Service, cfg::Cfg};
use ntex_util::{HashMap, HashSet, future::Either, future::join, hash_map};

use crate::error::{DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
use crate::payload::{Payload, PayloadStatus};
use crate::v5::codec::{Decoded, DisconnectReasonCode, Encoded, Packet};
use crate::v5::shared::{Ack, MqttShared};
use crate::v5::{Session, codec, control::Pkt, publish::Publish, publish::PublishAck};
use crate::{MqttServiceConfig, types::packet_type};

use super::control::{ProtocolMessage, ProtocolMessageAck};

/// mqtt5 protocol dispatcher
pub(super) fn create_dispatcher<St, T, C, E>(
    sink: Rc<MqttShared>,
    publish: T,
    control: C,
    max_receive: usize,
    max_topic_alias: u16,
    cfg: Cfg<MqttServiceConfig>,
) -> impl Service<Session<St>, Decoded, Res = Option<Encoded>, Error = DispatcherError<E>>
where
    St: 'static,
    E: From<T::Error> + 'static,
    T: Service<Session<St>, Publish, Res = Either<Publish, PublishAck>, Error = E> + 'static,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = E> + 'static,
{
    Dispatcher {
        cfg,
        publish,
        max_receive,
        max_topic_alias,
        inner: Inner {
            sink,
            control: control.map_err(DispatcherError::Service),
            info: RefCell::new(PublishInfo {
                aliases: HashMap::default(),
                inflight: HashSet::default(),
            }),
        },
        t: PhantomData,
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T, C, E> {
    publish: T,
    inner: Inner<C>,
    max_receive: usize,
    max_topic_alias: u16,
    cfg: Cfg<MqttServiceConfig>,
    t: PhantomData<(St, E)>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashSet<NonZeroU16>,
    aliases: HashMap<NonZeroU16, ByteString>,
}

impl<St, T, C, E> Service<St, Decoded> for Dispatcher<St, T, C, E>
where
    E: 'static,
    T: Service<St, Publish, Res = Either<Publish, PublishAck>, Error = E> + 'static,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>> + 'static,
{
    type Res = Option<Encoded>;
    type Error = DispatcherError<E>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), ctx.ready(&self.inner.control)).await;
        if (res1.is_err() || res2.is_err())
            && let Some(pl) = self.inner.sink.payload.take()
        {
            self.inner.sink.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }

        res1.map_err(DispatcherError::Service)?;
        res2?;
        Ok(())
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.inner.sink.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.drop_sink(true);

        ctx.shutdown(&self.publish).await;
        ctx.shutdown(&self.inner.control).await;
    }

    #[allow(clippy::too_many_lines, clippy::await_holding_refcell_ref)]
    async fn call(&self, req: Decoded, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        log::trace!("Dispatch packet: {req:#?}");

        match req {
            Decoded::Publish(mut publish, payload, size) => {
                // the Topic Name and the Response Topic must not contain wildcards,
                // [MQTT-3.3.2-2], [MQTT-3.3.2-14] (MQTT 5.0, 3.3.2.1, 3.3.2.3.5)
                if publish.topic.contains(['#', '+']) {
                    return Err(SpecViolation::Pub_3_3_2_2.into());
                }
                if publish
                    .properties
                    .response_topic
                    .as_ref()
                    .is_some_and(|t| t.contains(['#', '+']))
                {
                    return Err(SpecViolation::Pub_3_3_2_14.into());
                }

                let info = &self.inner;
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
                            // queued to keep acks in the order packets are received
                            return Ok(Some(Encoded::Packet(Packet::PublishAck(
                                codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                },
                            ))));
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        if publish.topic.is_empty() {
                            // lookup topic by provided alias
                            if let Some(aliased_topic) = inner.aliases.get(&alias) {
                                publish.topic = aliased_topic.clone();
                            } else {
                                return Err(MqttProtocolError::violation(
                                    DisconnectReasonCode::TopicAliasInvalid,
                                    "Unknown topic alias",
                                )
                                .into());
                            }
                        } else {
                            // record new alias
                            match inner.aliases.entry(alias) {
                                hash_map::Entry::Occupied(mut entry) => {
                                    if entry.get().as_str() != publish.topic.as_str() {
                                        let mut topic = publish.topic.clone();
                                        topic.trimdown();
                                        entry.insert(topic);
                                    }
                                }
                                hash_map::Entry::Vacant(entry) => {
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
                    self.inner
                        .control(ProtocolMessage::pubrel(pkt, size), ctx)
                        .await
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
                    self.inner
                        .control(ProtocolMessage::dis(pkt, size), ctx)
                        .await
                }
            }
            Decoded::Packet(Packet::Auth(_), ..) => Err(MqttProtocolError::unexpected_packet(
                packet_type::AUTH,
                "AUTH packet is not supported at this time",
            )
            .into()),
            Decoded::Packet(Packet::PingResponse, ..) => Ok(None),
            Decoded::Packet(
                pkt @ (Packet::PingRequest | Packet::Subscribe(_) | Packet::Unsubscribe(_)),
                _,
            ) => Err(MqttProtocolError::unexpected_packet(
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
async fn publish_fn<'f, St, T, C, E: 'static>(
    svc: &'f T,
    pkt: Publish,
    packet_id: u16,
    packet_size: u32,
    inner: &'f Inner<C>,
    ctx: Ctx<'f, Dispatcher<St, T, C, E>, St>,
) -> Result<Option<Encoded>, DispatcherError<E>>
where
    T: Service<St, Publish, Res = Either<Publish, PublishAck>, Error = E>,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>> + 'static,
{
    let ack = match ctx.call(svc, pkt).await.map_err(DispatcherError::Service)? {
        Either::Right(ack) => ack,
        Either::Left(pkt) => {
            let (pkt, payload) = pkt.into_inner();
            return inner
                .control_pkt(
                    ProtocolMessage::publish(pkt, payload, packet_size),
                    packet_id,
                    ctx,
                )
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
    async fn control<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, St>,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        self.control_pkt(pkt, 0, ctx).await
    }

    async fn control_pkt<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        packet_id: u16,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, St>,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        let result = match ctx.call(&self.control, pkt).await {
            Ok(result) => {
                if let Some(id) = NonZeroU16::new(packet_id) {
                    self.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Err(err) => {
                // do not handle nested error
                self.sink.drop_payload(&PayloadError::Service);
                self.sink.drop_sink(false);
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
            self.sink.drop_sink(true);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use ntex_bytes::Bytes;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg, fn_service};

    use super::*;
    use crate::{error::ViolationInner, v5::MqttSink};

    #[ntex::test]
    async fn test_publish_topic_wildcards() {
        let cfg: SharedCfg = SharedCfg::new("DBG").add(MqttServiceConfig::new()).into();
        let io = Io::new(IoTest::create().0, cfg.clone());
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            Rc::default(),
        ));
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            create_dispatcher(
                shared.clone(),
                fn_service(async |p: Publish| Ok::<_, ()>(Either::Right(p.ack()))),
                fn_service(async |msg: ProtocolMessage| Ok::<_, ()>(msg.ack())),
                16,
                16,
                cfg.get(),
            ),
        );
        let publish = |topic: &'static str, response_topic: Option<&'static str>| {
            let mut pkt = codec::Publish {
                topic: ByteString::from_static(topic),
                ..Default::default()
            };
            pkt.properties.response_topic = response_topic.map(ByteString::from_static);
            Decoded::Publish(pkt, Bytes::new(), 999)
        };
        let violation = |res: Result<Option<Encoded>, DispatcherError<()>>| {
            let Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err))) = res
            else {
                panic!("expected protocol violation")
            };
            assert_eq!(err.reason(), DisconnectReasonCode::ProtocolError);
            let ViolationInner::Spec(err) = err.inner else {
                panic!()
            };
            err
        };

        // [MQTT-3.3.2-2] the Topic Name must not contain wildcards
        for topic in ["a/+", "a/#", "+", "a+b"] {
            let err = violation(disp.call(publish(topic, None)).await);
            assert_eq!(err, SpecViolation::Pub_3_3_2_2);
        }
        // [MQTT-3.3.2-14] the Response Topic must not contain wildcards
        for topic in ["resp/+", "resp/#"] {
            let err = violation(disp.call(publish("a", Some(topic))).await);
            assert_eq!(err, SpecViolation::Pub_3_3_2_14);
        }
        assert!(disp.call(publish("a/b", Some("resp/a"))).await.is_ok());
    }
}
