use std::{cell::RefCell, marker::PhantomData, num::NonZero, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_service::{Ctx, Service, cfg::Cfg};
use ntex_util::{HashMap, future::Either, future::join, hash_map};

use crate::error::{DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
use crate::payload::{Payload, PayloadStatus};
use crate::v5::codec::{Decoded, DisconnectReasonCode, Encoded, Packet};
use crate::v5::shared::{Ack, MqttShared};
use crate::v5::{Session, codec, control::Pkt, publish::Publish, publish::PublishAck};
use crate::{MqttServiceConfig, types::QoS, types::packet_type};

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
                inflight: HashMap::default(),
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
    inflight: HashMap<NonZeroU16, InFlight>,
    aliases: HashMap<NonZeroU16, ByteString>,
}

/// State of an incoming publish packet id
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InFlight {
    /// Publish is being handled
    Publish(QoS),
    /// `QoS 2` publish is handled and `PublishReceived` is sent, waiting for `PublishRelease`
    Received,
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
                        match inner.inflight.entry(pid) {
                            hash_map::Entry::Occupied(_) => {
                                // queued to keep acks in the order packets are received
                                let ack = codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                };
                                return Ok(Some(Encoded::Packet(
                                    if publish.qos == QoS::ExactlyOnce {
                                        Packet::PublishReceived(ack)
                                    } else {
                                        Packet::PublishAck(ack)
                                    },
                                )));
                            }
                            hash_map::Entry::Vacant(entry) => {
                                entry.insert(InFlight::Publish(publish.qos));
                            }
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
                let packet_id = pkt.packet_id;
                let state = self.inner.info.borrow().inflight.get(&packet_id).copied();
                match state {
                    // packet id is released after PUBCOMP [MQTT-4.3.3-12]
                    Some(InFlight::Received) => {
                        self.inner
                            .control_pkt(ProtocolMessage::pubrel(pkt, size), packet_id.get(), ctx)
                            .await
                    }
                    // PUBREL is a response to PUBREC [MQTT-4.3.3-4]
                    Some(InFlight::Publish(_)) => Err(MqttProtocolError::unexpected_packet(
                        packet_type::PUBREL,
                        "PublishRelease packet before PublishReceived",
                    )
                    .into()),
                    None => Ok(Some(Encoded::Packet(codec::Packet::PublishComplete(
                        codec::PublishAck2 {
                            packet_id,
                            reason_code: codec::PublishAck2Reason::PacketIdNotFound,
                            properties: codec::UserProperties::default(),
                            reason_string: None,
                        },
                    )))),
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
        let qos2 =
            inner.info.borrow().inflight.get(&id) == Some(&InFlight::Publish(QoS::ExactlyOnce));
        let ack = codec::PublishAck {
            packet_id: id,
            reason_code: ack.reason_code,
            reason_string: ack.reason_string,
            properties: ack.properties,
        };
        let pkt = if qos2 {
            Packet::PublishReceived(ack)
        } else {
            Packet::PublishAck(ack)
        };
        inner.update_inflight(id, &pkt);
        Ok(Some(Encoded::Packet(pkt)))
    } else {
        Ok(None)
    }
}

impl<C> Inner<C> {
    /// Update state of the packet id after the response is sent
    ///
    /// `QoS 2` publish acknowledged by successful `PublishReceived` keeps the packet id in use
    /// until `PublishRelease` [MQTT-4.3.3-10], otherwise the packet id is released
    /// [MQTT-4.3.2-5], [MQTT-4.3.3-9], [MQTT-4.3.3-12]
    fn update_inflight(&self, packet_id: NonZeroU16, pkt: &Packet) {
        let mut info = self.info.borrow_mut();
        match pkt {
            Packet::PublishReceived(ack)
                if ack.packet_id == packet_id && u8::from(ack.reason_code) < 0x80 =>
            {
                info.inflight.insert(packet_id, InFlight::Received);
            }
            _ => {
                info.inflight.remove(&packet_id);
            }
        }
    }

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
                    match result.packet {
                        Pkt::Packet(ref pkt) => self.update_inflight(id, pkt),
                        _ => {
                            self.info.borrow_mut().inflight.remove(&id);
                        }
                    }
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
    use std::{cell::Cell, future::Future, pin::Pin};

    use ntex_bytes::Bytes;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg, fn_service};
    use ntex_util::{future::lazy, time::Millis, time::sleep};

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

    /// Publish service handles topics "publish*", "publish/slow" waits 100ms,
    /// other topics are passed to the control service, topics "*/err" are rejected
    macro_rules! qos2_dispatcher {
        ($pubrel:expr, $max_receive:expr) => {{
            let cfg: SharedCfg = SharedCfg::new("DBG").add(MqttServiceConfig::new()).into();
            let io = Io::new(IoTest::create().0, cfg.clone());
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                Rc::default(),
            ));
            let pubrel = $pubrel.clone();
            let reason = |topic: &str| {
                if topic.ends_with("/err") {
                    codec::PublishAckReason::UnspecifiedError
                } else {
                    codec::PublishAckReason::Success
                }
            };
            let disp = Pipeline::new(
                Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
                create_dispatcher(
                    shared.clone(),
                    fn_service(async move |p: Publish| {
                        if p.publish_topic() == "publish/slow" {
                            sleep(Millis(100)).await;
                        }
                        if p.publish_topic().starts_with("publish") {
                            Ok::<_, ()>(Either::Right(PublishAck::new(reason(p.publish_topic()))))
                        } else {
                            Ok(Either::Left(p))
                        }
                    }),
                    fn_service(move |msg: ProtocolMessage| {
                        let res = match msg {
                            ProtocolMessage::Publish(p) => {
                                let code = reason(&p.packet().topic);
                                p.ack(code)
                            }
                            ProtocolMessage::PublishRelease(msg) => {
                                pubrel.set(pubrel.get() + 1);
                                msg.ack()
                            }
                            msg => msg.ack(),
                        };
                        async move { Ok::<_, ()>(res) }
                    }),
                    $max_receive,
                    16,
                    cfg.get(),
                ),
            );
            (io, shared, disp)
        }};
    }

    fn pid(id: u16) -> NonZeroU16 {
        NonZeroU16::new(id).unwrap()
    }

    fn qos_publish(id: u16, qos: QoS, topic: &'static str) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                qos,
                topic: ByteString::from_static(topic),
                packet_id: NonZeroU16::new(id),
                ..Default::default()
            },
            Bytes::new(),
            999,
        )
    }

    fn pubrel(id: u16) -> Decoded {
        Decoded::Packet(
            Packet::PublishRelease(codec::PublishAck2 {
                packet_id: pid(id),
                ..Default::default()
            }),
            999,
        )
    }

    fn pubrec(id: u16, reason_code: codec::PublishAckReason) -> Encoded {
        Encoded::Packet(Packet::PublishReceived(codec::PublishAck {
            packet_id: pid(id),
            reason_code,
            ..Default::default()
        }))
    }

    fn puback(id: u16) -> Encoded {
        Encoded::Packet(Packet::PublishAck(codec::PublishAck {
            packet_id: pid(id),
            ..Default::default()
        }))
    }

    fn pubcomp(id: u16, reason_code: codec::PublishAck2Reason) -> Encoded {
        Encoded::Packet(Packet::PublishComplete(codec::PublishAck2 {
            packet_id: pid(id),
            reason_code,
            ..Default::default()
        }))
    }

    #[ntex::test]
    async fn test_publish_qos2() {
        use codec::{PublishAck2Reason as Ack2, PublishAckReason as Ack};

        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, shared, disp) = qos2_dispatcher!(pubrel_calls, 16);

        for (topic, err_topic) in [("publish", "publish/err"), ("control", "control/err")] {
            pubrel_calls.set(0);

            // QoS 2 publish is acknowledged with PUBREC [MQTT-4.3.3-8]
            let res = disp.call(qos_publish(1, QoS::ExactlyOnce, topic)).await;
            assert_eq!(res.unwrap(), Some(pubrec(1, Ack::Success)));

            // packet id stays in use until PUBREL
            let res = disp.call(qos_publish(1, QoS::ExactlyOnce, topic)).await;
            assert_eq!(res.unwrap(), Some(pubrec(1, Ack::PacketIdentifierInUse)));

            // PUBREL is passed to the control service, PUBCOMP is sent [MQTT-4.3.3-11]
            let res = disp.call(pubrel(1)).await;
            assert_eq!(res.unwrap(), Some(pubcomp(1, Ack2::Success)));
            assert_eq!(pubrel_calls.get(), 1);

            // packet id is released after PUBCOMP [MQTT-4.3.3-12]
            let res = disp.call(pubrel(1)).await;
            assert_eq!(res.unwrap(), Some(pubcomp(1, Ack2::PacketIdNotFound)));
            let res = disp.call(qos_publish(1, QoS::ExactlyOnce, topic)).await;
            assert_eq!(res.unwrap(), Some(pubrec(1, Ack::Success)));
            let res = disp.call(pubrel(1)).await;
            assert_eq!(res.unwrap(), Some(pubcomp(1, Ack2::Success)));
            assert_eq!(pubrel_calls.get(), 2);

            // packet id is released after PUBREC with an error [MQTT-4.3.3-9]
            let res = disp.call(qos_publish(2, QoS::ExactlyOnce, err_topic)).await;
            assert_eq!(res.unwrap(), Some(pubrec(2, Ack::UnspecifiedError)));
            let res = disp.call(pubrel(2)).await;
            assert_eq!(res.unwrap(), Some(pubcomp(2, Ack2::PacketIdNotFound)));
            assert_eq!(pubrel_calls.get(), 2);

            // QoS 1 publish is acknowledged with PUBACK, packet id is released
            let res = disp.call(qos_publish(3, QoS::AtLeastOnce, topic)).await;
            assert_eq!(res.unwrap(), Some(puback(3)));
            let res = disp.call(pubrel(3)).await;
            assert_eq!(res.unwrap(), Some(pubcomp(3, Ack2::PacketIdNotFound)));
            assert_eq!(pubrel_calls.get(), 2);
        }
        assert!(shared.is_active());
    }

    #[ntex::test]
    async fn test_publish_qos2_receive_max() {
        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, 1);

        let res = disp.call(qos_publish(1, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(
            res.unwrap(),
            Some(pubrec(1, codec::PublishAckReason::Success))
        );

        // QoS 2 publish counts against Receive Maximum until PUBCOMP [MQTT-3.3.4-9]
        let res = disp.call(qos_publish(2, QoS::AtLeastOnce, "publish")).await;
        assert!(matches!(
            res,
            Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                if err.inner == ViolationInner::Spec(SpecViolation::Pub_3_3_4_9)
        ));

        let res = disp.call(pubrel(1)).await;
        assert_eq!(
            res.unwrap(),
            Some(pubcomp(1, codec::PublishAck2Reason::Success))
        );
        let res = disp.call(qos_publish(2, QoS::AtLeastOnce, "publish")).await;
        assert_eq!(res.unwrap(), Some(puback(2)));
    }

    #[ntex::test]
    async fn test_pubrel_before_pubrec() {
        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, 16);

        let mut f = Box::pin(disp.call(qos_publish(1, QoS::ExactlyOnce, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        // PUBREL is a response to PUBREC [MQTT-4.3.3-4]
        let res = disp.call(pubrel(1)).await;
        assert!(matches!(
            res,
            Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                if matches!(err.inner, ViolationInner::UnexpectedPacket { .. })
        ));
        assert_eq!(pubrel_calls.get(), 0);
    }
}
