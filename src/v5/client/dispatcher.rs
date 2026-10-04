use std::{cell::Cell, cell::RefCell, marker::PhantomData, num::NonZero, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_service::{Ctx, Service, cfg::Cfg};
use ntex_util::{HashMap, future::Either, future::join, hash_map};

use crate::error::{DecodeError, DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
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
        discard_payload: Cell::new(false),
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
    /// Payload chunks of an ignored PUBLISH are dropped
    discard_payload: Cell<bool>,
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
        // payload backpressure, reading pauses while the streamed payload buffer
        // is above its high watermark, the connection is closed if the payload
        // receiver is dropped before the stream ends
        if res1.is_ok()
            && res2.is_ok()
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

                // response to a re-delivered PUBLISH with a packet id in use
                let mut redelivered = None;
                {
                    let mut inner = info.info.borrow_mut();

                    if let Some(pid) = packet_id {
                        redelivered = match inner.inflight.get(&pid).copied() {
                            // a re-delivery keeps the packet id [MQTT-3.3.1-1] and is not
                            // a new message until PUBACK is sent [MQTT-4.3.2-5],
                            // the ack of the first delivery acks both
                            Some(InFlight::Publish(qos)) if publish.dup && qos == publish.qos => {
                                log::trace!("Re-delivered publish packet is ignored: {pid:?}");
                                Some(None)
                            }
                            // until PUBREL, PUBLISH with the same packet id is acked by PUBREC
                            // and is not delivered [MQTT-4.3.3-10]
                            Some(InFlight::Received)
                                if publish.dup && publish.qos == QoS::ExactlyOnce =>
                            {
                                log::trace!("Re-delivered publish packet is received: {pid:?}");
                                Some(Some(Encoded::Packet(Packet::PublishReceived(
                                    codec::PublishAck {
                                        packet_id: pid,
                                        ..Default::default()
                                    },
                                ))))
                            }
                            _ => None,
                        };

                        // check for receive maximum
                        if redelivered.is_none()
                            && self.max_receive != 0
                            && inner.inflight.len() >= self.max_receive
                        {
                            log::trace!(
                                "Receive maximum exceeded: max: {} inflight: {}",
                                self.max_receive,
                                inner.inflight.len()
                            );
                            return Err(SpecViolation::Pub_3_3_4_9.into());
                        }

                        // check for duplicated packet id
                        if redelivered.is_none() {
                            match inner.inflight.entry(pid) {
                                hash_map::Entry::Occupied(_) => {
                                    log::trace!("Duplicated packet id for publish packet: {pid:?}");
                                    // queued to keep acks in the order packets are received
                                    let ack = codec::PublishAck {
                                        packet_id: pid,
                                        reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                        ..Default::default()
                                    };
                                    redelivered = Some(Some(Encoded::Packet(
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

                    if let Some(res) = redelivered {
                        if publish.payload_size != payload.len() as u32 {
                            self.discard_payload.set(true);
                        }
                        return Ok(res);
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
                if self.discard_payload.get() {
                    if eof {
                        self.discard_payload.set(false);
                    }
                    Ok(None)
                } else if let Some(pl) = self.inner.sink.payload.take() {
                    pl.feed_data(buf);
                    if eof {
                        pl.feed_eof();
                    } else {
                        self.inner.sink.payload.set(Some(pl));
                    }
                    Ok(None)
                } else {
                    // the publish of the chunk failed, the connection is closing
                    Err(MqttProtocolError::Decode(DecodeError::UnexpectedPayload).into())
                }
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
            // CONNACK is sent once [MQTT-3.2.0-2]
            Decoded::Packet(
                pkt @ (Packet::Connect(_)
                | Packet::ConnectAck(_)
                | Packet::PingRequest
                | Packet::Subscribe(_)
                | Packet::Unsubscribe(_)),
                _,
            ) => Err(MqttProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            )
            .into()),
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
        ($pubrel:expr, $published:expr, $max_receive:expr) => {{
            let cfg: SharedCfg = SharedCfg::new("DBG").add(MqttServiceConfig::new()).into();
            let io = Io::new(IoTest::create().0, cfg.clone());
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                Rc::default(),
            ));
            let pubrel = $pubrel.clone();
            let published = $published.clone();
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
                        published.set(published.get() + 1);
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
        let (_io, shared, disp) = qos2_dispatcher!(pubrel_calls, Rc::new(Cell::new(0)), 16);

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
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, Rc::new(Cell::new(0)), 1);

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
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, Rc::new(Cell::new(0)), 16);

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

    fn redelivery(id: u16, qos: QoS, chunked: bool) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                dup: true,
                qos,
                packet_id: NonZeroU16::new(id),
                topic: ByteString::from_static("publish"),
                payload_size: if chunked { 6 } else { 3 },
                ..Default::default()
            },
            Bytes::from_static(b"abc"),
            999,
        )
    }

    /// Chunks of a failed publish are a protocol error, not a panic
    #[ntex::test]
    async fn test_unexpected_payload_chunk() {
        let unexpected = |res: Result<Option<Encoded>, DispatcherError<()>>| {
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::Decode(
                    DecodeError::UnexpectedPayload
                )))
            )
        };
        let chunk = |eof| Decoded::PayloadChunk(Bytes::from_static(b"def"), eof);
        let (_io, _, disp) = qos2_dispatcher!(Rc::new(Cell::new(0)), Rc::new(Cell::new(0)), 16);

        // the streaming publish is rejected before its payload is set up
        let Decoded::Publish(mut pkt, payload, size) = redelivery(1, QoS::AtLeastOnce, true) else {
            unreachable!()
        };
        pkt.dup = false;
        pkt.topic = ByteString::from_static("a/+");
        let res = disp.call(Decoded::Publish(pkt, payload, size)).await;
        assert!(matches!(res, Err(DispatcherError::Protocol(_))), "{res:?}");
        assert!(unexpected(disp.call(chunk(true)).await));
        assert!(unexpected(disp.call(chunk(false)).await));
    }

    #[ntex::test]
    async fn test_publish_redelivery() {
        use codec::{PublishAck2Reason as Ack2, PublishAckReason as Ack};

        let ack = |id, reason_code| {
            Some(Encoded::Packet(Packet::PublishAck(codec::PublishAck {
                packet_id: pid(id),
                reason_code,
                ..Default::default()
            })))
        };
        let pubrel_calls = Rc::new(Cell::new(0));
        let published = Rc::new(Cell::new(0));
        let (_io, shared, disp) = qos2_dispatcher!(pubrel_calls, published, 16);

        // re-delivery is ignored until PUBACK [MQTT-4.3.2-5]
        let mut f = Box::pin(disp.call(qos_publish(1, QoS::AtLeastOnce, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp.call(redelivery(1, QoS::AtLeastOnce, false)).await;
        assert_eq!(res.unwrap(), None);
        let res = disp.call(redelivery(1, QoS::ExactlyOnce, false)).await;
        assert_eq!(res.unwrap(), Some(pubrec(1, Ack::PacketIdentifierInUse)));
        let res = disp.call(qos_publish(1, QoS::AtLeastOnce, "publish")).await;
        assert_eq!(res.unwrap(), ack(1, Ack::PacketIdentifierInUse));
        assert_eq!(f.await.unwrap(), ack(1, Ack::Success));
        assert_eq!(published.get(), 1);

        // re-delivery is acked by PUBREC until PUBREL [MQTT-4.3.3-10]
        let res = disp.call(qos_publish(2, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(res.unwrap(), Some(pubrec(2, Ack::Success)));
        let res = disp.call(redelivery(2, QoS::ExactlyOnce, true)).await;
        assert_eq!(res.unwrap(), Some(pubrec(2, Ack::Success)));
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"de"), false);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"f"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        let res = disp.call(redelivery(2, QoS::AtLeastOnce, false)).await;
        assert_eq!(res.unwrap(), ack(2, Ack::PacketIdentifierInUse));
        let res = disp.call(qos_publish(2, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(res.unwrap(), Some(pubrec(2, Ack::PacketIdentifierInUse)));
        assert_eq!(published.get(), 2);

        // after PUBCOMP re-delivery is a new message [MQTT-4.3.3-12]
        let res = disp.call(pubrel(2)).await;
        assert_eq!(res.unwrap(), Some(pubcomp(2, Ack2::Success)));
        assert_eq!(pubrel_calls.get(), 1);
        let res = disp.call(redelivery(2, QoS::ExactlyOnce, true)).await;
        assert_eq!(res.unwrap(), Some(pubrec(2, Ack::Success)));
        assert_eq!(published.get(), 3);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"def"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        assert!(shared.payload.take().is_none());

        // re-delivery does not use receive maximum quota
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, published, 1);
        let res = disp.call(qos_publish(1, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(res.unwrap(), Some(pubrec(1, Ack::Success)));
        let res = disp.call(redelivery(1, QoS::ExactlyOnce, false)).await;
        assert_eq!(res.unwrap(), Some(pubrec(1, Ack::Success)));
        assert_eq!(published.get(), 4);
    }

    fn assert_unexpected<E: std::fmt::Debug>(
        res: &Result<Option<Encoded>, DispatcherError<E>>,
        expected: u8,
    ) {
        assert!(
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)))
                    if matches!(
                        err.inner,
                        crate::error::ViolationInner::UnexpectedPacket { packet_type, .. }
                            if packet_type == expected
                    )
            ),
            "{res:?}"
        );
    }

    #[ntex::test]
    async fn test_unexpected_packets() {
        let (_io, _, disp) = qos2_dispatcher!(Rc::new(Cell::new(0)), Rc::new(Cell::new(0)), 16);

        let res = disp.call(Decoded::Packet(Packet::PingResponse, 999)).await;
        assert_eq!(res.unwrap(), None);

        // packets sent by the client only and a second CONNACK [MQTT-3.2.0-2]
        // are not expected from server
        for (pkt, tp) in [
            (Packet::Connect(Box::default()), packet_type::CONNECT),
            (Packet::ConnectAck(Box::default()), packet_type::CONNACK),
            (Packet::PingRequest, packet_type::PINGREQ),
        ] {
            assert_unexpected(&disp.call(Decoded::Packet(pkt, 999)).await, tp);
        }
    }

    #[ntex::test]
    async fn test_payload_backpressure() {
        use std::{cell::RefCell, task::Poll};

        struct FailReady<E>(Rc<Cell<bool>>, fn() -> E);

        impl<St, E> Service<St, ProtocolMessage> for FailReady<E> {
            type Res = ProtocolMessageAck;
            type Error = E;

            async fn ready(&self, _: Ctx<'_, Self, St>) -> Result<(), E> {
                if self.0.get() { Err((self.1)()) } else { Ok(()) }
            }

            async fn call(
                &self,
                msg: ProtocolMessage,
                _: Ctx<'_, Self, St>,
            ) -> Result<Self::Res, E> {
                Ok(msg.ack())
            }
        }

        struct Hold<R, E>(
            Rc<RefCell<Option<Publish>>>,
            Rc<Cell<bool>>,
            fn() -> E,
            fn() -> R,
        );

        impl<St, R, E> Service<St, Publish> for Hold<R, E> {
            type Res = R;
            type Error = E;

            async fn ready(&self, _: Ctx<'_, Self, St>) -> Result<(), E> {
                if self.1.get() { Err((self.2)()) } else { Ok(()) }
            }

            async fn call(&self, pkt: Publish, _: Ctx<'_, Self, St>) -> Result<R, E> {
                *self.0.borrow_mut() = Some(pkt);
                Ok((self.3)())
            }
        }

        let fail = Rc::new(Cell::new(false));
        let pfail = Rc::new(Cell::new(false));
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(MqttServiceConfig::new().set_max_payload_buffer_size(4))
            .into();
        let io = Io::new(IoTest::create().0, cfg.clone());
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            Rc::default(),
        ));
        let held = Rc::new(RefCell::new(None));
        let h = held.clone();
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            create_dispatcher(
                shared.clone(),
                Hold(
                    h,
                    pfail.clone(),
                    || (),
                    || Either::Right(PublishAck::new(codec::PublishAckReason::Success)),
                ),
                FailReady(fail.clone(), || ()),
                16,
                16,
                cfg.get(),
            ),
        );
        let chunk = |data: &'static [u8]| {
            disp.call_nowait(Decoded::PayloadChunk(Bytes::from_static(data), false))
        };

        // streamed payload below the high watermark
        let res = disp
            .call(Decoded::Publish(
                codec::Publish {
                    topic: ByteString::from_static("t"),
                    payload_size: 16,
                    ..Default::default()
                },
                Bytes::from_static(b"ab"),
                999,
            ))
            .await;
        assert_eq!(res.unwrap(), None);
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Ok(()))
        ));

        // the payload buffer reached the high watermark, reading pauses
        assert_eq!(chunk(b"cd").await.unwrap(), None);
        assert!(lazy(|cx| disp.poll_ready(cx)).await.is_pending());

        // the payload buffer is drained to the low watermark
        let pkt = held.borrow_mut().take().unwrap();
        assert_eq!(pkt.read().await.unwrap(), Some(Bytes::from_static(b"ab")));
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Ok(()))
        ));
        assert!(io.is_active());

        // a readiness error is not delayed by the paused payload
        assert_eq!(chunk(b"efgh").await.unwrap(), None);
        fail.set(true);
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Err(_))
        ));
        fail.set(false);
        pfail.set(true);
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Err(_))
        ));
        pfail.set(false);

        // the payload receiver is dropped before the stream ends
        assert!(lazy(|cx| disp.poll_ready(cx)).await.is_pending());
        drop(pkt);
        assert!(disp.ready().await.is_ok());
        assert!(!io.is_active());
    }
}
