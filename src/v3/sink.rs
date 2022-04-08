use std::future::{ready, Future};
use std::{fmt, num::NonZeroU16, rc::Rc};

use ntex::util::{ByteString, Bytes, Either, Ready};

use super::shared::{Ack, AckType, MqttShared};
use super::{codec, error::ProtocolError, error::SendPacketError};

pub struct MqttSink(Rc<MqttShared>);

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone())
    }
}

impl MqttSink {
    pub(crate) fn new(state: Rc<MqttShared>) -> Self {
        MqttSink(state)
    }

    /// Get client receive credit
    pub fn credit(&self) -> usize {
        self.0.cap.get() - self.0.with_queues(|q| q.inflight.len())
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive
    pub fn ready(&self) -> impl Future<Output = bool> {
        if !self.0.io.is_closed() {
            self.0
                .with_queues(|q| {
                    if q.inflight.len() >= self.0.cap.get() {
                        let (tx, rx) = self.0.pool.waiters.channel();
                        q.waiters.push_back(tx);
                        return Some(rx);
                    }
                    None
                })
                .map(|rx| Either::Right(async move { rx.await.is_ok() }))
                .unwrap_or_else(|| Either::Left(ready(true)))
        } else {
            Either::Left(ready(false))
        }
    }

    /// Close mqtt connection
    pub fn close(&self) {
        self.0.io.close();
        self.0.with_queues(|q| {
            q.inflight.clear();
            q.waiters.clear();
        });
    }

    /// Force close mqtt connection. mqtt dispatcher does not wait for uncompleted
    /// responses, but it flushes buffers.
    pub fn force_close(&self) {
        self.0.io.force_close();
        self.0.with_queues(|q| {
            q.inflight.clear();
            q.waiters.clear();
        });
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        self.0.io.encode(codec::Packet::PingRequest, &self.0.codec).is_ok()
    }

    /// Create publish message builder
    pub fn publish<U>(&self, topic: U, payload: Bytes) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        PublishBuilder {
            packet: codec::Publish {
                payload,
                dup: false,
                retain: false,
                topic: topic.into(),
                qos: codec::QoS::AtMostOnce,
                packet_id: None,
            },
            shared: self.0.clone(),
        }
    }

    /// Create subscribe packet builder
    ///
    /// panics if id is 0
    pub fn subscribe(&self) -> SubscribeBuilder {
        SubscribeBuilder { id: 0, topic_filters: Vec::new(), shared: self.0.clone() }
    }

    /// Create unsubscribe packet builder
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder { id: 0, topic_filters: Vec::new(), shared: self.0.clone() }
    }

    pub(super) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
        let result = self.0.with_queues(|queues| {
            // check ack order
            if let Some(idx) = queues.inflight_order.pop_front() {
                if idx != pkt.packet_id() {
                    log::trace!(
                    "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                    idx,
                    pkt.packet_id()
                );
                    Err(ProtocolError::PacketIdMismatch)
                } else {
                    // get publish ack channel
                    log::trace!("Ack packet with id: {}", pkt.packet_id());
                    let idx = pkt.packet_id();
                    if let Some((tx, tp)) = queues.inflight.remove(&idx) {
                        if pkt.is_match(tp) {
                            let _ = tx.send(pkt);

                            // wake up queued request (receive max limit)
                            while let Some(tx) = queues.waiters.pop_front() {
                                if tx.send(()).is_ok() {
                                    break;
                                }
                            }
                            Ok(())
                        } else {
                            log::trace!("MQTT protocol error, unexpected packet");
                            Err(ProtocolError::Unexpected(pkt.packet_type(), tp.name()))
                        }
                    } else {
                        log::error!("In-flight state inconsistency");
                        Err(ProtocolError::PacketIdMismatch)
                    }
                }
            } else {
                log::trace!("Unexpected PublishAck packet: {:?}", pkt.packet_id());
                Err(ProtocolError::PacketIdMismatch)
            }
        });
        result.map_err(|e| {
            self.close();
            e
        })
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

pub struct PublishBuilder {
    packet: codec::Publish,
    shared: Rc<MqttShared>,
}

impl PublishBuilder {
    /// Set packet id.
    ///
    /// Note: if packet id is not set, it gets generated automatically.
    /// Packet id management should not be mixed, it should be auto-generated
    /// or set by user. Otherwise collisions could occure.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        let id = NonZeroU16::new(id).expect("id 0 is not allowed");
        self.packet.packet_id = Some(id);
        self
    }

    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
    }

    /// Send publish packet with QoS 0
    pub fn send_at_most_once(self) -> Result<(), SendPacketError> {
        let packet = self.packet;

        if !self.shared.io.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", packet.topic);
            self.shared
                .io
                .encode(codec::Packet::Publish(packet), &self.shared.codec)
                .map_err(SendPacketError::Encode)
                .map(|_| ())
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    #[allow(clippy::await_holding_refcell_ref)]
    /// Send publish packet with QoS 1
    pub fn send_at_least_once(self) -> impl Future<Output = Result<(), SendPacketError>> {
        let shared = self.shared;
        let mut packet = self.packet;
        packet.qos = codec::QoS::AtLeastOnce;

        if !shared.io.is_closed() {
            // handle client receive maximum
            if !shared.has_credit() {
                let (tx, rx) = shared.pool.waiters.channel();
                shared.with_queues(|q| q.waiters.push_back(tx));

                return Either::Left(Either::Right(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    Self::send_at_least_once_inner(packet, shared).await
                }));
            }
            Either::Right(Self::send_at_least_once_inner(packet, shared))
        } else {
            Either::Left(Either::Left(Ready::Err(SendPacketError::Disconnected)))
        }
    }

    fn send_at_least_once_inner(
        mut packet: codec::Publish,
        shared: Rc<MqttShared>,
    ) -> impl Future<Output = Result<(), SendPacketError>> {
        let rx = shared.with_queues(|queues| {
            // publish ack channel
            let (tx, rx) = shared.pool.queue.channel();

            // packet id
            let mut idx = packet.packet_id.map(|i| i.get()).unwrap_or(0);
            if idx == 0 {
                idx = shared.next_id();
                packet.packet_id = NonZeroU16::new(idx);
            }
            if queues.inflight.contains_key(&idx) {
                return Err(SendPacketError::PacketIdInUse(idx));
            }
            queues.inflight.insert(idx, (tx, AckType::Publish));
            queues.inflight_order.push_back(idx);
            Ok(rx)
        });

        let rx = match rx {
            Ok(rx) => rx,
            Err(e) => return Either::Left(Ready::Err(e)),
        };

        log::trace!("Publish (QoS1) to {:#?}", packet);

        match shared.io.encode(codec::Packet::Publish(packet), &shared.codec) {
            Ok(_) => Either::Right(async move {
                rx.await.map(|_| ()).map_err(|_| SendPacketError::Disconnected)
            }),
            Err(err) => Either::Left(Ready::Err(SendPacketError::Encode(err))),
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: u16,
    shared: Rc<MqttShared>,
    topic_filters: Vec<(ByteString, codec::QoS)>,
}

impl SubscribeBuilder {
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if id == 0 {
            panic!("id 0 is not allowed");
        }
        self.id = id;
        self
    }

    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString, qos: codec::QoS) -> Self {
        self.topic_filters.push((filter, qos));
        self
    }

    #[allow(clippy::await_holding_refcell_ref)]
    /// Send subscribe packet
    pub async fn send(self) -> Result<Vec<codec::SubscribeReturnCode>, SendPacketError> {
        let shared = self.shared;
        let filters = self.topic_filters;

        if !shared.io.is_closed() {
            // handle client receive maximum
            if !shared.has_credit() {
                let (tx, rx) = shared.pool.waiters.channel();
                shared.with_queues(|q| q.waiters.push_back(tx));

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            let idx = if self.id == 0 { shared.next_id() } else { self.id };
            let rx = shared.with_queues(|queues| {
                // ack channel
                let (tx, rx) = shared.clone().pool.queue.channel();

                // allocate packet id
                if queues.inflight.contains_key(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.insert(idx, (tx, AckType::Subscribe));
                queues.inflight_order.push_back(idx);
                Ok(rx)
            })?;

            // send subscribe to client
            log::trace!("Sending subscribe packet id: {} filters:{:?}", idx, filters);

            match shared.io.encode(
                codec::Packet::Subscribe {
                    packet_id: NonZeroU16::new(idx).unwrap(),
                    topic_filters: filters,
                },
                &shared.codec,
            ) {
                Ok(_) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(|pkt| pkt.subscribe())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

/// Unsubscribe packet builder
pub struct UnsubscribeBuilder {
    id: u16,
    shared: Rc<MqttShared>,
    topic_filters: Vec<ByteString>,
}

impl UnsubscribeBuilder {
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if id == 0 {
            panic!("id 0 is not allowed");
        }
        self.id = id;
        self
    }

    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString) -> Self {
        self.topic_filters.push(filter);
        self
    }

    #[allow(clippy::await_holding_refcell_ref)]
    /// Send unsubscribe packet
    pub async fn send(self) -> Result<(), SendPacketError> {
        let shared = self.shared;
        let filters = self.topic_filters;

        if !shared.io.is_closed() {
            // handle client receive maximum
            if !shared.has_credit() {
                let (tx, rx) = shared.pool.waiters.channel();
                shared.with_queues(|q| q.waiters.push_back(tx));

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            let idx = if self.id == 0 { shared.next_id() } else { self.id };
            let rx = shared.with_queues(|queues| {
                // ack channel
                let (tx, rx) = shared.pool.queue.channel();

                // allocate packet id
                if queues.inflight.contains_key(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.insert(idx, (tx, AckType::Unsubscribe));
                queues.inflight_order.push_back(idx);
                Ok(rx)
            })?;

            // send subscribe to client
            log::trace!("Sending unsubscribe packet id: {} filters:{:?}", idx, filters);

            match shared.io.encode(
                codec::Packet::Unsubscribe {
                    packet_id: NonZeroU16::new(idx).unwrap(),
                    topic_filters: filters,
                },
                &shared.codec,
            ) {
                Ok(_) => {
                    // wait ack from peer
                    rx.await.map_err(|_| SendPacketError::Disconnected).map(|_| ())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}
