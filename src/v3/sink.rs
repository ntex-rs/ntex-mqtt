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

    #[inline]
    /// Check if io stream is open
    pub fn is_open(&self) -> bool {
        !self.0.io.is_closed()
    }

    #[inline]
    /// Check if sink is ready
    pub fn is_ready(&self) -> bool {
        if self.0.io.is_closed() {
            false
        } else {
            self.0.has_credit()
        }
    }

    #[inline]
    /// Get client receive credit
    pub fn credit(&self) -> usize {
        self.0.credit()
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive
    pub fn ready(&self) -> impl Future<Output = bool> {
        if !self.0.io.is_closed() {
            self.0
                .with_queues(|q| {
                    if q.inflight.len() >= self.0.cap() {
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

    #[inline]
    /// Close mqtt connection
    pub fn close(&self) {
        if self.0.client {
            let _ = self.0.io.encode(codec::Packet::Disconnect, &self.0.codec);
        }
        self.0.io.close();
        self.0.with_queues(|q| {
            q.inflight.clear();
            q.waiters.clear();
        });
    }

    #[inline]
    /// Force close mqtt connection. mqtt dispatcher does not wait for uncompleted
    /// responses, but it flushes buffers.
    pub fn force_close(&self) {
        self.0.io.force_close();
        self.0.with_queues(|q| {
            q.inflight.clear();
            q.waiters.clear();
        });
    }

    #[inline]
    /// Send ping
    pub(super) fn ping(&self) -> bool {
        self.0.io.encode(codec::Packet::PingRequest, &self.0.codec).is_ok()
    }

    #[inline]
    /// Create publish message builder
    pub fn publish<U>(&self, topic: U, payload: Bytes) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        self.publish_pkt(codec::Publish {
            payload,
            dup: false,
            retain: false,
            topic: topic.into(),
            qos: codec::QoS::AtMostOnce,
            packet_id: None,
        })
    }

    #[inline]
    /// Create publish builder with publish packet
    pub fn publish_pkt(&self, packet: codec::Publish) -> PublishBuilder {
        PublishBuilder { packet, shared: self.0.clone() }
    }

    #[inline]
    /// Create subscribe packet builder
    ///
    /// panics if id is 0
    pub fn subscribe(&self) -> SubscribeBuilder {
        SubscribeBuilder { id: None, topic_filters: Vec::new(), shared: self.0.clone() }
    }

    #[inline]
    /// Create unsubscribe packet builder
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder { id: None, topic_filters: Vec::new(), shared: self.0.clone() }
    }

    pub(super) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
        let result = self.0.with_queues(|queues| {
            // check ack order
            if let Some((idx, tx, tp)) = queues.inflight.pop_front() {
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
                    queues.inflight_ids.remove(&pkt.packet_id());

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
                        Err(ProtocolError::Unexpected(pkt.packet_type(), pkt.name()))
                    }
                }
            } else {
                log::trace!("Unexpected PublishAck packet: {:?}", pkt.packet_id());
                Err(ProtocolError::Unexpected(pkt.packet_type(), pkt.name()))
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
    #[inline]
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

    #[inline]
    /// This might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    #[inline]
    /// Set retain flag
    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
    }

    #[inline]
    /// Send publish packet with QoS 0
    pub fn send_at_most_once(mut self) -> Result<(), SendPacketError> {
        if !self.shared.io.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.packet.qos = codec::QoS::AtMostOnce;
            self.shared
                .io
                .encode(codec::Packet::Publish(self.packet), &self.shared.codec)
                .map_err(SendPacketError::Encode)
                .map(|_| ())
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 1
    pub fn send_at_least_once(self) -> impl Future<Output = Result<(), SendPacketError>> {
        if !self.shared.io.is_closed() {
            let shared = self.shared;
            let mut packet = self.packet;
            packet.qos = codec::QoS::AtLeastOnce;

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
        // packet id
        let idx = if let Some(idx) = packet.packet_id {
            idx
        } else {
            let idx = shared.next_id();
            packet.packet_id = Some(idx);
            idx
        };

        let in_use = shared.with_queues(|queues| queues.inflight_ids.contains(&idx));
        if in_use {
            return Either::Left(Ready::Err(SendPacketError::PacketIdInUse(idx)));
        }

        log::trace!("Publish (QoS1) to {:#?}", packet);

        match shared.io.encode(codec::Packet::Publish(packet), &shared.codec) {
            Ok(_) => {
                let rx = shared.with_queues(|queues| {
                    // publish ack channel
                    let (tx, rx) = shared.pool.queue.channel();
                    queues.inflight.push_back((idx, tx, AckType::Publish));
                    queues.inflight_ids.insert(idx);
                    rx
                });
                Either::Right(async move {
                    rx.await.map(|_| ()).map_err(|_| SendPacketError::Disconnected)
                })
            }
            Err(err) => Either::Left(Ready::Err(SendPacketError::Encode(err))),
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: Option<NonZeroU16>,
    shared: Rc<MqttShared>,
    topic_filters: Vec<(ByteString, codec::QoS)>,
}

impl SubscribeBuilder {
    #[inline]
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString, qos: codec::QoS) -> Self {
        self.topic_filters.push((filter, qos));
        self
    }

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
            let idx = self.id.unwrap_or_else(|| shared.next_id());
            let rx = shared.with_queues(|queues| {
                // ack channel
                let (tx, rx) = shared.clone().pool.queue.channel();

                // allocate packet id
                if queues.inflight_ids.contains(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.push_back((idx, tx, AckType::Subscribe));
                queues.inflight_ids.insert(idx);
                Ok(rx)
            })?;

            // send subscribe to client
            log::trace!("Sending subscribe packet id: {} filters:{:?}", idx, filters);

            match shared.io.encode(
                codec::Packet::Subscribe { packet_id: idx, topic_filters: filters },
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
    id: Option<NonZeroU16>,
    shared: Rc<MqttShared>,
    topic_filters: Vec<ByteString>,
}

impl UnsubscribeBuilder {
    #[inline]
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString) -> Self {
        self.topic_filters.push(filter);
        self
    }

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
            // allocate packet id
            let idx = self.id.unwrap_or_else(|| shared.next_id());
            let rx = shared.with_queues(|queues| {
                // ack channel
                let (tx, rx) = shared.pool.queue.channel();

                // allocate packet id
                if queues.inflight_ids.contains(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.push_back((idx, tx, AckType::Unsubscribe));
                queues.inflight_ids.insert(idx);
                Ok(rx)
            })?;

            // send subscribe to client
            log::trace!("Sending unsubscribe packet id: {} filters:{:?}", idx, filters);

            match shared.io.encode(
                codec::Packet::Unsubscribe { packet_id: idx, topic_filters: filters },
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
