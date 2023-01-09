use std::future::{ready, Future};
use std::{fmt, num::NonZeroU16, num::NonZeroU32, rc::Rc};

use ntex::util::{ByteString, Bytes, Either};

use super::codec;
use super::error::{ProtocolError, SendPacketError};
use super::shared::{Ack, AckType, MqttShared};
use crate::types::QoS;

pub struct MqttSink(Rc<MqttShared>);

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone())
    }
}

impl MqttSink {
    pub(super) fn new(state: Rc<MqttShared>) -> Self {
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
    /// Get client's receive credit
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
    /// Force close MQTT connection. Dispatcher does not wait for uncompleted
    /// responses (ending them with error), but it flushes buffers.
    pub fn force_close(&self) {
        if self.is_open() { // todo: mg: review: do we need to check if it's open?
            self.0.io.force_close();
        }
        self.clear_state();
    }

    #[inline]
    /// Close mqtt connection with default Disconnect message
    pub fn close(&self) {
        if self.is_open() {
            let _ = self
                .0
                .io
                .encode(codec::Packet::Disconnect(codec::Disconnect::default()), &self.0.codec);
            self.0.io.close();
        }
        self.clear_state();
    }

    #[inline]
    /// Close mqtt connection
    pub fn close_with_reason(&self, pkt: codec::Disconnect) {
        if self.is_open() {
            let _ = self.0.io.encode(codec::Packet::Disconnect(pkt), &self.0.codec);
            self.0.io.close();
        }
        self.clear_state();
    }

    fn clear_state(&self) {
        self.0.with_queues(|q| {
            q.waiters.clear();
            q.inflight.clear();
        });
    }

    pub(super) fn send(&self, pkt: codec::Packet) {
        let _ = self.0.io.encode(pkt, &self.0.codec);
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        self.0.io.encode(codec::Packet::PingRequest, &self.0.codec).is_ok()
    }

    /// Close mqtt connection, dont send disconnect message
    pub(super) fn drop_sink(&self) {
        self.clear_state();
        self.0.io.close();
    }

    pub(super) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
        self.0.with_queues(|queues| {
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

                    // cleanup ack queue
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
                        log::trace!("MQTT protocol error, unexpeted packet");
                        Err(ProtocolError::Unexpected(
                            pkt.packet_type(),
                            pkt.name(),
                        ))
                    }
                }
            } else {
                log::trace!("Unexpected PublishAck packet");
                Err(ProtocolError::Unexpected(
                    pkt.packet_type(),
                    pkt.name(),
                ))
            }
        })
    }

    #[inline]
    /// Create publish packet builder
    pub fn publish<U>(&self, topic: U, payload: Bytes) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        self.publish_pkt(codec::Publish {
            payload,
            dup: false,
            retain: false,
            topic: topic.into(),
            qos: QoS::AtMostOnce,
            packet_id: None,
            properties: codec::PublishProperties::default(),
        })
    }

    #[inline]
    /// Create publish builder with publish packet
    pub fn publish_pkt(&self, packet: codec::Publish) -> PublishBuilder {
        PublishBuilder { packet, shared: self.0.clone() }
    }

    #[inline]
    /// Create subscribe packet builder
    pub fn subscribe(&self, id: Option<NonZeroU32>) -> SubscribeBuilder {
        SubscribeBuilder {
            id: None,
            packet: codec::Subscribe {
                id,
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Vec::new(),
                topic_filters: Vec::new(),
            },
            shared: self.0.clone(),
        }
    }

    #[inline]
    /// Create unsubscribe packet builder
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder {
            id: None,
            packet: codec::Unsubscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Vec::new(),
                topic_filters: Vec::new(),
            },
            shared: self.0.clone(),
        }
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

pub struct PublishBuilder {
    shared: Rc<MqttShared>,
    packet: codec::Publish,
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
    /// Set publish packet properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
        self
    }

    #[inline]
    /// Set publish packet properties
    pub fn set_properties<F>(&mut self, f: F)
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
    }

    #[inline]
    /// Send publish packet with QoS 0
    pub fn send_at_most_once(mut self) -> Result<(), SendPacketError> {
        if !self.shared.io.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.packet.qos = QoS::AtMostOnce;
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
    pub async fn send_at_least_once(self) -> Result<codec::PublishAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;
        packet.qos = QoS::AtLeastOnce;

        if !shared.io.is_closed() {
            // handle client receive maximum
            if !shared.has_credit() {
                let (tx, rx) = shared.pool.waiters.channel();
                shared.with_queues(|q| q.waiters.push_back(tx));

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            Self::send_at_least_once_inner(packet, shared).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    async fn send_at_least_once_inner(
        mut packet: codec::Publish,
        shared: Rc<MqttShared>,
    ) -> Result<codec::PublishAck, SendPacketError> {
        // packet id
        let idx = if let Some(idx) = packet.packet_id {
            idx
        } else {
            let idx = shared.next_id();
            packet.packet_id = Some(idx);
            idx
        };

        let pkt_in_use = shared.with_queues(|queues| queues.inflight_ids.contains(&idx));
        if pkt_in_use {
            return Err(SendPacketError::PacketIdInUse(idx));
        }

        // send publish to client
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

                // wait ack from peer
                rx.await.map_err(|_| SendPacketError::Disconnected).map(|pkt| pkt.publish())
            }
            Err(err) => Err(SendPacketError::Encode(err)),
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: Option<NonZeroU16>,
    packet: codec::Subscribe,
    shared: Rc<MqttShared>,
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
    pub fn topic_filter(
        mut self,
        filter: ByteString,
        opts: codec::SubscriptionOptions,
    ) -> Self {
        self.packet.topic_filters.push((filter, opts));
        self
    }

    #[inline]
    /// Add user property
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    /// Send subscribe packet
    pub async fn send(self) -> Result<codec::SubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

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
            packet.packet_id = idx;
            let rx = shared.with_queues(|queues| {
                // ack channel
                let (tx, rx) = shared.pool.queue.channel();

                if queues.inflight_ids.contains(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.push_back((idx, tx, AckType::Subscribe));
                queues.inflight_ids.insert(idx);
                Ok(rx)
            })?;

            // send subscribe to client
            log::trace!("Sending subscribe packet {:#?}", packet);

            match shared.io.encode(codec::Packet::Subscribe(packet), &shared.codec) {
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
    packet: codec::Unsubscribe,
    shared: Rc<MqttShared>,
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
        self.packet.topic_filters.push(filter);
        self
    }

    #[inline]
    /// Add user property
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    /// Send unsubscribe packet
    pub async fn send(self) -> Result<codec::UnsubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

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

                if queues.inflight_ids.contains(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.push_back((idx, tx, AckType::Unsubscribe));
                queues.inflight_ids.insert(idx);
                Ok(rx)
            })?;
            packet.packet_id = idx;

            // send unsubscribe to client
            log::trace!("Sending unsubscribe packet {:#?}", packet);

            match shared.io.encode(codec::Packet::Unsubscribe(packet), &shared.codec) {
                Ok(_) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(|pkt| pkt.unsubscribe())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}
