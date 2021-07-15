use std::future::{ready, Future};
use std::{fmt, num::NonZeroU16, num::NonZeroU32, rc::Rc};

use ntex::util::{ByteString, Bytes, Either, Ready};

use super::codec;
use super::error::{ProtocolError, PublishQos1Error, SendPacketError};
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

    /// Check connection status
    pub fn is_open(&self) -> bool {
        self.0.state.is_open()
    }

    /// Get client's receive credit
    pub fn credit(&self) -> usize {
        let cap = self.0.cap.get();
        cap - self.0.with_queues(|q| q.inflight.len())
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive
    pub fn ready(&self) -> impl Future<Output = bool> {
        if self.0.state.is_open() {
            self.0
                .with_queues(|q| {
                    if q.inflight.len() >= self.0.cap.get() {
                        let (tx, rx) = self.0.pool.waiters.channel();
                        self.0.with_queues(move |q| q.waiters.push_back(tx));
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

    /// Close mqtt connection with default Disconnect message
    pub fn close(&self) {
        if self.is_open() {
            let _ = self
                .0
                .state
                .write()
                .encode(codec::Packet::Disconnect(codec::Disconnect::default()), &self.0.codec);
            self.0.state.close();
        }
        self.0.with_queues(|q| {
            q.inflight.clear();
            q.waiters.clear();
        });
    }

    /// Close mqtt connection
    pub fn close_with_reason(&self, pkt: codec::Disconnect) {
        if self.is_open() {
            let _ = self.0.state.write().encode(codec::Packet::Disconnect(pkt), &self.0.codec);
            self.0.state.close();
        }
        self.0.with_queues(|q| {
            q.inflight.clear();
            q.waiters.clear();
        });
    }

    pub(super) fn send(&self, pkt: codec::Packet) {
        let _ = self.0.state.write().encode(pkt, &self.0.codec);
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        self.0.state.write().encode(codec::Packet::PingRequest, &self.0.codec).is_ok()
    }

    /// Close mqtt connection, dont send disconnect message
    pub(super) fn drop_sink(&self) {
        self.0.with_queues(|q| {
            q.waiters.clear();
            q.inflight.clear();
        });
        self.0.state.close();
    }

    pub(super) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
        self.0.with_queues(|queues| loop {
            // check ack order
            if let Some(idx) = queues.inflight_order.pop_front() {
                // errored publish
                if idx == 0 {
                    continue;
                }

                if idx != pkt.packet_id() {
                    log::trace!(
                        "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                        idx,
                        pkt.packet_id()
                    );
                } else {
                    // get publish ack channel
                    log::trace!("Ack packet with id: {}", pkt.packet_id());
                    let idx = pkt.packet_id();
                    if let Some((tx, tp)) = queues.inflight.remove(&idx) {
                        // cleanup ack queue
                        if !pkt.is_match(tp) {
                            log::trace!("MQTT protocol error, unexpeted packet");
                            return Err(ProtocolError::Unexpected(
                                pkt.packet_type(),
                                tp.name(),
                            ));
                        }
                        let _ = tx.send(pkt);

                        // wake up queued request (receive max limit)
                        while let Some(tx) = queues.waiters.pop_front() {
                            if tx.send(()).is_ok() {
                                break;
                            }
                        }
                        return Ok(());
                    } else {
                        log::error!("In-flight state inconsistency")
                    }
                }
            } else {
                log::trace!("Unexpected PublishAck packet");
            }
            return Err(ProtocolError::PacketIdMismatch);
        })
    }

    /// Create publish packet builder
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
                qos: QoS::AtMostOnce,
                packet_id: None,
                properties: codec::PublishProperties::default(),
            },
            shared: self.0.clone(),
        }
    }

    /// Create subscribe packet builder
    pub fn subscribe(&self, id: Option<NonZeroU32>) -> SubscribeBuilder {
        SubscribeBuilder {
            id: 0,
            packet: codec::Subscribe {
                id,
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Vec::new(),
                topic_filters: Vec::new(),
            },
            shared: self.0.clone(),
        }
    }

    /// Create unsubscribe packet builder
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder {
            id: 0,
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

    /// This might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    /// Set retain flag
    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
    }

    /// Set publish packet properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
        self
    }

    /// Set publish packet properties
    pub fn set_properties<F>(&mut self, f: F)
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
    }

    /// Send publish packet with QoS 0
    pub fn send_at_most_once(self) -> Result<(), SendPacketError> {
        let packet = self.packet;

        if self.shared.state.is_open() {
            log::trace!("Publish (QoS-0) to {:?}", packet.topic);
            self.shared
                .state
                .write()
                .encode(codec::Packet::Publish(packet), &self.shared.codec)
                .map_err(SendPacketError::Encode)
                .map(|_| ())
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 1
    pub fn send_at_least_once(
        self,
    ) -> impl Future<Output = Result<codec::PublishAck, PublishQos1Error>> {
        let shared = self.shared;
        let mut packet = self.packet;
        packet.qos = QoS::AtLeastOnce;

        if shared.state.is_open() {
            // handle client receive maximum
            if !shared.has_credit() {
                let (tx, rx) = shared.pool.waiters.channel();
                shared.with_queues(|q| q.waiters.push_back(tx));

                return Either::Left(Either::Right(async move {
                    if rx.await.is_err() {
                        return Err(PublishQos1Error::Disconnected);
                    }
                    Self::send_at_least_once_inner(packet, shared).await
                }));
            }
            Either::Right(Self::send_at_least_once_inner(packet, shared))
        } else {
            Either::Left(Either::Left(Ready::Err(PublishQos1Error::Disconnected)))
        }
    }

    fn send_at_least_once_inner(
        mut packet: codec::Publish,
        shared: Rc<MqttShared>,
    ) -> impl Future<Output = Result<codec::PublishAck, PublishQos1Error>> {
        // packet id
        let mut idx = packet.packet_id.map(|i| i.get()).unwrap_or(0);
        if idx == 0 {
            idx = shared.next_id();
            packet.packet_id = NonZeroU16::new(idx);
        }

        let rx = shared.with_queues(|queues| {
            // publish ack channel
            let (tx, rx) = shared.pool.queue.channel();

            if queues.inflight.contains_key(&idx) {
                return Err(PublishQos1Error::PacketIdInUse(idx));
            }
            queues.inflight.insert(idx, (tx, AckType::Publish));
            queues.inflight_order.push_back(idx);
            Ok(rx)
        });

        let rx = match rx {
            Ok(rx) => rx,
            Err(e) => return Either::Left(Ready::Err(e)),
        };

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", packet);

        match shared.state.write().encode(codec::Packet::Publish(packet), &shared.codec) {
            Ok(_) => {
                // wait ack from peer
                Either::Right(async move {
                    rx.await.map_err(|_| PublishQos1Error::Disconnected).and_then(|pkt| {
                        let pkt = pkt.publish();
                        match pkt.reason_code {
                            codec::PublishAckReason::Success => Ok(pkt),
                            _ => Err(PublishQos1Error::Fail(pkt)),
                        }
                    })
                })
            }
            Err(err) => Either::Left(Ready::Err(PublishQos1Error::Encode(err))),
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: u16,
    packet: codec::Subscribe,
    shared: Rc<MqttShared>,
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
    pub fn topic_filter(
        mut self,
        filter: ByteString,
        opts: codec::SubscriptionOptions,
    ) -> Self {
        self.packet.topic_filters.push((filter, opts));
        self
    }

    /// Add user property
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    #[allow(clippy::await_holding_refcell_ref)]
    /// Send subscribe packet
    pub async fn send(self) -> Result<codec::SubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

        if shared.state.is_open() {
            // handle client receive maximum
            if !shared.has_credit() {
                let (tx, rx) = shared.pool.waiters.channel();
                shared.with_queues(|q| q.waiters.push_back(tx));

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            // allocate packet id
            let idx = if self.id == 0 { shared.next_id() } else { self.id };
            packet.packet_id = NonZeroU16::new(idx).unwrap();
            let rx = shared.with_queues(|queues| {
                // ack channel
                let (tx, rx) = shared.pool.queue.channel();

                if queues.inflight.contains_key(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.insert(idx, (tx, AckType::Subscribe));
                queues.inflight_order.push_back(idx);
                Ok(rx)
            })?;

            // send subscribe to client
            log::trace!("Sending subscribe packet {:#?}", packet);

            match shared.state.write().encode(codec::Packet::Subscribe(packet), &shared.codec) {
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
    packet: codec::Unsubscribe,
    shared: Rc<MqttShared>,
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
        self.packet.topic_filters.push(filter);
        self
    }

    /// Add user property
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    #[allow(clippy::await_holding_refcell_ref)]
    /// Send unsubscribe packet
    pub async fn send(self) -> Result<codec::UnsubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

        if shared.state.is_open() {
            // handle client receive maximum
            if !shared.has_credit() {
                let (tx, rx) = shared.pool.waiters.channel();
                shared.with_queues(|q| q.waiters.push_back(tx));

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            // allocate packet id
            let idx = if self.id == 0 { shared.next_id() } else { self.id };
            let rx = shared.with_queues(|queues| {
                // ack channel
                let (tx, rx) = shared.pool.queue.channel();

                if queues.inflight.contains_key(&idx) {
                    return Err(SendPacketError::PacketIdInUse(idx));
                }
                queues.inflight.insert(idx, (tx, AckType::Unsubscribe));
                queues.inflight_order.push_back(idx);
                Ok(rx)
            })?;
            packet.packet_id = NonZeroU16::new(idx).unwrap();

            // send unsubscribe to client
            log::trace!("Sending unsubscribe packet {:#?}", packet);

            match shared.state.write().encode(codec::Packet::Unsubscribe(packet), &shared.codec)
            {
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
