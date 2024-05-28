use std::{fmt, future::ready, future::Future, num::NonZeroU16, num::NonZeroU32, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::future::{Either, Ready};

use super::{
    codec, codec::EncodeLtd, error::SendPacketError, shared::AckType, shared::MqttShared,
};
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

    pub(super) fn shared(&self) -> Rc<MqttShared> {
        self.0.clone()
    }

    #[inline]
    /// Check if io stream is open
    pub fn is_open(&self) -> bool {
        !self.0.is_closed()
    }

    #[inline]
    /// Check if sink is ready
    pub fn is_ready(&self) -> bool {
        if self.0.is_closed() {
            false
        } else {
            self.0.is_ready()
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
        if !self.0.is_closed() {
            self.0
                .wait_readiness()
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
        self.0.force_close();
    }

    #[inline]
    /// Close mqtt connection with default Disconnect message
    pub fn close(&self) {
        self.0.close(codec::Disconnect::default());
    }

    #[inline]
    /// Close mqtt connection
    pub fn close_with_reason(&self, pkt: codec::Disconnect) {
        self.0.close(pkt);
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        self.0.encode_packet(codec::Packet::PingRequest).is_ok()
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

    /// Set publish ack callback
    ///
    /// Use non-blocking send, PublishBuilder::send_at_least_once_no_block()
    /// First argument is packet id, second argument is "disconnected" state
    pub fn publish_ack_cb<F>(&self, f: F)
    where
        F: Fn(codec::PublishAck, bool) + 'static,
    {
        self.0.set_publish_ack(Box::new(f));
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
    pub fn retain(mut self, val: bool) -> Self {
        self.packet.retain = val;
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
    /// Get size of the publish packet
    pub fn size(&self) -> u32 {
        self.packet.encoded_size(u32::MAX) as u32
    }

    #[inline]
    /// Send publish packet with QoS 0
    pub fn send_at_most_once(mut self) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.packet.qos = QoS::AtMostOnce;
            self.shared
                .encode_packet(codec::Packet::Publish(self.packet))
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
    ) -> impl Future<Output = Result<codec::PublishAck, SendPacketError>> {
        if !self.shared.is_closed() {
            let shared = self.shared;
            let mut packet = self.packet;
            packet.qos = QoS::AtLeastOnce;

            // handle client receive maximum
            if let Some(rx) = shared.wait_readiness() {
                Either::Left(Either::Left(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    Self::send_at_least_once_inner(packet, shared).await
                }))
            } else {
                Either::Left(Either::Right(Self::send_at_least_once_inner(packet, shared)))
            }
        } else {
            Either::Right(Ready::Err(SendPacketError::Disconnected))
        }
    }

    /// Non-blocking send publish packet with QoS 1
    ///
    /// Panics if sink is not ready or publish ack callback is not set
    pub fn send_at_least_once_no_block(self) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            let shared = self.shared;

            // check readiness
            if !shared.is_ready() {
                panic!("Mqtt sink is not ready");
            }
            let mut packet = self.packet;
            packet.qos = codec::QoS::AtLeastOnce;

            // packet id
            let idx = if let Some(idx) = packet.packet_id {
                idx
            } else {
                let idx = shared.next_id();
                packet.packet_id = Some(idx);
                idx
            };
            log::trace!("Publish (QoS1) to {:#?}", packet);

            shared.wait_packet_response_no_block(
                idx,
                AckType::Publish,
                codec::Packet::Publish(packet),
            )
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    fn send_at_least_once_inner(
        mut packet: codec::Publish,
        shared: Rc<MqttShared>,
    ) -> impl Future<Output = Result<codec::PublishAck, SendPacketError>> {
        // packet id
        let idx = if let Some(idx) = packet.packet_id {
            idx
        } else {
            let idx = shared.next_id();
            packet.packet_id = Some(idx);
            idx
        };

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", packet);

        let rx =
            shared.wait_packet_response(idx, AckType::Publish, codec::Packet::Publish(packet));
        async move { rx?.await.map(|pkt| pkt.publish()).map_err(|_| SendPacketError::Disconnected) }
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

    #[inline]
    /// Get size of the subscribe packet
    pub fn size(&self) -> u32 {
        self.packet.encoded_size(u32::MAX) as u32
    }

    /// Send subscribe packet
    pub async fn send(self) -> Result<codec::SubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

        if !shared.is_closed() {
            // handle client receive maximum
            if let Some(rx) = shared.wait_readiness() {
                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }

            // allocate packet id
            packet.packet_id = self.id.unwrap_or_else(|| shared.next_id());

            // send subscribe to client
            log::trace!("Sending subscribe packet {:#?}", packet);

            let rx = shared.wait_response(packet.packet_id, AckType::Subscribe)?;
            match shared.encode_packet(codec::Packet::Subscribe(packet)) {
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

    #[inline]
    /// Get size of the unsubscribe packet
    pub fn size(&self) -> u32 {
        self.packet.encoded_size(u32::MAX) as u32
    }

    /// Send unsubscribe packet
    pub async fn send(self) -> Result<codec::UnsubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

        if !shared.is_closed() {
            // handle client receive maximum
            if let Some(rx) = shared.wait_readiness() {
                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            // allocate packet id
            packet.packet_id = self.id.unwrap_or_else(|| shared.next_id());

            // send unsubscribe to client
            log::trace!("Sending unsubscribe packet {:#?}", packet);

            let rx = shared.wait_response(packet.packet_id, AckType::Unsubscribe)?;
            match shared.encode_packet(codec::Packet::Unsubscribe(packet)) {
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
