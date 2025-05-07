use std::num::{NonZeroU16, NonZeroU32};
use std::{cell::Cell, fmt, future::ready, future::Future, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::{channel::pool, future::Either, future::Ready};

use super::codec::{self, EncodeLtd};
use super::shared::{AckType, MqttShared};
use crate::{error::EncodeError, error::SendPacketError, types::QoS};

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
    pub fn publish<U>(&self, topic: U) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        self.publish_pkt(codec::Publish {
            dup: false,
            retain: false,
            topic: topic.into(),
            qos: QoS::AtMostOnce,
            packet_id: None,
            payload_size: 0,
            properties: codec::PublishProperties::default(),
        })
    }

    #[inline]
    /// Create publish builder with publish packet
    pub fn publish_pkt(&self, packet: codec::Publish) -> PublishBuilder {
        PublishBuilder::new(self.0.clone(), packet)
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
    fn new(shared: Rc<MqttShared>, mut packet: codec::Publish) -> Self {
        Self { shared, packet }
    }

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
    pub fn size(&self, payload_size: u32) -> u32 {
        self.packet.encoded_size(u32::MAX) as u32 + payload_size
    }

    #[inline]
    /// Send publish packet with QoS 0
    pub fn send_at_most_once(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.packet.qos = QoS::AtMostOnce;
            self.packet.payload_size = payload.len() as u32;
            self.shared
                .encode_publish(self.packet, Some(payload))
                .map_err(SendPacketError::Encode)
                .map(|_| ())
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 0
    pub fn stream_at_most_once(
        mut self,
        size: u32,
        chunk: Option<Bytes>,
    ) -> Result<StreamingPayload, SendPacketError> {
        if !self.shared.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);

            let stream = StreamingPayload {
                rx: Cell::new(None),
                shared: self.shared.clone(),
                inprocess: Cell::new(false),
            };

            self.packet.qos = QoS::AtMostOnce;
            self.packet.payload_size = size;
            self.shared
                .encode_publish(self.packet, chunk)
                .map_err(SendPacketError::Encode)
                .map(|_| stream)
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 1
    pub fn send_at_least_once(
        mut self,
        payload: Bytes,
    ) -> impl Future<Output = Result<codec::PublishAck, SendPacketError>> {
        if !self.shared.is_closed() {
            self.packet.qos = QoS::AtLeastOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            if let Some(rx) = self.shared.wait_readiness() {
                Either::Left(Either::Left(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    self.send_at_least_once_inner(payload).await
                }))
            } else {
                Either::Left(Either::Right(self.send_at_least_once_inner(payload)))
            }
        } else {
            Either::Right(Ready::Err(SendPacketError::Disconnected))
        }
    }

    /// Non-blocking send publish packet with QoS 1
    ///
    /// Panics if sink is not ready or publish ack callback is not set
    pub fn send_at_least_once_no_block(
        mut self,
        payload: Bytes,
    ) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            // check readiness
            if !self.shared.is_ready() {
                panic!("Mqtt sink is not ready");
            }
            self.packet.qos = codec::QoS::AtLeastOnce;
            self.packet.payload_size = payload.len() as u32;

            let idx = self.shared.set_publish_id(&mut self.packet);

            log::trace!("Publish (QoS1) to {:#?}", self.packet);
            self.shared.wait_publish_response_no_block(
                idx,
                AckType::Publish,
                self.packet,
                Some(payload),
            )
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 1
    pub fn stream_at_least_once(
        mut self,
        size: u32,
        chunk: Option<Bytes>,
    ) -> (StreamingPayload, impl Future<Output = Result<codec::PublishAck, SendPacketError>>)
    {
        if !self.shared.is_closed() {
            self.packet.qos = QoS::AtLeastOnce;
            self.packet.payload_size = size;

            let (tx, rx) = self.shared.pool.waiters.channel();
            let stream = StreamingPayload {
                rx: Cell::new(Some(rx)),
                shared: self.shared.clone(),
                inprocess: Cell::new(false),
            };

            // handle client receive maximum
            let fut = if let Some(rx) = self.shared.wait_readiness() {
                Either::Left(Either::Left(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    self.stream_at_least_once_inner(tx, chunk).await
                }))
            } else {
                Either::Left(Either::Right(self.stream_at_least_once_inner(tx, chunk)))
            };
            (stream, fut)
        } else {
            let (tx, rx) = self.shared.pool.waiters.channel();
            let stream = StreamingPayload {
                rx: Cell::new(Some(rx)),
                shared: self.shared.clone(),
                inprocess: Cell::new(false),
            };

            (stream, Either::Right(Ready::Err(SendPacketError::Disconnected)))
        }
    }

    async fn send_at_least_once_inner(
        mut self,
        payload: Bytes,
    ) -> Result<codec::PublishAck, SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", self.packet);
        self.shared
            .wait_publish_response(idx, AckType::Publish, self.packet, Some(payload))?
            .await
            .map(|pkt| pkt.publish())
            .map_err(|_| SendPacketError::Disconnected)
    }

    async fn stream_at_least_once_inner(
        mut self,
        tx: pool::Sender<()>,
        chunk: Option<Bytes>,
    ) -> Result<codec::PublishAck, SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        if tx.is_canceled() {
            Err(SendPacketError::StreamingCancelled)
        } else {
            let rx =
                self.shared.wait_publish_response(idx, AckType::Publish, self.packet, chunk);
            let _ = tx.send(());

            rx?.await.map(|pkt| pkt.publish()).map_err(|_| SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 2
    pub fn send_exactly_once(
        mut self,
        payload: Bytes,
    ) -> impl Future<Output = Result<PublishReceived, SendPacketError>> {
        if !self.shared.is_closed() {
            self.packet.qos = codec::QoS::ExactlyOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            if let Some(rx) = self.shared.wait_readiness() {
                Either::Left(Either::Left(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    self.send_exactly_once_inner(payload).await
                }))
            } else {
                Either::Left(Either::Right(self.send_exactly_once_inner(payload)))
            }
        } else {
            Either::Right(Ready::Err(SendPacketError::Disconnected))
        }
    }

    fn send_exactly_once_inner(
        mut self,
        payload: Bytes,
    ) -> impl Future<Output = Result<PublishReceived, SendPacketError>> {
        let shared = self.shared.clone();
        let idx = shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS2) to {:#?}", self.packet);

        let rx =
            shared.wait_publish_response(idx, AckType::Receive, self.packet, Some(payload));
        async move {
            rx?.await
                .map(move |ack| PublishReceived::new(ack.receive(), shared))
                .map_err(|_| SendPacketError::Disconnected)
        }
    }
}

/// Publish released for QoS2
pub struct PublishReceived {
    ack: codec::PublishAck,
    result: Option<codec::PublishAck2>,
    shared: Rc<MqttShared>,
}

impl PublishReceived {
    fn new(ack: codec::PublishAck, shared: Rc<MqttShared>) -> Self {
        let packet_id = ack.packet_id;
        Self {
            ack,
            shared,
            result: Some(codec::PublishAck2 {
                packet_id,
                reason_code: codec::PublishAck2Reason::Success,
                properties: codec::UserProperties::default(),
                reason_string: None,
            }),
        }
    }

    /// Returns reference to auth packet
    pub fn packet(&self) -> &codec::PublishAck {
        &self.ack
    }

    /// Update user properties
    #[inline]
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.as_mut().unwrap().properties);
        self
    }

    /// Set ack reason string
    #[inline]
    pub fn reason(mut self, reason: ByteString) -> Self {
        self.result.as_mut().unwrap().reason_string = Some(reason);
        self
    }

    /// Release publish
    pub async fn release(mut self) -> Result<(), SendPacketError> {
        let rx = self.shared.release_publish(self.result.take().unwrap())?;

        rx.await.map(|_| ()).map_err(|_| SendPacketError::Disconnected)
    }
}

impl Drop for PublishReceived {
    fn drop(&mut self) {
        if let Some(ack) = self.result.take() {
            self.shared.release_publish(ack);
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

pub struct StreamingPayload {
    shared: Rc<MqttShared>,
    rx: Cell<Option<pool::Receiver<()>>>,
    inprocess: Cell<bool>,
}

impl StreamingPayload {
    fn drop(&mut self) {
        if self.inprocess.get() {
            if self.shared.is_streaming() {
                self.shared.streaming_dropped();
            }
        }
    }
}

impl StreamingPayload {
    /// Send payload chunk
    pub async fn send(&self, chunk: Bytes) -> Result<(), SendPacketError> {
        if let Some(rx) = self.rx.take() {
            if rx.await.is_err() {
                return Err(SendPacketError::StreamingCancelled);
            }
            log::trace!("Publish is encoded, ready to process payload");
            self.inprocess.set(true);
        }

        if !self.inprocess.get() {
            Err(EncodeError::UnexpectedPayload.into())
        } else {
            log::trace!("Sending payload chunk: {:?}", chunk.len());
            self.shared.want_payload_stream().await?;

            if !self.shared.encode_publish_payload(chunk)? {
                self.inprocess.set(false);
            }
            Ok(())
        }
    }
}
