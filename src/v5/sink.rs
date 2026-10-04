use std::num::{NonZeroU16, NonZeroU32};
use std::{cell::Cell, fmt, future::Future, future::ready, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::{channel::pool, future::Either};

use super::codec::{self, EncodeLtd};
use super::shared::{Ack, AckType, MqttShared, SendPermit};
use crate::{error::EncodeError, error::SendPacketError, types::QoS};

/// Mqtt client/server sink, it is used to send packets to the peer
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
    /// Check if connection is active
    ///
    /// Returns `false` as soon as the connection starts closing, either
    /// locally or because the peer has gone. Buffered data may still be flushing.
    pub fn is_open(&self) -> bool {
        self.0.is_active()
    }

    #[inline]
    /// Check if Disconnect packet is received
    pub fn is_disconnect_recv(&self) -> bool {
        self.0.is_disconnect_recv()
    }

    #[inline]
    /// Check if sink is ready
    pub fn is_ready(&self) -> bool {
        if self.0.is_active() {
            self.0.is_ready()
        } else {
            false
        }
    }

    #[inline]
    /// Get remaining send credit
    ///
    /// Number of `QoS 1` and `QoS 2` publish packets that can be sent before
    /// the peer's receive maximum is exhausted.
    pub fn credit(&self) -> usize {
        self.0.credit()
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive. Waiting publishes get send credit
    /// in the order of the calls. The credit is not reserved once the future
    /// completes, the next waiting publish gets it.
    pub fn ready(&self) -> impl Future<Output = bool> {
        if !self.0.is_active() {
            Either::Left(ready(false))
        } else if self.0.is_ready() {
            Either::Left(ready(true))
        } else {
            let permit = self.0.send_permit();
            Either::Right(async move { permit.await.is_ok() })
        }
    }

    #[inline]
    /// Force close MQTT connection.
    ///
    /// The connection is aborted immediately, dispatcher does not wait for
    /// uncompleted responses (ending them with error) and buffered data is discarded.
    /// Use [`close`](Self::close) to close connection gracefully.
    pub fn force_close(&self) {
        self.0.force_close();
    }

    #[inline]
    /// Close mqtt connection with default Disconnect message
    pub fn close(&self) {
        self.0.close(Some(codec::Disconnect::default()));
    }

    #[inline]
    /// Close mqtt connection
    pub fn close_with_reason(&self, pkt: codec::Disconnect) {
        self.0.close(Some(pkt));
    }

    #[inline]
    /// Close mqtt connection
    ///
    /// This method does not send `disconnect` packet to the peer
    pub fn close_with_no_reason(&self) {
        self.0.close(None);
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        if self.0.encode_packet(codec::Packet::PingRequest).is_ok() {
            self.0.set_ping_pending(true);
            true
        } else {
            false
        }
    }

    /// Check if PINGREQ is sent and PINGRESP is not received yet
    pub(super) fn is_ping_pending(&self) -> bool {
        self.0.is_ping_pending()
    }

    /// Check if newly encoded packets wait behind a streaming payload or write backpressure
    pub(super) fn is_write_blocked(&self) -> bool {
        self.0.is_write_blocked()
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
    /// Use non-blocking send, `PublishBuilder::send_at_least_once_no_block()`
    ///
    /// First argument is received `PublishAck` packet (on disconnect, a synthetic
    /// ack that carries only packet id), second argument is "disconnected" state.
    pub fn publish_ack_cb<F>(&self, f: F)
    where
        F: Fn(codec::PublishAck, bool) + 'static,
    {
        self.0.set_publish_ack(Box::new(f));
    }

    #[inline]
    #[allow(clippy::missing_panics_doc)]
    /// Create subscribe packet builder
    ///
    /// Only a client sink sends the packet, on a server sink
    /// [`SubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
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
    #[allow(clippy::missing_panics_doc)]
    /// Create unsubscribe packet builder
    ///
    /// Only a client sink sends the packet, on a server sink
    /// [`UnsubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
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

/// Publish packet builder
pub struct PublishBuilder {
    shared: Rc<MqttShared>,
    packet: codec::Publish,
}

impl fmt::Debug for PublishBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishBuilder")
            .field("packet", &self.packet)
            .finish()
    }
}

impl PublishBuilder {
    fn new(shared: Rc<MqttShared>, packet: codec::Publish) -> Self {
        Self { shared, packet }
    }

    #[inline]
    #[must_use]
    /// Set packet id.
    ///
    /// Note: if packet id is not set, it gets generated automatically.
    /// Packet id management should not be mixed, it should be auto-generated
    /// or set by user. Otherwise collisions could occure.
    ///
    /// # Panics
    ///
    /// Panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        let id = NonZeroU16::new(id).expect("id 0 is not allowed");
        self.packet.packet_id = Some(id);
        self
    }

    #[inline]
    #[must_use]
    /// This might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    #[inline]
    #[must_use]
    /// Set retain flag
    pub fn retain(mut self, val: bool) -> Self {
        self.packet.retain = val;
        self
    }

    #[inline]
    #[must_use]
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
    pub fn size(&self, payload_size: usize) -> u32 {
        (self.packet.encoded_size(u32::MAX) + payload_size) as u32
    }

    /// Send publish packet with `QoS 0`
    ///
    /// Waits while an outgoing streaming publish is in progress, packets cannot
    /// interleave with its payload, and while write backpressure is enabled,
    /// until the write buffer drains to half of the high watermark.
    /// Fails with `SendPacketError::Disconnected` if the connection is closed
    /// or the write timeout of the io expires.
    pub async fn send_at_most_once(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if self.shared.wait_publish_ready().await {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.packet.qos = QoS::AtMostOnce;
            self.packet.payload_size = payload.len() as u32;
            self.shared
                .encode_publish(self.packet, Some(payload))
                .map_err(SendPacketError::Encode)
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with `QoS 0`
    ///
    /// Waits while an outgoing streaming publish is in progress, packets cannot
    /// interleave with its payload, and while write backpressure is enabled,
    /// until the write buffer drains to half of the high watermark.
    /// Fails with `SendPacketError::Disconnected` if the connection is closed
    /// or the write timeout of the io expires.
    pub async fn stream_at_most_once(
        mut self,
        size: u32,
    ) -> Result<StreamingPayload, SendPacketError> {
        if self.shared.wait_publish_ready().await {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);

            let stream = StreamingPayload {
                rx: Cell::new(None),
                shared: self.shared.clone(),
                inprocess: Cell::new(true),
            };

            self.packet.qos = QoS::AtMostOnce;
            self.packet.payload_size = size;
            self.shared
                .encode_publish(self.packet, None)
                .map_err(SendPacketError::Encode)
                .map(|()| stream)
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with `QoS 1`
    pub async fn send_at_least_once(
        mut self,
        payload: Bytes,
    ) -> Result<codec::PublishAck, SendPacketError> {
        if self.shared.is_active() {
            self.packet.qos = QoS::AtLeastOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            let permit = self.shared.send_permit().await?;
            self.send_at_least_once_inner(payload, permit).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Non-blocking send publish packet with `QoS 1`
    ///
    /// # Panics
    ///
    /// Panics if sink is not ready. If publish ack callback is not set,
    /// connection task panics later, when ack is received.
    pub fn send_at_least_once_no_block(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if self.shared.is_active() {
            // check readiness
            assert!(self.shared.is_ready(), "Mqtt sink is not ready");

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

    /// Send publish packet with `QoS 1`
    pub fn stream_at_least_once(
        mut self,
        size: u32,
    ) -> (
        impl Future<Output = Result<codec::PublishAck, SendPacketError>>,
        StreamingPayload,
    ) {
        let (tx, rx) = self.shared.pool.waiters.channel();
        let stream = StreamingPayload {
            rx: Cell::new(Some(rx)),
            shared: self.shared.clone(),
            inprocess: Cell::new(false),
        };

        if self.shared.is_active() {
            self.packet.qos = QoS::AtLeastOnce;
            self.packet.payload_size = size;

            // handle client receive maximum
            let permit = self.shared.send_permit();
            let fut = async move {
                let permit = permit.await?;
                self.stream_at_least_once_inner(tx, None, permit).await
            };
            (Either::Left(fut), stream)
        } else {
            (
                Either::Right(async { Err(SendPacketError::Disconnected) }),
                stream,
            )
        }
    }

    async fn send_at_least_once_inner(
        mut self,
        payload: Bytes,
        permit: SendPermit,
    ) -> Result<codec::PublishAck, SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client, the in-flight publish holds the send credit
        log::trace!("Publish (QoS1) to {:#?}", self.packet);
        let rx =
            self.shared
                .wait_publish_response(idx, AckType::Publish, self.packet, Some(payload));
        drop(permit);
        rx?.await
            .map(Ack::publish)
            .map_err(|_| SendPacketError::Disconnected)
    }

    async fn stream_at_least_once_inner(
        mut self,
        tx: pool::Sender<()>,
        chunk: Option<Bytes>,
        permit: SendPermit,
    ) -> Result<codec::PublishAck, SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        if tx.is_canceled() {
            Err(SendPacketError::StreamingCancelled)
        } else {
            let rx = self
                .shared
                .wait_publish_response(idx, AckType::Publish, self.packet, chunk);
            drop(permit);
            let _ = tx.send(());

            rx?.await
                .map(Ack::publish)
                .map_err(|_| SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with `QoS 2`
    ///
    /// If the returned future is dropped after the publish is sent, the publish
    /// is released, `PublishRelease` is sent once `PublishReceived` is received.
    pub async fn send_exactly_once(
        mut self,
        payload: Bytes,
    ) -> Result<PublishReceived, SendPacketError> {
        if self.shared.is_active() {
            self.packet.qos = codec::QoS::ExactlyOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            let permit = self.shared.send_permit().await?;
            self.send_exactly_once_inner(payload, permit).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    async fn send_exactly_once_inner(
        mut self,
        payload: Bytes,
        permit: SendPermit,
    ) -> Result<PublishReceived, SendPacketError> {
        let shared = self.shared.clone();
        let idx = shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS2) to {:#?}", self.packet);

        // the in-flight publish holds the send credit
        let rx = shared.wait_publish_response(idx, AckType::Receive, self.packet, Some(payload));
        drop(permit);
        let rx = rx?;
        let guard = ReleaseGuard(idx, &shared);
        let result = rx.await;
        std::mem::forget(guard);

        result
            .map(move |ack| PublishReceived::new(ack.receive(), shared))
            .map_err(|_| SendPacketError::Disconnected)
    }
}

/// Releases `QoS 2` publish if the publish future is dropped after `PublishReceived`
/// packet is delivered to it but before the future is polled, PUBREL must be sent
/// for a PUBREC below 0x80 [MQTT-4.3.3-4]. If the PUBREC is not received yet,
/// the dispatcher releases the publish.
struct ReleaseGuard<'a>(NonZeroU16, &'a MqttShared);

impl Drop for ReleaseGuard<'_> {
    fn drop(&mut self) {
        let _ = self.1.release_publish(codec::PublishAck2 {
            packet_id: self.0,
            ..Default::default()
        });
    }
}

/// `PublishReceived` packet is received for `QoS 2` publish
///
/// Call [`release`](Self::release) to send `PublishRelease` packet and wait for
/// `PublishComplete`. If the value is dropped, `PublishRelease` is sent without
/// waiting for `PublishComplete`.
///
/// If `PublishReceived` packet has a reason code of 0x80 or greater, the publish
/// is rejected and `PublishRelease` is never sent, check
/// [`packet`](Self::packet) for the reason code.
pub struct PublishReceived {
    ack: codec::PublishAck,
    result: Option<codec::PublishAck2>,
    shared: Rc<MqttShared>,
}

impl fmt::Debug for PublishReceived {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishReceived")
            .field("ack", &self.ack)
            .finish()
    }
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

    /// Returns reference to received `PublishReceived` packet
    pub fn packet(&self) -> &codec::PublishAck {
        &self.ack
    }

    #[inline]
    #[must_use]
    /// Update user properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.as_mut().unwrap().properties);
        self
    }

    #[inline]
    #[must_use]
    /// Set ack reason string
    pub fn reason(mut self, reason: ByteString) -> Self {
        self.result.as_mut().unwrap().reason_string = Some(reason);
        self
    }

    /// Release publish
    ///
    /// Returns [`SendPacketError::UnexpectedRelease`] if the publish is rejected,
    /// `PublishReceived` packet has a reason code of 0x80 or greater.
    pub async fn release(mut self) -> Result<(), SendPacketError> {
        let ack = self.result.take().unwrap();
        if self.is_rejected() {
            return Err(SendPacketError::UnexpectedRelease);
        }
        let rx = self.shared.release_publish(ack)?;

        rx.await
            .map(|_| ())
            .map_err(|_| SendPacketError::Disconnected)
    }

    // PUBREL must not be sent for a PUBREC with a reason code of 0x80 or greater,
    // its packet id is already released (MQTT 5.0, 4.3.3 [MQTT-4.3.3-4])
    fn is_rejected(&self) -> bool {
        u8::from(self.ack.reason_code) >= 0x80
    }
}

impl Drop for PublishReceived {
    fn drop(&mut self) {
        if let Some(ack) = self.result.take()
            && !self.is_rejected()
        {
            let _ = self.shared.release_publish(ack);
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: Option<NonZeroU16>,
    packet: codec::Subscribe,
    shared: Rc<MqttShared>,
}

impl fmt::Debug for SubscribeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeBuilder")
            .field("id", &self.id)
            .field("packet", &self.packet)
            .finish()
    }
}

impl SubscribeBuilder {
    #[inline]
    #[must_use]
    /// Set packet id.
    ///
    /// # Panics
    ///
    /// Panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    #[must_use]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString, opts: codec::SubscriptionOptions) -> Self {
        self.packet.topic_filters.push((filter, opts));
        self
    }

    #[inline]
    #[must_use]
    /// Add user property
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    #[inline]
    #[must_use]
    /// Get size of the subscribe packet
    pub fn size(&self) -> u32 {
        self.packet.encoded_size(u32::MAX) as u32
    }

    /// Send subscribe packet
    ///
    /// Receive Maximum does not apply to SUBSCRIBE packets, the packet waits
    /// for write backpressure only and does not use send credit.
    ///
    /// Only the client sends SUBSCRIBE packets (MQTT 5.0, 3.8), a server sink
    /// returns [`SendPacketError::NotAllowed`].
    pub async fn send(self) -> Result<codec::SubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

        if !shared.is_client() {
            return Err(SendPacketError::NotAllowed);
        }

        if shared.is_active() {
            // receive maximum does not apply, wait for write backpressure only
            shared.wait_wr_readiness().await?;

            // allocate packet id
            packet.packet_id = self.id.unwrap_or_else(|| shared.next_id());

            // send subscribe to client
            log::trace!("Sending subscribe packet {packet:#?}");

            let rx = shared.wait_response(packet.packet_id, AckType::Subscribe)?;
            match shared.encode_packet(codec::Packet::Subscribe(packet)) {
                Ok(()) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(Ack::subscribe)
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

impl fmt::Debug for UnsubscribeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnsubscribeBuilder")
            .field("id", &self.id)
            .field("packet", &self.packet)
            .finish()
    }
}

impl UnsubscribeBuilder {
    #[inline]
    #[must_use]
    /// Set packet id.
    ///
    /// # Panics
    ///
    /// Panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    #[must_use]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString) -> Self {
        self.packet.topic_filters.push(filter);
        self
    }

    #[inline]
    #[must_use]
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
    ///
    /// Receive Maximum does not apply to UNSUBSCRIBE packets, the packet waits
    /// for write backpressure only and does not use send credit.
    ///
    /// Only the client sends UNSUBSCRIBE packets (MQTT 5.0, 3.10), a server
    /// sink returns [`SendPacketError::NotAllowed`].
    pub async fn send(self) -> Result<codec::UnsubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

        if !shared.is_client() {
            return Err(SendPacketError::NotAllowed);
        }

        if shared.is_active() {
            // receive maximum does not apply, wait for write backpressure only
            shared.wait_wr_readiness().await?;
            // allocate packet id
            packet.packet_id = self.id.unwrap_or_else(|| shared.next_id());

            // send unsubscribe to client
            log::trace!("Sending unsubscribe packet {packet:#?}");

            let rx = shared.wait_response(packet.packet_id, AckType::Unsubscribe)?;
            match shared.encode_packet(codec::Packet::Unsubscribe(packet)) {
                Ok(()) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(Ack::unsubscribe)
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

/// Sender for a streaming publish payload
pub struct StreamingPayload {
    shared: Rc<MqttShared>,
    rx: Cell<Option<pool::Receiver<()>>>,
    inprocess: Cell<bool>,
}

impl fmt::Debug for StreamingPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingPayload").finish()
    }
}

impl Drop for StreamingPayload {
    fn drop(&mut self) {
        if self.inprocess.get() && self.shared.is_streaming() {
            self.shared.streaming_dropped();
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

        if self.inprocess.get() {
            log::trace!("Sending payload chunk: {:?}", chunk.len());
            self.shared.want_payload_stream().await?;

            if !self.shared.encode_publish_payload(chunk)? {
                self.inprocess.set(false);
            }
            Ok(())
        } else {
            Err(EncodeError::UnexpectedPayload.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;
    use crate::v5::shared::MqttShared;

    #[ntex::test]
    async fn test_debug() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("test"));
        let codec_v5 = codec::Codec::new();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec_v5, Rc::default()));
        let sink = MqttSink::new(shared);

        // MqttSink
        assert!(format!("{sink:?}").contains("MqttSink"));

        // PublishBuilder
        let pb = sink.publish("test/topic");
        assert!(format!("{pb:?}").contains("PublishBuilder"));

        // SubscribeBuilder
        let sb = sink.subscribe(None);
        assert!(format!("{sb:?}").contains("SubscribeBuilder"));

        // UnsubscribeBuilder
        let ub = sink.unsubscribe();
        assert!(format!("{ub:?}").contains("UnsubscribeBuilder"));
    }

    #[ntex::test]
    async fn test_server_subscribe_not_allowed() {
        use std::{future::Future, pin::pin, task::Poll};

        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::new(),
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared);

        // MQTT 5.0, 3.8 and 3.10: SUBSCRIBE and UNSUBSCRIBE are client packets
        let mut f = pin!(
            sink.subscribe(None)
                .topic_filter("a".into(), codec::SubscriptionOptions::default())
                .send()
        );
        let res = lazy(|cx| f.as_mut().poll(cx)).await;
        assert_eq!(
            res.map(|r| r.map(|_| ())),
            Poll::Ready(Err(SendPacketError::NotAllowed))
        );
        let mut f = pin!(sink.unsubscribe().topic_filter("a".into()).send());
        let res = lazy(|cx| f.as_mut().poll(cx)).await;
        assert_eq!(
            res.map(|r| r.map(|_| ())),
            Poll::Ready(Err(SendPacketError::NotAllowed))
        );
        assert!(sink.is_open());
        assert!(client.read_any().is_empty());
    }

    #[ntex::test]
    async fn test_ack_type_mismatch() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        use crate::{error::MqttProtocolError, types::packet_type};

        fn setup() -> ((IoTest, Io), Rc<MqttShared>, MqttSink) {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_client();
            shared.set_cap(16);
            ((client, io), shared.clone(), MqttSink::new(shared))
        }
        async fn is_pending<F: Future>(f: &mut std::pin::Pin<&mut F>) -> bool {
            lazy(|cx| f.as_mut().poll(cx).is_pending()).await
        }
        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn rec(packet_id: u16) -> Ack {
            Ack::Receive(codec::PublishAck {
                packet_id: id(packet_id),
                ..Default::default()
            })
        }
        fn comp(packet_id: u16) -> Ack {
            Ack::Complete(codec::PublishAck2 {
                packet_id: id(packet_id),
                reason_code: codec::PublishAck2Reason::Success,
                properties: codec::UserProperties::default(),
                reason_string: None,
            })
        }
        fn err(pkt: u8, expected: &'static str) -> Result<(), MqttProtocolError> {
            Err(MqttProtocolError::unexpected_packet(pkt, expected))
        }

        // QoS 1 PUBLISH acknowledged with PUBREC or PUBCOMP
        for (ack, expected) in [
            (rec(1), err(packet_type::PUBREC, "Expected PUBACK packet")),
            (comp(1), err(packet_type::PUBCOMP, "Expected PUBACK packet")),
        ] {
            let (_io, shared, sink) = setup();
            let mut f = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
            assert!(is_pending(&mut f).await);
            assert_eq!(shared.pkt_ack(ack), expected);
            assert!(!sink.is_open());
            assert_eq!(f.await, Err(SendPacketError::Disconnected));
        }

        // SUBSCRIBE acknowledged with PUBREC
        let (_io, shared, sink) = setup();
        let mut f = pin!(
            sink.subscribe(None)
                .topic_filter("a".into(), codec::SubscriptionOptions::default())
                .send()
        );
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(rec(1)),
            err(packet_type::PUBREC, "Expected SUBACK packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // UNSUBSCRIBE acknowledged with PUBCOMP
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.unsubscribe().topic_filter("a".into()).send());
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(comp(1)),
            err(packet_type::PUBCOMP, "Expected UNSUBACK packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // QoS 2 PUBLISH acknowledged with PUBCOMP before PUBREC
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(comp(1)),
            err(packet_type::PUBCOMP, "Expected PUBREC packet")
        );
        assert!(matches!(f.await, Err(SendPacketError::Disconnected)));

        // QoS 2 PUBREL acknowledged with a second PUBREC
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
        let mut f = pin!(f.await.unwrap().release());
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(rec(1)),
            err(packet_type::PUBREC, "Expected PUBCOMP packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // valid QoS 2 flow
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
        let mut f = pin!(f.await.unwrap().release());
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
        assert_eq!(f.await, Ok(()));
        assert!(sink.is_open());
    }

    #[ntex::test]
    async fn test_release_multiple() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        async fn is_pending<F: Future>(f: &mut std::pin::Pin<&mut F>) -> bool {
            lazy(|cx| f.as_mut().poll(cx).is_pending()).await
        }
        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn rec(packet_id: u16) -> Ack {
            Ack::Receive(codec::PublishAck {
                packet_id: id(packet_id),
                ..Default::default()
            })
        }
        fn comp(packet_id: u16) -> Ack {
            Ack::Complete(codec::PublishAck2 {
                packet_id: id(packet_id),
                reason_code: codec::PublishAck2Reason::Success,
                properties: codec::UserProperties::default(),
                reason_string: None,
            })
        }

        for drop_second in [false, true] {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            // two QoS 2 PUBLISH packets are received before release
            let mut f1 = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
            let mut f2 = pin!(sink.publish("b").send_exactly_once(Bytes::new()));
            assert!(is_pending(&mut f1).await);
            assert!(is_pending(&mut f2).await);
            let _ = client.read().await.unwrap();
            assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
            assert_eq!(shared.pkt_ack(rec(2)), Ok(()));
            let rec1 = f1.await.unwrap();
            let rec2 = f2.await.unwrap();

            // each release sends PUBREL with its own packet id
            let mut r1 = pin!(rec1.release());
            assert!(is_pending(&mut r1).await);
            let mut r2 = if drop_second {
                drop(rec2);
                None
            } else {
                let mut r2 = Box::pin(rec2.release());
                assert!(is_pending(&mut r2.as_mut()).await);
                Some(r2)
            };
            let buf = client.read().await.unwrap();
            assert_eq!(
                buf,
                Bytes::from_static(b"\x62\x04\x00\x01\x00\x00\x62\x04\x00\x02\x00\x00")
            );

            assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
            assert_eq!(shared.pkt_ack(comp(2)), Ok(()));
            assert_eq!(r1.await, Ok(()));
            if let Some(r2) = r2.take() {
                assert_eq!(r2.await, Ok(()));
            }
            assert!(sink.is_open());
            assert_eq!(shared.credit(), 16);
        }
    }

    #[ntex::test]
    async fn test_rejected_pubrec() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        async fn is_pending<F: Future>(f: &mut std::pin::Pin<&mut F>) -> bool {
            lazy(|cx| f.as_mut().poll(cx).is_pending()).await
        }
        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn rec(packet_id: u16, reason_code: codec::PublishAckReason) -> Ack {
            Ack::Receive(codec::PublishAck {
                packet_id: id(packet_id),
                reason_code,
                ..Default::default()
            })
        }
        fn comp(packet_id: u16) -> Ack {
            Ack::Complete(codec::PublishAck2 {
                packet_id: id(packet_id),
                reason_code: codec::PublishAck2Reason::Success,
                properties: codec::UserProperties::default(),
                reason_string: None,
            })
        }
        const PUBLISH: &[u8] = b"\x34\x06\x00\x01a\x00\x01\x00";

        for drop_rejected in [false, true] {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_cap(1);
            let sink = MqttSink::new(shared.clone());

            // second PUBLISH waits for receive maximum and reuses packet id 1
            let mut f1 = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
            let mut f2 = pin!(
                sink.publish("a")
                    .packet_id(1)
                    .send_exactly_once(Bytes::new())
            );
            assert!(is_pending(&mut f1).await);
            assert!(is_pending(&mut f2).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBLISH)
            );

            // rejected PUBREC ends the flow and releases packet id and quota
            assert_eq!(
                shared.pkt_ack(rec(1, codec::PublishAckReason::NotAuthorized)),
                Ok(())
            );
            let rejected = f1.await.unwrap();
            assert_eq!(
                rejected.packet().reason_code,
                codec::PublishAckReason::NotAuthorized
            );
            assert!(is_pending(&mut f2).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBLISH)
            );
            assert_eq!(
                shared.pkt_ack(rec(1, codec::PublishAckReason::Success)),
                Ok(())
            );
            let received = f2.await.unwrap();

            // rejected publish is not released and does not affect the new one
            if drop_rejected {
                drop(rejected);
            } else {
                assert_eq!(
                    timeout(Millis(1000), rejected.release()).await,
                    Ok(Err(SendPacketError::UnexpectedRelease))
                );
            }
            let mut r = pin!(received.release());
            assert!(is_pending(&mut r).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(b"\x62\x04\x00\x01\x00\x00")
            );
            assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
            assert_eq!(timeout(Millis(1000), r).await, Ok(Ok(())));
            assert!(sink.is_open());
            assert_eq!(shared.credit(), 1);
        }
    }

    #[ntex::test]
    async fn test_rejected_publish_does_not_start_streaming() {
        let (client, server) = IoTest::create();
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::new(),
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared.clone());
        let err = Err(SendPacketError::Encode(
            crate::error::EncodeError::MalformedPacket,
        ));

        let res = sink
            .publish("a/+")
            .stream_at_most_once(10)
            .await
            .map(|_| ());
        assert_eq!(res, err);
        assert!(!shared.is_streaming());

        let (fut, _stream) = sink.publish("a/+").stream_at_least_once(10);
        assert_eq!(fut.await.map(|_| ()), err);
        assert!(!shared.is_streaming());

        // sink is still usable
        assert!(sink.is_open());
        sink.publish("a/b")
            .send_at_most_once(Bytes::from_static(b"data"))
            .await
            .unwrap();
        drop(client);
    }

    /// Payload chunk waiting for write backpressure fails when the connection
    /// is closed, backpressure is never disabled after close
    #[ntex::test]
    async fn test_streaming_waiter_fails_on_close() {
        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        for close in 0..3 {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            let stream = sink.publish("a/b").stream_at_most_once(4).await.unwrap();
            stream.send(Bytes::from_static(b"ab")).await.unwrap();
            shared.enable_wr_backpressure();
            let mut chunk = Box::pin(stream.send(Bytes::from_static(b"cd")));
            assert!(lazy(|cx| chunk.as_mut().poll(cx).is_pending()).await);

            match close {
                0 => shared.close(None),
                1 => sink.force_close(),
                _ => shared.drop_sink(true),
            }
            assert_eq!(
                timeout(Millis(1000), chunk).await.unwrap(),
                Err(SendPacketError::Disconnected)
            );
        }
    }

    /// Publish waiting while the streaming payload is released first by
    /// `disable_wr_backpressure` is sent once the payload is complete
    #[ntex::test]
    async fn test_streaming_completion_releases_waiters() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::new(),
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared.clone());

        let stream = sink.publish("a/b").stream_at_most_once(4).await.unwrap();
        stream.send(Bytes::from_static(b"ab")).await.unwrap();
        shared.enable_wr_backpressure();
        let mut chunk = pin!(stream.send(Bytes::from_static(b"cd")));
        assert!(lazy(|cx| chunk.as_mut().poll(cx).is_pending()).await);
        let mut publish = pin!(sink.publish("c").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);

        // the payload goes first, the publish cannot interleave with it
        shared.disable_wr_backpressure();
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 16);
        chunk.await.unwrap();
        assert!(!shared.is_streaming());

        // the completed payload releases the waiting publish
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 15);
    }

    /// `QoS 0` publish waiting for the streaming payload fails once the
    /// connection is closed
    #[ntex::test]
    async fn test_at_most_once_streaming_close() {
        use std::task::Poll;

        use ntex_util::future::lazy;

        for close in 0..3 {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            let payload = sink.publish("a").stream_at_most_once(4).await.unwrap();
            let mut send = Box::pin(sink.publish("b").send_at_most_once(Bytes::new()));
            assert!(lazy(|cx| send.as_mut().poll(cx).is_pending()).await);

            match close {
                0 => shared.close(None),
                1 => sink.force_close(),
                _ => drop(payload),
            }
            // waiting publish is woken by the close, not by the io shutdown
            assert_eq!(
                lazy(|cx| send.as_mut().poll(cx)).await,
                Poll::Ready(Err(SendPacketError::Disconnected))
            );
        }
    }

    /// `QoS 0` publish waits for write backpressure to be released, and fails
    /// once the connection is closed
    #[ntex::test]
    async fn test_at_most_once_write_backpressure() {
        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        for close in 0..3 {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(0);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            sink.publish("a")
                .send_at_most_once(Bytes::from(vec![0u8; 128 * 1024]))
                .await
                .unwrap();
            assert!(lazy(|cx| io.poll_flush(cx, false).is_pending()).await);
            assert!(io.is_wr_backpressure());

            let mut send = Box::pin(sink.publish("b").send_at_most_once(Bytes::new()));
            let mut stream = Box::pin(sink.publish("c").stream_at_most_once(2));
            assert!(lazy(|cx| send.as_mut().poll(cx).is_pending()).await);
            assert!(lazy(|cx| stream.as_mut().poll(cx).is_pending()).await);

            if close == 0 {
                client.remote_buffer_cap(1024 * 1024);
                let _ = client.read().await;
                timeout(Millis(1000), send).await.unwrap().unwrap();
                let payload = timeout(Millis(1000), stream).await.unwrap().unwrap();
                assert!(shared.is_streaming());
                payload.send(Bytes::from_static(b"ab")).await.unwrap();
                assert!(!shared.is_streaming());
            } else {
                if close == 1 {
                    sink.force_close();
                } else {
                    // the write buffer drains during graceful shutdown
                    shared.close(None);
                    client.remote_buffer_cap(1024 * 1024);
                    let _ = client.read().await;
                }
                assert_eq!(
                    timeout(Millis(1000), send).await.unwrap(),
                    Err(SendPacketError::Disconnected)
                );
                assert!(matches!(
                    timeout(Millis(1000), stream).await.unwrap(),
                    Err(SendPacketError::Disconnected)
                ));
            }
        }
    }

    #[ntex::test]
    async fn test_packets_deferred_while_streaming() {
        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::new(),
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared.clone());

        let stream = sink.publish("a/b").stream_at_most_once(6).await.unwrap();
        stream.send(Bytes::from_static(b"ab")).await.unwrap();

        // client keep-alive and dispatcher responses
        assert!(sink.ping());
        let ack = codec::Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::Success,
            properties: codec::UserProperties::default(),
            reason_string: None,
        });
        io.encode(codec::Encoded::Packet(ack), &shared).unwrap();

        // publish cannot interleave with payload, it waits for the payload
        let mut qos0 = Box::pin(sink.publish("c").send_at_most_once(Bytes::new()));
        assert!(lazy(|cx| qos0.as_mut().poll(cx).is_pending()).await);

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\x30\x0c\x00\x03a/b\x00ab"));

        // incomplete payload, packets are still deferred
        stream.send(Bytes::from_static(b"cd")).await.unwrap();
        assert!(shared.is_streaming());
        assert!(lazy(|cx| qos0.as_mut().poll(cx).is_pending()).await);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"cd"));

        stream.send(Bytes::from_static(b"ef")).await.unwrap();
        assert!(!shared.is_streaming());
        let buf = client.read().await.unwrap();
        assert_eq!(
            buf,
            Bytes::from_static(b"ef\xc0\x00\x40\x04\x00\x01\x00\x00")
        );

        // completed payload releases the waiting publish
        qos0.await.unwrap();
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\x30\x04\x00\x01c\x00"));

        // nothing left behind
        assert!(sink.ping());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\xc0\x00"));
    }

    /// Dropped `QoS 2` publish future is released, PUBREL is sent and
    /// packet id and in-flight slot are freed [MQTT-4.3.3-4]
    #[ntex::test]
    async fn test_dropped_exactly_once() {
        use std::future::Future;

        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn rec(packet_id: u16) -> Ack {
            Ack::Receive(codec::PublishAck {
                packet_id: id(packet_id),
                ..Default::default()
            })
        }
        fn comp(packet_id: u16) -> Ack {
            Ack::Complete(codec::PublishAck2 {
                packet_id: id(packet_id),
                ..Default::default()
            })
        }
        const PUBLISH: &[u8] = b"\x34\x06\x00\x01a\x00\x01\x00";
        const PUBREL: &[u8] = b"\x62\x04\x00\x01\x00\x00";

        // PUBREC received before or after the future is dropped
        for delivered in [false, true] {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_cap(1);
            let sink = MqttSink::new(shared.clone());

            let mut f = Box::pin(sink.publish("a").send_exactly_once(Bytes::new()));
            assert!(lazy(|cx| f.as_mut().poll(cx).is_pending()).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBLISH)
            );
            if delivered {
                assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
                drop(f);
            } else {
                drop(f);
                assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
            }
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBREL)
            );
            assert_eq!(shared.credit(), 0);
            assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
            assert_eq!(shared.credit(), 1);

            // packet id and in-flight slot are available
            let f = sink
                .publish("a")
                .packet_id(1)
                .send_exactly_once(Bytes::new());
            let mut f = Box::pin(f);
            assert!(lazy(|cx| f.as_mut().poll(cx).is_pending()).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBLISH)
            );
            assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
            let received = timeout(Millis(1000), f).await.unwrap().unwrap();
            let mut r = Box::pin(received.release());
            assert!(lazy(|cx| r.as_mut().poll(cx).is_pending()).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBREL)
            );
            assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
            assert_eq!(timeout(Millis(1000), r).await, Ok(Ok(())));
            assert_eq!(shared.credit(), 1);
        }
    }

    /// Receive Maximum counts `QoS 1` and `QoS 2` PUBLISH packets only [MQTT-4.9.0-2],
    /// SUBSCRIBE and UNSUBSCRIBE wait for write backpressure only
    #[ntex::test]
    async fn test_receive_max_subscribe() {
        use std::{future::Future, pin::Pin};

        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        async fn is_pending<F: Future>(f: &mut Pin<Box<F>>) -> bool {
            lazy(|cx| f.as_mut().poll(cx).is_pending()).await
        }
        async fn read(client: &IoTest) -> u8 {
            timeout(Millis(1000), client.read()).await.unwrap().unwrap()[0]
        }
        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn suback(packet_id: u16) -> Ack {
            Ack::Subscribe(codec::SubscribeAck {
                packet_id: id(packet_id),
                properties: codec::UserProperties::default(),
                reason_string: None,
                status: vec![codec::SubscribeAckReason::GrantedQos0],
            })
        }
        fn unsuback(packet_id: u16) -> Ack {
            Ack::Unsubscribe(codec::UnsubscribeAck {
                packet_id: id(packet_id),
                properties: codec::UserProperties::default(),
                reason_string: None,
                status: vec![codec::UnsubscribeAckReason::Success],
            })
        }
        fn puback(packet_id: u16) -> Ack {
            Ack::Publish(codec::PublishAck {
                packet_id: id(packet_id),
                ..Default::default()
            })
        }
        const SUBSCRIBE: u8 = 0x82;
        const UNSUBSCRIBE: u8 = 0xa2;
        const PUBLISH: u8 = 0x32;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        // write backpressure at two SUBSCRIBE packets
        let io = Io::new(
            server,
            SharedCfg::new("test").add(ntex_io::IoConfig::default().set_write_buf(16)),
        );
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::new(),
            Rc::default(),
        ));
        shared.set_client();
        shared.set_cap(1);
        let sink = MqttSink::new(shared.clone());
        let sub = |packet_id| {
            Box::pin(
                sink.subscribe(None)
                    .packet_id(packet_id)
                    .topic_filter("a".into(), codec::SubscriptionOptions::default())
                    .send(),
            )
        };
        let unsub = |packet_id| {
            Box::pin(
                sink.unsubscribe()
                    .packet_id(packet_id)
                    .topic_filter("a".into())
                    .send(),
            )
        };
        let publish = |packet_id| {
            Box::pin(
                sink.publish("a")
                    .packet_id(packet_id)
                    .send_at_least_once(Bytes::new()),
            )
        };

        // pending SUBSCRIBE and UNSUBSCRIBE do not use send credit
        let mut s1 = sub(1);
        assert!(is_pending(&mut s1).await);
        assert_eq!(read(&client).await, SUBSCRIBE);
        let mut u2 = unsub(2);
        assert!(is_pending(&mut u2).await);
        assert_eq!(read(&client).await, UNSUBSCRIBE);
        assert_eq!(sink.credit(), 1);
        assert!(sink.is_ready());

        let mut p3 = publish(3);
        assert!(is_pending(&mut p3).await);
        assert_eq!(read(&client).await, PUBLISH);
        assert_eq!(sink.credit(), 0);
        assert!(!sink.is_ready());

        // SUBSCRIBE and UNSUBSCRIBE do not wait for send credit
        let mut s4 = sub(4);
        assert!(is_pending(&mut s4).await);
        assert_eq!(read(&client).await, SUBSCRIBE);
        let mut u9 = unsub(9);
        assert!(is_pending(&mut u9).await);
        assert_eq!(read(&client).await, UNSUBSCRIBE);

        assert_eq!(shared.pkt_ack(suback(1)), Ok(()));
        assert!(s1.await.is_ok());
        assert_eq!(shared.pkt_ack(unsuback(2)), Ok(()));
        assert!(u2.await.is_ok());
        assert_eq!(sink.credit(), 0);
        assert_eq!(shared.pkt_ack(puback(3)), Ok(()));
        assert!(p3.await.is_ok());
        assert_eq!(sink.credit(), 1);

        assert_eq!(shared.pkt_ack(suback(4)), Ok(()));
        assert!(s4.await.is_ok());
        assert_eq!(shared.pkt_ack(unsuback(9)), Ok(()));
        assert!(u9.await.is_ok());

        // SUBSCRIBE and UNSUBSCRIBE wait for io write backpressure
        client.remote_buffer_cap(0);
        let mut s5 = sub(5);
        let mut s6 = sub(6);
        assert!(is_pending(&mut s5).await);
        assert!(is_pending(&mut s6).await);
        assert!(io.is_wr_backpressure());
        let mut s7 = sub(7);
        let mut u8 = unsub(8);
        assert!(is_pending(&mut s7).await);
        assert!(is_pending(&mut u8).await);
        // nothing is encoded while waiting
        drop(s7);
        client.remote_buffer_cap(1024);
        let mut buf = Vec::new();
        while buf.len() < 18 {
            buf.extend_from_slice(&timeout(Millis(1000), client.read()).await.unwrap().unwrap());
        }
        assert_eq!(buf.len(), 18);
        // the io write task releases waiters, the flag is cleared by the dispatcher
        assert!(is_pending(&mut u8).await);
        assert_eq!(read(&client).await, UNSUBSCRIBE);
        assert_eq!(client.read_any(), Bytes::new());

        assert_eq!(shared.pkt_ack(suback(5)), Ok(()));
        assert!(s5.await.is_ok());
        assert_eq!(shared.pkt_ack(suback(6)), Ok(()));
        assert!(s6.await.is_ok());
        assert_eq!(shared.pkt_ack(unsuback(8)), Ok(()));
        assert!(u8.await.is_ok());
        assert_eq!(sink.credit(), 1);

        // SUBSCRIBE waiting for write backpressure fails on disconnect
        client.remote_buffer_cap(0);
        let mut s10 = sub(10);
        let mut s11 = sub(11);
        let mut s12 = sub(12);
        assert!(is_pending(&mut s10).await);
        assert!(is_pending(&mut s11).await);
        assert!(io.is_wr_backpressure());
        assert!(is_pending(&mut s12).await);
        shared.close(None);
        client.remote_buffer_cap(1024);
        for f in [s10, s11, s12] {
            assert_eq!(
                timeout(Millis(1000), f).await.unwrap(),
                Err(SendPacketError::Disconnected)
            );
        }
        assert_eq!(sink.credit(), 1);
    }

    mod receive_max {
        use std::{future::Future, pin::Pin};

        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, sleep, timeout};

        use super::*;

        fn setup(cap: usize) -> (IoTest, Io, Rc<MqttShared>, MqttSink) {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::new(),
                Rc::default(),
            ));
            shared.set_cap(cap);
            (client, io, shared.clone(), MqttSink::new(shared))
        }

        fn send(
            sink: &MqttSink,
            id: u16,
        ) -> Pin<Box<impl Future<Output = Result<codec::PublishAck, SendPacketError>>>> {
            Box::pin(
                sink.publish("a")
                    .packet_id(id)
                    .send_at_least_once(Bytes::new()),
            )
        }

        fn ack(id: u16) -> Ack {
            Ack::Publish(codec::PublishAck {
                packet_id: NonZeroU16::new(id).unwrap(),
                ..Default::default()
            })
        }

        /// Encoded `QoS 1` PUBLISH packets
        fn publishes(ids: &[u8]) -> Bytes {
            let mut buf = Vec::new();
            for id in ids {
                buf.extend_from_slice(&[0x32, 0x06, 0x00, 0x01, b'a', 0x00, *id, 0x00]);
            }
            Bytes::from(buf)
        }

        async fn is_pending<F: Future>(f: &mut Pin<Box<F>>) -> bool {
            lazy(|cx| f.as_mut().poll(cx).is_pending()).await
        }

        /// Read packets written to the peer
        async fn written(client: &IoTest) -> Bytes {
            sleep(Millis(25)).await;
            client.read_any()
        }

        /// The send credit released by an ack is reserved for the first waiter,
        /// the peer never receives more than Receive Maximum publishes [MQTT-4.9.0-1]
        #[ntex::test]
        async fn test_granted_waiter_keeps_credit() {
            let (client, _io, shared, sink) = setup(1);

            let mut a = send(&sink, 1);
            let mut b = send(&sink, 2);
            assert!(is_pending(&mut a).await);
            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, publishes(&[1]));

            // the waiter is woken but is not polled yet
            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(!sink.is_ready());
            assert_eq!(sink.credit(), 0);
            let mut c = send(&sink, 3);
            assert!(is_pending(&mut c).await);
            assert_eq!(written(&client).await, Bytes::new());

            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, publishes(&[2]));
            assert_eq!(
                timeout(Millis(1000), a)
                    .await
                    .unwrap()
                    .unwrap()
                    .packet_id
                    .get(),
                1
            );

            assert_eq!(shared.pkt_ack(ack(2)), Ok(()));
            assert!(is_pending(&mut c).await);
            assert_eq!(written(&client).await, publishes(&[3]));
            assert_eq!(shared.pkt_ack(ack(3)), Ok(()));
            assert!(b.await.is_ok());
            assert!(c.await.is_ok());
            assert_eq!(sink.credit(), 1);
        }

        /// Cancelled granted waiter passes the send credit to the next waiter
        #[ntex::test]
        async fn test_cancelled_waiter_passes_credit() {
            let (client, _io, shared, sink) = setup(1);

            let mut a = send(&sink, 1);
            let mut b = send(&sink, 2);
            let mut c = send(&sink, 3);
            let mut d = send(&sink, 4);
            for f in [&mut a, &mut b, &mut c, &mut d] {
                assert!(is_pending(f).await);
            }
            assert_eq!(written(&client).await, publishes(&[1]));

            // waiting entry is removed
            drop(c);
            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(a.await.is_ok());
            assert_eq!(sink.credit(), 0);

            // granted entry passes the credit
            drop(b);
            assert_eq!(sink.credit(), 0);
            let mut e = send(&sink, 5);
            assert!(is_pending(&mut e).await);
            assert!(is_pending(&mut d).await);
            assert_eq!(written(&client).await, publishes(&[4]));

            assert_eq!(shared.pkt_ack(ack(4)), Ok(()));
            assert!(d.await.is_ok());
            assert!(is_pending(&mut e).await);
            assert_eq!(written(&client).await, publishes(&[5]));
            assert_eq!(shared.pkt_ack(ack(5)), Ok(()));
            assert!(e.await.is_ok());
            assert_eq!(sink.credit(), 1);
        }

        /// `ready()` waits in order with publishes and passes the credit on
        #[ntex::test]
        async fn test_ready_waits_in_order() {
            let (client, _io, shared, sink) = setup(1);

            let mut a = send(&sink, 1);
            assert!(is_pending(&mut a).await);
            assert_eq!(written(&client).await, publishes(&[1]));

            let mut r = Box::pin(sink.ready());
            let mut b = send(&sink, 2);
            assert!(is_pending(&mut r).await);
            assert!(is_pending(&mut b).await);

            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(a.await.is_ok());
            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, Bytes::new());

            assert!(r.await);
            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, publishes(&[2]));
            assert_eq!(shared.pkt_ack(ack(2)), Ok(()));
            assert!(b.await.is_ok());
            assert!(sink.ready().await);
            assert_eq!(sink.credit(), 1);
        }

        /// Increased Receive Maximum grants the credit to the first waiters
        #[ntex::test]
        async fn test_set_cap_grants_in_order() {
            let (client, _io, shared, sink) = setup(1);

            let mut a = send(&sink, 1);
            let mut b = send(&sink, 2);
            let mut c = send(&sink, 3);
            let mut d = send(&sink, 4);
            for f in [&mut a, &mut b, &mut c, &mut d] {
                assert!(is_pending(f).await);
            }
            assert_eq!(written(&client).await, publishes(&[1]));

            // a publish is in flight
            shared.set_cap(3);
            assert_eq!(sink.credit(), 0);
            assert!(is_pending(&mut d).await);
            assert_eq!(written(&client).await, Bytes::new());

            // granted entries are skipped
            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(is_pending(&mut d).await);
            assert!(is_pending(&mut c).await);
            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, publishes(&[4, 3, 2]));
            assert_eq!(sink.credit(), 0);

            assert_eq!(shared.pkt_ack(ack(4)), Ok(()));
            assert_eq!(shared.pkt_ack(ack(3)), Ok(()));
            assert_eq!(shared.pkt_ack(ack(2)), Ok(()));
            for f in [a, b, c, d] {
                assert!(f.await.is_ok());
            }
            assert_eq!(sink.credit(), 3);
        }

        /// `QoS 2` publish keeps the send credit until PUBCOMP
        #[ntex::test]
        async fn test_exactly_once_keeps_credit() {
            let (client, _io, shared, sink) = setup(1);

            let mut a = Box::pin(
                sink.publish("a")
                    .packet_id(1)
                    .send_exactly_once(Bytes::new()),
            );
            let mut b = send(&sink, 2);
            assert!(is_pending(&mut a).await);
            assert!(is_pending(&mut b).await);
            assert_eq!(
                written(&client).await,
                Bytes::from_static(&[0x34, 0x06, 0x00, 0x01, b'a', 0x00, 0x01, 0x00])
            );

            assert_eq!(
                shared.pkt_ack(Ack::Receive(codec::PublishAck::default())),
                Ok(())
            );
            let rec = timeout(Millis(1000), a).await.unwrap().unwrap();
            let mut rel = Box::pin(rec.release());
            assert!(is_pending(&mut rel).await);
            assert!(is_pending(&mut b).await);
            assert_eq!(
                written(&client).await,
                Bytes::from_static(&[0x62, 0x04, 0x00, 0x01, 0x00, 0x00])
            );

            assert_eq!(
                shared.pkt_ack(Ack::Complete(codec::PublishAck2::default())),
                Ok(())
            );
            assert!(rel.await.is_ok());
            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, publishes(&[2]));
            assert_eq!(shared.pkt_ack(ack(2)), Ok(()));
            assert!(b.await.is_ok());
            assert_eq!(sink.credit(), 1);
        }

        /// Streaming publish gets the send credit in the order of the calls
        #[ntex::test]
        async fn test_stream_waits_in_call_order() {
            let (client, _io, shared, sink) = setup(1);

            let mut a = send(&sink, 1);
            assert!(is_pending(&mut a).await);
            assert_eq!(written(&client).await, publishes(&[1]));

            let (s, stream) = sink.publish("a").packet_id(2).stream_at_least_once(1);
            let mut s = Box::pin(s);
            let mut b = send(&sink, 3);
            assert!(is_pending(&mut b).await);

            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(a.await.is_ok());
            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, Bytes::new());

            assert!(is_pending(&mut s).await);
            assert_eq!(
                written(&client).await,
                Bytes::from_static(&[0x32, 0x07, 0x00, 0x01, b'a', 0x00, 0x02, 0x00])
            );
            assert!(stream.send(Bytes::from_static(b"x")).await.is_ok());
            assert_eq!(written(&client).await, Bytes::from_static(b"x"));

            assert_eq!(shared.pkt_ack(ack(2)), Ok(()));
            assert!(s.await.is_ok());
            assert!(is_pending(&mut b).await);
            assert_eq!(written(&client).await, publishes(&[3]));
            assert_eq!(shared.pkt_ack(ack(3)), Ok(()));
            assert!(b.await.is_ok());
        }

        /// Cleared queues fail waiting publishes and release the granted credit
        #[ntex::test]
        async fn test_drop_sink_fails_waiters() {
            let (client, _io, shared, sink) = setup(1);

            let mut a = send(&sink, 1);
            let mut b = send(&sink, 2);
            let mut c = send(&sink, 3);
            for f in [&mut a, &mut b, &mut c] {
                assert!(is_pending(f).await);
            }
            assert_eq!(written(&client).await, publishes(&[1]));
            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(a.await.is_ok());

            // `b` is granted, `c` is waiting
            shared.drop_sink(false);
            assert_eq!(sink.credit(), 1);
            assert!(sink.is_ready());

            // closed entries are skipped
            let mut d = send(&sink, 4);
            let mut e = send(&sink, 5);
            assert!(is_pending(&mut d).await);
            assert!(is_pending(&mut e).await);
            assert_eq!(written(&client).await, publishes(&[4]));
            assert_eq!(shared.pkt_ack(ack(4)), Ok(()));
            assert!(d.await.is_ok());
            assert!(is_pending(&mut e).await);
            assert_eq!(written(&client).await, publishes(&[5]));

            for f in [b, c] {
                assert_eq!(
                    timeout(Millis(1000), f).await.unwrap(),
                    Err(SendPacketError::Disconnected)
                );
            }
            assert_eq!(shared.pkt_ack(ack(5)), Ok(()));
            assert!(e.await.is_ok());
            assert_eq!(sink.credit(), 1);
        }
    }
}
