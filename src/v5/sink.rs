use std::num::{NonZeroU16, NonZeroU32};
use std::{cell::Cell, fmt, future::Future, future::ready, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::{channel::pool, future::Either};

use super::codec::{self, EncodeLtd};
use super::shared::{Ack, AckType, MqttShared};
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
    /// Result indicates if connection is alive
    pub fn ready(&self) -> impl Future<Output = bool> {
        if self.0.is_active() {
            self.0.wait_readiness().map_or_else(
                || Either::Left(ready(true)),
                |rx| Either::Right(async move { rx.await.is_ok() }),
            )
        } else {
            Either::Left(ready(false))
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

    #[inline]
    /// Send publish packet with `QoS 0`
    pub fn send_at_most_once(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if self.shared.is_active() {
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
    pub fn stream_at_most_once(mut self, size: u32) -> Result<StreamingPayload, SendPacketError> {
        if self.shared.is_active() {
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
            if let Some(rx) = self.shared.wait_readiness() {
                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
                self.send_at_least_once_inner(payload).await
            } else {
                self.send_at_least_once_inner(payload).await
            }
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
            let fut = if let Some(rx) = self.shared.wait_readiness() {
                Either::Left(Either::Left(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    self.stream_at_least_once_inner(tx, None).await
                }))
            } else {
                Either::Left(Either::Right(self.stream_at_least_once_inner(tx, None)))
            };
            (fut, stream)
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
    ) -> Result<codec::PublishAck, SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", self.packet);
        self.shared
            .wait_publish_response(idx, AckType::Publish, self.packet, Some(payload))?
            .await
            .map(Ack::publish)
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
            let rx = self
                .shared
                .wait_publish_response(idx, AckType::Publish, self.packet, chunk);
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
            if let Some(rx) = self.shared.wait_readiness() {
                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
                self.send_exactly_once_inner(payload).await
            } else {
                self.send_exactly_once_inner(payload).await
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    async fn send_exactly_once_inner(
        mut self,
        payload: Bytes,
    ) -> Result<PublishReceived, SendPacketError> {
        let shared = self.shared.clone();
        let idx = shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS2) to {:#?}", self.packet);

        let rx = shared.wait_publish_response(idx, AckType::Receive, self.packet, Some(payload))?;
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
    pub async fn send(self) -> Result<codec::SubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

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
    pub async fn send(self) -> Result<codec::UnsubscribeAck, SendPacketError> {
        let shared = self.shared;
        let mut packet = self.packet;

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

        let res = sink.publish("a/+").stream_at_most_once(10).map(|_| ());
        assert_eq!(res, err);
        assert!(!shared.is_streaming());

        let (fut, _stream) = sink.publish("a/+").stream_at_least_once(10);
        assert_eq!(fut.await.map(|_| ()), err);
        assert!(!shared.is_streaming());

        // sink is still usable
        assert!(sink.is_open());
        sink.publish("a/b")
            .send_at_most_once(Bytes::from_static(b"data"))
            .unwrap();
        drop(client);
    }

    #[ntex::test]
    async fn test_packets_deferred_while_streaming() {
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

        let stream = sink.publish("a/b").stream_at_most_once(6).unwrap();
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

        // publish cannot interleave with payload
        assert_eq!(
            sink.publish("c").send_at_most_once(Bytes::new()),
            Err(SendPacketError::Encode(EncodeError::ExpectPayload))
        );

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\x30\x0c\x00\x03a/b\x00ab"));

        // incomplete payload, packets are still deferred
        stream.send(Bytes::from_static(b"cd")).await.unwrap();
        assert!(shared.is_streaming());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"cd"));

        stream.send(Bytes::from_static(b"ef")).await.unwrap();
        assert!(!shared.is_streaming());
        let buf = client.read().await.unwrap();
        assert_eq!(
            buf,
            Bytes::from_static(b"ef\xc0\x00\x40\x04\x00\x01\x00\x00")
        );

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
}
