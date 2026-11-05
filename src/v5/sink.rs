use std::num::{NonZeroU16, NonZeroU32};
use std::{cell::Cell, fmt, future::Future, future::ready, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::{channel::pool, future::Either};

use super::codec::{self, EncodeLtd};
use super::shared::{Ack, AckType, MqttShared, SendPermit};
use crate::{error::EncodeError, error::SendPacketError, types::QoS};

/// Cloneable handle for sending MQTT packets on a client or server connection.
///
/// Clones share send credit, acknowledgement tracking, and connection shutdown.
pub struct MqttSink(Rc<MqttShared>);

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone())
    }
}

impl MqttSink {
    /// Creates a sink sharing the connection state and its outgoing packet queues.
    pub(super) fn new(state: Rc<MqttShared>) -> Self {
        MqttSink(state)
    }

    /// Clones the connection state for internal packet dispatch and flow control.
    pub(super) fn shared(&self) -> Rc<MqttShared> {
        self.0.clone()
    }

    /// Returns whether the connection is active.
    ///
    /// Returns `false` as soon as the connection starts closing, either
    /// locally or because the peer has gone. Buffered data may still be flushing.
    #[inline]
    pub fn is_open(&self) -> bool {
        self.0.is_active()
    }

    /// Returns whether the peer has sent a `DISCONNECT` packet.
    #[inline]
    pub fn is_disconnect_recv(&self) -> bool {
        self.0.is_disconnect_recv()
    }

    /// Returns whether an acknowledged publish can be sent without waiting.
    ///
    /// Requires an active connection, available send credit, and no write
    /// backpressure. Waiting publishes have priority over new sends.
    /// The dispatcher clears the backpressure flag, so it may remain set briefly
    /// after the buffer drains. This check does not account for an active streaming
    /// payload; a new `QoS 1` or `QoS 2` publish can still fail with
    /// [`EncodeError::ExpectPayload`].
    #[inline]
    pub fn is_ready(&self) -> bool {
        if self.0.is_active() {
            self.0.is_ready()
        } else {
            false
        }
    }

    /// Returns the number of available acknowledged-publish send slots.
    ///
    /// The peer's receive maximum limits `QoS 1` and `QoS 2` publishes.
    /// Slots reserved for waiting sends are excluded; `QoS 0`, subscriptions,
    /// and unsubscriptions do not consume this credit.
    #[inline]
    pub fn credit(&self) -> usize {
        self.0.credit()
    }

    /// Waits for send credit and writable connection state.
    ///
    /// Returns `false` when closure is observed, otherwise `true` when the
    /// readiness check succeeds. A waiting call queues immediately with publishes
    /// and temporarily reserves credit. Completion releases that reservation to
    /// the next waiter; the caller does not retain permission to send.
    /// An immediately ready result reflects state when this method is called,
    /// not when its future is later polled. This notification does not ensure
    /// that an active streaming payload has finished.
    pub fn ready(&self) -> impl Future<Output = bool> {
        if !self.0.is_active() {
            Either::Left(ready(false))
        } else if self.0.is_ready() {
            Either::Left(ready(true))
        } else {
            let permit = self.0.wait_send_ready();
            Either::Right(async move { permit.await.is_ok() })
        }
    }

    /// Aborts the MQTT connection immediately.
    ///
    /// Pending acknowledgement waits are cancelled and buffered data is discarded.
    /// Use [`close`](Self::close) to initiate graceful shutdown instead.
    #[inline]
    pub fn force_close(&self) {
        self.0.force_close();
    }

    /// Initiates graceful MQTT connection shutdown.
    ///
    /// Attempts to queue a default `DISCONNECT` packet if the connection is
    /// active and none has been sent, then closes the transport. Buffered data may
    /// still flush, but pending acknowledgement waits are cancelled with
    /// [`SendPacketError::Disconnected`].
    #[inline]
    pub fn close(&self) {
        self.0.close(Some(codec::Disconnect::default()));
    }

    /// Initiates graceful shutdown with the supplied `DISCONNECT` packet.
    ///
    /// Attempts to encode the packet only while active and before any earlier
    /// `DISCONNECT`. Cancels pending acknowledgement waits immediately; already
    /// buffered data may still flush.
    #[inline]
    pub fn close_with_reason(&self, pkt: codec::Disconnect) {
        self.0.close(Some(pkt));
    }

    /// Initiates graceful shutdown without sending `DISCONNECT`.
    ///
    /// Cancels pending acknowledgement waits immediately; already buffered data
    /// may still flush.
    #[inline]
    pub fn close_with_no_reason(&self) {
        self.0.close(None);
    }

    /// Queues `PINGREQ` and marks a response pending only if encoding succeeds.
    pub(super) fn ping(&self) -> bool {
        if self.0.encode_packet(codec::Packet::PingRequest).is_ok() {
            self.0.set_ping_pending(true);
            true
        } else {
            false
        }
    }

    /// Returns whether an encoded `PINGREQ` is still awaiting `PINGRESP`.
    pub(super) fn is_ping_pending(&self) -> bool {
        self.0.is_ping_pending()
    }

    /// Waits for any active streaming payload and write backpressure to drain.
    ///
    /// Returns `false` if the connection closes; this does not wait for peer
    /// acknowledgements.
    pub(super) async fn wait_write_unblocked(&self) -> bool {
        self.0.wait_write_unblocked().await
    }

    /// Creates a publish builder for a topic with `QoS 0`, no payload, and cleared flags.
    #[inline]
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

    /// Creates a builder from the supplied publish packet.
    ///
    /// The selected send method replaces its `QoS` and payload size. A supplied
    /// packet identifier is preserved for `QoS 1` and `QoS 2` sends.
    #[inline]
    pub fn publish_pkt(&self, packet: codec::Publish) -> PublishBuilder {
        PublishBuilder::new(self.0.clone(), packet)
    }

    /// Sets the callback for nonblocking `QoS 1` publish acknowledgements.
    ///
    /// Used by [`PublishBuilder::send_at_least_once_no_block`], not by awaited sends.
    /// The first argument is the received `PublishAck`; on disconnect it is a
    /// synthetic packet with the identifier and default fields. The second argument
    /// is `true` for cancellation on disconnect, not for a negative acknowledgement.
    /// Inspect the acknowledgement reason code when it is `false`.
    ///
    /// The callback runs synchronously while the sink queues are mutably borrowed.
    /// Do not call back into the sink: queue access panics and replacing the callback
    /// may be lost. Hand off work through a channel or spawned task instead.
    /// Acknowledgements are reported in arrival order; disconnection reports
    /// in-flight nonblocking publishes in unspecified order.
    pub fn publish_ack_cb<F>(&self, f: F)
    where
        F: Fn(codec::PublishAck, bool) + 'static,
    {
        self.0.set_publish_ack(Box::new(f));
    }

    /// Creates a subscribe packet builder with an optional subscription identifier.
    ///
    /// Only a client sink sends the packet; on a server sink
    /// [`SubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
    #[inline]
    #[allow(clippy::missing_panics_doc)]
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

    /// Creates an unsubscribe packet builder.
    ///
    /// Only a client sink sends the packet; on a server sink
    /// [`UnsubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
    #[inline]
    #[allow(clippy::missing_panics_doc)]
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

/// Builder for one publish packet, with payload and `QoS` chosen by its send method.
pub struct PublishBuilder {
    /// Connection state retained for encoding, send credit, and acknowledgement tracking.
    shared: Rc<MqttShared>,
    /// Publish header retained until the selected send method encodes it.
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
    /// Creates a publish builder retaining the supplied packet and shared connection state.
    fn new(shared: Rc<MqttShared>, packet: codec::Publish) -> Self {
        Self { shared, packet }
    }

    /// Sets the packet identifier for an acknowledged publish.
    ///
    /// If unset, an identifier is generated when sending. Use either automatic
    /// or manual identifier allocation consistently to avoid collisions; an
    /// in-flight collision returns [`SendPacketError::PacketIdInUse`].
    ///
    /// # Panics
    ///
    /// Panics if `id` is zero.
    #[inline]
    #[must_use]
    pub fn packet_id(mut self, id: u16) -> Self {
        let id = NonZeroU16::new(id).expect("id 0 is not allowed");
        self.packet.packet_id = Some(id);
        self
    }

    /// Sets whether this publish is a retransmission of an earlier packet.
    #[inline]
    #[must_use]
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    /// Sets whether the peer should retain this publish.
    #[inline]
    #[must_use]
    pub fn retain(mut self, val: bool) -> Self {
        self.packet.retain = val;
        self
    }

    /// Updates publish properties and returns the builder.
    #[inline]
    #[must_use]
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
        self
    }

    /// Updates publish properties in place.
    #[inline]
    pub fn set_properties<F>(&mut self, f: F)
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
    }

    /// Returns the publish body size in bytes, excluding the fixed header.
    ///
    /// Uses the packet's current `QoS` and stored payload size, then adds
    /// `payload_size`. Builders from [`MqttSink::publish`] start with `QoS 0`
    /// and a zero stored payload size; add two bytes for an acknowledged send.
    /// Builders from [`MqttSink::publish_pkt`] retain the supplied packet values.
    ///
    /// # Panics
    ///
    /// Panics in builds with overflow checks if adding `payload_size` overflows `usize`.
    #[inline]
    pub fn size(&self, payload_size: usize) -> u32 {
        (self.packet.encoded_size(u32::MAX) + payload_size) as u32
    }

    /// Queues a complete publish with `QoS 0`, without waiting for acknowledgement.
    ///
    /// Waits for an outgoing streaming payload to finish and for the write buffer
    /// to drain to the backpressure release threshold. Does not consume send credit.
    /// Returns [`SendPacketError::Disconnected`] if the connection closes or the
    /// write-readiness wait times out. Success means the packet is encoded, not
    /// that the peer has received it.
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

    /// Starts a streaming publish with `QoS 0`.
    ///
    /// `size` is the total payload size in bytes. Send exactly that many bytes
    /// through the returned [`StreamingPayload`]; no chunks are needed for zero.
    /// Dropping the sender with an incomplete payload terminates the connection.
    ///
    /// Waits for any outgoing streaming payload to finish and for the write buffer
    /// to drain to the backpressure release threshold, without consuming send credit.
    /// Returns [`SendPacketError::Disconnected`] if the connection closes or the
    /// write-readiness wait times out.
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

    /// Sends a complete `QoS 1` publish and waits for `PublishAck`.
    ///
    /// Reserves send credit in waiter order and holds it while waiting for write
    /// capacity. An already active streaming payload can cause
    /// [`EncodeError::ExpectPayload`].
    /// Returns the acknowledgement even if its reason code rejects the publish;
    /// inspect the reason code to determine acceptance.
    ///
    /// The acknowledgement is read by the connection dispatcher. Awaiting it in a
    /// handler of the same connection can stall while reading is paused; see
    /// [read pause](crate::MqttServiceConfig#read-pause).
    pub async fn send_at_least_once(
        mut self,
        payload: Bytes,
    ) -> Result<codec::PublishAck, SendPacketError> {
        if self.shared.is_active() {
            self.packet.qos = QoS::AtLeastOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            let permit = self.shared.wait_send_ready().await?;
            self.send_at_least_once_inner(payload, permit).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Queues a complete `QoS 1` publish without waiting for acknowledgement.
    ///
    /// Install [`MqttSink::publish_ack_cb`] first to receive acknowledgement and
    /// disconnection notifications. Success means the packet was encoded. An active
    /// streaming payload returns [`EncodeError::ExpectPayload`]; a closed connection
    /// returns [`SendPacketError::Disconnected`].
    ///
    /// # Panics
    ///
    /// Panics if the active sink is not ready. Without an installed callback, the
    /// connection dispatcher panics later when the acknowledgement arrives.
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

    /// Creates a streaming `QoS 1` send future and its payload sender.
    ///
    /// `size` is the total payload size in bytes. Poll the returned future
    /// concurrently with [`StreamingPayload::send`]: it waits for readiness, encodes
    /// the header, then waits for `PublishAck`. Send exactly `size` payload bytes
    /// in sequential chunks; no chunks are needed for zero.
    /// This call immediately enters the send-credit queue, even before the future
    /// is polled.
    /// Dropping the sender before the header is encoded cancels the send. Once
    /// payload sending has begun, dropping it with remaining bytes terminates the
    /// connection. An already active stream can cause [`EncodeError::ExpectPayload`].
    ///
    /// The acknowledgement is read by the connection dispatcher. Awaiting it in a
    /// handler of the same connection can stall while reading is paused; see
    /// [read pause](crate::MqttServiceConfig#read-pause).
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
            let permit = self.shared.wait_send_ready();
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

    /// Assigns an identifier, encodes a `QoS 1` publish, and awaits its acknowledgement.
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

    /// Encodes a streaming `QoS 1` header, signals payload readiness, and awaits acknowledgement.
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

    /// Sends a complete `QoS 2` publish and waits for `PublishReceived`.
    ///
    /// Reserves send credit in waiter order and holds it while waiting for write
    /// capacity. An already active streaming payload can cause
    /// [`EncodeError::ExpectPayload`].
    /// The returned handle controls release; send credit stays in use until
    /// `PublishComplete` or a rejecting `PublishReceived`.
    /// If the send future is dropped after encoding, the publish is automatically
    /// released once an accepted `PublishReceived` arrives.
    ///
    /// The acknowledgement is read by the connection dispatcher. Awaiting it in a
    /// handler of the same connection can stall while reading is paused; see
    /// [read pause](crate::MqttServiceConfig#read-pause).
    pub async fn send_exactly_once(
        mut self,
        payload: Bytes,
    ) -> Result<PublishReceived, SendPacketError> {
        if self.shared.is_active() {
            self.packet.qos = codec::QoS::ExactlyOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            let permit = self.shared.wait_send_ready().await?;
            self.send_exactly_once_inner(payload, permit).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Registers a `QoS 2` publish and guards its release while awaiting `PublishReceived`.
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

/// Handle for a `QoS 2` publish acknowledged with `PublishReceived`.
///
/// Call [`release`](Self::release) to send `PublishRelease` packet and wait for
/// `PublishComplete`. If the value is dropped, `PublishRelease` is sent without
/// waiting for `PublishComplete`.
///
/// If `PublishReceived` packet has a reason code of 0x80 or greater, the publish
/// is rejected and `PublishRelease` is never sent, check
/// [`packet`](Self::packet) for the reason code.
pub struct PublishReceived {
    /// Received `PublishReceived` packet, including the acceptance or rejection reason.
    ack: codec::PublishAck,
    /// Pending `PublishRelease` packet; taken on explicit release or drop to prevent duplicate release.
    result: Option<codec::PublishAck2>,
    /// Connection state retained until release or automatic release on drop.
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
    /// Stores `PublishReceived` and prepares a default release packet with the same identifier.
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

    /// Returns the received `PublishReceived` acknowledgement, including its reason code.
    pub fn packet(&self) -> &codec::PublishAck {
        &self.ack
    }

    /// Updates user properties on the outgoing `PublishRelease` packet.
    #[inline]
    #[must_use]
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.as_mut().unwrap().properties);
        self
    }

    /// Sets the reason string on the outgoing `PublishRelease` packet.
    #[inline]
    #[must_use]
    pub fn reason(mut self, reason: ByteString) -> Self {
        self.result.as_mut().unwrap().reason_string = Some(reason);
        self
    }

    /// Queues `PublishRelease` and waits for `PublishComplete`.
    ///
    /// Returns [`SendPacketError::UnexpectedRelease`] if `PublishReceived` rejected
    /// the publish with a reason code of `0x80` or greater, or if the release is
    /// no longer registered.
    /// Completion releases the in-flight credit. Dropping the future after encoding
    /// does not retract the release packet.
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
    /// Checks whether the acknowledgement rejects the publish and forbids `PublishRelease`.
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

/// Builder for a client subscription request and its acknowledgement wait.
pub struct SubscribeBuilder {
    /// Optional caller-selected identifier; otherwise allocated when sending.
    id: Option<NonZeroU16>,
    /// Subscription options, properties, and topic filters accumulated by this builder.
    packet: codec::Subscribe,
    /// Connection state retained for client-role checks, encoding, and acknowledgement tracking.
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
    /// Sets the subscription packet identifier instead of generating one when sending.
    ///
    /// # Panics
    ///
    /// Panics if `id` is zero.
    #[inline]
    #[must_use]
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    /// Appends a topic filter and its subscription options.
    #[inline]
    #[must_use]
    pub fn topic_filter(mut self, filter: ByteString, opts: codec::SubscriptionOptions) -> Self {
        self.packet.topic_filters.push((filter, opts));
        self
    }

    /// Appends a user-property key/value pair to the subscription packet.
    #[inline]
    #[must_use]
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    /// Returns the subscription packet body size in bytes, excluding the fixed header.
    #[inline]
    #[must_use]
    pub fn size(&self) -> u32 {
        self.packet.encoded_size(u32::MAX) as u32
    }

    /// Sends the subscription packet and waits for its acknowledgement.
    ///
    /// The peer's receive maximum does not apply: this request waits for write
    /// capacity only and does not consume publish send credit. The returned packet
    /// contains per-filter reason codes; inspect them for individual failures.
    ///
    /// Only a client may send this packet; a server sink returns
    /// [`SendPacketError::NotAllowed`]. A closed connection cancels the wait with
    /// [`SendPacketError::Disconnected`].
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

/// Builder for a client unsubscription request and its acknowledgement wait.
pub struct UnsubscribeBuilder {
    /// Optional caller-selected identifier; otherwise allocated when sending.
    id: Option<NonZeroU16>,
    /// Unsubscription properties and topic filters accumulated by this builder.
    packet: codec::Unsubscribe,
    /// Connection state retained for client-role checks, encoding, and acknowledgement tracking.
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
    /// Sets the unsubscription packet identifier instead of generating one when sending.
    ///
    /// # Panics
    ///
    /// Panics if `id` is zero.
    #[inline]
    #[must_use]
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    /// Appends a topic filter to unsubscribe from.
    #[inline]
    #[must_use]
    pub fn topic_filter(mut self, filter: ByteString) -> Self {
        self.packet.topic_filters.push(filter);
        self
    }

    /// Appends a user-property key/value pair to the unsubscription packet.
    #[inline]
    #[must_use]
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    /// Returns the unsubscription packet body size in bytes, excluding the fixed header.
    #[inline]
    pub fn size(&self) -> u32 {
        self.packet.encoded_size(u32::MAX) as u32
    }

    /// Sends the unsubscription packet and waits for its acknowledgement.
    ///
    /// The peer's receive maximum does not apply: this request waits for write
    /// capacity only and does not consume publish send credit. The returned packet
    /// contains per-filter reason codes; inspect them for individual failures.
    ///
    /// Only a client may send this packet; a server sink returns
    /// [`SendPacketError::NotAllowed`]. A closed connection cancels the wait with
    /// [`SendPacketError::Disconnected`].
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

/// Sender for the declared payload bytes of one streaming publish.
///
/// Send chunks sequentially with [`send`](Self::send). For `QoS 1`, drive the
/// companion send future concurrently to encode the header and receive its
/// acknowledgement. Once payload sending begins, dropping an incomplete sender
/// terminates the connection.
pub struct StreamingPayload {
    /// Connection state owning the remaining-byte count and transport buffers.
    shared: Rc<MqttShared>,
    /// One-shot header-ready notification for `QoS 1`; taken by the first `send` call.
    rx: Cell<Option<pool::Receiver<()>>>,
    /// Active immediately for `QoS 0`, or after the header-ready wait for `QoS 1`.
    /// Cleared on the final encoded chunk so dropping a completed sender does not abort.
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
    /// Queues one chunk of the streaming payload, waiting for write capacity.
    ///
    /// For `QoS 1`, first waits until the companion send future has encoded the
    /// header; poll that future concurrently. Send chunks sequentially and send
    /// exactly the declared payload size. Success means bytes are encoded, not
    /// that the peer has received or acknowledged them.
    ///
    /// Returns [`SendPacketError::StreamingCancelled`] if the header-ready channel
    /// is cancelled, and [`SendPacketError::Disconnected`] if the connection closes.
    /// A chunk exceeding the remaining size returns [`EncodeError::OverPublishSize`]
    /// and terminates the connection. Sending after completion, including a
    /// chunk for a zero-size payload, returns [`EncodeError::UnexpectedPayload`].
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

/// Unit tests for sink encoding, acknowledgements, streaming, and flow control.
#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin, pin::pin, rc::Rc, task::Poll};

    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;
    use ntex_util::{future::lazy, time::Millis, time::sleep, time::timeout};

    use super::*;
    use crate::v5::shared::MqttShared;

    /// Fill the write buffer over the high watermark, the peer does not read
    fn block_writes(client: &IoTest, io: &Io) {
        client.remote_buffer_cap(0);
        io.encode_slice(&vec![0u8; 128 * 1024]).unwrap();
        assert!(io.is_wr_backpressure());
    }

    /// The write buffer drains, the backpressure flag stays set until the
    /// connection dispatcher clears it
    async fn unblock_writes(client: &IoTest, io: &Io) {
        client.remote_buffer_cap(1024 * 1024);
        io.write_ready().await.unwrap();
        assert!(io.is_wr_backpressure());
    }

    /// Polls a pinned future once and reports whether it is still pending.
    async fn is_pending<P>(f: &mut Pin<P>) -> bool
    where
        P: std::ops::DerefMut<Target: Future>,
    {
        lazy(|cx| f.as_mut().poll(cx).is_pending()).await
    }

    /// Builds a nonzero packet identifier for test packets.
    ///
    /// # Panics
    ///
    /// Panics if `id` is zero.
    fn id(id: u16) -> NonZeroU16 {
        NonZeroU16::new(id).unwrap()
    }

    /// Creates a successful `PublishReceived` acknowledgement for the specified identifier.
    fn rec(packet_id: u16) -> Ack {
        Ack::Receive(codec::PublishAck {
            packet_id: id(packet_id),
            ..Default::default()
        })
    }

    /// Creates a successful `PublishComplete` acknowledgement for the specified identifier.
    fn comp(packet_id: u16) -> Ack {
        Ack::Complete(codec::PublishAck2 {
            packet_id: id(packet_id),
            ..Default::default()
        })
    }

    /// Server sink with send capacity, the peer accepts 1KB
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

    /// STATEMENT: Sink handles and packet builders identify their types in debug output.
    ///
    /// HOW:
    /// - Create a sink and publish, subscribe, and unsubscribe builders.
    /// - Format each with `Debug` and assert that its type name appears.
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

    /// STATEMENT: A server sink rejects client-only subscription packets without closing the connection.
    ///
    /// HOW:
    /// - Poll subscribe and unsubscribe sends on a server sink.
    /// - Assert immediate `SendPacketError::NotAllowed`, an open sink, and no bytes written to the peer.
    #[ntex::test]
    async fn test_server_subscribe_not_allowed() {
        let (client, _io, _shared, sink) = setup(16);

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

    /// STATEMENT: Acknowledgements must match the current phase of each in-flight packet.
    ///
    /// HOW:
    /// - Inject incorrect acknowledgement types into publish, subscribe, unsubscribe, and release flows.
    /// - Assert the expected protocol errors and disconnected send futures.
    /// - Complete a valid `QoS 2` exchange and assert success with the sink still open.
    #[ntex::test]
    async fn test_ack_type_mismatch() {
        use crate::{error::MqttProtocolError, types::packet_type};

        /// Builds the expected acknowledgement-type mismatch error for a test assertion.
        fn err(pkt: u8, expected: &'static str) -> Result<(), MqttProtocolError> {
            Err(MqttProtocolError::unexpected_packet(pkt, expected))
        }

        // QoS 1 PUBLISH acknowledged with PUBREC or PUBCOMP
        for (ack, expected) in [
            (rec(1), err(packet_type::PUBREC, "Expected PUBACK packet")),
            (comp(1), err(packet_type::PUBCOMP, "Expected PUBACK packet")),
        ] {
            let (_client, _io, shared, sink) = setup(16);
            shared.set_client();
            let mut f = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
            assert!(is_pending(&mut f).await);
            assert_eq!(shared.pkt_ack(ack), expected);
            assert!(!sink.is_open());
            assert_eq!(f.await, Err(SendPacketError::Disconnected));
        }

        // SUBSCRIBE acknowledged with PUBREC
        let (_client, _io, shared, sink) = setup(16);
        shared.set_client();
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
        let (_client, _io, shared, sink) = setup(16);
        shared.set_client();
        let mut f = pin!(sink.unsubscribe().topic_filter("a".into()).send());
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(comp(1)),
            err(packet_type::PUBCOMP, "Expected UNSUBACK packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // QoS 2 PUBLISH acknowledged with PUBCOMP before PUBREC
        let (_client, _io, shared, sink) = setup(16);
        shared.set_client();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(comp(1)),
            err(packet_type::PUBCOMP, "Expected PUBREC packet")
        );
        assert!(matches!(f.await, Err(SendPacketError::Disconnected)));

        // QoS 2 PUBREL acknowledged with a second PUBREC
        let (_client, _io, shared, sink) = setup(16);
        shared.set_client();
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
        let (_client, _io, shared, sink) = setup(16);
        shared.set_client();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
        let mut f = pin!(f.await.unwrap().release());
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
        assert_eq!(f.await, Ok(()));
        assert!(sink.is_open());
    }

    /// STATEMENT: Independent `QoS 2` publishes release their own packet identifiers.
    ///
    /// HOW:
    /// - Receive acknowledgements for two publishes, then explicitly release both or drop the second handle.
    /// - Inspect both encoded `PublishRelease` identifiers, deliver completion acknowledgements, and assert restored credit.
    #[ntex::test]
    async fn test_release_multiple() {
        for drop_second in [false, true] {
            let (client, _io, shared, sink) = setup(16);

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

    /// STATEMENT: A rejected `QoS 2` publish frees its identifier and quota without releasing a later publish using the same identifier.
    ///
    /// HOW:
    /// - Queue two publishes with identifier `1`, then reject the first with `PublishAckReason::NotAuthorized`.
    /// - Assert the rejection reason is retained and the second publish starts using the freed identifier.
    /// - Drop or release the rejected handle, complete the accepted publish, and inspect exactly one release packet and restored credit.
    #[ntex::test]
    async fn test_rejected_pubrec() {
        /// Creates a `PublishReceived` acknowledgement with the requested rejection or success reason.
        fn rec(packet_id: u16, reason_code: codec::PublishAckReason) -> Ack {
            Ack::Receive(codec::PublishAck {
                packet_id: id(packet_id),
                reason_code,
                ..Default::default()
            })
        }
        /// Expected encoded publish packet, or its fixed-header byte in packet-type assertions.
        const PUBLISH: &[u8] = b"\x34\x06\x00\x01a\x00\x01\x00";

        for drop_rejected in [false, true] {
            let (client, _io, shared, sink) = setup(1);

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

    /// STATEMENT: A malformed publish does not leave the sink in streaming mode.
    ///
    /// HOW:
    /// - Attempt `QoS 0` and `QoS 1` streams with a wildcard publish topic.
    /// - Assert `EncodeError::MalformedPacket` and no active stream after each attempt.
    /// - Send valid publishes and acknowledge the `QoS 1` publish to verify the sink remains usable.
    #[ntex::test]
    async fn test_rejected_publish_does_not_start_streaming() {
        let (client, _io, shared, sink) = setup(16);
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
        let mut publish = Box::pin(sink.publish("a/b").send_at_least_once(Bytes::new()));
        assert!(is_pending(&mut publish).await);
        assert_eq!(
            shared.pkt_ack(Ack::Publish(codec::PublishAck {
                packet_id: id(2),
                ..Default::default()
            })),
            Ok(())
        );
        assert!(publish.await.is_ok());
        drop(client);
    }

    /// STATEMENT: A payload chunk waiting for write capacity fails on every tested connection shutdown path.
    ///
    /// HOW:
    /// - Start a stream, send part of its payload, block writes, and poll the final chunk until pending.
    /// - Close the shared state, force-close the sink, or drop the shared sink with transport shutdown.
    /// - Resume reads and assert `SendPacketError::Disconnected` within the timeout.
    #[ntex::test]
    async fn test_streaming_waiter_fails_on_close() {
        for close in 0..3 {
            let (client, io, shared, sink) = setup(16);

            let stream = sink.publish("a/b").stream_at_most_once(4).await.unwrap();
            stream.send(Bytes::from_static(b"ab")).await.unwrap();
            block_writes(&client, &io);
            let mut chunk = Box::pin(stream.send(Bytes::from_static(b"cd")));
            assert!(lazy(|cx| chunk.as_mut().poll(cx).is_pending()).await);

            match close {
                0 => shared.close(None),
                1 => sink.force_close(),
                _ => shared.drop_sink(true),
            }
            // the write buffer drains during graceful shutdown
            client.remote_buffer_cap(1024 * 1024);
            assert_eq!(
                timeout(Millis(1000), chunk).await.unwrap(),
                Err(SendPacketError::Disconnected)
            );
        }
    }

    /// STATEMENT: A backpressured publish follows the active streaming payload rather than interleaving with it.
    ///
    /// HOW:
    /// - Start a stream, block writes, and poll its final chunk and a `QoS 1` publish.
    /// - Drain the write buffer and verify the publish remains pending until the final chunk completes.
    /// - Assert send-credit accounting and inspect the peer bytes for the final chunk followed by the publish.
    #[ntex::test]
    async fn test_streaming_completion_releases_waiters() {
        let (client, io, shared, sink) = setup(16);

        let stream = sink.publish("a/b").stream_at_most_once(4).await.unwrap();
        stream.send(Bytes::from_static(b"ab")).await.unwrap();
        block_writes(&client, &io);
        let mut chunk = pin!(stream.send(Bytes::from_static(b"cd")));
        assert!(lazy(|cx| chunk.as_mut().poll(cx).is_pending()).await);
        let mut publish = pin!(sink.publish("c").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);

        // the payload goes first, the publish cannot interleave with it,
        // the publish holds its send credit
        unblock_writes(&client, &io).await;
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 15);
        assert!(shared.is_streaming());
        chunk.await.unwrap();
        assert!(!shared.is_streaming());

        // the completed payload releases the waiting publish
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 15);
        sleep(Millis(50)).await;
        assert!(
            client
                .read_any()
                .ends_with(b"cd\x32\x06\x00\x01c\x00\x01\x00")
        );
    }

    /// STATEMENT: Closing an incomplete stream wakes a waiting `QoS 0` publish with a disconnect error.
    ///
    /// HOW:
    /// - Start a four-byte stream and poll another publish until pending.
    /// - Gracefully close, force-close, or drop the active payload sender.
    /// - Poll the waiting publish immediately and assert `SendPacketError::Disconnected`.
    #[ntex::test]
    async fn test_at_most_once_streaming_close() {
        for close in 0..3 {
            let (_client, _io, shared, sink) = setup(16);

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

    /// STATEMENT: `QoS 0` sends wait for write capacity and reject a connection closed while waiting.
    ///
    /// HOW:
    /// - Block peer reads and fill the write buffer, then poll buffered and streaming publishes.
    /// - Resume reads and assert both sends proceed and the streaming payload completes.
    /// - Repeat with graceful and forced shutdown and assert disconnect errors for both waiting sends.
    #[ntex::test]
    async fn test_at_most_once_write_backpressure() {
        for close in 0..3 {
            let (client, io, shared, sink) = setup(16);
            client.remote_buffer_cap(0);

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

    /// STATEMENT: Control packets and subsequent publishes cannot interleave with a streaming payload.
    ///
    /// HOW:
    /// - Stream a six-byte payload in three chunks while queueing a ping, an acknowledgement, and another publish.
    /// - Inspect peer bytes after each chunk and assert control packets follow only the final chunk.
    /// - Complete the waiting publish and send another ping to verify no deferred bytes remain.
    #[ntex::test]
    async fn test_packets_deferred_while_streaming() {
        let (client, io, shared, sink) = setup(16);

        let stream = sink.publish("a/b").stream_at_most_once(6).await.unwrap();
        stream.send(Bytes::from_static(b"ab")).await.unwrap();

        // client keep-alive and dispatcher responses
        assert!(sink.ping());
        let ack = codec::Packet::PublishAck(codec::PublishAck {
            packet_id: id(1),
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

    /// STATEMENT: Dropping a `QoS 2` send future still releases the publish and eventually frees its identifier and credit.
    ///
    /// HOW:
    /// - Drop the future before or after delivering `PublishReceived` and inspect the resulting `PublishRelease` bytes.
    /// - Deliver `PublishComplete` and assert restored send credit.
    /// - Reuse the same packet identifier in a second exchange and explicitly release it successfully.
    #[ntex::test]
    async fn test_dropped_exactly_once() {
        /// Expected encoded publish packet, or its fixed-header byte in packet-type assertions.
        const PUBLISH: &[u8] = b"\x34\x06\x00\x01a\x00\x01\x00";
        /// Expected encoded `PublishRelease` packet for identifier `1`.
        const PUBREL: &[u8] = b"\x62\x04\x00\x01\x00\x00";

        // PUBREC received before or after the future is dropped
        for delivered in [false, true] {
            let (client, _io, shared, sink) = setup(1);

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

    /// STATEMENT: Subscriptions do not consume publish credit but still wait for write capacity and fail on shutdown.
    ///
    /// HOW:
    /// - Use publish capacity one and a small write watermark to send subscriptions, unsubscriptions, and a publish.
    /// - Inspect packet types and assert subscription acknowledgements do not change publish credit.
    /// - Block writes, cancel a waiting subscription, and inspect bytes after draining to verify no cancelled packet was encoded.
    /// - Close with subscriptions pending and assert all their send futures report disconnection.
    #[ntex::test]
    async fn test_receive_max_subscribe() {
        /// Reads the first byte of the next peer buffer within the test timeout.
        async fn read(client: &IoTest) -> u8 {
            timeout(Millis(1000), client.read()).await.unwrap().unwrap()[0]
        }
        /// Creates a subscription acknowledgement granting `QoS 0` for one filter.
        fn suback(packet_id: u16) -> Ack {
            Ack::Subscribe(codec::SubscribeAck {
                packet_id: id(packet_id),
                properties: codec::UserProperties::default(),
                reason_string: None,
                status: vec![codec::SubscribeAckReason::GrantedQos0],
            })
        }
        /// Creates a successful unsubscription acknowledgement for one filter.
        fn unsuback(packet_id: u16) -> Ack {
            Ack::Unsubscribe(codec::UnsubscribeAck {
                packet_id: id(packet_id),
                properties: codec::UserProperties::default(),
                reason_string: None,
                status: vec![codec::UnsubscribeAckReason::Success],
            })
        }
        /// Creates a successful `PublishAck` for the specified identifier.
        fn puback(packet_id: u16) -> Ack {
            Ack::Publish(codec::PublishAck {
                packet_id: id(packet_id),
                ..Default::default()
            })
        }
        /// Fixed-header byte identifying a subscription request.
        const SUBSCRIBE: u8 = 0x82;
        /// Fixed-header byte identifying an unsubscription request.
        const UNSUBSCRIBE: u8 = 0xa2;
        /// Expected encoded publish packet, or its fixed-header byte in packet-type assertions.
        const PUBLISH: u8 = 0x32;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        // write backpressure at two SUBSCRIBE packets
        let io = Io::new(
            server,
            SharedCfg::new("test").add(ntex_io::IoConfig::default().set_write_backpressure(16)),
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

    /// Tests of publish-credit reservations, cancellation, and acknowledgement ordering.
    mod receive_max {

        use super::*;

        /// Creates a pinned `QoS 1` send future for acknowledgement and flow-control tests.
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

        /// Creates a successful `PublishAck` for the specified identifier.
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

        /// Read packets written to the peer
        async fn written(client: &IoTest) -> Bytes {
            sleep(Millis(25)).await;
            client.read_any()
        }

        /// STATEMENT: Acknowledgement-released credit stays reserved for the first publish waiter.
        ///
        /// HOW:
        /// - Fill capacity one, queue a second publish, and acknowledge the first without polling the second.
        /// - Assert zero credit and no readiness, then start a third publish and verify it writes nothing.
        /// - Poll and acknowledge the second and third sends in turn, inspecting their encoded identifiers and restored credit.
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

        /// STATEMENT: Cancelling a granted publish waiter passes its reserved credit to the next live waiter.
        ///
        /// HOW:
        /// - Queue four sends at capacity one, cancel an ungranted waiter, and acknowledge the in-flight publish.
        /// - Cancel the granted waiter and queue another send.
        /// - Inspect the remaining publishes in order, acknowledge them, and assert full credit is restored.
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

        /// STATEMENT: A readiness notification queues with publishes and passes its granted credit onward when completed.
        ///
        /// HOW:
        /// - Fill capacity one, create a readiness future, and queue another publish.
        /// - Acknowledge the in-flight publish and assert the later publish writes nothing before readiness is polled.
        /// - Complete readiness, then send and acknowledge the waiting publish; assert immediate readiness and restored credit.
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

        /// STATEMENT: Increasing publish capacity reserves credit for the earliest waiting requests.
        ///
        /// HOW:
        /// - Queue four sends with capacity one, then increase capacity to three.
        /// - Assert no free credit and no bytes from the still-ungranted fourth send.
        /// - Acknowledge the first send, poll the granted sends in reverse order, inspect their packet order, and acknowledge all to restore capacity.
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

        /// STATEMENT: A `QoS 2` publish retains its send credit until `PublishComplete`.
        ///
        /// HOW:
        /// - Fill capacity one with a `QoS 2` publish and queue a `QoS 1` publish.
        /// - Deliver `PublishReceived`, begin release, and inspect that only `PublishRelease` is sent while the later publish waits.
        /// - Deliver `PublishComplete`, inspect the newly sent publish, acknowledge it, and assert restored credit.
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

        /// STATEMENT: A streaming publish enters the credit queue when created, before its future is polled.
        ///
        /// HOW:
        /// - Fill capacity one, create an unpolled stream with identifier `2`, and poll a later publish with identifier `3`.
        /// - Acknowledge the first publish and verify the later publish still writes nothing.
        /// - Poll the stream future, send its chunk, acknowledge it, then inspect and complete the later publish.
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

        /// STATEMENT: Clearing sink queues cancels old waiters and releases reservations without preventing new sends on the active transport.
        ///
        /// HOW:
        /// - Queue three sends at capacity one and acknowledge the first so the second holds a grant.
        /// - Call `drop_sink(false)` and assert restored credit and readiness.
        /// - Send and acknowledge two new publishes while asserting the old granted and waiting futures report disconnection.
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

        /// STATEMENT: `QoS 1` acknowledgements follow publish send order, not numeric packet-identifier order.
        ///
        /// HOW:
        /// - Send publishes with identifiers `3` and `1`, reject a duplicate identifier, and queue reuse of `3`.
        /// - Send a `QoS 0` publish and acknowledge the `QoS 1` publishes in their send order.
        /// - Assert successful futures, identifier reuse, restored credit, and an open sink.
        #[ntex::test]
        async fn test_ack_in_order() {
            let (client, _io, shared, sink) = setup(2);

            let mut a = send(&sink, 3);
            assert!(is_pending(&mut a).await);
            assert_eq!(
                send(&sink, 3).await,
                Err(SendPacketError::PacketIdInUse(id(3)))
            );
            let mut b = send(&sink, 1);
            let mut c = send(&sink, 3);
            for f in [&mut a, &mut b, &mut c] {
                assert!(is_pending(f).await);
            }
            assert_eq!(written(&client).await, publishes(&[3, 1]));

            sink.publish("a")
                .send_at_most_once(Bytes::new())
                .await
                .unwrap();
            assert_eq!(
                written(&client).await,
                Bytes::from_static(b"\x30\x04\x00\x01a\x00")
            );

            // FIFO follows send order, not packet id, and permits id reuse.
            assert_eq!(shared.pkt_ack(ack(3)), Ok(()));
            assert!(a.await.is_ok());
            assert!(is_pending(&mut b).await);
            assert!(is_pending(&mut c).await);
            assert_eq!(written(&client).await, publishes(&[3]));
            assert_eq!(sink.credit(), 0);

            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(b.await.is_ok());
            assert_eq!(shared.pkt_ack(ack(3)), Ok(()));
            assert!(c.await.is_ok());
            assert_eq!(sink.credit(), 2);
            assert!(sink.is_open());
        }

        /// STATEMENT: An out-of-order `QoS 1` acknowledgement closes the sink and cancels pending sends.
        ///
        /// HOW:
        /// - Start two publishes and a credit waiter, optionally dropping the first send future.
        /// - Acknowledge the second publish before the first and assert a protocol violation.
        /// - Assert the sink closes and all remaining send futures report `SendPacketError::Disconnected`.
        /// - Inspect the outgoing disconnect packet and assert protocol-error reason code `0x82`.
        #[ntex::test]
        async fn test_ack_out_of_order() {
            for drop_first in [false, true] {
                let (client, _io, shared, sink) = setup(2);
                let mut a = send(&sink, 1);
                let mut b = send(&sink, 2);
                let mut c = send(&sink, 3);
                for f in [&mut a, &mut b, &mut c] {
                    assert!(is_pending(f).await);
                }
                assert_eq!(written(&client).await, publishes(&[1, 2]));
                let a = if drop_first {
                    drop(a);
                    None
                } else {
                    Some(a)
                };

                assert_eq!(
                    shared.pkt_ack(ack(2)),
                    Err(crate::error::MqttProtocolError::generic_violation(
                        "Received PUBACK out of order"
                    ))
                );
                assert!(!sink.is_open());
                if let Some(a) = a {
                    assert_eq!(a.await, Err(SendPacketError::Disconnected));
                }
                for f in [b, c] {
                    assert_eq!(f.await, Err(SendPacketError::Disconnected));
                }
                let disconnect = written(&client).await;
                assert_eq!(disconnect[0], 0xe0);
                assert_eq!(disconnect[2], 0x82);
            }
        }

        /// STATEMENT: A later `QoS 1` acknowledgement may arrive before completion of an earlier `QoS 2` release.
        ///
        /// HOW:
        /// - Receive a `QoS 2` publish acknowledgement, send a `QoS 1` publish, and begin the release.
        /// - Inspect the encoded packets and acknowledge the `QoS 1` publish while the release remains pending.
        /// - Complete the release and assert restored credit and an open sink.
        #[ntex::test]
        async fn test_ack_qos2_interleaved() {
            let (client, _io, shared, sink) = setup(16);

            let mut a = Box::pin(
                sink.publish("a")
                    .packet_id(1)
                    .send_exactly_once(Bytes::new()),
            );
            assert!(is_pending(&mut a).await);
            let pubrec = codec::PublishAck {
                packet_id: id(1),
                ..Default::default()
            };
            assert_eq!(shared.pkt_ack(Ack::Receive(pubrec)), Ok(()));
            let received = a.await.unwrap();

            let mut b = send(&sink, 2);
            assert!(is_pending(&mut b).await);
            let mut rel = Box::pin(received.release());
            assert!(is_pending(&mut rel).await);
            assert_eq!(
                written(&client).await,
                Bytes::from_static(
                    b"\x34\x06\x00\x01a\x00\x01\x00\x32\x06\x00\x01a\x00\x02\x00\x62\x04\x00\x01\x00\x00"
                )
            );

            assert_eq!(shared.pkt_ack(ack(2)), Ok(()));
            assert!(b.await.is_ok());
            assert!(is_pending(&mut rel).await);
            let pubcomp = codec::PublishAck2 {
                packet_id: id(1),
                ..Default::default()
            };
            assert_eq!(shared.pkt_ack(Ack::Complete(pubcomp)), Ok(()));
            assert!(rel.await.is_ok());
            assert_eq!(sink.credit(), 16);
            assert!(sink.is_open());
        }

        /// STATEMENT: Subscription acknowledgement ordering is independent of publish ordering and credit.
        ///
        /// HOW:
        /// - Send a subscription and a later `QoS 1` publish with capacity one.
        /// - Acknowledge the publish before the subscription and assert restored publish credit after both.
        /// - Start another publish and assert it consumes the free credit without closing the sink.
        #[ntex::test]
        async fn test_suback_after_puback() {
            let (_client, _io, shared, sink) = setup(1);
            shared.set_client();

            let mut sub = Box::pin(
                sink.subscribe(None)
                    .packet_id(1)
                    .topic_filter("a".into(), codec::SubscriptionOptions::default())
                    .send(),
            );
            assert!(is_pending(&mut sub).await);
            let mut a = send(&sink, 2);
            assert!(is_pending(&mut a).await);
            assert_eq!(sink.credit(), 0);

            assert_eq!(shared.pkt_ack(ack(2)), Ok(()));
            assert!(a.await.is_ok());
            assert_eq!(sink.credit(), 1);
            let suback = Ack::Subscribe(codec::SubscribeAck {
                packet_id: id(1),
                properties: codec::UserProperties::default(),
                reason_string: None,
                status: vec![codec::SubscribeAckReason::GrantedQos0],
            });
            assert_eq!(shared.pkt_ack(suback), Ok(()));
            assert!(sub.await.is_ok());
            assert_eq!(sink.credit(), 1);

            // the next publish takes the credit
            let mut b = send(&sink, 3);
            assert!(is_pending(&mut b).await);
            assert_eq!(sink.credit(), 0);
            assert!(sink.is_open());
        }

        /// STATEMENT: An acknowledgement for an identifier not in flight is a connection-closing protocol error.
        ///
        /// HOW:
        /// - Inject an acknowledgement with no sends, with a different identifier in flight, and after a completed send.
        /// - Assert unknown-identifier errors and a closed sink in all three scenarios.
        /// - Assert that the unrelated in-flight send is cancelled with `SendPacketError::Disconnected`.
        #[ntex::test]
        async fn test_ack_unknown_id() {
            let err = Err(crate::error::MqttProtocolError::unknown_packet_id());

            // nothing in flight
            let (_client, _io, shared, sink) = setup(16);
            assert_eq!(shared.pkt_ack(ack(1)), err);
            assert!(!sink.is_open());

            // other packet id in flight
            let (_client, _io, shared, sink) = setup(16);
            let mut a = send(&sink, 1);
            assert!(is_pending(&mut a).await);
            assert_eq!(shared.pkt_ack(ack(2)), err);
            assert!(!sink.is_open());
            assert_eq!(
                timeout(Millis(1000), a).await.unwrap(),
                Err(SendPacketError::Disconnected)
            );

            // second ack of the same packet id
            let (_client, _io, shared, sink) = setup(16);
            let mut a = send(&sink, 1);
            assert!(is_pending(&mut a).await);
            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert!(a.await.is_ok());
            assert_eq!(shared.pkt_ack(ack(1)), err);
            assert!(!sink.is_open());
        }

        /// STATEMENT: Nonblocking publish callbacks report valid acknowledgements and cancel remaining publishes on an ordering error.
        ///
        /// HOW:
        /// - Install a recording callback and send three nonblocking publishes.
        /// - Acknowledge the first publish, then inject an out-of-order acknowledgement for the third.
        /// - Assert the first callback reports success, the sink closes, and the other identifiers each report disconnection once.
        #[ntex::test]
        async fn test_ack_cb_out_of_order() {
            let (_client, _io, shared, sink) = setup(3);
            let calls = Rc::new(std::cell::RefCell::new(Vec::new()));
            let calls2 = calls.clone();
            sink.publish_ack_cb(move |ack, disconnected| {
                calls2
                    .borrow_mut()
                    .push((ack.packet_id.get(), disconnected));
            });

            for _ in 0..3 {
                sink.publish("a")
                    .send_at_least_once_no_block(Bytes::new())
                    .unwrap();
            }
            assert_eq!(sink.credit(), 0);
            assert_eq!(shared.pkt_ack(ack(1)), Ok(()));
            assert_eq!(sink.credit(), 1);
            assert_eq!(*calls.borrow(), [(1, false)]);
            assert_eq!(
                shared.pkt_ack(ack(3)),
                Err(crate::error::MqttProtocolError::generic_violation(
                    "Received PUBACK out of order"
                ))
            );
            assert!(!sink.is_open());
            sink.close();
            let mut calls = calls.borrow_mut();
            assert_eq!(calls[0], (1, false));
            calls[1..].sort_unstable();
            assert_eq!(*calls, [(1, false), (2, true), (3, true)]);
        }
    }
}
