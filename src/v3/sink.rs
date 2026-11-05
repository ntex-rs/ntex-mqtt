use std::{cell::Cell, fmt, future::Future, future::ready, num::NonZeroU16, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::{channel::pool, future::Either};

use crate::v3::shared::{Ack, AckType, MqttShared};
use crate::v3::{codec, error::SendPacketError};
use crate::{error::EncodeError, types::QoS};

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
    pub(crate) fn new(state: Rc<MqttShared>) -> Self {
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

    /// Returns whether an acknowledged publish can be sent without waiting.
    ///
    /// Requires an active connection, available send credit, and no write
    /// backpressure.
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
    /// The configured in-flight limit counts `QoS 1` and `QoS 2` publishes,
    /// subscriptions, and unsubscriptions until their final acknowledgements.
    /// `QoS 0` does not consume credit.
    #[inline]
    pub fn credit(&self) -> usize {
        self.0.credit()
    }

    /// Waits for send credit and writable connection state.
    ///
    /// Returns `false` when closure is observed, otherwise `true` when the
    /// readiness check succeeds. Credit is not reserved for the caller; another
    /// send can consume it before the caller sends.
    /// An immediately ready result reflects state when this method is called,
    /// not when its future is later polled. This notification does not ensure
    /// that an active streaming payload has finished.
    pub fn ready(&self) -> impl Future<Output = bool> {
        if !self.0.is_active() {
            Either::Left(ready(false))
        } else if self.0.is_ready() {
            Either::Left(ready(true))
        } else {
            let shared = self.0.clone();
            Either::Right(async move { shared.wait_readiness().await })
        }
    }

    /// Initiates graceful MQTT connection shutdown.
    ///
    /// A client sink first attempts to queue `DISCONNECT`; a server sink does not.
    /// Buffered data may still flush, but pending acknowledgement waits are cancelled
    /// with [`SendPacketError::Disconnected`].
    #[inline]
    pub fn close(&self) {
        self.0.close();
    }

    /// Aborts the MQTT connection immediately.
    ///
    /// Pending acknowledgement waits are cancelled and buffered data is discarded.
    /// Use [`close`](Self::close) to initiate graceful shutdown instead.
    #[inline]
    pub fn force_close(&self) {
        self.0.force_close();
    }

    /// Queues `PINGREQ` and marks a response pending only if encoding succeeds.
    #[inline]
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
            qos: codec::QoS::AtMostOnce,
            packet_id: None,
            payload_size: 0,
        })
    }

    /// Creates a builder from the supplied publish packet.
    ///
    /// The selected send method replaces its `QoS` and payload size. A supplied
    /// packet identifier is preserved for `QoS 1` and `QoS 2` sends.
    #[inline]
    pub fn publish_pkt(&self, packet: codec::Publish) -> PublishBuilder {
        PublishBuilder {
            packet,
            shared: self.0.clone(),
        }
    }

    /// Sets the callback for nonblocking `QoS 1` publish acknowledgements.
    ///
    /// Used by [`PublishBuilder::send_at_least_once_no_block`], not by awaited sends.
    /// The first argument is the packet identifier; the second is `true` when
    /// disconnection cancels the publish rather than an acknowledgement completing it.
    ///
    /// The callback runs synchronously while the sink queues are mutably borrowed.
    /// Do not call back into the sink: queue access panics and replacing the callback
    /// may be lost. Hand off work through a channel or spawned task instead.
    /// Acknowledgements are reported in arrival order; disconnection reports
    /// in-flight nonblocking publishes in unspecified order.
    pub fn publish_ack_cb<F>(&self, f: F)
    where
        F: Fn(NonZeroU16, bool) + 'static,
    {
        self.0.set_publish_ack(Box::new(f));
    }

    /// Creates a subscribe packet builder.
    ///
    /// Only a client sink sends the packet; on a server sink
    /// [`SubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
    #[inline]
    pub fn subscribe(&self) -> SubscribeBuilder {
        SubscribeBuilder {
            id: None,
            topic_filters: Vec::new(),
            shared: self.0.clone(),
        }
    }

    /// Creates an unsubscribe packet builder.
    ///
    /// Only a client sink sends the packet; on a server sink
    /// [`UnsubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
    #[inline]
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder {
            id: None,
            topic_filters: Vec::new(),
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
    /// Publish header retained until the selected send method encodes it.
    packet: codec::Publish,
    /// Connection state retained for encoding, send credit, and acknowledgement tracking.
    shared: Rc<MqttShared>,
}

impl fmt::Debug for PublishBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishBuilder")
            .field("packet", &self.packet)
            .finish()
    }
}

impl PublishBuilder {
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

    /// Sets the retain flag so the peer can retain this publish.
    #[inline]
    #[must_use]
    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
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
        (codec::encode::get_encoded_publish_size(&self.packet) + payload_size) as u32
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
            self.packet.qos = codec::QoS::AtMostOnce;
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
    /// through the returned `StreamingPayload`; no chunks are needed for zero.
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
    /// Waits for send credit and writable connection state before encoding.
    /// An already active streaming payload can cause [`EncodeError::ExpectPayload`].
    ///
    /// The acknowledgement is read by the connection dispatcher. Awaiting it in a
    /// handler of the same connection can stall while reading is paused; see
    /// [read pause](crate::MqttServiceConfig#read-pause).
    pub async fn send_at_least_once(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if self.shared.is_active() {
            self.packet.qos = codec::QoS::AtLeastOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            if !self.shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
            }
            self.send_at_least_once_inner(payload).await
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
    /// Panics if the active sink is not ready. Panics after successful encoding if
    /// the publish acknowledgement callback has not been installed.
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

    /// Assigns an identifier, encodes a `QoS 1` publish, and awaits its acknowledgement.
    async fn send_at_least_once_inner(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        let idx = self.shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        self.shared
            .wait_publish_response(idx, AckType::Publish, self.packet, Some(payload))?
            .await
            .map(|_| ())
            .map_err(|_| SendPacketError::Disconnected)
    }

    /// Sends a complete `QoS 2` publish and waits for `PublishReceived`.
    ///
    /// Waits for send credit and writable connection state before encoding.
    /// An already active streaming payload can cause [`EncodeError::ExpectPayload`].
    /// The returned handle controls release; send credit stays in use until
    /// `PublishComplete`.
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
            if !self.shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
            }
            self.send_exactly_once_inner(payload).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Registers a `QoS 2` publish and guards its release while awaiting `PublishReceived`.
    async fn send_exactly_once_inner(
        mut self,
        payload: Bytes,
    ) -> Result<PublishReceived, SendPacketError> {
        let idx = self.shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS2) to {:#?}", self.packet);

        let rx =
            self.shared
                .wait_publish_response(idx, AckType::Receive, self.packet, Some(payload))?;
        let guard = ReleaseGuard(idx, &self.shared);
        let result = rx.await;
        std::mem::forget(guard);

        result
            .map(move |_| PublishReceived {
                packet_id: Some(idx),
                shared: self.shared,
            })
            .map_err(|_| SendPacketError::Disconnected)
    }

    /// Creates a streaming `QoS 1` send future and its payload sender.
    ///
    /// `size` is the total payload size in bytes. Poll the returned future
    /// concurrently with `StreamingPayload::send`: it waits for readiness, encodes
    /// the header, then waits for `PublishAck`. Send exactly `size` payload bytes
    /// in sequential chunks; no chunks are needed for zero.
    /// The credit wait begins when the returned future is polled.
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
        impl Future<Output = Result<(), SendPacketError>>,
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
            let fut = Either::Left(async move {
                if !self.shared.wait_readiness().await {
                    return Err(SendPacketError::Disconnected);
                }
                self.stream_at_least_once_inner(tx).await
            });
            (fut, stream)
        } else {
            (
                Either::Right(async { Err(SendPacketError::Disconnected) }),
                stream,
            )
        }
    }

    /// Encodes a streaming `QoS 1` header, signals payload readiness, and awaits acknowledgement.
    async fn stream_at_least_once_inner(
        mut self,
        tx: pool::Sender<()>,
    ) -> Result<(), SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        if tx.is_canceled() {
            Err(SendPacketError::StreamingCancelled)
        } else {
            let rx = self
                .shared
                .wait_publish_response(idx, AckType::Publish, self.packet, None);
            let _ = tx.send(());

            rx?.await
                .map(|_| ())
                .map_err(|_| SendPacketError::Disconnected)
        }
    }
}

/// Handle for a `QoS 2` publish acknowledged with `PublishReceived`.
///
/// Call [`release`](Self::release) to send `PublishRelease` packet and wait for
/// `PublishComplete`. If the value is dropped, `PublishRelease` is sent without
/// waiting for `PublishComplete`.
pub struct PublishReceived {
    /// Identifier awaiting release; taken by `release` so `Drop` does not send it twice.
    packet_id: Option<NonZeroU16>,
    /// Connection state retained until release or automatic release on drop.
    shared: Rc<MqttShared>,
}

impl fmt::Debug for PublishReceived {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishReceived")
            .field("packet_id", &self.packet_id)
            .finish()
    }
}

impl PublishReceived {
    /// Queues `PublishRelease` and waits for `PublishComplete`.
    ///
    /// Returns [`SendPacketError::UnexpectedRelease`] if the publish has already
    /// been released or is no longer registered for release.
    /// Completion releases the in-flight credit. Dropping the future after encoding
    /// does not retract the release packet.
    pub async fn release(mut self) -> Result<(), SendPacketError> {
        let rx = self
            .shared
            .release_publish(self.packet_id.take().unwrap())?;

        rx.await
            .map(|_| ())
            .map_err(|_| SendPacketError::Disconnected)
    }
}

impl Drop for PublishReceived {
    fn drop(&mut self) {
        if let Some(id) = self.packet_id.take() {
            let _ = self.shared.release_publish(id);
        }
    }
}

/// Releases `QoS 2` publish if the publish future is dropped after `PublishReceived`
/// packet is delivered to it but before the future is polled, PUBREC must be
/// answered with PUBREL (MQTT 3.1.1, 4.3.3). If the PUBREC is not received yet,
/// the dispatcher releases the publish.
struct ReleaseGuard<'a>(NonZeroU16, &'a MqttShared);

impl Drop for ReleaseGuard<'_> {
    fn drop(&mut self) {
        let _ = self.1.release_publish(self.0);
    }
}

/// Builder for a client subscription request and its acknowledgement wait.
pub struct SubscribeBuilder {
    /// Optional caller-selected identifier; otherwise allocated when sending.
    id: Option<NonZeroU16>,
    /// Connection state retained for client-role checks, encoding, and acknowledgement tracking.
    shared: Rc<MqttShared>,
    /// Ordered topic filters paired with their requested maximum `QoS`.
    topic_filters: Vec<(ByteString, codec::QoS)>,
}

impl fmt::Debug for SubscribeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeBuilder")
            .field("id", &self.id)
            .field("topic_filters", &self.topic_filters)
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

    /// Appends a topic filter and its requested maximum `QoS`.
    #[inline]
    #[must_use]
    pub fn topic_filter(mut self, filter: ByteString, qos: codec::QoS) -> Self {
        self.topic_filters.push((filter, qos));
        self
    }

    /// Returns the subscription packet body size in bytes, excluding the fixed header.
    #[inline]
    pub fn size(&self) -> u32 {
        codec::encode::get_encoded_subscribe_size(&self.topic_filters) as u32
    }

    /// Sends the subscription packet and waits for its acknowledgement.
    ///
    /// Waits for the configured in-flight credit and writable connection state;
    /// the request consumes credit until its acknowledgement arrives.
    ///
    /// Only a client may send this packet; a server sink returns
    /// [`SendPacketError::NotAllowed`]. A closed connection cancels the wait with
    /// [`SendPacketError::Disconnected`].
    pub async fn send(self) -> Result<Vec<codec::SubscribeReturnCode>, SendPacketError> {
        if !self.shared.is_client() {
            return Err(SendPacketError::NotAllowed);
        }
        if self.shared.is_active() {
            // handle client receive maximum
            if !self.shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
            }
            let idx = self.id.unwrap_or_else(|| self.shared.next_id());
            let rx = self.shared.wait_response(idx, AckType::Subscribe)?;

            // send subscribe to client
            log::trace!(
                "Sending subscribe packet id: {} filters:{:?}",
                idx,
                self.topic_filters
            );

            match self.shared.encode_packet(codec::Packet::Subscribe {
                packet_id: idx,
                topic_filters: self.topic_filters,
            }) {
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
    /// Connection state retained for client-role checks, encoding, and acknowledgement tracking.
    shared: Rc<MqttShared>,
    /// Ordered topic filters to remove from the peer's subscriptions.
    topic_filters: Vec<ByteString>,
}

impl fmt::Debug for UnsubscribeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnsubscribeBuilder")
            .field("id", &self.id)
            .field("topic_filters", &self.topic_filters)
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
        self.topic_filters.push(filter);
        self
    }

    /// Returns the unsubscription packet body size in bytes, excluding the fixed header.
    #[inline]
    pub fn size(&self) -> u32 {
        codec::encode::get_encoded_unsubscribe_size(&self.topic_filters) as u32
    }

    /// Sends the unsubscription packet and waits for its acknowledgement.
    ///
    /// Waits for the configured in-flight credit and writable connection state;
    /// the request consumes credit until its acknowledgement arrives.
    ///
    /// Only a client may send this packet; a server sink returns
    /// [`SendPacketError::NotAllowed`]. A closed connection cancels the wait with
    /// [`SendPacketError::Disconnected`].
    pub async fn send(self) -> Result<(), SendPacketError> {
        let shared = self.shared;
        let filters = self.topic_filters;

        if !shared.is_client() {
            return Err(SendPacketError::NotAllowed);
        }

        if shared.is_active() {
            // handle client receive maximum
            if !shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
            }
            // allocate packet id
            let idx = self.id.unwrap_or_else(|| shared.next_id());
            let rx = shared.wait_response(idx, AckType::Unsubscribe)?;

            // send subscribe to client
            log::trace!("Sending unsubscribe packet id: {idx} filters:{filters:?}");

            match shared.encode_packet(codec::Packet::Unsubscribe {
                packet_id: idx,
                topic_filters: filters,
            }) {
                Ok(()) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(|_| ())
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
    use crate::v3::shared::MqttShared;

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
        Ack::Receive(id(packet_id))
    }

    /// Creates a successful `PublishComplete` acknowledgement for the specified identifier.
    fn comp(packet_id: u16) -> Ack {
        Ack::Complete(id(packet_id))
    }

    /// Client sink with send capacity, the peer accepts 1KB
    fn setup(cap: usize) -> (IoTest, Io, Rc<MqttShared>, MqttSink) {
        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
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
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, true, Rc::default()));
        let sink = MqttSink::new(shared);

        // MqttSink
        assert!(format!("{sink:?}").contains("MqttSink"));

        // PublishBuilder
        let pb = sink.publish("test/topic");
        assert!(format!("{pb:?}").contains("PublishBuilder"));

        // SubscribeBuilder
        let sb = sink.subscribe();
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
        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            false,
            Rc::default(),
        ));
        let sink = MqttSink::new(shared);

        // MQTT 3.1.1, 3.8 and 3.10: SUBSCRIBE and UNSUBSCRIBE are client packets
        let mut f = pin!(
            sink.subscribe()
                .topic_filter("a".into(), codec::QoS::AtLeastOnce)
                .send()
        );
        let res = lazy(|cx| f.as_mut().poll(cx)).await;
        assert_eq!(
            res.map(|r| r.map(|_| ())),
            Poll::Ready(Err(SendPacketError::NotAllowed))
        );
        let mut f = pin!(sink.unsubscribe().topic_filter("a".into()).send());
        let res = lazy(|cx| f.as_mut().poll(cx)).await;
        assert_eq!(res, Poll::Ready(Err(SendPacketError::NotAllowed)));
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
            (
                Ack::Receive(id(1)),
                err(packet_type::PUBREC, "Expected PUBACK packet"),
            ),
            (
                Ack::Complete(id(1)),
                err(packet_type::PUBCOMP, "Expected PUBACK packet"),
            ),
        ] {
            let (_client, _io, shared, sink) = setup(16);
            let mut f = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
            assert!(is_pending(&mut f).await);
            assert_eq!(shared.pkt_ack(ack), expected);
            assert!(!sink.is_open());
            assert_eq!(f.await, Err(SendPacketError::Disconnected));
        }

        // SUBSCRIBE acknowledged with PUBREC
        let (_client, _io, shared, sink) = setup(16);
        let mut f = pin!(
            sink.subscribe()
                .topic_filter("a".into(), QoS::AtMostOnce)
                .send()
        );
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Receive(id(1))),
            err(packet_type::PUBREC, "Expected SUBACK packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // UNSUBSCRIBE acknowledged with PUBCOMP
        let (_client, _io, shared, sink) = setup(16);
        let mut f = pin!(sink.unsubscribe().topic_filter("a".into()).send());
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Complete(id(1))),
            err(packet_type::PUBCOMP, "Expected UNSUBACK packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // QoS 2 PUBLISH acknowledged with PUBCOMP before PUBREC
        let (_client, _io, shared, sink) = setup(16);
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Complete(id(1))),
            err(packet_type::PUBCOMP, "Expected PUBREC packet")
        );
        assert!(matches!(f.await, Err(SendPacketError::Disconnected)));

        // QoS 2 PUBREL acknowledged with a second PUBREC
        let (_client, _io, shared, sink) = setup(16);
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(Ack::Receive(id(1))), Ok(()));
        let mut f = pin!(f.await.unwrap().release());
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Receive(id(1))),
            err(packet_type::PUBREC, "Expected PUBCOMP packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // valid QoS 2 flow
        let (_client, _io, shared, sink) = setup(16);
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(Ack::Receive(id(1))), Ok(()));
        let mut f = pin!(f.await.unwrap().release());
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(Ack::Complete(id(1))), Ok(()));
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
            assert_eq!(buf, Bytes::from_static(b"\x62\x02\x00\x01\x62\x02\x00\x02"));

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
        assert_eq!(fut.await, err);
        assert!(!shared.is_streaming());

        // sink is still usable
        assert!(sink.is_open());
        sink.publish("a/b")
            .send_at_most_once(Bytes::from_static(b"data"))
            .await
            .unwrap();
        let mut publish = Box::pin(sink.publish("a/b").send_at_least_once(Bytes::new()));
        assert!(is_pending(&mut publish).await);
        assert_eq!(shared.pkt_ack(Ack::Publish(id(2))), Ok(()));
        assert_eq!(publish.await, Ok(()));
        drop(client);
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
                0 => shared.close(),
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
                    shared.close();
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
        let ack = codec::Packet::PublishAck { packet_id: id(1) };
        io.encode(codec::Encoded::Packet(ack), &shared).unwrap();

        // publish cannot interleave with payload, it waits for the payload
        let mut qos0 = Box::pin(sink.publish("c").send_at_most_once(Bytes::new()));
        assert!(lazy(|cx| qos0.as_mut().poll(cx).is_pending()).await);

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\x30\x0b\x00\x03a/bab"));

        // incomplete payload, packets are still deferred
        stream.send(Bytes::from_static(b"cd")).await.unwrap();
        assert!(shared.is_streaming());
        assert!(lazy(|cx| qos0.as_mut().poll(cx).is_pending()).await);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"cd"));

        stream.send(Bytes::from_static(b"ef")).await.unwrap();
        assert!(!shared.is_streaming());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"ef\xc0\x00\x40\x02\x00\x01"));

        // completed payload releases the waiting publish
        qos0.await.unwrap();
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\x30\x03\x00\x01c"));

        // nothing left behind
        assert!(sink.ping());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\xc0\x00"));
    }

    /// STATEMENT: A payload chunk waiting for write capacity fails when the connection closes.
    ///
    /// HOW:
    /// - Start a stream, send part of its payload, block writes, and poll the final chunk until pending.
    /// - Gracefully close and allow draining, or force-close the sink.
    /// - Assert the chunk completes with `SendPacketError::Disconnected` within the timeout.
    #[ntex::test]
    async fn test_streaming_waiter_fails_on_close() {
        for close in 0..2 {
            let (client, io, shared, sink) = setup(16);

            let stream = sink.publish("a/b").stream_at_most_once(4).await.unwrap();
            stream.send(Bytes::from_static(b"ab")).await.unwrap();
            block_writes(&client, &io);
            let mut chunk = Box::pin(stream.send(Bytes::from_static(b"cd")));
            assert!(lazy(|cx| chunk.as_mut().poll(cx).is_pending()).await);

            match close {
                0 => {
                    // the write buffer drains during graceful shutdown
                    shared.close();
                    client.remote_buffer_cap(1024 * 1024);
                }
                _ => sink.force_close(),
            }
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

        // the payload goes first, the publish cannot interleave with it
        unblock_writes(&client, &io).await;
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 16);
        chunk.await.unwrap();
        assert!(!shared.is_streaming());

        // the completed payload releases the waiting publish
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 15);
        sleep(Millis(50)).await;
        assert!(client.read_any().ends_with(b"cd\x32\x05\x00\x01c\x00\x01"));
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
        const PUBLISH: &[u8] = b"\x34\x05\x00\x01a\x00\x01";
        /// Expected encoded `PublishRelease` packet for identifier `1`.
        const PUBREL: &[u8] = b"\x62\x02\x00\x01";

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

    /// STATEMENT: A woken `QoS 1` waiter rechecks credit after another publish takes the freed slot.
    ///
    /// HOW:
    /// - Fill the only send slot, then queue a second publish and a readiness notification.
    /// - Acknowledge the first publish and start a third publish before polling the waiter.
    /// - Inspect the sent bytes and assert the waiter and readiness notification remain pending until their turn.
    #[ntex::test]
    async fn test_woken_waiter_rechecks_credit() {
        let (client, _io, shared, sink) = setup(1);

        let mut a = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| a.as_mut().poll(cx).is_pending()).await);
        let mut b = pin!(sink.publish("b").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        let mut ready = pin!(sink.ready());
        assert!(lazy(|cx| ready.as_mut().poll(cx).is_pending()).await);

        // ack wakes "b", "c" takes the credit before "b" is polled
        assert_eq!(shared.pkt_ack(Ack::Publish(id(1))), Ok(()));
        assert!(a.await.is_ok());
        let mut c = pin!(sink.publish("c").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| c.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert!(lazy(|cx| ready.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        sleep(Millis(50)).await;
        assert_eq!(
            client.read_any(),
            Bytes::from_static(b"\x32\x05\x00\x01a\x00\x01\x32\x05\x00\x01c\x00\x02")
        );

        // "b" is still first in line
        assert_eq!(shared.pkt_ack(Ack::Publish(id(2))), Ok(()));
        assert!(c.await.is_ok());
        assert!(lazy(|cx| ready.as_mut().poll(cx).is_pending()).await);
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        assert_eq!(shared.pkt_ack(Ack::Publish(id(3))), Ok(()));
        assert!(b.await.is_ok());
        assert!(ready.await);
    }

    /// STATEMENT: A publish woken by an acknowledgement still waits for write backpressure to drain.
    ///
    /// HOW:
    /// - Fill the only send slot, queue another publish, and block writes.
    /// - Acknowledge the first publish and assert the waiter remains pending with one free credit.
    /// - Drain writes and assert the second publish consumes that credit while awaiting its acknowledgement.
    #[ntex::test]
    async fn test_ack_respects_write_backpressure() {
        let (client, io, shared, sink) = setup(1);

        let mut a = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| a.as_mut().poll(cx).is_pending()).await);
        let mut b = pin!(sink.publish("b").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);

        block_writes(&client, &io);
        assert_eq!(shared.pkt_ack(Ack::Publish(id(1))), Ok(()));
        assert!(a.await.is_ok());
        assert!(!sink.is_ready());
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 1);

        unblock_writes(&client, &io).await;
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
    }

    /// STATEMENT: Increasing send capacity wakes only as many publishes as available credit permits.
    ///
    /// HOW:
    /// - Start four sends with capacity one, increase capacity to three, and poll the waiters.
    /// - Assert only the first two waiters consume the two new credits.
    /// - Increase capacity under write backpressure and verify the final waiter consumes credit only after writes drain.
    #[ntex::test]
    async fn test_set_cap_wakes_within_credit() {
        let (client, io, shared, sink) = setup(1);

        let mut a = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| a.as_mut().poll(cx).is_pending()).await);
        let mut b = pin!(sink.publish("b").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        let mut c = pin!(sink.publish("c").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| c.as_mut().poll(cx).is_pending()).await);
        let mut d = pin!(sink.publish("d").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);

        // one in flight, cap 3 leaves credit for "b" and "c"
        shared.set_cap(3);
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 2);
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert!(lazy(|cx| c.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);

        // no publish while write backpressure is enabled
        block_writes(&client, &io);
        shared.set_cap(4);
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 1);
        unblock_writes(&client, &io).await;
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
    }

    /// Tests of acknowledgement ordering and identifier matching across packet flows.
    mod ack_order {

        use super::*;
        use crate::error::MqttProtocolError;

        /// Creates a pinned `QoS 1` send future for acknowledgement and flow-control tests.
        fn send(sink: &MqttSink) -> Pin<Box<impl Future<Output = Result<(), SendPacketError>>>> {
            Box::pin(sink.publish("a").send_at_least_once(Bytes::new()))
        }

        /// Read packets written to the peer
        async fn written(client: &IoTest) -> Bytes {
            sleep(Millis(25)).await;
            client.read_any()
        }

        /// STATEMENT: `QoS 1` acknowledgements follow publish send order, not numeric packet-identifier order.
        ///
        /// HOW:
        /// - Send publishes with identifiers `3` and `1`, reject a duplicate identifier, and queue reuse of `3`.
        /// - Send a `QoS 0` publish and acknowledge the `QoS 1` publishes in their send order.
        /// - Assert successful futures, identifier reuse, restored credit, and an open sink.
        #[ntex::test]
        async fn test_ack_in_order() {
            let (_client, _io, shared, sink) = setup(2);
            let send = |packet_id| {
                Box::pin(
                    sink.publish("a")
                        .packet_id(packet_id)
                        .send_at_least_once(Bytes::new()),
                )
            };

            let mut a = send(3);
            assert!(is_pending(&mut a).await);
            assert_eq!(send(3).await, Err(SendPacketError::PacketIdInUse(id(3))));
            let mut b = send(1);
            let mut c = send(3);
            for f in [&mut a, &mut b, &mut c] {
                assert!(is_pending(f).await);
            }

            sink.publish("a")
                .send_at_most_once(Bytes::new())
                .await
                .unwrap();

            // FIFO follows send order, not packet id, and permits id reuse.
            assert_eq!(shared.pkt_ack(Ack::Publish(id(3))), Ok(()));
            assert_eq!(a.await, Ok(()));
            assert!(is_pending(&mut b).await);
            assert!(is_pending(&mut c).await);
            assert_eq!(sink.credit(), 0);

            assert_eq!(shared.pkt_ack(Ack::Publish(id(1))), Ok(()));
            assert_eq!(b.await, Ok(()));
            assert_eq!(shared.pkt_ack(Ack::Publish(id(3))), Ok(()));
            assert_eq!(c.await, Ok(()));
            assert_eq!(sink.credit(), 2);
            assert!(sink.is_open());
        }

        /// STATEMENT: An out-of-order `QoS 1` acknowledgement closes the sink and cancels pending sends.
        ///
        /// HOW:
        /// - Start two publishes and a credit waiter, optionally dropping the first send future.
        /// - Acknowledge the second publish before the first and assert a protocol violation.
        /// - Assert the sink closes and all remaining send futures report `SendPacketError::Disconnected`.
        #[ntex::test]
        async fn test_ack_out_of_order() {
            for drop_first in [false, true] {
                let (_client, _io, shared, sink) = setup(2);
                let mut a = send(&sink);
                let mut b = send(&sink);
                let mut c = send(&sink);
                for f in [&mut a, &mut b, &mut c] {
                    assert!(is_pending(f).await);
                }
                let a = if drop_first {
                    drop(a);
                    None
                } else {
                    Some(a)
                };

                assert_eq!(
                    shared.pkt_ack(Ack::Publish(id(2))),
                    Err(MqttProtocolError::generic_violation(
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

            let mut a = Box::pin(sink.publish("a").send_exactly_once(Bytes::new()));
            assert!(is_pending(&mut a).await);
            assert_eq!(shared.pkt_ack(Ack::Receive(id(1))), Ok(()));
            let received = a.await.unwrap();

            let mut b = send(&sink);
            assert!(is_pending(&mut b).await);
            let mut rel = Box::pin(received.release());
            assert!(is_pending(&mut rel).await);
            assert_eq!(
                written(&client).await,
                Bytes::from_static(
                    b"\x34\x05\x00\x01a\x00\x01\x32\x05\x00\x01a\x00\x02\x62\x02\x00\x01"
                )
            );

            assert_eq!(shared.pkt_ack(Ack::Publish(id(2))), Ok(()));
            assert_eq!(b.await, Ok(()));
            assert!(is_pending(&mut rel).await);
            assert_eq!(shared.pkt_ack(Ack::Complete(id(1))), Ok(()));
            assert_eq!(rel.await, Ok(()));
            assert_eq!(sink.credit(), 16);
            assert!(sink.is_open());
        }

        /// STATEMENT: A subscription acknowledgement may arrive after the acknowledgement of a later publish.
        ///
        /// HOW:
        /// - Send a subscription followed by a `QoS 1` publish.
        /// - Deliver the publish acknowledgement first, then the subscription acknowledgement.
        /// - Assert both sends succeed, credit is restored, and the connection remains open.
        #[ntex::test]
        async fn test_suback_after_puback() {
            let (_client, _io, shared, sink) = setup(16);

            let mut sub = Box::pin(
                sink.subscribe()
                    .topic_filter("a".into(), QoS::AtMostOnce)
                    .send(),
            );
            assert!(is_pending(&mut sub).await);
            let mut a = send(&sink);
            assert!(is_pending(&mut a).await);

            assert_eq!(shared.pkt_ack(Ack::Publish(id(2))), Ok(()));
            assert_eq!(a.await, Ok(()));
            let ack = Ack::Subscribe {
                packet_id: id(1),
                status: vec![codec::SubscribeReturnCode::Success(QoS::AtMostOnce)],
            };
            assert_eq!(shared.pkt_ack(ack), Ok(()));
            assert!(sub.await.is_ok());
            assert_eq!(sink.credit(), 16);
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
            let err = Err(MqttProtocolError::unknown_packet_id());

            // nothing in flight
            let (_client, _io, shared, sink) = setup(16);
            assert_eq!(shared.pkt_ack(Ack::Publish(id(1))), err);
            assert!(!sink.is_open());

            // other packet id in flight
            let (_client, _io, shared, sink) = setup(16);
            let mut a = send(&sink);
            assert!(is_pending(&mut a).await);
            assert_eq!(shared.pkt_ack(Ack::Publish(id(2))), err);
            assert!(!sink.is_open());
            assert_eq!(a.await, Err(SendPacketError::Disconnected));

            // second ack of the same packet id
            let (_client, _io, shared, sink) = setup(16);
            let mut a = send(&sink);
            assert!(is_pending(&mut a).await);
            assert_eq!(shared.pkt_ack(Ack::Publish(id(1))), Ok(()));
            assert_eq!(a.await, Ok(()));
            assert_eq!(shared.pkt_ack(Ack::Publish(id(1))), err);
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
            use std::cell::RefCell;

            let (_client, _io, shared, sink) = setup(3);
            let calls = Rc::new(RefCell::new(Vec::new()));
            let calls2 = calls.clone();
            sink.publish_ack_cb(move |idx, disconnected| {
                calls2.borrow_mut().push((idx.get(), disconnected));
            });

            for _ in 0..3 {
                sink.publish("a")
                    .send_at_least_once_no_block(Bytes::new())
                    .unwrap();
            }
            assert_eq!(sink.credit(), 0);
            assert_eq!(shared.pkt_ack(Ack::Publish(id(1))), Ok(()));
            assert_eq!(sink.credit(), 1);
            assert_eq!(*calls.borrow(), [(1, false)]);
            assert_eq!(
                shared.pkt_ack(Ack::Publish(id(3))),
                Err(MqttProtocolError::generic_violation(
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
