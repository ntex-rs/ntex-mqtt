#![allow(clippy::type_complexity)]
use std::{cell::Cell, cell::RefCell, collections::VecDeque, fmt, num, rc::Rc};

use ntex_bytes::{BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};
use ntex_io::IoRef;
use ntex_util::{HashMap, channel::pool};

use crate::error::{DecodeError, EncodeError, MqttProtocolError, PayloadError, SendPacketError};
use crate::io::{FrameState, STREAM_TAG};
use crate::v3::codec::{self, Encoded, Publish};
use crate::{QoS, payload::PayloadSource, types::packet_type};

/// Received acknowledgment routed to the corresponding outbound operation.
#[derive(Debug)]
pub(super) enum Ack {
    /// PUBACK completing a `QoS 1` publish.
    Publish(num::NonZeroU16),
    /// PUBREC acknowledging receipt of a `QoS 2` publish.
    Receive(num::NonZeroU16),
    /// PUBCOMP completing the `QoS 2` release handshake.
    Complete(num::NonZeroU16),
    /// SUBACK carrying subscription results.
    Subscribe {
        /// Identifier of the SUBSCRIBE being acknowledged.
        packet_id: num::NonZeroU16,
        /// One success quality of service or failure code per requested subscription.
        status: Vec<codec::SubscribeReturnCode>,
    },
    /// UNSUBACK completing an unsubscribe operation.
    Unsubscribe(num::NonZeroU16),
}

/// Acknowledgment type expected for an outstanding packet identifier.
#[derive(Copy, Clone, Debug)]
pub(super) enum AckType {
    /// PUBACK completing a `QoS 1` publish.
    Publish,
    /// PUBREC acknowledging receipt of a `QoS 2` publish.
    Receive,
    /// PUBCOMP completing the `QoS 2` release handshake.
    Complete,
    /// SUBACK carrying subscription results.
    Subscribe,
    /// UNSUBACK completing an unsubscribe operation.
    Unsubscribe,
}

/// Reusable channel storage for acknowledgment receivers and readiness notifications.
pub(super) struct MqttSinkPool {
    /// Channels carrying acknowledgments back to the sending operation.
    queue: pool::Pool<Ack>,
    /// Channels for readiness or streaming-start notifications.
    pub(super) waiters: pool::Pool<()>,
}

impl Default for MqttSinkPool {
    fn default() -> Self {
        Self {
            queue: pool::new(),
            waiters: pool::new(),
        }
    }
}

bitflags::bitflags! {
    /// Connection role and lifecycle markers, with negotiated options where applicable.
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Flags: u8 {
        /// Marks the client side of the connection.
        const CLIENT          = 0b0000_0001;
        /// Marks that a callback for nonblocking publish acknowledgments was installed.
        const ON_PUBLISH_ACK  = 0b0000_0100; // on-publish-ack callback
        /// Set after PINGREQ until the matching PINGRESP is received.
        const PING_PENDING    = 0b0000_1000; // PINGREQ is sent, PINGRESP is expected

        /// Records that a DISCONNECT send has been claimed, even if encoding fails.
        const DISCONNECT      = 0b0010_0000; // Disconnect frame is sent
        /// Marker for a dispatcher stop notification.
        const STOPPED         = 0b1000_0000; // DispatchItem::Stop() is sent
    }
}

/// Connection state shared by the MQTT 3.1.1 sink, codec, and dispatcher.
///
/// Tracks outgoing acknowledgments, streaming payloads, and send capacity for one connection.
pub struct MqttShared {
    /// Connection handle used to encode packets, wait for writes, and close transport.
    io: IoRef,
    /// Maximum number of outstanding acknowledgment flows allowed for sends.
    cap: Cell<usize>,
    /// Acknowledgment tracking and send-credit waiters for this connection.
    queues: RefCell<MqttSharedQueues>,
    /// Last generated packet identifier; reset to zero after allocating `u16::MAX`.
    inflight_idx: Cell<u16>,
    /// Connection role, keep-alive state, and shutdown/callback markers.
    flags: Cell<Flags>,
    /// Encoding failure retained when an outgoing streaming payload is abandoned.
    encode_error: Cell<Option<EncodeError>>,
    /// Outgoing payload bytes still required; `None` when no streaming publish is open.
    streaming_remaining: Cell<Option<num::NonZeroU32>>,
    /// Encoded non-publish packets buffered until the outgoing payload completes.
    deferred: Cell<Option<BytePages>>,
    /// Callback for nonblocking publishes; the boolean marks disconnection without an ack.
    on_publish_ack: Cell<Option<Box<dyn Fn(num::NonZeroU16, bool)>>>,
    /// Source through which the dispatcher delivers an incoming streaming payload.
    pub(super) payload: PayloadSource,
    /// Per-connection codec, including partial incoming and outgoing payload state.
    pub(super) codec: codec::Codec,
    /// Reusable acknowledgment and notification channels shared with other connections.
    pub(super) pool: Rc<MqttSinkPool>,
}

impl fmt::Debug for MqttShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttShared").finish()
    }
}

/// Outgoing acknowledgment flows and send-credit bookkeeping.
#[derive(Debug)]
struct MqttSharedQueues {
    /// Packet identifiers awaiting an ack and their expected types.
    ///
    /// An absent sender routes `QoS 1` completion through the callback, or marks
    /// a `QoS 2` flow whose caller has gone away.
    // packets awaiting ack, acks of different flows may arrive in any order
    // (MQTT 3.1.1 and 5.0, 4.6)
    inflight: HashMap<num::NonZeroU16, (Option<pool::Sender<Ack>>, AckType)>,
    /// `QoS 1` packet identifiers in send order, used to reject reordered PUBACKs.
    // QoS 1 PUBLISH packet ids in send order
    pubacks: VecDeque<num::NonZeroU16>,
    /// Readiness notifications queued until send capacity becomes available.
    waiters: VecDeque<pool::Sender<()>>,
    /// PUBCOMP receivers retained after PUBREC until release or completion.
    // PUBCOMP receivers, one per QoS 2 PUBLISH awaiting release
    rx: HashMap<num::NonZeroU16, pool::Receiver<Ack>>,
}

impl MqttShared {
    /// Creates shared state with no send capacity until the handshake sets its limit.
    pub(super) fn new(
        io: IoRef,
        codec: codec::Codec,
        client: bool,
        pool: Rc<MqttSinkPool>,
    ) -> Self {
        Self {
            io,
            codec,
            pool,
            cap: Cell::new(0),
            flags: Cell::new(if client { Flags::CLIENT } else { Flags::empty() }),
            queues: RefCell::new(MqttSharedQueues {
                inflight: HashMap::default(),
                pubacks: VecDeque::new(),
                waiters: VecDeque::new(),
                rx: HashMap::default(),
            }),
            inflight_idx: Cell::new(0),
            encode_error: Cell::new(None),
            streaming_remaining: Cell::new(None),
            deferred: Cell::new(None),
            on_publish_ack: Cell::new(None),
            payload: PayloadSource::default(),
        }
    }

    /// Returns the transport tag used in connection diagnostics.
    pub(super) fn tag(&self) -> &'static str {
        self.io.tag()
    }

    /// Returns whether this is the client side of the connection.
    pub(super) fn is_client(&self) -> bool {
        self.flags.get().contains(Flags::CLIENT)
    }

    /// Requests graceful transport closure and fails pending sends.
    ///
    /// Attempts DISCONNECT first on clients, unless a previous send already claimed it.
    pub(super) fn close(&self) {
        if self.is_client() && !self.is_disconnect_sent() {
            let _ = self.encode_packet(codec::Packet::Disconnect);
        }
        self.io.close();
        self.clear_queues();
    }

    /// Terminates the transport immediately and fails pending sends without DISCONNECT.
    pub(super) fn force_close(&self) {
        self.io.terminate();
        self.clear_queues();
    }

    /// Terminates a connection whose outgoing streaming payload was abandoned incomplete.
    pub(super) fn streaming_dropped(&self) {
        self.force_close();
        self.encode_error.set(Some(EncodeError::PublishIncomplete));
    }

    /// Fails the incoming payload source with a cloned, converted dispatcher error.
    pub(super) fn drop_payload<E>(&self, err: &E)
    where
        E: Clone,
        PayloadError: From<E>,
    {
        self.payload.set_error(err.clone().into());
    }

    /// Returns whether more bytes are required for an outgoing publish payload.
    pub(super) fn is_streaming(&self) -> bool {
        self.streaming_remaining.get().is_some()
    }

    /// Waits until a `QoS 0` publish can be encoded.
    ///
    /// Returns `false` if the connection closes or the write-readiness wait fails.
    ///
    /// Waits for an outgoing streaming payload to complete, packets cannot
    /// interleave with it, and for the write buffer to drain to the release
    /// threshold of write backpressure.
    pub(super) async fn wait_publish_ready(&self) -> bool {
        loop {
            if !self.is_active() {
                return false;
            }
            if self.is_streaming() {
                self.io.waiter(STREAM_TAG).await;
                continue;
            }
            if self.io.write_ready().await.is_err() {
                return false;
            }
            // a streaming publish may start while waiting for the write buffer
            if !self.is_streaming() {
                return self.is_active();
            }
        }
    }

    /// Sets whether PINGREQ is awaiting PINGRESP.
    pub(super) fn set_ping_pending(&self, pending: bool) {
        let mut flags = self.flags.get();
        flags.set(Flags::PING_PENDING, pending);
        self.flags.set(flags);
    }

    /// Returns whether PINGREQ is still awaiting PINGRESP.
    pub(super) fn is_ping_pending(&self) -> bool {
        self.flags.get().contains(Flags::PING_PENDING)
    }

    /// Waits until previously encoded packets are no longer blocked by payload or buffer state.
    ///
    /// Returns `false` if the connection closes; does not wait for a full transport flush.
    ///
    /// Packets encoded during an outgoing streaming payload are written once the
    /// payload completes, then the write buffer has to drain to the release threshold
    /// of write backpressure. A streaming payload started later does not delay them.
    pub(super) async fn wait_write_unblocked(&self) -> bool {
        if self.is_streaming() && self.is_active() {
            self.io.waiter(STREAM_TAG).await;
        }
        self.wait_write_ready().await
    }

    /// Waits for the write buffer to drain to the backpressure release threshold.
    ///
    /// Returns `false` if the connection closes, retrying write-readiness timeouts.
    ///
    /// The backpressure flag is not checked again once the buffer is drained;
    /// it is cleared only when the I/O dispatcher runs.
    async fn wait_write_ready(&self) -> bool {
        loop {
            if !self.is_active() {
                return false;
            }
            // write timeout does not close the connection, keep waiting
            if self.io.write_ready().await.is_ok() {
                return self.is_active();
            }
        }
    }

    /// Waits for publish write capacity, returning `false` if the connection closes.
    ///
    /// Write backpressure is the I/O-level flag, set as soon as the write
    /// buffer reaches the high watermark. A streaming payload in progress goes
    /// first, publishes cannot interleave with it.
    async fn wait_writable(&self) -> bool {
        loop {
            if !self.is_active() {
                return false;
            }
            if !self.io.is_wr_backpressure() {
                return true;
            }
            if !self.wait_write_ready().await {
                return false;
            }
            if !self.is_streaming() {
                return true;
            }
            self.io.waiter(STREAM_TAG).await;
        }
    }

    /// Returns whether the transport still accepts connection operations.
    pub(super) fn is_active(&self) -> bool {
        self.io.is_active()
    }

    /// Returns whether send credit is available and write backpressure is off.
    ///
    /// Does not check connection activity, exclude an outgoing stream, or reserve credit.
    pub(super) fn is_ready(&self) -> bool {
        self.credit() > 0 && !self.io.is_wr_backpressure()
    }

    /// Claims the DISCONNECT marker and returns whether it was already set.
    ///
    /// The first call returns `false`; later calls return `true`, regardless of encoding success.
    pub(super) fn is_disconnect_sent(&self) -> bool {
        let mut flags = self.flags.get();
        let sent = flags.contains(Flags::DISCONNECT);
        if !sent {
            flags.insert(Flags::DISCONNECT);
            self.flags.set(flags);
        }
        sent
    }

    /// Returns unused send capacity after counting all outstanding acknowledgment flows.
    pub(super) fn credit(&self) -> usize {
        self.cap
            .get()
            .saturating_sub(self.queues.borrow().inflight.len())
    }

    /// Allocates the next nonzero packet identifier, wrapping after `u16::MAX`.
    ///
    /// Does not skip identifiers still in flight; registration rejects collisions.
    pub(super) fn next_id(&self) -> num::NonZeroU16 {
        let idx = self.inflight_idx.get() + 1;
        let idx = if idx == u16::MAX {
            self.inflight_idx.set(0);
            u16::MAX
        } else {
            self.inflight_idx.set(idx);
            idx
        };
        num::NonZeroU16::new(idx).unwrap()
    }

    /// Preserves an explicit publish identifier or assigns the next generated one.
    pub(super) fn set_publish_id(&self, pkt: &mut Publish) -> num::NonZeroU16 {
        if let Some(idx) = pkt.packet_id {
            idx
        } else {
            let idx = self.next_id();
            pkt.packet_id = Some(idx);
            idx
        }
    }

    /// Updates send capacity and wakes queued operations that may now proceed.
    pub(super) fn set_cap(&self, cap: usize) {
        self.cap.set(cap);
        // wake up queued request (receive max limit)
        self.wake_waiters();
    }

    /// Installs the callback for nonblocking publish acks and disconnect notifications.
    ///
    /// The callback must not reenter shared state: acknowledgment queues remain borrowed
    /// while it runs, and the callback is temporarily removed from its cell.
    pub(super) fn set_publish_ack(&self, f: Box<dyn Fn(num::NonZeroU16, bool)>) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ON_PUBLISH_ACK);
        self.flags.set(flags);
        self.on_publish_ack.set(Some(f));
    }

    /// Encodes a packet, deferring its bytes until any outgoing publish payload completes.
    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), EncodeError> {
        self.io.encode(pkt.into(), self)
    }

    /// Encodes an outgoing publish header and optional initial payload bytes.
    ///
    /// Rejects overlapping publish streams and records remaining bytes only after success.
    /// The initial payload must not exceed `pkt.payload_size`.
    ///
    /// # Panics
    ///
    /// In debug builds, panics if the initial payload exceeds its declared size.
    pub(super) fn encode_publish(
        &self,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<(), EncodeError> {
        self.check_streaming()?;
        let remaining = Self::streaming_size(&pkt, payload.as_ref());
        self.io
            .encode(Encoded::Publish(pkt, payload), &self.codec)?;
        self.streaming_remaining.set(remaining);
        Ok(())
    }

    /// Appends a payload chunk and returns whether more bytes are required.
    ///
    /// Rejects chunks without an open stream; an oversized chunk terminates the connection.
    /// Completing the payload flushes deferred packets and wakes waiting publishers.
    pub(super) fn encode_publish_payload(&self, payload: Bytes) -> Result<bool, EncodeError> {
        if let Some(remaining) = self.streaming_remaining.get() {
            let len = payload.len() as u32;
            if len > remaining.get() {
                self.force_close();
                Err(EncodeError::OverPublishSize)
            } else {
                self.io.encode(Encoded::PayloadChunk(payload), self)?;
                let remaining = num::NonZeroU32::new(remaining.get() - len);
                self.streaming_remaining.set(remaining);
                if remaining.is_none() {
                    // publishes waiting for the streaming payload to complete
                    self.io.wake(STREAM_TAG);
                }
                Ok(remaining.is_some())
            }
        } else {
            Err(EncodeError::UnexpectedPayload)
        }
    }

    /// Fails pending ack receivers, notifies nonblocking sends, and wakes waiting publishers.
    ///
    /// Removes the publish callback and clears acknowledgment tracking; retained PUBCOMP
    /// receivers are discarded on release, completion, or shared-state destruction.
    fn clear_queues(&self) {
        // publishes waiting for the streaming payload fail
        self.io.wake(STREAM_TAG);

        let mut queues = self.queues.borrow_mut();
        queues.pubacks.clear();
        queues.waiters.clear();

        if let Some(cb) = self.on_publish_ack.take() {
            for (idx, (tx, _)) in queues.inflight.drain() {
                if tx.is_none() {
                    (*cb)(idx, true);
                }
            }
        } else {
            queues.inflight.clear();
        }
    }

    /// Wakes queued operations up to the available send capacity without reserving it.
    fn wake_waiters(&self) {
        self.wake_waiters_inner(&mut self.queues.borrow_mut());
    }

    /// Notifies at most the available send capacity, skipping canceled waiters.
    ///
    /// Notifications do not reserve credit; woken operations must check readiness again.
    fn wake_waiters_inner(&self, queues: &mut MqttSharedQueues) {
        if queues.inflight.len() < self.cap.get() {
            let mut num = self.cap.get() - queues.inflight.len();
            while num > 0 {
                if let Some(tx) = queues.waiters.pop_front() {
                    if tx.send(()).is_ok() {
                        num -= 1;
                    }
                } else {
                    break;
                }
            }
        }
    }

    /// Waits for transport write capacity before encoding an outgoing payload chunk.
    ///
    /// Returns `SendPacketError::Disconnected` when closed; does not check for an open stream.
    pub(super) async fn want_payload_stream(&self) -> Result<(), SendPacketError> {
        if self.is_active() && (!self.io.is_wr_backpressure() || self.wait_write_ready().await) {
            Ok(())
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Rejects starting a publish while another outgoing payload is incomplete.
    fn check_streaming(&self) -> Result<(), EncodeError> {
        if self.streaming_remaining.get().is_some() {
            Err(EncodeError::ExpectPayload)
        } else {
            Ok(())
        }
    }

    /// Computes remaining outgoing payload bytes after an optional initial chunk.
    ///
    /// The caller applies this value only after encoding succeeds.
    ///
    /// # Panics
    ///
    /// In debug builds, panics if the initial chunk exceeds `pkt.payload_size`.
    fn streaming_size(pkt: &Publish, payload: Option<&Bytes>) -> Option<num::NonZeroU32> {
        let len = payload.map_or(0, Bytes::len);
        num::NonZeroU32::new(pkt.payload_size - len as u32)
    }

    /// Processes an acknowledgment and closes the connection on a protocol violation.
    ///
    /// # Panics
    ///
    /// Panics if a nonblocking PUBACK arrives without an installed publish callback.
    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), MqttProtocolError> {
        self.pkt_ack_inner(ack).inspect_err(|_| {
            self.close();
        })
    }

    /// Matches an ack to its flow, enforces PUBACK order, and advances or completes it.
    ///
    /// Successful PUBREC keeps the flow in flight until PUBCOMP and automatically sends
    /// PUBREL if its caller has gone away; completion returns send credit.
    ///
    /// # Panics
    ///
    /// Panics if a nonblocking PUBACK arrives without an installed publish callback.
    fn pkt_ack_inner(&self, pkt: Ack) -> Result<(), MqttProtocolError> {
        let mut queues = self.queues.borrow_mut();
        let idx = pkt.packet_id();

        // acks are matched by packet id, the order is defined only within
        // PUBACK, PUBREC and PUBREL flows (MQTT 3.1.1, 4.6)
        let Some(&mut (ref mut tx, tp)) = queues.inflight.get_mut(&idx) else {
            log::trace!("MQTT protocol error, unknown packet id: {idx}");
            return Err(MqttProtocolError::unknown_packet_id());
        };

        if !pkt.is_match(tp) {
            // ack type must match the in-flight packet, PUBREC acknowledges only
            // a QoS 2 PUBLISH and PUBCOMP only a PUBREL (MQTT 3.1.1, 4.3.2, 4.3.3)
            log::trace!(
                "MQTT protocol error, unexpected packet {}, {}",
                pkt.packet_type(),
                tp.expected_str()
            );
            Err(MqttProtocolError::unexpected_packet(
                pkt.packet_type(),
                tp.expected_str(),
            ))
        } else if matches!(pkt, Ack::Receive(_)) {
            // get publish ack channel
            log::trace!("Ack packet with id: {idx}");

            if tx.take().is_none_or(|tx| tx.send(pkt).is_err()) {
                // the publish future is dropped, nothing can release the publish,
                // PUBREC must be answered with PUBREL (MQTT 3.1.1, 4.3.3)
                log::trace!("Release dropped publish with id: {idx}");
                let _ = self.io.encode(
                    Encoded::Packet(codec::Packet::PublishRelease { packet_id: idx }),
                    self,
                );
                queues.inflight.insert(idx, (None, AckType::Complete));
            } else {
                let (tx, rx) = self.pool.queue.channel();
                queues.rx.insert(idx, rx);
                queues.inflight.insert(idx, (Some(tx), AckType::Complete));
            }
            Ok(())
        } else {
            if matches!(pkt, Ack::Publish(_)) {
                if queues.pubacks.front() != Some(&idx) {
                    log::trace!("MQTT protocol error, out of order PUBACK: {idx}");
                    return Err(MqttProtocolError::generic_violation(
                        "Received PUBACK out of order",
                    ));
                }
                queues.pubacks.pop_front();
            }

            // get publish ack channel
            log::trace!("Ack packet with id: {idx}");
            let tx = queues.inflight.remove(&idx).and_then(|(tx, _)| tx);

            if matches!(pkt, Ack::Complete(_)) {
                queues.rx.remove(&idx);
            }
            if let Some(tx) = tx {
                let _ = tx.send(pkt);
            } else if matches!(pkt, Ack::Publish(_)) {
                let cb = self.on_publish_ack.take().unwrap();
                (*cb)(idx, false);
                self.on_publish_ack.set(Some(cb));
            }

            // wake up queued request (receive max limit)
            self.wake_waiters_inner(&mut queues);
            Ok(())
        }
    }

    /// Encodes a request and registers its expected acknowledgment after success.
    ///
    /// Rejects identifiers already in flight. Encoding errors leave no in-flight
    /// entry behind, so the identifier and send credit remain available.
    pub(super) fn send_with_response(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
        pkt: codec::Packet,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight.contains_key(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(pkt.into(), self) {
                Ok(()) => {
                    let (tx, rx) = self.pool.queue.channel();
                    queues.inflight.insert(id, (Some(tx), ack));
                    Ok(rx)
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    /// Encodes a publish and registers its expected acknowledgment after success.
    ///
    /// Rejects an in-use identifier or an overlapping stream; `QoS 1` publishes are
    /// also queued for PUBACK ordering. The initial payload must fit `pkt.payload_size`.
    ///
    /// # Panics
    ///
    /// In debug builds, panics if the initial payload exceeds its declared size.
    pub(super) fn wait_publish_response(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        self.check_streaming()?;
        let remaining = Self::streaming_size(&pkt, payload.as_ref());

        let mut queues = self.queues.borrow_mut();
        if queues.inflight.contains_key(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(()) => {
                    self.streaming_remaining.set(remaining);
                    let (tx, rx) = self.pool.queue.channel();
                    queues.inflight.insert(id, (Some(tx), ack));
                    if matches!(ack, AckType::Publish) {
                        queues.pubacks.push_back(id);
                    }
                    Ok(rx)
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    /// Encodes a publish and tracks its ack without allocating a response receiver.
    ///
    /// Rejects an in-use packet identifier or an overlapping stream. Completion is
    /// reported through the installed publish callback for `QoS 1`.
    /// The initial payload must not exceed `pkt.payload_size`.
    ///
    /// # Panics
    ///
    /// Panics after successful encoding if no publish callback was installed.
    /// In debug builds, panics if the initial payload exceeds its declared size.
    pub(super) fn wait_publish_response_no_block(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<(), SendPacketError> {
        self.check_streaming()?;
        let remaining = Self::streaming_size(&pkt, payload.as_ref());

        let mut queues = self.queues.borrow_mut();
        if queues.inflight.contains_key(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(()) => {
                    self.streaming_remaining.set(remaining);
                    assert!(
                        self.flags.get().contains(Flags::ON_PUBLISH_ACK),
                        "Publish ack callback is not set"
                    );
                    queues.inflight.insert(id, (None, ack));
                    if matches!(ack, AckType::Publish) {
                        queues.pubacks.push_back(id);
                    }
                    Ok(())
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    /// Waits for write backpressure to clear and send credit to become available.
    ///
    /// Returns `false` if the connection closes; readiness does not reserve credit.
    ///
    /// A woken credit waiter checks again, a packet sent without waiting may take
    /// the credit first or write backpressure may be enabled again, the waiter is
    /// queued again in front of the others.
    pub(super) async fn wait_readiness(&self) -> bool {
        let mut woken = false;
        loop {
            if !self.wait_writable().await {
                return false;
            }
            let rx = {
                let mut queues = self.queues.borrow_mut();
                if queues.inflight.len() < self.cap.get() {
                    return true;
                }
                let (tx, rx) = self.pool.waiters.channel();
                if woken {
                    queues.waiters.push_front(tx);
                } else {
                    queues.waiters.push_back(tx);
                }
                rx
            };
            if rx.await.is_err() {
                return false;
            }
            woken = true;
        }
    }

    /// Encodes PUBREL and takes the PUBCOMP receiver retained after a successful PUBREC.
    ///
    /// Fails with `SendPacketError::UnexpectedRelease` if this flow has no stored receiver.
    pub(super) fn release_publish(
        &self,
        id: num::NonZeroU16,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let Some(rx) = self.queues.borrow_mut().rx.remove(&id) else {
            return Err(SendPacketError::UnexpectedRelease);
        };
        match self.io.encode(
            Encoded::Packet(codec::Packet::PublishRelease { packet_id: id }),
            self,
        ) {
            Ok(()) => Ok(rx),
            Err(e) => Err(SendPacketError::Encode(e)),
        }
    }
}

impl Encoder for MqttShared {
    type Item = Encoded;
    type Error = EncodeError;

    fn encode(&self, item: Self::Item, dst: &mut BytePages) -> Result<(), Self::Error> {
        match item {
            // packets cannot be written in the middle of a publish payload,
            // they are written after the payload is complete
            Encoded::Packet(pkt) if self.codec.is_encoding_payload() => {
                let mut buf = self
                    .deferred
                    .take()
                    .unwrap_or_else(|| BytePages::new(self.io.cfg().write_size()));
                let res = codec::Codec::encode_packet(&pkt, &mut buf);
                self.deferred.set(Some(buf));
                res
            }
            Encoded::PayloadChunk(_) => {
                self.codec.encode(item, dst)?;
                if !self.codec.is_encoding_payload()
                    && let Some(mut buf) = self.deferred.take()
                {
                    buf.move_to(dst);
                }
                Ok(())
            }
            _ => self.codec.encode(item, dst),
        }
    }
}

impl FrameState for MqttShared {
    type Response = codec::Packet;
    type Queued = codec::Packet;

    #[inline]
    fn is_partial(&self) -> bool {
        self.codec.is_payload_pending()
    }

    #[inline]
    fn is_ordered(&self, item: &codec::Decoded) -> bool {
        // mqtt orders acks of publish packets only
        // payload chunks have no response, they do not keep a queue slot
        // while the publish handler is pending
        match item {
            codec::Decoded::Publish(publish, ..) => publish.qos != QoS::AtMostOnce,
            codec::Decoded::Packet(pkt, _) => !matches!(
                pkt,
                codec::Packet::PingRequest
                    | codec::Packet::Subscribe { .. }
                    | codec::Packet::Unsubscribe { .. }
                    | codec::Packet::PublishRelease { .. }
            ),
            codec::Decoded::PayloadChunk(..) => false,
        }
    }
}

impl Decoder for MqttShared {
    type Item = codec::Decoded;
    type Error = DecodeError;

    #[inline]
    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.codec.decode(src)
    }
}

impl Ack {
    /// Returns the MQTT control-packet type for protocol error reporting.
    pub(super) fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Receive(_) => packet_type::PUBREC,
            Ack::Complete(_) => packet_type::PUBCOMP,
            Ack::Subscribe { .. } => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    /// Returns the identifier used to match this ack to an outbound operation.
    pub(super) fn packet_id(&self) -> num::NonZeroU16 {
        match self {
            Ack::Subscribe { packet_id, .. } => *packet_id,
            Ack::Publish(id) | Ack::Receive(id) | Ack::Complete(id) | Ack::Unsubscribe(id) => *id,
        }
    }

    /// Consumes the acknowledgment and extracts its SUBACK data.
    ///
    /// # Panics
    ///
    /// Panics unless this is `Ack::Subscribe`.
    pub(super) fn subscribe(self) -> Vec<codec::SubscribeReturnCode> {
        if let Ack::Subscribe { status, .. } = self {
            status
        } else {
            panic!()
        }
    }

    /// Checks whether this acknowledgment has the type expected by its in-flight flow.
    pub(super) fn is_match(&self, tp: AckType) -> bool {
        match (self, tp) {
            (Ack::Publish(_), AckType::Publish)
            | (Ack::Receive(_), AckType::Receive)
            | (Ack::Complete(_), AckType::Complete)
            | (Ack::Subscribe { .. }, AckType::Subscribe)
            | (Ack::Unsubscribe(_), AckType::Unsubscribe) => true,
            (_, _) => false,
        }
    }
}

impl AckType {
    /// Returns a diagnostic naming the acknowledgment packet expected by this flow.
    pub(super) fn expected_str(self) -> &'static str {
        match self {
            AckType::Publish => "Expected PUBACK packet",
            AckType::Receive => "Expected PUBREC packet",
            AckType::Complete => "Expected PUBCOMP packet",
            AckType::Subscribe => "Expected SUBACK packet",
            AckType::Unsubscribe => "Expected UNSUBACK packet",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;
    use ntex_util::time::{Millis, timeout};

    /// STATEMENT: Automatic packet identifiers wrap from `u16::MAX` to one without encoding zero.
    ///
    /// HOW:
    /// - Set the identifier cursor just before wraparound and send two nonblocking publications.
    /// - Decode their identifiers at the peer, acknowledge them in order, and verify all send credit returns.
    #[ntex::test]
    async fn test_packet_id_wraps_on_wire() {
        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let peer = Io::new(client, SharedCfg::new("peer"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(2);
        shared.inflight_idx.set(u16::MAX - 1);
        let sink = crate::v3::MqttSink::new(shared.clone());
        sink.publish_ack_cb(|_, _| {});
        for _ in 0..2 {
            sink.publish("a")
                .send_at_least_once_no_block(Bytes::new())
                .unwrap();
        }
        let codec = codec::Codec::default();
        for id in [u16::MAX, 1] {
            let received = timeout(Millis(2000), peer.recv(&codec))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let codec::Decoded::Publish(packet, _, _, _) = received else {
                panic!("expected publication")
            };
            assert_eq!(packet.packet_id, num::NonZeroU16::new(id));
            shared
                .pkt_ack(Ack::Publish(num::NonZeroU16::new(id).unwrap()))
                .unwrap();
        }
        assert_eq!(sink.credit(), 2);
    }

    /// STATEMENT: Internal write and credit readiness waits reject an already closed transport.
    ///
    /// HOW:
    /// - Create shared connection state with available credit and force-close its transport.
    /// - Assert write and credit waits return false and payload writes return `Disconnected`.
    #[ntex::test]
    async fn test_closed_transport_readiness() {
        let (_peer, server) = IoTest::create();
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = MqttShared::new(io.get_ref(), codec::Codec::default(), true, Rc::default());
        shared.set_cap(4);
        shared.force_close();
        assert!(!shared.wait_write_unblocked().await);
        assert!(!shared.wait_readiness().await);
        assert_eq!(
            shared.want_payload_stream().await,
            Err(SendPacketError::Disconnected)
        );
    }

    /// STATEMENT: A publish waiting for write capacity rechecks whether another streaming payload has started.
    ///
    /// HOW:
    /// - Block writes, poll publish readiness until pending, and encode a streaming header while it waits.
    /// - Drain writes and assert readiness stays pending until the stream's final byte is encoded.
    /// - Complete the payload and assert the readiness waiter succeeds.
    #[ntex::test]
    async fn test_stream_started_during_write_wait() {
        use ntex_util::future::lazy;
        use std::{future::Future, pin::pin};

        let (peer, transport) = IoTest::create();
        peer.remote_buffer_cap(0);
        let io = Io::new(transport, SharedCfg::new("test"));
        let shared = MqttShared::new(io.get_ref(), codec::Codec::default(), true, Rc::default());
        io.encode_slice(&vec![0; 128 * 1024]).unwrap();
        assert!(io.is_wr_backpressure());
        let mut readiness = pin!(shared.wait_publish_ready());
        assert!(lazy(|cx| readiness.as_mut().poll(cx).is_pending()).await);
        shared
            .encode_publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtMostOnce,
                    topic: "a".into(),
                    packet_id: None,
                    payload_size: 1,
                },
                None,
            )
            .unwrap();
        peer.remote_buffer_cap(1024 * 1024);
        timeout(Millis(2000), io.write_ready())
            .await
            .unwrap()
            .unwrap();
        assert!(lazy(|cx| readiness.as_mut().poll(cx).is_pending()).await);
        assert!(shared.is_streaming());
        assert_eq!(
            shared.encode_publish_payload(Bytes::from_static(b"x")),
            Ok(false)
        );
        assert!(timeout(Millis(2000), readiness).await.unwrap());
    }
}
