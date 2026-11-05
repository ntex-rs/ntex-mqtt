#![allow(clippy::type_complexity)]
use std::task::{Context, Poll, Waker};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, fmt, num, pin::Pin, rc::Rc};

use ntex_bytes::{BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};
use ntex_io::IoRef;
use ntex_util::{HashMap, channel::pool};

use crate::io::{FrameState, STREAM_TAG};
use crate::v5::codec::{self, Decoded, Encoded, Packet, Publish};
use crate::{QoS, error, error::SendPacketError, payload::PayloadSource, types::packet_type};

bitflags::bitflags! {
    /// Connection role and lifecycle markers, with negotiated options where applicable.
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct Flags: u16 {
        /// Marks that a callback for nonblocking publish acknowledgments was installed.
        const ON_PUBLISH_ACK  = 0b0000_0010; // on-publish-ack callback

        /// Selects `QoS::AtLeastOnce` as the maximum accepted incoming publish quality.
        const QOS_ATLEAST     = 0b0000_0100; // AtLeastOnce
        /// Selects `QoS::ExactlyOnce` as the maximum accepted incoming publish quality.
        const QOS_EXACTLY     = 0b0000_1000; // ExactlyOnce

        /// Records a zero session expiry interval in the received CONNECT packet.
        const ZERO_SES_EXPIRY = 0b0001_0000; // Session expiry is zero in Connect

        /// Records that a DISCONNECT send has been claimed, even if encoding fails.
        const DISCONNECT      = 0b0010_0000; // Disconnect frame is sent
        /// Records that the peer has sent DISCONNECT.
        const DISCONNECT_RECV = 0b0100_0000; // Disconnect frame is received
        /// Marker for a dispatcher stop notification.
        const STOPPED         = 0b1000_0000; // DispatchItem::Stop() is sent
        /// Marks the client side of the connection.
        const CLIENT          = 0b1_0000_0000; // Client side of the connection
        /// Set after PINGREQ until the matching PINGRESP is received.
        const PING_PENDING    = 0b10_0000_0000; // PINGREQ is sent, PINGRESP is expected
    }
}

/// Connection state shared by the MQTT 5.0 sink, codec, and dispatcher.
///
/// Tracks outgoing acknowledgments, streaming payloads, and send capacity for one connection.
pub struct MqttShared {
    /// Connection handle used to encode packets, wait for writes, and close transport.
    io: IoRef,
    /// Outbound publish limit negotiated from local configuration and peer Receive Maximum.
    cap: Cell<usize>,
    /// Inbound publish limit advertised locally; zero disables the dispatcher limit check.
    receive_max: Cell<u16>,
    /// Largest topic alias accepted on incoming publishes; zero disables aliases.
    topic_alias_max: Cell<u16>,
    /// Last generated packet identifier; reset to zero after allocating `u16::MAX`.
    inflight_idx: Cell<u16>,
    /// Acknowledgment tracking and send-credit waiters for this connection.
    queues: RefCell<MqttSharedQueues>,
    /// Encoding failure retained when an outgoing streaming payload is abandoned.
    encode_error: Cell<Option<error::EncodeError>>,
    /// Outgoing payload bytes still required; `None` when no streaming publish is open.
    streaming_remaining: Cell<Option<num::NonZeroU32>>,
    /// Encoded non-publish packets buffered until the outgoing payload completes.
    deferred: Cell<Option<BytePages>>,
    /// Callback for nonblocking publishes; disconnects supply a synthetic ack and `true`.
    on_publish_ack: Cell<Option<Box<dyn Fn(codec::PublishAck, bool)>>>,
    /// Source through which the dispatcher delivers an incoming streaming payload.
    pub(super) payload: PayloadSource,
    /// Connection role, negotiated options, and shutdown/keep-alive markers.
    pub(super) flags: Cell<Flags>,
    /// Reusable acknowledgment and notification channels shared with other connections.
    pub(super) pool: Rc<MqttSinkPool>,
    /// Per-connection codec, including partial incoming and outgoing payload state.
    pub(super) codec: codec::Codec,
}

impl fmt::Debug for MqttShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttShared").finish()
    }
}

/// Outgoing acknowledgment flows and send-credit bookkeeping.
#[derive(Debug)]
pub(super) struct MqttSharedQueues {
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
    /// Count of subscription flows in `inflight`, excluded from publish credit.
    // SUBSCRIBE and UNSUBSCRIBE packets in `inflight`
    subscribes: usize,
    /// Send-credit waiters in ticket order, retained until polled or dropped.
    // publishes that wait for send credit, in the order of arrival
    waiters: VecDeque<Waiter>,
    /// Send credit held by granted waiters or permits, released on their drop.
    // send credit granted to waiters and held by permits, not in `inflight` yet
    reserved: usize,
    /// Next monotonically increasing identifier for a queued send-credit waiter.
    next_ticket: u64,
    /// PUBCOMP receivers retained after PUBREC until release or completion.
    // PUBCOMP receivers, one per QoS 2 PUBLISH awaiting release
    rx: HashMap<num::NonZeroU16, pool::Receiver<Ack>>,
}

impl MqttSharedQueues {
    /// Counts in-flight `QoS 1` and `QoS 2` publishes, excluding subscription flows.
    ///
    /// Receive Maximum counts PUBLISH packets only [MQTT-4.9.0-2].
    fn publishes(&self) -> usize {
        self.inflight.len() - self.subscribes
    }

    /// Returns whether any queued publish still needs a send-credit grant.
    ///
    /// Waiting entries follow the granted and closed ones.
    fn has_waiting(&self) -> bool {
        self.waiters
            .back()
            .is_some_and(|w| w.state == WaiterState::Waiting)
    }

    /// Finds a queued waiter using ascending ticket order.
    ///
    /// # Panics
    ///
    /// Panics if the ticket no longer belongs to a queued waiter.
    fn find(&self, ticket: u64) -> usize {
        self.waiters
            .binary_search_by_key(&ticket, |w| w.ticket)
            .expect("waiter is queued")
    }
}

/// Queued publish waiting for, holding, or losing a reservation of send credit.
#[derive(Debug)]
struct Waiter {
    /// Stable identifier used to find this entry after other waiters leave.
    ticket: u64,
    /// Whether credit is still needed, reserved, or invalidated by queue cleanup.
    state: WaiterState,
    /// Most recent task waiting for a grant or connection closure.
    waker: Option<Waker>,
}

/// Lifecycle of a queued publish reservation.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum WaiterState {
    /// No send credit is reserved yet.
    Waiting,
    /// Send credit is reserved until the waiter yields a permit or is dropped.
    Granted,
    /// Queue cleanup has invalidated the waiter and released any reservation.
    Closed,
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

impl MqttShared {
    /// Creates shared state with no send capacity until the handshake sets its limit.
    pub(super) fn new(io: IoRef, codec: codec::Codec, pool: Rc<MqttSinkPool>) -> Self {
        Self {
            io,
            pool,
            codec,
            cap: Cell::new(0),
            queues: RefCell::new(MqttSharedQueues {
                inflight: HashMap::default(),
                pubacks: VecDeque::new(),
                subscribes: 0,
                waiters: VecDeque::new(),
                reserved: 0,
                next_ticket: 0,
                rx: HashMap::default(),
            }),
            receive_max: Cell::new(0),
            topic_alias_max: Cell::new(0),
            inflight_idx: Cell::new(0),
            flags: Cell::new(Flags::QOS_ATLEAST),
            payload: PayloadSource::default(),
            on_publish_ack: Cell::new(None),
            encode_error: Cell::new(None),
            streaming_remaining: Cell::new(None),
            deferred: Cell::new(None),
        }
    }

    /// Returns the transport tag used in connection diagnostics.
    pub(super) fn tag(&self) -> &'static str {
        self.io.tag()
    }

    /// Returns publish credit not already held by in-flight publishes or reservations.
    ///
    /// Subscription flows do not consume the peer's Receive Maximum [MQTT-4.9.0-1].
    pub(super) fn credit(&self) -> usize {
        let queues = self.queues.borrow();
        self.cap
            .get()
            .saturating_sub(queues.publishes() + queues.reserved)
    }

    /// Returns the inbound publish limit; zero disables the dispatcher limit check.
    pub(super) fn receive_max(&self) -> u16 {
        self.receive_max.get()
    }

    /// Returns the largest topic alias allowed on incoming publishes.
    pub(super) fn topic_alias_max(&self) -> u16 {
        self.topic_alias_max.get()
    }

    /// Returns the maximum quality of service allowed on incoming publishes.
    pub(super) fn max_qos(&self) -> QoS {
        let flags = self.flags.get();
        if flags.contains(Flags::QOS_ATLEAST) {
            QoS::AtLeastOnce
        } else if flags.contains(Flags::QOS_EXACTLY) {
            QoS::ExactlyOnce
        } else {
            QoS::AtMostOnce
        }
    }

    /// Sets the inbound publish limit, with zero disabling the dispatcher limit check.
    pub(super) fn set_receive_max(&self, val: u16) {
        self.receive_max.set(val);
    }

    /// Sets the largest incoming topic alias; zero disables topic aliases.
    pub(super) fn set_topic_alias_max(&self, val: u16) {
        self.topic_alias_max.set(val);
    }

    /// Sets mutually exclusive flags for the maximum incoming publish quality of service.
    pub(super) fn set_max_qos(&self, val: QoS) {
        let mut flags = self.flags.get();
        match val {
            QoS::AtLeastOnce => {
                flags.insert(Flags::QOS_ATLEAST);
                flags.remove(Flags::QOS_EXACTLY);
            }
            QoS::ExactlyOnce => {
                flags.insert(Flags::QOS_EXACTLY);
                flags.remove(Flags::QOS_ATLEAST);
            }
            QoS::AtMostOnce => {
                flags.remove(Flags::QOS_ATLEAST);
                flags.remove(Flags::QOS_EXACTLY);
            }
        }
        self.flags.set(flags);
    }

    /// Returns whether the received CONNECT specified zero session expiry.
    pub(super) fn is_zero_session_expiry(&self) -> bool {
        self.flags.get().contains(Flags::ZERO_SES_EXPIRY)
    }

    /// Returns whether this is the client side of the connection.
    pub(super) fn is_client(&self) -> bool {
        self.flags.get().contains(Flags::CLIENT)
    }

    /// Marks this connection as the client side.
    pub(super) fn set_client(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::CLIENT);
        self.flags.set(flags);
    }

    /// Records that the received CONNECT specified zero session expiry.
    pub(super) fn set_zero_session_expiry(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ZERO_SES_EXPIRY);
        self.flags.set(flags);
    }

    /// Requests graceful transport closure and fails pending sends.
    ///
    /// If active, attempts the supplied DISCONNECT unless one was already claimed.
    pub(super) fn close(&self, pkt: Option<codec::Disconnect>) {
        if self.is_active() {
            if let Some(pkt) = pkt
                && !self.is_disconnect_sent()
            {
                let _ = self.io.encode(Encoded::Packet(Packet::from(pkt)), self);
            }
            self.io.close();
        }
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
        self.encode_error
            .set(Some(error::EncodeError::PublishIncomplete));
    }

    /// Returns whether the transport still accepts connection operations.
    pub(super) fn is_active(&self) -> bool {
        self.io.is_active()
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

    /// Returns whether publish credit is available and write backpressure is off.
    ///
    /// Waiting publishes get send credit first. This does not check connection activity
    /// or whether an outgoing streaming payload prevents starting another publish.
    pub(super) fn is_ready(&self) -> bool {
        self.has_credit() && !self.io.is_wr_backpressure()
    }

    /// Returns whether a new publish can claim credit without bypassing waiting publishes.
    fn has_credit(&self) -> bool {
        self.credit() > 0 && !self.queues.borrow().has_waiting()
    }

    /// Claims the DISCONNECT marker and returns whether it was already set.
    ///
    /// The first call returns `false`; later calls return `true`, regardless of encoding success.
    pub(super) fn is_disconnect_sent(&self) -> bool {
        let mut flags = self.flags.get();
        let disconnect = flags.contains(Flags::DISCONNECT);
        if !disconnect {
            flags.insert(Flags::DISCONNECT);
            self.flags.set(flags);
        }
        disconnect
    }

    /// Records receipt of the peer's DISCONNECT packet.
    pub(super) fn set_disconnect_recv(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::DISCONNECT_RECV);
        self.flags.set(flags);
    }

    /// Returns whether the peer has sent DISCONNECT.
    pub(super) fn is_disconnect_recv(&self) -> bool {
        self.flags.get().contains(Flags::DISCONNECT_RECV)
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

    /// Allocates the next nonzero packet identifier, wrapping after `u16::MAX`.
    ///
    /// Does not skip identifiers still in flight; registration rejects collisions.
    pub(super) fn next_id(&self) -> num::NonZeroU16 {
        let idx = self.inflight_idx.get() + 1;
        self.inflight_idx.set(idx);
        let idx = if idx == u16::MAX {
            self.inflight_idx.set(0);
            u16::MAX
        } else {
            self.inflight_idx.set(idx);
            idx
        };
        num::NonZeroU16::new(idx).unwrap()
    }

    /// Updates the outbound publish limit and reserves newly available credit for waiters.
    pub(super) fn set_cap(&self, cap: usize) {
        self.cap.set(cap);
        self.grant(&mut self.queues.borrow_mut());
    }

    /// Installs the callback for nonblocking publish acks and disconnect notifications.
    ///
    /// The callback must not reenter shared state: acknowledgment queues remain borrowed
    /// while it runs, and the callback is temporarily removed from its cell.
    pub(super) fn set_publish_ack(&self, f: Box<dyn Fn(codec::PublishAck, bool)>) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ON_PUBLISH_ACK);
        self.flags.set(flags);
        self.on_publish_ack.set(Some(f));
    }

    /// Fails pending sends and optionally closes the transport without DISCONNECT.
    pub(super) fn drop_sink(&self, io: bool) {
        self.clear_queues();
        if io {
            self.io.close();
        }
    }

    /// Fails the incoming payload source with a cloned, converted dispatcher error.
    pub(super) fn drop_payload<E>(&self, err: &E)
    where
        E: Clone,
        error::PayloadError: From<E>,
    {
        self.payload.set_error(err.clone().into());
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
        queues.subscribes = 0;
        let MqttSharedQueues {
            waiters, reserved, ..
        } = &mut *queues;
        for waiter in waiters {
            if waiter.state == WaiterState::Granted {
                *reserved -= 1;
            }
            waiter.state = WaiterState::Closed;
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }

        if let Some(cb) = self.on_publish_ack.take() {
            for (idx, (tx, _)) in queues.inflight.drain() {
                if tx.is_none() {
                    (*cb)(
                        codec::PublishAck {
                            packet_id: idx,
                            ..Default::default()
                        },
                        true,
                    );
                }
            }
        } else {
            queues.inflight.clear();
        }
    }

    /// Reserves available send credit for waiting publishes in arrival order.
    ///
    /// A granted waiter keeps the credit until its publish is in flight, so that
    /// the number of in-flight publishes stays within the peer's Receive Maximum
    /// [MQTT-4.9.0-1].
    fn grant(&self, queues: &mut MqttSharedQueues) {
        let publishes = queues.publishes();
        let MqttSharedQueues {
            waiters, reserved, ..
        } = queues;
        for waiter in waiters.iter_mut() {
            if waiter.state != WaiterState::Waiting {
                continue;
            }
            if publishes + *reserved >= self.cap.get() {
                break;
            }
            waiter.state = WaiterState::Granted;
            *reserved += 1;
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }
    }

    /// Returns a future reserving send credit for one `QoS 1` or `QoS 2` publish.
    ///
    /// Publishes get the credit in the order of the calls, the waiter is queued
    /// immediately, not on the first poll. Immediate grants do not check connection activity.
    pub(super) fn send_permit(self: &Rc<Self>) -> WaitSendPermit {
        if self.has_credit() {
            self.queues.borrow_mut().reserved += 1;
            return WaitSendPermit {
                shared: self.clone(),
                ticket: None,
                permit: Some(SendPermit(self.clone())),
            };
        }

        let mut queues = self.queues.borrow_mut();
        let ticket = queues.next_ticket;
        queues.next_ticket += 1;
        queues.waiters.push_back(Waiter {
            ticket,
            state: WaiterState::Waiting,
            waker: None,
        });
        WaitSendPermit {
            shared: self.clone(),
            ticket: Some(ticket),
            permit: None,
        }
    }

    /// Reserves publish credit and waits for write backpressure to be released.
    ///
    /// The publish is queued for send credit immediately, the permit is held
    /// while waiting for write backpressure.
    pub(super) fn wait_send_ready(
        self: &Rc<Self>,
    ) -> impl Future<Output = Result<SendPermit, SendPacketError>> + use<> {
        let permit = self.send_permit();
        let shared = self.clone();
        async move {
            let permit = permit.await?;
            if shared.wait_writable().await {
                Ok(permit)
            } else {
                Err(SendPacketError::Disconnected)
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
    fn check_streaming(&self) -> Result<(), error::EncodeError> {
        if self.streaming_remaining.get().is_some() {
            Err(error::EncodeError::ExpectPayload)
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

    /// Encodes a packet, deferring its bytes until any outgoing publish payload completes.
    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), error::EncodeError> {
        self.io.encode(Encoded::Packet(pkt), self)
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
    ) -> Result<(), error::EncodeError> {
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
    pub(super) fn encode_publish_payload(
        &self,
        payload: Bytes,
    ) -> Result<bool, error::EncodeError> {
        if let Some(remaining) = self.streaming_remaining.get() {
            let len = payload.len() as u32;
            if len > remaining.get() {
                self.force_close();
                Err(error::EncodeError::OverPublishSize)
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
            Err(error::EncodeError::UnexpectedPayload)
        }
    }

    /// Processes an acknowledgment and closes the connection on a protocol violation.
    ///
    /// # Panics
    ///
    /// Panics if a nonblocking PUBACK arrives without an installed publish callback.
    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), error::MqttProtocolError> {
        // invalid ack is a protocol error, DISCONNECT uses its reason code,
        // 0x82 (Protocol Error) for unexpected or unknown acks (MQTT 5.0, 4.13.1)
        self.pkt_ack_inner(ack).inspect_err(|e| {
            self.close(Some(codec::Disconnect::from_proto_error(e)));
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
    fn pkt_ack_inner(&self, pkt: Ack) -> Result<(), error::MqttProtocolError> {
        let mut queues = self.queues.borrow_mut();
        let idx = pkt.packet_id();

        // acks are matched by packet id, the order is defined only within
        // PUBACK, PUBREC and PUBREL flows (MQTT 5.0, 4.6)
        let Some(&mut (ref mut tx, tp)) = queues.inflight.get_mut(&idx) else {
            log::trace!("MQTT protocol error, unknown packet id: {idx}");
            return Err(error::MqttProtocolError::unknown_packet_id());
        };

        if !pkt.is_match(tp) {
            // ack type must match the in-flight packet, PUBREC acknowledges only
            // a QoS 2 PUBLISH and PUBCOMP only a PUBREL (MQTT 5.0, 4.3.2, 4.3.3)
            log::trace!(
                "MQTT protocol error, unexpected packet {}, {}",
                pkt.packet_type(),
                tp.expected_str()
            );
            return Err(error::MqttProtocolError::unexpected_packet(
                pkt.packet_type(),
                tp.expected_str(),
            ));
        }

        if let Ack::Receive(ref ack) = pkt
            && u8::from(ack.reason_code) < 0x80
        {
            // get publish ack channel
            log::trace!("Ack packet receive with id: {idx}");

            if tx.take().is_none_or(|tx| tx.send(pkt).is_err()) {
                // the publish future is dropped, nothing can release the publish,
                // PUBREL must be sent for a PUBREC below 0x80 [MQTT-4.3.3-4]
                log::trace!("Release dropped publish with id: {idx}");
                let _ = self.io.encode(
                    Encoded::Packet(Packet::PublishRelease(codec::PublishAck2 {
                        packet_id: idx,
                        ..Default::default()
                    })),
                    self,
                );
                queues.inflight.insert(idx, (None, AckType::Complete));
            } else {
                let (tx, rx) = self.pool.queue.channel();
                queues.rx.insert(idx, rx);
                queues.inflight.insert(idx, (Some(tx), AckType::Complete));
            }
            return Ok(());
        }

        if matches!(pkt, Ack::Publish(_)) {
            if queues.pubacks.front() != Some(&idx) {
                log::trace!("MQTT protocol error, out of order PUBACK: {idx}");
                return Err(error::MqttProtocolError::generic_violation(
                    "Received PUBACK out of order",
                ));
            }
            queues.pubacks.pop_front();
        }

        // PUBREL is sent only for a PUBREC with a reason code below 0x80
        // [MQTT-4.3.3-4], a failed PUBREC ends the QoS 2 flow and the packet id
        // is available for reuse (MQTT 5.0, 4.3.3)
        log::trace!("Ack packet with id: {idx}");
        let tx = queues.inflight.remove(&idx).and_then(|(tx, _)| tx);
        if matches!(tp, AckType::Subscribe | AckType::Unsubscribe) {
            queues.subscribes -= 1;
        }

        if matches!(pkt, Ack::Complete(_)) {
            queues.rx.remove(&idx);
        }
        if let Some(tx) = tx {
            let _ = tx.send(pkt);
        } else if matches!(pkt, Ack::Publish(_)) {
            let cb = self.on_publish_ack.take().unwrap();
            (*cb)(pkt.publish(), false);
            self.on_publish_ack.set(Some(cb));
        }

        // wake up queued request (receive max limit)
        self.grant(&mut queues);
        Ok(())
    }

    /// Registers an expected acknowledgment and returns its response receiver.
    ///
    /// Rejects identifiers already in flight; the caller encodes the request separately.
    pub(super) fn wait_response(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight.contains_key(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            let (tx, rx) = self.pool.queue.channel();
            queues.inflight.insert(id, (Some(tx), ack));
            if matches!(ack, AckType::Subscribe | AckType::Unsubscribe) {
                queues.subscribes += 1;
            }
            Ok(rx)
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
    /// reported through the installed publish callback for `QoS 1`; if absent,
    /// processing PUBACK will panic later. The initial payload must fit `pkt.payload_size`.
    ///
    /// # Panics
    ///
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

    /// Waits for transport write capacity without consuming publish credit.
    ///
    /// Receive Maximum limits `QoS 1` and `QoS 2` PUBLISH packets only
    /// [MQTT-4.9.0-2], SUBSCRIBE and UNSUBSCRIBE do not wait for send credit.
    /// The I/O write task wakes waiters directly; a write timeout is reported
    /// as disconnect. Backpressure can be released while I/O is closing,
    /// a packet registered after `clear_queues` would never be acked.
    pub(super) async fn wait_wr_readiness(&self) -> Result<(), SendPacketError> {
        if self.io.write_ready().await.is_ok() && self.is_active() {
            Ok(())
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Encodes PUBREL and takes the PUBCOMP receiver retained after a successful PUBREC.
    ///
    /// Fails with `SendPacketError::UnexpectedRelease` if this flow has no stored receiver.
    pub(super) fn release_publish(
        &self,
        pkt: codec::PublishAck2,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let Some(rx) = self.queues.borrow_mut().rx.remove(&pkt.packet_id) else {
            return Err(SendPacketError::UnexpectedRelease);
        };

        match self
            .io
            .encode(Encoded::Packet(codec::Packet::PublishRelease(pkt)), self)
        {
            Ok(()) => Ok(rx),
            Err(e) => Err(SendPacketError::Encode(e)),
        }
    }
}

/// One reserved send-credit slot for a `QoS 1` or `QoS 2` publish.
///
/// Dropping the permit releases its reservation and grants waiting publishes.
/// Register the publish in flight before dropping it to transfer accounting
/// from the reservation to the outstanding publish.
pub(super) struct SendPermit(Rc<MqttShared>);

impl Drop for SendPermit {
    fn drop(&mut self) {
        let mut queues = self.0.queues.borrow_mut();
        queues.reserved -= 1;
        self.0.grant(&mut queues);
    }
}

/// Future yielding the send-credit reservation requested by [`MqttShared::send_permit`].
///
/// Fails if the connection is closed while the publish waits. A dropped waiter
/// passes granted credit to the next one. An immediate grant does not check connection activity.
pub(super) struct WaitSendPermit {
    /// Keeps the connection and reservation queue alive until completion or cancellation.
    shared: Rc<MqttShared>,
    /// Queued entry to remove on completion or drop; absent for immediate grants.
    ticket: Option<u64>,
    /// Immediately reserved credit, returned by the first poll without queueing.
    permit: Option<SendPermit>,
}

impl Future for WaitSendPermit {
    type Output = Result<SendPermit, SendPacketError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(permit) = this.permit.take() {
            return Poll::Ready(Ok(permit));
        }
        let Some(ticket) = this.ticket else {
            return Poll::Ready(Err(SendPacketError::Disconnected));
        };

        let mut queues = this.shared.queues.borrow_mut();
        let idx = queues.find(ticket);
        let waiter = &mut queues.waiters[idx];
        let state = waiter.state;
        if state == WaiterState::Waiting {
            match waiter.waker {
                Some(ref mut waker) => waker.clone_from(cx.waker()),
                None => waiter.waker = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        }
        queues.waiters.remove(idx);
        this.ticket = None;

        if state == WaiterState::Granted {
            Poll::Ready(Ok(SendPermit(this.shared.clone())))
        } else {
            Poll::Ready(Err(SendPacketError::Disconnected))
        }
    }
}

impl Drop for WaitSendPermit {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            let mut queues = self.shared.queues.borrow_mut();
            let idx = queues.find(ticket);
            if queues.waiters.remove(idx).map(|w| w.state) == Some(WaiterState::Granted) {
                queues.reserved -= 1;
                self.shared.grant(&mut queues);
            }
        }
    }
}

impl Encoder for MqttShared {
    type Item = Encoded;
    type Error = error::EncodeError;

    fn encode(&self, item: Self::Item, dst: &mut BytePages) -> Result<(), Self::Error> {
        match item {
            // packets cannot be written in the middle of a publish payload,
            // they are written after the payload is complete
            Encoded::Packet(pkt) if self.codec.is_encoding_payload() => {
                let mut buf = self
                    .deferred
                    .take()
                    .unwrap_or_else(|| BytePages::new(self.io.cfg().write_size()));
                let res = self.codec.encode_packet(pkt, &mut buf);
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

/// Packet response retained until its ordered dispatcher call can be written.
///
/// Common publish acknowledgments are stored inline; DISCONNECT keeps its existing
/// box, and other packet types are boxed to keep queue entries compact.
#[derive(Debug)]
pub enum QueuedPacket {
    /// PUBACK response for an incoming `QoS 1` publish.
    PublishAck(codec::PublishAck),
    /// PUBREC response for an incoming `QoS 2` publish.
    PublishReceived(codec::PublishAck),
    /// DISCONNECT response retaining the original packet allocation.
    Disconnect(Box<codec::Disconnect>),
    /// Any other response packet, boxed to keep the queue entry small.
    Other(Box<Packet>),
}

impl From<Packet> for QueuedPacket {
    fn from(pkt: Packet) -> Self {
        match pkt {
            Packet::PublishAck(ack) => QueuedPacket::PublishAck(ack),
            Packet::PublishReceived(ack) => QueuedPacket::PublishReceived(ack),
            Packet::Disconnect(pkt) => QueuedPacket::Disconnect(pkt),
            pkt => QueuedPacket::Other(Box::new(pkt)),
        }
    }
}

impl From<QueuedPacket> for Encoded {
    fn from(pkt: QueuedPacket) -> Self {
        Encoded::Packet(match pkt {
            QueuedPacket::PublishAck(ack) => Packet::PublishAck(ack),
            QueuedPacket::PublishReceived(ack) => Packet::PublishReceived(ack),
            QueuedPacket::Disconnect(pkt) => Packet::Disconnect(pkt),
            QueuedPacket::Other(pkt) => *pkt,
        })
    }
}

impl FrameState for MqttShared {
    type Response = Packet;
    type Queued = QueuedPacket;

    #[inline]
    fn is_partial(&self) -> bool {
        self.codec.is_payload_pending()
    }

    #[inline]
    fn is_ordered(&self, item: &Decoded) -> bool {
        // mqtt orders acks of publish packets only [MQTT-4.6.0-2..4], other
        // packets flow during re-authentication [MQTT 5.0, 4.12.1]
        // payload chunks have no response, they do not keep a queue slot
        // while the publish handler is pending
        match item {
            Decoded::Publish(publish, ..) => publish.qos != QoS::AtMostOnce,
            Decoded::Packet(pkt, _) => !matches!(
                pkt,
                Packet::PingRequest
                    | Packet::Subscribe(_)
                    | Packet::Unsubscribe(_)
                    | Packet::PublishRelease(_)
                    | Packet::Auth(_)
            ),
            Decoded::PayloadChunk(..) => false,
        }
    }
}

impl Decoder for MqttShared {
    type Item = Decoded;
    type Error = error::DecodeError;

    #[inline]
    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.codec.decode(src)
    }
}

/// Acknowledgment type expected for an outstanding packet identifier.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

/// Received acknowledgment routed to the corresponding outbound operation.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Ack {
    /// PUBACK completing a `QoS 1` publish.
    Publish(codec::PublishAck),
    /// PUBREC acknowledging receipt of a `QoS 2` publish.
    Receive(codec::PublishAck),
    /// PUBCOMP completing the `QoS 2` release handshake.
    Complete(codec::PublishAck2),
    /// SUBACK carrying subscription results.
    Subscribe(codec::SubscribeAck),
    /// UNSUBACK completing an unsubscribe operation.
    Unsubscribe(codec::UnsubscribeAck),
}

impl Ack {
    /// Returns the MQTT control-packet type for protocol error reporting.
    pub(super) fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Receive(_) => packet_type::PUBREC,
            Ack::Complete(_) => packet_type::PUBCOMP,
            Ack::Subscribe(_) => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    /// Returns the identifier used to match this ack to an outbound operation.
    pub(super) fn packet_id(&self) -> num::NonZeroU16 {
        match self {
            Ack::Publish(pkt) | Ack::Receive(pkt) => pkt.packet_id,
            Ack::Complete(pkt) => pkt.packet_id,
            Ack::Subscribe(pkt) => pkt.packet_id,
            Ack::Unsubscribe(pkt) => pkt.packet_id,
        }
    }

    /// Consumes the acknowledgment and extracts its PUBACK data.
    ///
    /// # Panics
    ///
    /// Panics unless this is `Ack::Publish`.
    pub(super) fn publish(self) -> codec::PublishAck {
        if let Ack::Publish(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    /// Consumes the acknowledgment and extracts its PUBREC data.
    ///
    /// # Panics
    ///
    /// Panics unless this is `Ack::Receive`.
    pub(super) fn receive(self) -> codec::PublishAck {
        if let Ack::Receive(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    /// Consumes the acknowledgment and extracts its SUBACK data.
    ///
    /// # Panics
    ///
    /// Panics unless this is `Ack::Subscribe`.
    pub(super) fn subscribe(self) -> codec::SubscribeAck {
        if let Ack::Subscribe(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    /// Consumes the acknowledgment and extracts its UNSUBACK data.
    ///
    /// # Panics
    ///
    /// Panics unless this is `Ack::Unsubscribe`.
    pub(super) fn unsubscribe(self) -> codec::UnsubscribeAck {
        if let Ack::Unsubscribe(pkt) = self {
            pkt
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
            | (Ack::Subscribe(_), AckType::Subscribe)
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

/// Unit tests for compact ordered-response storage and packet conversions.
#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: Ordered responses preserve their packet values and specialized queue variants.
    ///
    /// HOW:
    /// - Round-trip PUBACK, PUBREC, DISCONNECT, and PINGRESP through `QueuedPacket`
    ///   into `Encoded` and compare each result with the original packet.
    /// - Confirm PUBREC and DISCONNECT use their dedicated `QueuedPacket` variants.
    #[test]
    fn queued_packet_roundtrip() {
        let ack = codec::PublishAck {
            packet_id: num::NonZeroU16::new(1).unwrap(),
            ..Default::default()
        };
        for pkt in [
            Packet::PublishAck(ack.clone()),
            Packet::PublishReceived(ack),
            Packet::from(codec::Disconnect::default()),
            Packet::PingResponse,
        ] {
            assert_eq!(
                Encoded::from(QueuedPacket::from(pkt.clone())),
                Encoded::Packet(pkt)
            );
        }
        assert!(matches!(
            QueuedPacket::from(Packet::PublishReceived(codec::PublishAck::default())),
            QueuedPacket::PublishReceived(_)
        ));
        // the boxed disconnect packet is kept, it is not boxed again
        assert!(matches!(
            QueuedPacket::from(Packet::from(codec::Disconnect::default())),
            QueuedPacket::Disconnect(_)
        ));
    }
}
