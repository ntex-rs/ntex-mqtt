#![allow(clippy::type_complexity)]
use std::{cell::Cell, cell::RefCell, collections::VecDeque, fmt, num, rc::Rc};

use ntex_bytes::{BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};
use ntex_io::IoRef;
use ntex_util::{HashMap, HashSet, channel::pool};

use crate::error::{DecodeError, EncodeError, MqttProtocolError, PayloadError, SendPacketError};
use crate::io::{FrameState, STREAM_TAG};
use crate::v3::codec::{self, Encoded, Publish};
use crate::{QoS, payload::PlSender, types::packet_type};

#[derive(Debug)]
pub(super) enum Ack {
    Publish(num::NonZeroU16),
    Receive(num::NonZeroU16),
    Complete(num::NonZeroU16),
    Subscribe {
        packet_id: num::NonZeroU16,
        status: Vec<codec::SubscribeReturnCode>,
    },
    Unsubscribe(num::NonZeroU16),
}

#[derive(Copy, Clone, Debug)]
pub(super) enum AckType {
    Publish,
    Receive,
    Complete,
    Subscribe,
    Unsubscribe,
}

pub(super) struct MqttSinkPool {
    queue: pool::Pool<Ack>,
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
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Flags: u8 {
        const CLIENT          = 0b0000_0001;
        const ON_PUBLISH_ACK  = 0b0000_0100; // on-publish-ack callback
        const PING_PENDING    = 0b0000_1000; // PINGREQ is sent, PINGRESP is expected

        const DISCONNECT      = 0b0010_0000; // Disconnect frame is sent
        const STOPPED         = 0b1000_0000; // DispatchItem::Stop() is sent
    }
}

pub struct MqttShared {
    io: IoRef,
    cap: Cell<usize>,
    queues: RefCell<MqttSharedQueues>,
    inflight_idx: Cell<u16>,
    flags: Cell<Flags>,
    encode_error: Cell<Option<EncodeError>>,
    streaming_remaining: Cell<Option<num::NonZeroU32>>,
    /// Packets encoded while a publish payload is incomplete
    deferred: Cell<Option<BytePages>>,
    on_publish_ack: Cell<Option<Box<dyn Fn(num::NonZeroU16, bool)>>>,
    pub(super) payload: Cell<Option<PlSender>>,
    pub(super) codec: codec::Codec,
    pub(super) pool: Rc<MqttSinkPool>,
}

impl fmt::Debug for MqttShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttShared").finish()
    }
}

#[derive(Debug)]
struct MqttSharedQueues {
    inflight: VecDeque<(num::NonZeroU16, Option<pool::Sender<Ack>>, AckType)>,
    inflight_ids: HashSet<num::NonZeroU16>,
    waiters: VecDeque<pool::Sender<()>>,
    // PUBCOMP receivers, one per QoS 2 PUBLISH awaiting release
    rx: HashMap<num::NonZeroU16, pool::Receiver<Ack>>,
}

impl MqttShared {
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
                inflight: VecDeque::with_capacity(8),
                inflight_ids: HashSet::default(),
                waiters: VecDeque::new(),
                rx: HashMap::default(),
            }),
            inflight_idx: Cell::new(0),
            encode_error: Cell::new(None),
            streaming_remaining: Cell::new(None),
            deferred: Cell::new(None),
            on_publish_ack: Cell::new(None),
            payload: Cell::new(None),
        }
    }

    pub(super) fn tag(&self) -> &'static str {
        self.io.tag()
    }

    pub(super) fn is_client(&self) -> bool {
        self.flags.get().contains(Flags::CLIENT)
    }

    pub(super) fn close(&self) {
        if self.is_client() && !self.is_disconnect_sent() {
            let _ = self.encode_packet(codec::Packet::Disconnect);
        }
        self.io.close();
        self.clear_queues();
    }

    pub(super) fn force_close(&self) {
        self.io.terminate();
        self.clear_queues();
    }

    pub(super) fn streaming_dropped(&self) {
        self.force_close();
        self.encode_error.set(Some(EncodeError::PublishIncomplete));
    }

    pub(super) fn drop_payload<E>(&self, err: &E)
    where
        E: Clone,
        PayloadError: From<E>,
    {
        if let Some(pl) = self.payload.take() {
            pl.set_error(err.clone().into());
        }
    }

    pub(super) fn is_streaming(&self) -> bool {
        self.streaming_remaining.get().is_some()
    }

    /// Wait until a `QoS 0` publish can be encoded, returns `false` if the
    /// connection is closed
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

    /// Marks whether PINGREQ is sent and PINGRESP is not received yet
    pub(super) fn set_ping_pending(&self, pending: bool) {
        let mut flags = self.flags.get();
        flags.set(Flags::PING_PENDING, pending);
        self.flags.set(flags);
    }

    pub(super) fn is_ping_pending(&self) -> bool {
        self.flags.get().contains(Flags::PING_PENDING)
    }

    /// Wait until already encoded packets can be written, returns `false` if the
    /// connection is closed
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

    /// Wait until the write buffer drains to the release threshold of write
    /// backpressure, returns `false` if the connection is closed
    ///
    /// The backpressure flag is not checked again once the buffer is drained,
    /// it is cleared only when the io dispatcher runs.
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

    /// Wait for write backpressure to be released, returns `false` if the
    /// connection is closed
    ///
    /// Write backpressure is the io level flag, it is set as soon as the write
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

    pub(super) fn is_active(&self) -> bool {
        self.io.is_active()
    }

    pub(super) fn is_ready(&self) -> bool {
        self.credit() > 0 && !self.io.is_wr_backpressure()
    }

    pub(super) fn is_disconnect_sent(&self) -> bool {
        let mut flags = self.flags.get();
        let sent = flags.contains(Flags::DISCONNECT);
        if !sent {
            flags.insert(Flags::DISCONNECT);
            self.flags.set(flags);
        }
        sent
    }

    pub(super) fn credit(&self) -> usize {
        self.cap
            .get()
            .saturating_sub(self.queues.borrow().inflight.len())
    }

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

    /// publish packet id
    pub(super) fn set_publish_id(&self, pkt: &mut Publish) -> num::NonZeroU16 {
        if let Some(idx) = pkt.packet_id {
            idx
        } else {
            let idx = self.next_id();
            pkt.packet_id = Some(idx);
            idx
        }
    }

    pub(super) fn set_cap(&self, cap: usize) {
        self.cap.set(cap);
        // wake up queued request (receive max limit)
        self.wake_waiters();
    }

    pub(super) fn set_publish_ack(&self, f: Box<dyn Fn(num::NonZeroU16, bool)>) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ON_PUBLISH_ACK);
        self.flags.set(flags);
        self.on_publish_ack.set(Some(f));
    }

    /// Encodes a packet, it is written after the payload of a streaming publish
    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), EncodeError> {
        self.io.encode(pkt.into(), self)
    }

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

    fn clear_queues(&self) {
        // publishes waiting for the streaming payload fail
        self.io.wake(STREAM_TAG);

        let mut queues = self.queues.borrow_mut();
        queues.waiters.clear();

        if let Some(cb) = self.on_publish_ack.take() {
            for (idx, tx, _) in queues.inflight.drain(..) {
                if tx.is_none() {
                    (*cb)(idx, true);
                }
            }
        } else {
            queues.inflight.clear();
        }
    }

    /// Wake waiters within the send credit
    fn wake_waiters(&self) {
        self.wake_waiters_inner(&mut self.queues.borrow_mut());
    }

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

    /// Wait until a payload chunk can be encoded
    pub(super) async fn want_payload_stream(&self) -> Result<(), SendPacketError> {
        if self.is_active() && (!self.io.is_wr_backpressure() || self.wait_write_ready().await) {
            Ok(())
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    fn check_streaming(&self) -> Result<(), EncodeError> {
        if self.streaming_remaining.get().is_some() {
            Err(EncodeError::ExpectPayload)
        } else {
            Ok(())
        }
    }

    /// Remaining streaming payload size, it is applied only after the
    /// publish packet is encoded successfully
    fn streaming_size(pkt: &Publish, payload: Option<&Bytes>) -> Option<num::NonZeroU32> {
        let len = payload.map_or(0, Bytes::len);
        num::NonZeroU32::new(pkt.payload_size - len as u32)
    }

    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), MqttProtocolError> {
        self.pkt_ack_inner(ack).inspect_err(|_| {
            self.close();
        })
    }

    fn pkt_ack_inner(&self, pkt: Ack) -> Result<(), MqttProtocolError> {
        let mut queues = self.queues.borrow_mut();

        // check ack order
        if let Some((idx, tx, tp)) = queues.inflight.pop_front() {
            if idx != pkt.packet_id() {
                log::trace!(
                    "MQTT protocol error: packet id order does not match; expected {}, got: {}",
                    idx,
                    pkt.packet_id()
                );
                Err(MqttProtocolError::packet_id_mismatch())
            } else if !pkt.is_match(tp) {
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
                log::trace!("Ack packet with id: {}", pkt.packet_id());

                if tx.is_none_or(|tx| tx.send(pkt).is_err()) {
                    // the publish future is dropped, nothing can release the publish,
                    // PUBREC must be answered with PUBREL (MQTT 3.1.1, 4.3.3)
                    log::trace!("Release dropped publish with id: {idx}");
                    let _ = self.io.encode(
                        Encoded::Packet(codec::Packet::PublishRelease { packet_id: idx }),
                        self,
                    );
                    queues.inflight.push_back((idx, None, AckType::Complete));
                } else {
                    let (tx, rx) = self.pool.queue.channel();
                    queues.rx.insert(idx, rx);
                    queues
                        .inflight
                        .push_back((idx, Some(tx), AckType::Complete));
                }
                Ok(())
            } else if matches!(pkt, Ack::Complete(_)) {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());
                queues.inflight_ids.remove(&pkt.packet_id());
                queues.rx.remove(&idx);

                if let Some(tx) = tx {
                    let _ = tx.send(pkt);
                }

                // wake up queued request (receive max limit)
                self.wake_waiters_inner(&mut queues);
                Ok(())
            } else {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());
                queues.inflight_ids.remove(&pkt.packet_id());

                if let Some(tx) = tx {
                    let _ = tx.send(pkt);
                } else {
                    let cb = self.on_publish_ack.take().unwrap();
                    (*cb)(pkt.packet_id(), false);
                    self.on_publish_ack.set(Some(cb));
                }

                // wake up queued request (receive max limit)
                self.wake_waiters_inner(&mut queues);
                Ok(())
            }
        } else {
            log::trace!("Unexpected PUBACK packet: {:?}", pkt.packet_id());
            Err(MqttProtocolError::generic_violation(
                "Received PUBACK packet while there are no unacknowledged PUBLISH packets",
            ))
        }
    }

    /// Register ack in response channel
    pub(super) fn wait_response(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            let (tx, rx) = self.pool.queue.channel();
            queues.inflight.push_back((id, Some(tx), ack));
            queues.inflight_ids.insert(id);
            Ok(rx)
        }
    }

    /// Register ack in response channel
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
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(()) => {
                    self.streaming_remaining.set(remaining);
                    let (tx, rx) = self.pool.queue.channel();
                    queues.inflight.push_back((id, Some(tx), ack));
                    queues.inflight_ids.insert(id);
                    Ok(rx)
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    /// Register ack in response channel
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
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(()) => {
                    self.streaming_remaining.set(remaining);
                    assert!(
                        self.flags.get().contains(Flags::ON_PUBLISH_ACK),
                        "Publish ack callback is not set"
                    );
                    queues.inflight.push_back((id, None, ack));
                    queues.inflight_ids.insert(id);
                    Ok(())
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    /// Wait for write backpressure to be released and for send credit, returns
    /// `false` if the connection is closed
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

    /// Register ack in response channel
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
                    .unwrap_or_else(|| BytePages::new(self.io.cfg().write_page_size()));
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
    pub(super) fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Receive(_) => packet_type::PUBREC,
            Ack::Complete(_) => packet_type::PUBCOMP,
            Ack::Subscribe { .. } => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    pub(super) fn packet_id(&self) -> num::NonZeroU16 {
        match self {
            Ack::Subscribe { packet_id, .. } => *packet_id,
            Ack::Publish(id) | Ack::Receive(id) | Ack::Complete(id) | Ack::Unsubscribe(id) => *id,
        }
    }

    pub(super) fn subscribe(self) -> Vec<codec::SubscribeReturnCode> {
        if let Ack::Subscribe { status, .. } = self {
            status
        } else {
            panic!()
        }
    }

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
