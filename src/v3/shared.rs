use std::{cell::Cell, cell::RefCell, collections::VecDeque, num, rc::Rc};

use ntex_bytes::{Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};
use ntex_io::IoRef;
use ntex_util::{HashSet, channel::pool};

use crate::error::{DecodeError, EncodeError, ProtocolError, SendPacketError};
use crate::types::packet_type;
use crate::v3::codec::{self, Encoded, Publish};

pub(super) enum Ack {
    Publish(num::NonZeroU16),
    Receive(num::NonZeroU16),
    Complete(num::NonZeroU16),
    Subscribe { packet_id: num::NonZeroU16, status: Vec<codec::SubscribeReturnCode> },
    Unsubscribe(num::NonZeroU16),
}

#[derive(Copy, Clone)]
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
        Self { queue: pool::new(), waiters: pool::new() }
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Flags: u8 {
        const CLIENT         = 0b1000_0000;
        const WRB_ENABLED    = 0b0100_0000; // write-backpressure
        const ON_PUBLISH_ACK = 0b0010_0000; // on-publish-ack callback
    }
}

pub struct MqttShared {
    io: IoRef,
    cap: Cell<usize>,
    queues: RefCell<MqttSharedQueues>,
    inflight_idx: Cell<u16>,
    flags: Cell<Flags>,
    encode_error: Cell<Option<EncodeError>>,
    streaming_waiter: Cell<Option<pool::Sender<()>>>,
    streaming_remaining: Cell<Option<num::NonZeroU32>>,
    on_publish_ack: Cell<Option<Box<dyn Fn(num::NonZeroU16, bool)>>>,
    pub(super) codec: codec::Codec,
    pub(super) pool: Rc<MqttSinkPool>,
}

struct MqttSharedQueues {
    inflight: VecDeque<(num::NonZeroU16, Option<pool::Sender<Ack>>, AckType)>,
    inflight_ids: HashSet<num::NonZeroU16>,
    waiters: VecDeque<pool::Sender<()>>,
    rx: Option<pool::Receiver<Ack>>,
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
                rx: None,
            }),
            inflight_idx: Cell::new(0),
            encode_error: Cell::new(None),
            streaming_waiter: Cell::new(None),
            streaming_remaining: Cell::new(None),
            on_publish_ack: Cell::new(None),
        }
    }

    pub(super) fn tag(&self) -> &'static str {
        self.io.tag()
    }

    pub(super) fn close(&self) {
        if self.flags.get().contains(Flags::CLIENT) {
            let _ = self.encode_packet(codec::Packet::Disconnect);
        }
        self.io.close();
        self.clear_queues();
    }

    pub(super) fn force_close(&self) {
        self.io.force_close();
        self.clear_queues();
    }

    pub(super) fn streaming_dropped(&self) {
        self.force_close();
        self.encode_error.set(Some(EncodeError::PublishIncomplete));
    }

    pub(super) fn is_streaming(&self) -> bool {
        self.streaming_remaining.get().is_some()
    }

    pub(super) fn is_closed(&self) -> bool {
        self.io.is_closed()
    }

    pub(super) fn is_ready(&self) -> bool {
        self.credit() > 0 && !self.flags.get().contains(Flags::WRB_ENABLED)
    }

    pub(super) fn credit(&self) -> usize {
        self.cap.get().saturating_sub(self.queues.borrow().inflight.len())
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
        let mut queues = self.queues.borrow_mut();

        // wake up queued request (receive max limit)
        'outer: for _ in 0..cap {
            while let Some(tx) = queues.waiters.pop_front() {
                if tx.send(()).is_ok() {
                    continue 'outer;
                }
            }
            break;
        }
        self.cap.set(cap);
    }

    pub(super) fn set_publish_ack(&self, f: Box<dyn Fn(num::NonZeroU16, bool)>) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ON_PUBLISH_ACK);
        self.flags.set(flags);
        self.on_publish_ack.set(Some(f));
    }

    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), EncodeError> {
        self.check_streaming()?;
        self.io.encode(pkt.into(), &self.codec)
    }

    pub(super) fn encode_publish(
        &self,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<(), EncodeError> {
        self.check_streaming()?;
        self.enable_streaming(&pkt, payload.as_ref());
        self.io.encode(Encoded::Publish(pkt, payload), &self.codec)
    }

    pub(super) fn encode_publish_payload(&self, payload: Bytes) -> Result<bool, EncodeError> {
        if let Some(remaining) = self.streaming_remaining.get() {
            let len = payload.len() as u32;
            if len > remaining.get() {
                self.force_close();
                Err(EncodeError::OverPublishSize)
            } else {
                self.io.encode(Encoded::PayloadChunk(payload), &self.codec)?;
                self.streaming_remaining.set(num::NonZeroU32::new(remaining.get() - len));
                Ok(self.streaming_remaining.get().is_some())
            }
        } else {
            Err(EncodeError::UnexpectedPayload)
        }
    }

    fn clear_queues(&self) {
        let mut queues = self.queues.borrow_mut();
        queues.waiters.clear();

        if let Some(cb) = self.on_publish_ack.take() {
            for (idx, tx, _) in queues.inflight.drain(..) {
                if tx.is_none() {
                    (*cb)(idx, true);
                }
            }
        } else {
            queues.inflight.clear()
        }
    }

    pub(super) fn enable_wr_backpressure(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::WRB_ENABLED);
        self.flags.set(flags);
    }

    pub(super) fn disable_wr_backpressure(&self) {
        let mut flags = self.flags.get();
        flags.remove(Flags::WRB_ENABLED);
        self.flags.set(flags);

        // streaming waiter
        if let Some(tx) = self.streaming_waiter.take()
            && tx.send(()).is_ok()
        {
            return;
        }

        // check if there are waiters
        let mut queues = self.queues.borrow_mut();
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

    pub(super) async fn want_payload_stream(&self) -> Result<(), SendPacketError> {
        if !self.is_closed() {
            if self.flags.get().contains(Flags::WRB_ENABLED) {
                let (tx, rx) = self.pool.waiters.channel();
                self.streaming_waiter.set(Some(tx));
                if rx.await.is_ok() {
                    Ok(())
                } else {
                    Err(SendPacketError::Disconnected)
                }
            } else {
                Ok(())
            }
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

    fn enable_streaming(&self, pkt: &Publish, payload: Option<&Bytes>) {
        let len = payload.map(|b| b.len()).unwrap_or(0);
        self.streaming_remaining.set(num::NonZeroU32::new(pkt.payload_size - len as u32));
    }

    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), ProtocolError> {
        self.pkt_ack_inner(ack).inspect_err(|_| {
            self.close();
        })
    }

    fn pkt_ack_inner(&self, pkt: Ack) -> Result<(), ProtocolError> {
        let mut queues = self.queues.borrow_mut();

        // check ack order
        if let Some((idx, tx, tp)) = queues.inflight.pop_front() {
            if idx != pkt.packet_id() {
                log::trace!(
                    "MQTT protocol error: packet id order does not match; expected {}, got: {}",
                    idx,
                    pkt.packet_id()
                );
                Err(ProtocolError::packet_id_mismatch())
            } else if matches!(pkt, Ack::Receive(_)) {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());

                if let Some(tx) = tx {
                    let _ = tx.send(pkt);
                }
                let (tx, rx) = self.pool.queue.channel();
                queues.rx = Some(rx);
                queues.inflight.push_back((idx, Some(tx), AckType::Complete));
                Ok(())
            } else if matches!(pkt, Ack::Complete(_)) {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());
                queues.inflight_ids.remove(&pkt.packet_id());
                queues.rx.take();

                if let Some(tx) = tx {
                    let _ = tx.send(pkt);
                }

                // wake up queued request (receive max limit)
                while let Some(tx) = queues.waiters.pop_front() {
                    if tx.send(()).is_ok() {
                        break;
                    }
                }
                Ok(())
            } else {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());
                queues.inflight_ids.remove(&pkt.packet_id());

                if pkt.is_match(tp) {
                    if let Some(tx) = tx {
                        let _ = tx.send(pkt);
                    } else {
                        let cb = self.on_publish_ack.take().unwrap();
                        (*cb)(pkt.packet_id(), false);
                        self.on_publish_ack.set(Some(cb));
                    }

                    // wake up queued request (receive max limit)
                    while let Some(tx) = queues.waiters.pop_front() {
                        if tx.send(()).is_ok() {
                            break;
                        }
                    }
                    Ok(())
                } else {
                    log::trace!("MQTT protocol error, unexpected packet");
                    Err(ProtocolError::unexpected_packet(pkt.packet_type(), tp.expected_str()))
                }
            }
        } else {
            log::trace!("Unexpected PUBACK packet: {:?}", pkt.packet_id());
            Err(ProtocolError::generic_violation(
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
        self.enable_streaming(&pkt, payload.as_ref());

        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(_) => {
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
        self.enable_streaming(&pkt, payload.as_ref());

        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(_) => {
                    queues.inflight.push_back((id, None, ack));
                    queues.inflight_ids.insert(id);
                    if !self.flags.get().contains(Flags::ON_PUBLISH_ACK) {
                        panic!("Publish ack callback is not set");
                    }
                    Ok(())
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    pub(super) fn wait_readiness(&self) -> Option<pool::Receiver<()>> {
        let mut queues = self.queues.borrow_mut();

        if queues.inflight.len() >= self.cap.get()
            || self.flags.get().contains(Flags::WRB_ENABLED)
        {
            let (tx, rx) = self.pool.waiters.channel();
            queues.waiters.push_back(tx);
            Some(rx)
        } else {
            None
        }
    }

    /// Register ack in response channel
    pub(super) fn release_publish(
        &self,
        id: num::NonZeroU16,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let rx = if let Some(rx) = self.queues.borrow_mut().rx.take() {
            rx
        } else {
            return Err(SendPacketError::UnexpectedRelease);
        };

        match self.io.encode(
            Encoded::Packet(codec::Packet::PublishRelease { packet_id: id }),
            &self.codec,
        ) {
            Ok(_) => Ok(rx),
            Err(e) => Err(SendPacketError::Encode(e)),
        }
    }
}

impl Encoder for MqttShared {
    type Item = Encoded;
    type Error = EncodeError;

    #[inline]
    fn encode(&self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.codec.encode(item, dst)
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
            Ack::Publish(id) => *id,
            Ack::Receive(id) => *id,
            Ack::Complete(id) => *id,
            Ack::Subscribe { packet_id, .. } => *packet_id,
            Ack::Unsubscribe(id) => *id,
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
            (Ack::Publish(_), AckType::Publish) => true,
            (Ack::Receive(_), AckType::Receive) => true,
            (Ack::Subscribe { .. }, AckType::Subscribe) => true,
            (Ack::Unsubscribe(_), AckType::Unsubscribe) => true,
            (_, _) => false,
        }
    }
}

impl AckType {
    pub(super) fn expected_str(&self) -> &'static str {
        match self {
            AckType::Publish => "Expected PUBACK packet",
            AckType::Receive => "Expected PUBREC packet",
            AckType::Complete => "Expected PUBCOMP packet",
            AckType::Subscribe => "Expected SUBACK packet",
            AckType::Unsubscribe => "Expected UNSUBACK packet",
        }
    }
}
