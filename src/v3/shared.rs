use std::{cell::Cell, cell::RefCell, collections::VecDeque, num::NonZeroU16, rc::Rc};

use ntex::channel::pool;
use ntex::codec::{Decoder, Encoder};
use ntex::io::IoRef;
use ntex::util::{BytesMut, HashSet, PoolId, PoolRef};

use crate::error::{DecodeError, EncodeError, ProtocolError, SendPacketError};
use crate::{types::packet_type, v3::codec};

pub(super) enum Ack {
    Publish(NonZeroU16),
    Subscribe { packet_id: NonZeroU16, status: Vec<codec::SubscribeReturnCode> },
    Unsubscribe(NonZeroU16),
}

#[derive(Copy, Clone)]
pub(super) enum AckType {
    Publish,
    Subscribe,
    Unsubscribe,
}

pub(super) struct MqttSinkPool {
    queue: pool::Pool<Ack>,
    waiters: pool::Pool<()>,
    pub(super) pool: Cell<PoolRef>,
}

impl Default for MqttSinkPool {
    fn default() -> Self {
        Self {
            queue: pool::new(),
            waiters: pool::new(),
            pool: Cell::new(PoolId::P5.pool_ref()),
        }
    }
}

bitflags::bitflags! {
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
    pool: Rc<MqttSinkPool>,
    flags: Cell<Flags>,
    on_publish_ack: Cell<Option<Box<dyn Fn(NonZeroU16, bool)>>>,
    pub(super) codec: codec::Codec,
}

struct MqttSharedQueues {
    inflight: VecDeque<(NonZeroU16, Option<pool::Sender<Ack>>, AckType)>,
    inflight_ids: HashSet<NonZeroU16>,
    waiters: VecDeque<pool::Sender<()>>,
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
            }),
            inflight_idx: Cell::new(0),
            on_publish_ack: Cell::new(None),
        }
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

    pub(super) fn is_closed(&self) -> bool {
        self.io.is_closed()
    }

    pub(super) fn is_ready(&self) -> bool {
        self.credit() > 0 && !self.flags.get().contains(Flags::WRB_ENABLED)
    }

    pub(super) fn credit(&self) -> usize {
        self.cap.get().saturating_sub(self.queues.borrow().inflight.len())
    }

    pub(super) fn next_id(&self) -> NonZeroU16 {
        let idx = self.inflight_idx.get() + 1;
        let idx = if idx == u16::max_value() {
            self.inflight_idx.set(0);
            u16::max_value()
        } else {
            self.inflight_idx.set(idx);
            idx
        };
        NonZeroU16::new(idx).unwrap()
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

    pub(super) fn set_publish_ack(&self, f: Box<dyn Fn(NonZeroU16, bool)>) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ON_PUBLISH_ACK);
        self.flags.set(flags);
        self.on_publish_ack.set(Some(f));
    }

    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), EncodeError> {
        self.io.encode(pkt, &self.codec)
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

    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), ProtocolError> {
        self.pkt_ack_inner(ack).map_err(|e| {
            self.close();
            e
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
        id: NonZeroU16,
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
    pub(super) fn wait_packet_response(
        &self,
        id: NonZeroU16,
        ack: AckType,
        pkt: codec::Packet,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(pkt, &self.codec) {
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
    pub(super) fn wait_packet_response_no_block(
        &self,
        id: NonZeroU16,
        ack: AckType,
        pkt: codec::Packet,
    ) -> Result<(), SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(pkt, &self.codec) {
                Ok(_) => {
                    queues.inflight.push_back((id, None, ack));
                    queues.inflight_ids.insert(id);
                    if self.flags.get().contains(Flags::ON_PUBLISH_ACK) {
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
}

impl Drop for MqttShared {
    fn drop(&mut self) {
        self.clear_queues();
    }
}

impl Encoder for MqttShared {
    type Item = codec::Packet;
    type Error = EncodeError;

    #[inline]
    fn encode(&self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.codec.encode(item, dst)
    }
}

impl Decoder for MqttShared {
    type Item = codec::Packet;
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
            Ack::Subscribe { .. } => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    pub(super) fn packet_id(&self) -> NonZeroU16 {
        match self {
            Ack::Publish(id) => *id,
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
            AckType::Subscribe => "Expected SUBACK packet",
            AckType::Unsubscribe => "Expected UNSUBACK packet",
        }
    }
}
