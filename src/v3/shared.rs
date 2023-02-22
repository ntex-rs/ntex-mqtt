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

pub struct MqttShared {
    io: IoRef,
    cap: Cell<usize>,
    queues: RefCell<MqttSharedQueues>,
    inflight_idx: Cell<u16>,
    pool: Rc<MqttSinkPool>,
    client: bool,
    pub(super) codec: codec::Codec,
}

struct MqttSharedQueues {
    inflight: VecDeque<(NonZeroU16, pool::Sender<Ack>, AckType)>,
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
            client,
            pool,
            cap: Cell::new(0),
            queues: RefCell::new(MqttSharedQueues {
                inflight: VecDeque::with_capacity(8),
                inflight_ids: HashSet::default(),
                waiters: VecDeque::new(),
            }),
            inflight_idx: Cell::new(0),
        }
    }

    pub(super) fn close(&self) {
        if self.client {
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

    pub(super) fn credit(&self) -> usize {
        self.cap.get().saturating_sub(self.queues.borrow().inflight.len())
    }

    pub(super) fn has_credit(&self) -> bool {
        self.credit() > 0
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

    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), EncodeError> {
        self.io.encode(pkt, &self.codec)
    }

    fn clear_queues(&self) {
        let mut queues = self.queues.borrow_mut();
        queues.inflight.clear();
        queues.waiters.clear();
    }

    pub(super) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
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
                    let _ = tx.send(pkt);

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
            queues.inflight.push_back((id, tx, ack));
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
                    queues.inflight.push_back((id, tx, ack));
                    queues.inflight_ids.insert(id);
                    Ok(rx)
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    pub(super) fn wait_credit(&self) -> Option<pool::Receiver<()>> {
        if !self.has_credit() {
            let (tx, rx) = self.pool.waiters.channel();
            self.queues.borrow_mut().waiters.push_back(tx);
            Some(rx)
        } else {
            None
        }
    }

    pub(super) fn wait_readiness(&self) -> Option<pool::Receiver<()>> {
        let mut queues = self.queues.borrow_mut();

        if queues.inflight.len() >= self.cap.get() {
            let (tx, rx) = self.pool.waiters.channel();
            queues.waiters.push_back(tx);
            Some(rx)
        } else {
            None
        }
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
