use std::{cell::Cell, cell::RefCell, collections::VecDeque, num::NonZeroU16, rc::Rc};

use ntex_bytes::{Bytes, BytesMut, PoolId, PoolRef};
use ntex_codec::{Decoder, Encoder};
use ntex_io::IoRef;
use ntex_util::{channel::pool, HashSet};

use crate::v5::codec::{self, DecodedPacket, EncodePacket, Packet, Publish};
use crate::{error, error::SendPacketError, payload, types::packet_type, QoS};

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Flags: u8 {
        const WRB_ENABLED    = 0b0100_0000; // write-backpressure
        const ON_PUBLISH_ACK = 0b0010_0000; // on-publish-ack callback
    }
}

pub struct MqttShared {
    io: IoRef,
    cap: Cell<usize>,
    max_qos: Cell<QoS>,
    receive_max: Cell<u16>,
    topic_alias_max: Cell<u16>,
    inflight_idx: Cell<u16>,
    queues: RefCell<MqttSharedQueues>,
    flags: Cell<Flags>,
    pool: Rc<MqttSinkPool>,
    on_publish_ack: Cell<Option<Box<dyn Fn(codec::PublishAck, bool)>>>,
    pub(super) codec: codec::Codec,
}

pub(super) struct MqttSharedQueues {
    inflight: VecDeque<(NonZeroU16, Option<pool::Sender<Ack>>, AckType)>,
    inflight_ids: HashSet<NonZeroU16>,
    waiters: VecDeque<pool::Sender<()>>,
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

impl MqttShared {
    pub(super) fn new(io: IoRef, codec: codec::Codec, pool: Rc<MqttSinkPool>) -> Self {
        Self {
            io,
            pool,
            codec,
            cap: Cell::new(0),
            queues: RefCell::new(MqttSharedQueues {
                inflight: VecDeque::with_capacity(8),
                inflight_ids: HashSet::default(),
                waiters: VecDeque::new(),
            }),
            receive_max: Cell::new(0),
            topic_alias_max: Cell::new(0),
            max_qos: Cell::new(QoS::AtLeastOnce),
            inflight_idx: Cell::new(0),
            flags: Cell::new(Flags::empty()),
            on_publish_ack: Cell::new(None),
        }
    }

    pub(super) fn receive_max(&self) -> u16 {
        self.receive_max.get()
    }

    pub(super) fn topic_alias_max(&self) -> u16 {
        self.topic_alias_max.get()
    }

    pub(super) fn max_qos(&self) -> QoS {
        self.max_qos.get()
    }

    pub(super) fn set_receive_max(&self, val: u16) {
        self.receive_max.set(val);
    }

    pub(super) fn set_topic_alias_max(&self, val: u16) {
        self.topic_alias_max.set(val);
    }

    pub(super) fn set_max_qos(&self, val: QoS) {
        self.max_qos.set(val);
    }

    pub(super) fn close(&self, pkt: codec::Disconnect) {
        if !self.is_closed() {
            let _ = self.io.encode(EncodePacket::Packet(Packet::Disconnect(pkt)), &self.codec);
            self.io.close();
        }
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

    pub(super) fn is_ready(&self) -> bool {
        self.credit() > 0 && !self.flags.get().contains(Flags::WRB_ENABLED)
    }

    pub(super) fn next_id(&self) -> NonZeroU16 {
        let idx = self.inflight_idx.get() + 1;
        self.inflight_idx.set(idx);
        let idx = if idx == u16::MAX {
            self.inflight_idx.set(0);
            u16::MAX
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

    pub(super) fn set_publish_ack(&self, f: Box<dyn Fn(codec::PublishAck, bool)>) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ON_PUBLISH_ACK);
        self.flags.set(flags);
        self.on_publish_ack.set(Some(f));
    }

    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), error::EncodeError> {
        self.io.encode(EncodePacket::Packet(pkt), &self.codec)
    }

    pub(super) fn encode_publish(
        &self,
        pkt: Publish,
        payload: Bytes,
    ) -> Result<(), error::EncodeError> {
        self.io.encode(EncodePacket::Publish(pkt, Some(payload)), &self.codec)
    }

    /// Close mqtt connection, dont send disconnect message
    pub(super) fn drop_sink(&self) {
        self.clear_queues();
        self.io.close();
    }

    fn clear_queues(&self) {
        let mut queues = self.queues.borrow_mut();
        queues.waiters.clear();

        if let Some(cb) = self.on_publish_ack.take() {
            for (idx, tx, _) in queues.inflight.drain(..) {
                if tx.is_none() {
                    (*cb)(codec::PublishAck { packet_id: idx, ..Default::default() }, true);
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

    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), error::ProtocolError> {
        self.pkt_ack_inner(ack).map_err(|e| {
            self.close(codec::Disconnect {
                reason_code: codec::DisconnectReasonCode::ImplementationSpecificError,
                ..Default::default()
            });
            e
        })
    }

    fn pkt_ack_inner(&self, pkt: Ack) -> Result<(), error::ProtocolError> {
        let mut queues = self.queues.borrow_mut();

        // check ack order
        if let Some((idx, tx, tp)) = queues.inflight.pop_front() {
            if idx != pkt.packet_id() {
                log::trace!(
                    "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                    idx,
                    pkt.packet_id()
                );
                Err(error::ProtocolError::packet_id_mismatch())
            } else {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());

                // cleanup ack queue
                queues.inflight_ids.remove(&pkt.packet_id());

                if pkt.is_match(tp) {
                    if let Some(tx) = tx {
                        let _ = tx.send(pkt);
                    } else {
                        let cb = self.on_publish_ack.take().unwrap();
                        (*cb)(pkt.publish(), false);
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
                    log::trace!("MQTT protocol error, unexpeted packet");
                    Err(error::ProtocolError::unexpected_packet(
                        pkt.packet_type(),
                        tp.expected_str(),
                    ))
                }
            }
        } else {
            log::trace!("Unexpected PublishAck packet");
            Err(error::ProtocolError::generic_violation(
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
    pub(super) fn wait_publish_response(
        &self,
        id: NonZeroU16,
        ack: AckType,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(EncodePacket::Publish(pkt, payload), &self.codec) {
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

    pub(super) fn send_chunk(&self, chunk: Bytes) {}

    pub(super) fn wait_publish_response_no_block(
        &self,
        id: NonZeroU16,
        ack: AckType,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<(), SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(EncodePacket::Publish(pkt, payload), &self.codec) {
                Ok(_) => {
                    queues.inflight.push_back((id, None, ack));
                    queues.inflight_ids.insert(id);
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

impl Encoder for MqttShared {
    type Item = EncodePacket;
    type Error = error::EncodeError;

    #[inline]
    fn encode(&self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.codec.encode(item, dst)
    }
}

impl Decoder for MqttShared {
    type Item = DecodedPacket;
    type Error = error::DecodeError;

    #[inline]
    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.codec.decode(src)
    }
}

#[derive(Copy, Clone)]
pub(super) enum AckType {
    Publish,
    Subscribe,
    Unsubscribe,
}

pub(super) enum Ack {
    Publish(codec::PublishAck),
    Subscribe(codec::SubscribeAck),
    Unsubscribe(codec::UnsubscribeAck),
}

impl Ack {
    pub(super) fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Subscribe(_) => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    pub(super) fn packet_id(&self) -> NonZeroU16 {
        match self {
            Ack::Publish(ref pkt) => pkt.packet_id,
            Ack::Subscribe(ref pkt) => pkt.packet_id,
            Ack::Unsubscribe(ref pkt) => pkt.packet_id,
        }
    }

    pub(super) fn publish(self) -> codec::PublishAck {
        if let Ack::Publish(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn subscribe(self) -> codec::SubscribeAck {
        if let Ack::Subscribe(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn unsubscribe(self) -> codec::UnsubscribeAck {
        if let Ack::Unsubscribe(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn is_match(&self, tp: AckType) -> bool {
        match (self, tp) {
            (Ack::Publish(_), AckType::Publish) => true,
            (Ack::Subscribe(_), AckType::Subscribe) => true,
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
