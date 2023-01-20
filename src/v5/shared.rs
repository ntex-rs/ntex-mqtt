use std::{cell::Cell, cell::RefCell, collections::VecDeque, num::NonZeroU16, rc::Rc};

use ntex::channel::pool;
use ntex::codec::{Decoder, Encoder};
use ntex::io::IoRef;
use ntex::util::{BytesMut, HashSet, PoolId, PoolRef};

use super::codec;
use crate::{error, types::packet_type};

pub struct MqttShared {
    pub(super) io: IoRef,
    cap: Cell<usize>,
    queues: RefCell<MqttSharedQueues>,
    pub(super) inflight_idx: Cell<u16>,
    pub(super) pool: Rc<MqttSinkPool>,
    pub(super) codec: codec::Codec,
}

pub(super) struct MqttSharedQueues {
    pub(super) inflight: VecDeque<(NonZeroU16, pool::Sender<Ack>, AckType)>,
    pub(super) inflight_ids: HashSet<NonZeroU16>,
    pub(super) waiters: VecDeque<pool::Sender<()>>,
}

pub(super) struct MqttSinkPool {
    pub(super) queue: pool::Pool<Ack>,
    pub(super) waiters: pool::Pool<()>,
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
            inflight_idx: Cell::new(0),
        }
    }

    pub(super) fn cap(&self) -> usize {
        self.cap.get()
    }

    pub(super) fn with_queues<R>(&self, f: impl FnOnce(&mut MqttSharedQueues) -> R) -> R {
        let mut queues = self.queues.borrow_mut();
        f(&mut queues)
    }

    pub(super) fn credit(&self) -> usize {
        self.cap.get().saturating_sub(self.queues.borrow().inflight.len())
    }

    pub(super) fn has_credit(&self) -> bool {
        self.credit() > 0
    }

    pub(super) fn next_id(&self) -> NonZeroU16 {
        let idx = self.inflight_idx.get() + 1;
        self.inflight_idx.set(idx);
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
}

impl Encoder for MqttShared {
    type Item = codec::Packet;
    type Error = error::EncodeError;

    #[inline]
    fn encode(&self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.codec.encode(item, dst)
    }
}

impl Decoder for MqttShared {
    type Item = codec::Packet;
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
    pub(super) fn name(&self) -> &'static str {
        match self {
            Ack::Publish(_) => "PublishAck",
            Ack::Subscribe(_) => "SubscribeAck",
            Ack::Unsubscribe(_) => "UnsubscribeAck",
        }
    }

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
    pub(super) fn name(&self) -> &'static str {
        match self {
            AckType::Publish => "PublishAck",
            AckType::Subscribe => "SubscribeAck",
            AckType::Unsubscribe => "UnsubscribeAck",
        }
    }
}
