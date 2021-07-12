use std::{cell::Cell, cell::RefCell, collections::VecDeque, rc::Rc};

use ntex::channel::pool;
use ntex::codec::{Decoder, Encoder};
use ntex::util::{BytesMut, HashMap};

use super::codec;
use crate::{error, io::State, types::packet_type};

pub(crate) struct MqttShared {
    pub(super) cap: Cell<usize>,
    queues: RefCell<MqttSharedQueues>,
    pub(super) inflight_idx: Cell<u16>,
    pub(super) pool: Rc<MqttSinkPool>,
    pub(super) state: State,
    pub(super) codec: codec::Codec,
}

pub(super) struct MqttSharedQueues {
    pub(super) inflight: HashMap<u16, (pool::Sender<Ack>, AckType)>,
    pub(super) inflight_order: VecDeque<u16>,
    pub(super) waiters: VecDeque<pool::Sender<()>>,
}

pub(super) struct MqttSinkPool {
    pub(super) queue: pool::Pool<Ack>,
    pub(super) waiters: pool::Pool<()>,
}

impl Default for MqttSinkPool {
    fn default() -> Self {
        Self { queue: pool::new(), waiters: pool::new() }
    }
}

impl MqttShared {
    pub(super) fn new(
        state: State,
        codec: codec::Codec,
        cap: usize,
        pool: Rc<MqttSinkPool>,
    ) -> Self {
        Self {
            state,
            pool,
            codec,
            cap: Cell::new(cap),
            queues: RefCell::new(MqttSharedQueues {
                inflight: HashMap::default(),
                inflight_order: VecDeque::with_capacity(8),
                waiters: VecDeque::new(),
            }),
            inflight_idx: Cell::new(0),
        }
    }

    pub(super) fn with_queues<R>(&self, f: impl FnOnce(&mut MqttSharedQueues) -> R) -> R {
        let mut queues = self.queues.borrow_mut();
        f(&mut queues)
    }

    pub(super) fn has_credit(&self) -> bool {
        self.cap.get() - self.queues.borrow().inflight.len() > 0
    }

    pub(super) fn next_id(&self) -> u16 {
        let idx = self.inflight_idx.get() + 1;
        self.inflight_idx.set(idx);
        if idx == u16::max_value() {
            self.inflight_idx.set(0);
            u16::max_value()
        } else {
            self.inflight_idx.set(idx);
            idx
        }
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
    pub(super) fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Subscribe(_) => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    pub(super) fn packet_id(&self) -> u16 {
        match self {
            Ack::Publish(ref pkt) => pkt.packet_id.get(),
            Ack::Subscribe(ref pkt) => pkt.packet_id.get(),
            Ack::Unsubscribe(ref pkt) => pkt.packet_id.get(),
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
