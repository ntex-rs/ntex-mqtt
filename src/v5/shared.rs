#![allow(clippy::type_complexity)]
use std::{cell::Cell, cell::RefCell, collections::VecDeque, num, rc::Rc};

use ntex_bytes::{Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};
use ntex_io::IoRef;
use ntex_util::{HashSet, channel::pool};

use crate::v5::codec::{self, Decoded, Encoded, Packet, Publish};
use crate::{QoS, error, error::SendPacketError, types::packet_type};

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct Flags: u8 {
        const WRB_ENABLED    = 0b0000_0001; // write-backpressure
        const ON_PUBLISH_ACK = 0b0000_0010; // on-publish-ack callback

        const QOS_ATLEAST    = 0b0000_0100; // AtLeastOnce
        const QOS_EXACTLY    = 0b0000_1000; // ExactlyOnce

        const DISCONNECT     = 0b0100_0000; // Disconnect frame is sent
        const STOPPED        = 0b1000_0000; // DispatchItem::Stop() is sent
    }
}

pub struct MqttShared {
    io: IoRef,
    cap: Cell<usize>,
    receive_max: Cell<u16>,
    topic_alias_max: Cell<u16>,
    inflight_idx: Cell<u16>,
    queues: RefCell<MqttSharedQueues>,
    encode_error: Cell<Option<error::EncodeError>>,
    streaming_waiter: Cell<Option<pool::Sender<()>>>,
    streaming_remaining: Cell<Option<num::NonZeroU32>>,
    on_publish_ack: Cell<Option<Box<dyn Fn(codec::PublishAck, bool)>>>,
    pub(super) flags: Cell<Flags>,
    pub(super) pool: Rc<MqttSinkPool>,
    pub(super) codec: codec::Codec,
}

#[derive(Debug)]
pub(super) struct MqttSharedQueues {
    inflight: VecDeque<(num::NonZeroU16, Option<pool::Sender<Ack>>, AckType)>,
    inflight_ids: HashSet<num::NonZeroU16>,
    waiters: VecDeque<pool::Sender<()>>,
    rx: Option<pool::Receiver<Ack>>,
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
                rx: None,
            }),
            receive_max: Cell::new(0),
            topic_alias_max: Cell::new(0),
            inflight_idx: Cell::new(0),
            flags: Cell::new(Flags::QOS_ATLEAST),
            on_publish_ack: Cell::new(None),
            encode_error: Cell::new(None),
            streaming_waiter: Cell::new(None),
            streaming_remaining: Cell::new(None),
        }
    }

    pub(super) fn tag(&self) -> &'static str {
        self.io.tag()
    }

    pub(super) fn receive_max(&self) -> u16 {
        self.receive_max.get()
    }

    pub(super) fn topic_alias_max(&self) -> u16 {
        self.topic_alias_max.get()
    }

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

    pub(super) fn set_receive_max(&self, val: u16) {
        self.receive_max.set(val);
    }

    pub(super) fn set_topic_alias_max(&self, val: u16) {
        self.topic_alias_max.set(val);
    }

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

    pub(super) fn close(&self, pkt: codec::Disconnect) {
        if !self.is_closed() {
            if !self.is_disconnect_sent() {
                let _ = self.io.encode(Encoded::Packet(Packet::Disconnect(pkt)), &self.codec);
            }
            self.io.close();
        }
        self.clear_queues();
    }

    pub(super) fn force_close(&self) {
        self.io.force_close();
        self.clear_queues();
    }

    pub(super) fn streaming_dropped(&self) {
        self.force_close();
        self.encode_error.set(Some(error::EncodeError::PublishIncomplete));
    }

    pub(super) fn is_closed(&self) -> bool {
        self.io.is_closed()
    }

    pub(super) fn is_streaming(&self) -> bool {
        self.streaming_remaining.get().is_some()
    }

    pub(super) fn credit(&self) -> usize {
        self.cap.get().saturating_sub(self.queues.borrow().inflight.len())
    }

    pub(super) fn is_ready(&self) -> bool {
        self.credit() > 0 && !self.flags.get().contains(Flags::WRB_ENABLED)
    }

    pub(super) fn is_dispatcher_stopped(&self) -> bool {
        let mut flags = self.flags.get();
        let stopped = flags.contains(Flags::STOPPED);
        if !stopped {
            flags.insert(Flags::STOPPED);
            self.flags.set(flags);
        }
        stopped
    }

    pub(super) fn is_disconnect_sent(&self) -> bool {
        let mut flags = self.flags.get();
        let disconnect = flags.contains(Flags::DISCONNECT);
        if !disconnect {
            flags.insert(Flags::DISCONNECT);
            self.flags.set(flags);
        }
        disconnect
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
            queues.inflight.clear();
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
        if self.is_closed() {
            Err(SendPacketError::Disconnected)
        } else if self.flags.get().contains(Flags::WRB_ENABLED) {
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
    }

    fn check_streaming(&self) -> Result<(), error::EncodeError> {
        if self.streaming_remaining.get().is_some() {
            Err(error::EncodeError::ExpectPayload)
        } else {
            Ok(())
        }
    }

    fn enable_streaming(&self, pkt: &Publish, payload: Option<&Bytes>) {
        let len = payload.map_or(0, Bytes::len);
        self.streaming_remaining.set(num::NonZeroU32::new(pkt.payload_size - len as u32));
    }

    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), error::EncodeError> {
        self.check_streaming()?;
        self.io.encode(Encoded::Packet(pkt), &self.codec)
    }

    pub(super) fn encode_publish(
        &self,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<(), error::EncodeError> {
        self.check_streaming()?;
        self.enable_streaming(&pkt, payload.as_ref());
        self.io.encode(Encoded::Publish(pkt, payload), &self.codec)
    }

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
                self.io.encode(Encoded::PayloadChunk(payload), &self.codec)?;
                self.streaming_remaining.set(num::NonZeroU32::new(remaining.get() - len));
                Ok(self.streaming_remaining.get().is_some())
            }
        } else {
            Err(error::EncodeError::UnexpectedPayload)
        }
    }

    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), error::ProtocolError> {
        self.pkt_ack_inner(ack).inspect_err(|_| {
            self.close(codec::Disconnect {
                reason_code: codec::DisconnectReasonCode::ImplementationSpecificError,
                ..Default::default()
            });
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
            } else if matches!(pkt, Ack::Receive(_)) {
                // get publish ack channel
                log::trace!("Ack packet receive with id: {}", pkt.packet_id());

                if let Some(tx) = tx {
                    let _ = tx.send(pkt);
                }
                let (tx, rx) = self.pool.queue.channel();
                queues.rx = Some(rx);
                queues.inflight.push_back((idx, Some(tx), AckType::Complete));
                Ok(())
            } else if matches!(pkt, Ack::Complete(_)) {
                // get publish ack channel
                log::trace!("Ack packet complete with id: {}", pkt.packet_id());
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
                Ok(()) => {
                    let (tx, rx) = self.pool.queue.channel();
                    queues.inflight.push_back((id, Some(tx), ack));
                    queues.inflight_ids.insert(id);
                    Ok(rx)
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

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
                Ok(()) => {
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

    /// Register ack in response channel
    pub(super) fn release_publish(
        &self,
        pkt: codec::PublishAck2,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let Some(rx) = self.queues.borrow_mut().rx.take() else {
            return Err(SendPacketError::UnexpectedRelease);
        };

        match self.io.encode(Encoded::Packet(codec::Packet::PublishRelease(pkt)), &self.codec) {
            Ok(()) => Ok(rx),
            Err(e) => Err(SendPacketError::Encode(e)),
        }
    }
}

impl Encoder for MqttShared {
    type Item = Encoded;
    type Error = error::EncodeError;

    #[inline]
    fn encode(&self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.codec.encode(item, dst)
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum AckType {
    Publish,
    Receive,
    Complete,
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Ack {
    Publish(codec::PublishAck),
    Receive(codec::PublishAck),
    Complete(codec::PublishAck2),
    Subscribe(codec::SubscribeAck),
    Unsubscribe(codec::UnsubscribeAck),
}

impl Ack {
    pub(super) fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Receive(_) => packet_type::PUBREC,
            Ack::Complete(_) => packet_type::PUBCOMP,
            Ack::Subscribe(_) => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    pub(super) fn packet_id(&self) -> num::NonZeroU16 {
        match self {
            Ack::Publish(pkt) | Ack::Receive(pkt) => pkt.packet_id,
            Ack::Complete(pkt) => pkt.packet_id,
            Ack::Subscribe(pkt) => pkt.packet_id,
            Ack::Unsubscribe(pkt) => pkt.packet_id,
        }
    }

    pub(super) fn publish(self) -> codec::PublishAck {
        if let Ack::Publish(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn receive(self) -> codec::PublishAck {
        if let Ack::Receive(pkt) = self {
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
