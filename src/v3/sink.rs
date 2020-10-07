use std::{cell::RefCell, collections::VecDeque, fmt, num::NonZeroU16, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{err, ok, Either, Future, TryFutureExt};
use fxhash::FxHashMap;
use ntex::channel::{mpsc, pool};

use super::{codec, error::ProtocolError, error::SendPacketError};
use crate::types::packet_type;

pub struct MqttSink(Rc<RefCell<MqttSinkInner>>);

pub(crate) enum Ack {
    Publish(NonZeroU16),
    Subscribe { packet_id: NonZeroU16, status: Vec<codec::SubscribeReturnCode> },
    Unsubscribe(NonZeroU16),
}

#[derive(Copy, Clone)]
pub(crate) enum AckType {
    Publish,
    Subscribe,
    Unsubscribe,
}

pub(crate) struct MqttSinkPool {
    queue: pool::Pool<Ack>,
    waiters: pool::Pool<()>,
}

impl Default for MqttSinkPool {
    fn default() -> Self {
        Self { queue: pool::new(), waiters: pool::new() }
    }
}

pub(crate) struct MqttSinkInner {
    cap: usize,
    sink: Option<mpsc::Sender<(codec::Packet, usize)>>,
    inflight: FxHashMap<u16, (pool::Sender<Ack>, AckType)>,
    inflight_idx: u16,
    inflight_order: VecDeque<u16>,
    waiters: VecDeque<pool::Sender<()>>,
    pool: Rc<MqttSinkPool>,
}

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone())
    }
}

impl MqttSink {
    pub(crate) fn new(
        sink: mpsc::Sender<(codec::Packet, usize)>,
        max_send: usize,
        pool: Rc<MqttSinkPool>,
    ) -> Self {
        MqttSink(Rc::new(RefCell::new(MqttSinkInner {
            pool,
            cap: max_send,
            sink: Some(sink),
            inflight: FxHashMap::with_capacity_and_hasher(
                max_send + 1,
                fxhash::FxBuildHasher::default(),
            ),
            inflight_idx: 0,
            inflight_order: VecDeque::with_capacity(max_send),
            waiters: VecDeque::with_capacity(8),
        })))
    }

    /// Get client receive credit
    pub fn credit(&self) -> usize {
        let inner = self.0.borrow();
        inner.cap - inner.inflight.len()
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Err(()) indicates disconnected connection
    pub fn ready(&self) -> impl Future<Output = Result<(), ()>> {
        let mut inner = self.0.borrow_mut();
        if inner.is_closed() {
            Either::Left(err(()))
        } else if inner.inflight.len() >= inner.cap {
            let (tx, rx) = inner.pool.waiters.channel();
            inner.waiters.push_back(tx);
            Either::Right(rx.map_err(|_| ()))
        } else {
            Either::Left(ok(()))
        }
    }

    /// Close mqtt connection
    pub fn close(&self) {
        let mut inner = self.0.borrow_mut();
        let _ = inner.sink.take();
        inner.inflight.clear();
        inner.waiters.clear();
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        if let Some(sink) = self.0.borrow_mut().sink.take() {
            sink.send((codec::Packet::PingRequest, 0)).is_ok()
        } else {
            false
        }
    }

    /// Create publish message builder
    pub fn publish(&self, topic: ByteString, payload: Bytes) -> PublishBuilder {
        PublishBuilder {
            packet: codec::Publish {
                topic,
                payload,
                dup: false,
                retain: false,
                qos: codec::QoS::AtMostOnce,
                packet_id: None,
            },
            sink: self.0.clone(),
        }
    }

    /// Create subscribe packet builder
    ///
    /// panics if id is 0
    pub fn subscribe(&self) -> SubscribeBuilder {
        SubscribeBuilder { id: 0, topic_filters: Vec::new(), sink: self.0.clone() }
    }

    /// Create unsubscribe packet builder
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder { id: 0, topic_filters: Vec::new(), sink: self.0.clone() }
    }

    pub(crate) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
        let mut inner = self.0.borrow_mut();

        // check ack order
        if let Some(idx) = inner.inflight_order.pop_front() {
            if idx != pkt.packet_id() {
                log::trace!(
                    "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                    idx,
                    pkt.packet_id()
                );
            } else {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());
                let idx = pkt.packet_id();
                if let Some((tx, tp)) = inner.inflight.remove(&idx) {
                    if !pkt.is_match(tp) {
                        log::trace!("MQTT protocol error, unexpeted packet");
                        return Err(ProtocolError::Unexpected(pkt.packet_type(), tp.name()));
                    }
                    let _ = tx.send(pkt);

                    // wake up queued request (receive max limit)
                    while let Some(tx) = inner.waiters.pop_front() {
                        if tx.send(()).is_ok() {
                            break;
                        }
                    }
                    return Ok(());
                } else {
                    log::error!("Inflight state inconsistency")
                }
            }
        } else {
            log::trace!("Unexpected PublishAck packet");
        }
        self.close();
        Err(ProtocolError::PacketIdMismatch)
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

impl MqttSinkInner {
    fn has_credit(&self) -> bool {
        self.cap - self.inflight.len() > 0
    }

    fn is_closed(&self) -> bool {
        if let Some(ref tx) = self.sink {
            tx.is_closed()
        } else {
            true
        }
    }

    fn next_id(&mut self) -> u16 {
        self.inflight_idx += 1;
        if self.inflight_idx == u16::max_value() {
            self.inflight_idx = 0;
            u16::max_value()
        } else {
            self.inflight_idx
        }
    }
}

impl Ack {
    fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Subscribe { .. } => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    fn packet_id(&self) -> u16 {
        match self {
            Ack::Publish(id) => id.get(),
            Ack::Subscribe { packet_id, .. } => packet_id.get(),
            Ack::Unsubscribe(id) => id.get(),
        }
    }

    fn subscribe(self) -> Vec<codec::SubscribeReturnCode> {
        if let Ack::Subscribe { status, .. } = self {
            status
        } else {
            panic!()
        }
    }

    fn is_match(&self, tp: AckType) -> bool {
        match (self, tp) {
            (Ack::Publish(_), AckType::Publish) => true,
            (Ack::Subscribe { .. }, AckType::Subscribe) => true,
            (Ack::Unsubscribe(_), AckType::Unsubscribe) => true,
            (_, _) => false,
        }
    }
}

impl AckType {
    fn name(&self) -> &'static str {
        match self {
            AckType::Publish => "PublishAck",
            AckType::Subscribe => "SubscribeAck",
            AckType::Unsubscribe => "UnsubscribeAck",
        }
    }
}

pub struct PublishBuilder {
    sink: Rc<RefCell<MqttSinkInner>>,
    packet: codec::Publish,
}

impl PublishBuilder {
    /// Set packet id.
    ///
    /// Note: if packet id is not set, it gets generated automatically.
    /// Packet id management should not be mixed, it should be auto-generated
    /// or set by user. Otherwise collisions could occure.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        let id = NonZeroU16::new(id).expect("id 0 is not allowed");
        self.packet.packet_id = Some(id);
        self
    }

    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
    }

    /// Send publish packet with QoS 0
    pub fn send_at_most_once(self) {
        let packet = self.packet;
        if let Some(ref sink) = self.sink.borrow().sink {
            log::trace!("Publish (QoS-0) to {:?}", packet.topic);
            let _ = sink.send((codec::Packet::Publish(packet), 0)).map_err(|_| {
                log::error!("Mqtt sink is disconnected");
            });
        } else {
            log::error!("Mqtt sink is disconnected");
        }
    }

    /// Send publish packet with QoS 1
    pub async fn send_at_least_once(self) -> Result<(), SendPacketError> {
        let mut packet = self.packet;
        let mut inner = self.sink.borrow_mut();

        if inner.sink.is_some() {
            // handle client receive maximum
            if !inner.has_credit() {
                let (tx, rx) = inner.pool.waiters.channel();
                inner.waiters.push_back(tx);

                // do not borrow cross yield points
                drop(inner);

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }

                inner = self.sink.borrow_mut();
            }

            // publish ack channel
            let (tx, rx) = inner.pool.queue.channel();

            // packet id
            let mut idx = packet.packet_id.map(|i| i.get()).unwrap_or(0);
            if idx == 0 {
                idx = inner.next_id();
                packet.packet_id = NonZeroU16::new(idx);
            }
            if inner.inflight.contains_key(&idx) {
                return Err(SendPacketError::PacketIdInUse(idx));
            }
            inner.inflight.insert(idx, (tx, AckType::Publish));
            inner.inflight_order.push_back(idx);
            packet.qos = codec::QoS::AtLeastOnce;

            log::trace!("Publish (QoS1) to {:#?}", packet);
            if inner.sink.as_ref().unwrap().send((codec::Packet::Publish(packet), 0)).is_err() {
                Err(SendPacketError::Disconnected)
            } else {
                // do not borrow cross yield points
                drop(inner);

                rx.await.map(|_| ()).map_err(|_| SendPacketError::Disconnected)
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: u16,
    sink: Rc<RefCell<MqttSinkInner>>,
    topic_filters: Vec<(ByteString, codec::QoS)>,
}

impl SubscribeBuilder {
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if id == 0 {
            panic!("id 0 is not allowed");
        }
        self.id = id;
        self
    }

    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString, qos: codec::QoS) -> Self {
        self.topic_filters.push((filter, qos));
        self
    }

    /// Send subscribe packet
    pub async fn send(self) -> Result<Vec<codec::SubscribeReturnCode>, SendPacketError> {
        let sink = self.sink;
        let filters = self.topic_filters;

        let mut inner = sink.borrow_mut();
        if inner.sink.is_some() {
            // handle client receive maximum
            if !inner.has_credit() {
                let (tx, rx) = inner.pool.waiters.channel();
                inner.waiters.push_back(tx);

                // do not borrow cross yield points
                drop(inner);

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }

                inner = sink.borrow_mut();
            }

            // ack channel
            let (tx, rx) = inner.pool.queue.channel();

            // allocate packet id
            let idx = if self.id == 0 { inner.next_id() } else { self.id };
            if inner.inflight.contains_key(&idx) {
                return Err(SendPacketError::PacketIdInUse(idx));
            }
            inner.inflight.insert(idx, (tx, AckType::Subscribe));
            inner.inflight_order.push_back(idx);

            // send subscribe to client
            log::trace!("Sending subscribe packet id: {} filters:{:?}", idx, filters);

            let send_result = inner.sink.as_ref().unwrap().send((
                codec::Packet::Subscribe {
                    packet_id: NonZeroU16::new(idx).unwrap(),
                    topic_filters: filters,
                },
                0,
            ));

            if send_result.is_err() {
                Err(SendPacketError::Disconnected)
            } else {
                // do not borrow cross yield points
                drop(inner);

                // wait ack from peer
                rx.await.map_err(|_| SendPacketError::Disconnected).map(|pkt| pkt.subscribe())
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

/// Unsubscribe packet builder
pub struct UnsubscribeBuilder {
    id: u16,
    sink: Rc<RefCell<MqttSinkInner>>,
    topic_filters: Vec<ByteString>,
}

impl UnsubscribeBuilder {
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if id == 0 {
            panic!("id 0 is not allowed");
        }
        self.id = id;
        self
    }

    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString) -> Self {
        self.topic_filters.push(filter);
        self
    }

    /// Send unsubscribe packet
    pub async fn send(self) -> Result<(), SendPacketError> {
        let sink = self.sink;
        let filters = self.topic_filters;

        let mut inner = sink.borrow_mut();
        if inner.sink.is_some() {
            // handle client receive maximum
            if !inner.has_credit() {
                let (tx, rx) = inner.pool.waiters.channel();
                inner.waiters.push_back(tx);

                // do not borrow cross yield points
                drop(inner);

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }

                inner = sink.borrow_mut();
            }

            // ack channel
            let (tx, rx) = inner.pool.queue.channel();

            // allocate packet id
            // allocate packet id
            let idx = if self.id == 0 { inner.next_id() } else { self.id };
            if inner.inflight.contains_key(&idx) {
                return Err(SendPacketError::PacketIdInUse(idx));
            }
            inner.inflight.insert(idx, (tx, AckType::Unsubscribe));
            inner.inflight_order.push_back(idx);

            // send subscribe to client
            log::trace!("Sending unsubscribe packet id: {} filters:{:?}", idx, filters);

            let send_result = inner.sink.as_ref().unwrap().send((
                codec::Packet::Unsubscribe {
                    packet_id: NonZeroU16::new(idx).unwrap(),
                    topic_filters: filters,
                },
                0,
            ));

            if send_result.is_err() {
                Err(SendPacketError::Disconnected)
            } else {
                // do not borrow cross yield points
                drop(inner);

                // wait ack from peer
                rx.await.map_err(|_| SendPacketError::Disconnected).map(|_| ())
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}
