use std::num::{NonZeroU16, NonZeroU32};
use std::{cell::RefCell, collections::VecDeque, fmt, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{ready, Either, Future, FutureExt};
use fxhash::FxHashMap;
use ntex::channel::pool;

use crate::v5::error::{ProtocolError, PublishQos1Error, SendPacketError};
use crate::{io::IoState, io::IoStateInner, types::packet_type, types::QoS, v5::codec};

pub struct MqttSink(Rc<RefCell<MqttSinkInner>>, IoState<codec::Codec>);

pub(crate) enum Ack {
    Publish(codec::PublishAck),
    Subscribe(codec::SubscribeAck),
    Unsubscribe(codec::UnsubscribeAck),
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

struct MqttSinkInner {
    cap: usize,
    inflight: FxHashMap<u16, (pool::Sender<Ack>, AckType)>,
    inflight_idx: u16,
    inflight_order: VecDeque<u16>,
    waiters: VecDeque<pool::Sender<()>>,
    pool: Rc<MqttSinkPool>,
}

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone(), self.1.clone())
    }
}

impl MqttSink {
    pub(crate) fn new(
        state: IoState<codec::Codec>,
        max_send: usize,
        pool: Rc<MqttSinkPool>,
    ) -> Self {
        MqttSink(
            Rc::new(RefCell::new(MqttSinkInner {
                pool,
                cap: max_send,
                inflight: FxHashMap::with_capacity_and_hasher(
                    max_send + 1,
                    fxhash::FxBuildHasher::default(),
                ),
                inflight_idx: 0,
                inflight_order: VecDeque::with_capacity(max_send),
                waiters: VecDeque::with_capacity(8),
            })),
            state,
        )
    }

    /// Check connection status
    pub fn is_opened(&self) -> bool {
        self.1.inner.borrow().is_opened()
    }

    /// Get client receive credit
    pub fn credit(&self) -> usize {
        let inner = self.0.borrow();
        inner.cap - inner.inflight.len()
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive
    pub fn ready(&self) -> impl Future<Output = bool> {
        let mut inner = self.0.borrow_mut();
        if !self.1.inner.borrow().is_opened() {
            Either::Left(ready(false))
        } else if inner.inflight.len() >= inner.cap {
            let (tx, rx) = inner.pool.waiters.channel();
            inner.waiters.push_back(tx);
            Either::Right(rx.map(|v| v.is_ok()))
        } else {
            Either::Left(ready(true))
        }
    }

    /// Close mqtt connection with default Disconnect message
    pub fn close(&self) {
        let mut st = self.1.inner.borrow_mut();
        if st.is_opened() {
            let _ = st.send(codec::Packet::Disconnect(codec::Disconnect::default()));
            st.close();
        }
        let mut inner = self.0.borrow_mut();
        inner.waiters.clear();
        inner.inflight.clear();
    }

    /// Close mqtt connection
    pub fn close_with_reason(&self, pkt: codec::Disconnect) {
        let mut st = self.1.inner.borrow_mut();
        if st.is_opened() {
            let _ = st.send(codec::Packet::Disconnect(pkt));
            st.close();
        }

        let mut inner = self.0.borrow_mut();
        inner.waiters.clear();
        inner.inflight.clear();
    }

    pub(super) fn send(&self, pkt: codec::Packet) {
        let _ = self.1.inner.borrow_mut().send(pkt);
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        self.1.inner.borrow_mut().send(codec::Packet::PingRequest).is_ok()
    }

    /// Close mqtt connection, dont send disconnect message
    pub(super) fn drop_sink(&self) {
        let mut inner = self.0.borrow_mut();
        inner.waiters.clear();
        inner.inflight.clear();
        self.1.inner.borrow_mut().close();
    }

    pub(super) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
        let mut inner = self.0.borrow_mut();

        loop {
            // check ack order
            if let Some(idx) = inner.inflight_order.pop_front() {
                // errored publish
                if idx == 0 {
                    continue;
                }

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
                        // cleanup ack queue
                        if !pkt.is_match(tp) {
                            log::trace!("MQTT protocol error, unexpeted packet");
                            return Err(ProtocolError::Unexpected(
                                pkt.packet_type(),
                                tp.name(),
                            ));
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
            return Err(ProtocolError::PacketIdMismatch);
        }
    }

    /// Create publish packet builder
    pub fn publish<U>(&self, topic: U, payload: Bytes) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        PublishBuilder {
            packet: codec::Publish {
                payload,
                dup: false,
                retain: false,
                topic: topic.into(),
                qos: QoS::AtMostOnce,
                packet_id: None,
                properties: codec::PublishProperties::default(),
            },
            sink: self.0.clone(),
            state: self.1.inner.clone(),
        }
    }

    /// Create subscribe packet builder
    pub fn subscribe(&self, id: Option<NonZeroU32>) -> SubscribeBuilder {
        SubscribeBuilder {
            id: 0,
            packet: codec::Subscribe {
                id,
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Vec::new(),
                topic_filters: Vec::new(),
            },
            sink: self.0.clone(),
            state: self.1.inner.clone(),
        }
    }

    /// Create unsubscribe packet builder
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder {
            id: 0,
            packet: codec::Unsubscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Vec::new(),
                topic_filters: Vec::new(),
            },
            sink: self.0.clone(),
            state: self.1.inner.clone(),
        }
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
            Ack::Subscribe(_) => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    fn packet_id(&self) -> u16 {
        match self {
            Ack::Publish(ref pkt) => pkt.packet_id.get(),
            Ack::Subscribe(ref pkt) => pkt.packet_id.get(),
            Ack::Unsubscribe(ref pkt) => pkt.packet_id.get(),
        }
    }

    fn publish(self) -> codec::PublishAck {
        if let Ack::Publish(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    fn subscribe(self) -> codec::SubscribeAck {
        if let Ack::Subscribe(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    fn unsubscribe(self) -> codec::UnsubscribeAck {
        if let Ack::Unsubscribe(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }
    fn is_match(&self, tp: AckType) -> bool {
        match (self, tp) {
            (Ack::Publish(_), AckType::Publish) => true,
            (Ack::Subscribe(_), AckType::Subscribe) => true,
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
    state: Rc<RefCell<IoStateInner<codec::Codec>>>,
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

    /// Set retain flag
    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
    }

    /// Set publish packet properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
        self
    }

    /// Set publish packet properties
    pub fn set_properties<F>(&mut self, f: F)
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        f(&mut self.packet.properties);
    }

    /// Send publish packet with QoS 0
    pub fn send_at_most_once(self) -> Result<(), SendPacketError> {
        let packet = self.packet;
        let mut state = self.state.borrow_mut();

        if state.is_opened() {
            log::trace!("Publish (QoS-0) to {:?}", packet.topic);
            state.send(codec::Packet::Publish(packet)).map_err(SendPacketError::Encode)
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 1
    pub async fn send_at_least_once(self) -> Result<codec::PublishAck, PublishQos1Error> {
        let st = self.state;
        let sink = self.sink;
        let mut packet = self.packet;
        packet.qos = QoS::AtLeastOnce;

        let mut state = st.borrow_mut();
        let mut inner = sink.borrow_mut();
        if state.is_opened() {
            // handle client receive maximum
            if !inner.has_credit() {
                let (tx, rx) = inner.pool.waiters.channel();
                inner.waiters.push_back(tx);

                // do not borrow cross yield points
                drop(inner);
                drop(state);

                if rx.await.is_err() {
                    return Err(PublishQos1Error::Disconnected);
                }

                state = st.borrow_mut();
                inner = sink.borrow_mut();
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
                return Err(PublishQos1Error::PacketIdInUse(idx));
            }
            inner.inflight.insert(idx, (tx, AckType::Publish));
            inner.inflight_order.push_back(idx);

            // send publish to client
            log::trace!("Publish (QoS1) to {:#?}", packet);

            match state.send(codec::Packet::Publish(packet)) {
                Ok(_) => {
                    // do not borrow cross yield points
                    drop(inner);
                    drop(state);

                    // wait ack from peer
                    rx.await.map_err(|_| PublishQos1Error::Disconnected).and_then(|pkt| {
                        let pkt = pkt.publish();
                        match pkt.reason_code {
                            codec::PublishAckReason::Success => Ok(pkt),
                            _ => Err(PublishQos1Error::Fail(pkt)),
                        }
                    })
                }
                Err(err) => Err(PublishQos1Error::Encode(err)),
            }
        } else {
            Err(PublishQos1Error::Disconnected)
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: u16,
    packet: codec::Subscribe,
    sink: Rc<RefCell<MqttSinkInner>>,
    state: Rc<RefCell<IoStateInner<codec::Codec>>>,
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
    pub fn topic_filter(
        mut self,
        filter: ByteString,
        opts: codec::SubscriptionOptions,
    ) -> Self {
        self.packet.topic_filters.push((filter, opts));
        self
    }

    /// Add user property
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    /// Send subscribe packet
    pub async fn send(self) -> Result<codec::SubscribeAck, SendPacketError> {
        let st = self.state;
        let sink = self.sink;
        let mut packet = self.packet;

        let mut inner = sink.borrow_mut();
        let mut state = st.borrow_mut();
        if state.is_opened() {
            // handle client receive maximum
            if !inner.has_credit() {
                let (tx, rx) = inner.pool.waiters.channel();
                inner.waiters.push_back(tx);

                // do not borrow cross yield points
                drop(inner);
                drop(state);

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }

                state = st.borrow_mut();
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
            packet.packet_id = NonZeroU16::new(idx).unwrap();

            // send subscribe to client
            log::trace!("Sending subscribe packet {:#?}", packet);

            match state.send(codec::Packet::Subscribe(packet)) {
                Ok(_) => {
                    // do not borrow cross yield points
                    drop(state);
                    drop(inner);

                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(|pkt| pkt.subscribe())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

/// Unsubscribe packet builder
pub struct UnsubscribeBuilder {
    id: u16,
    packet: codec::Unsubscribe,
    sink: Rc<RefCell<MqttSinkInner>>,
    state: Rc<RefCell<IoStateInner<codec::Codec>>>,
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
        self.packet.topic_filters.push(filter);
        self
    }

    /// Add user property
    pub fn property(mut self, key: ByteString, value: ByteString) -> Self {
        self.packet.user_properties.push((key, value));
        self
    }

    /// Send unsubscribe packet
    pub async fn send(self) -> Result<codec::UnsubscribeAck, SendPacketError> {
        let st = self.state;
        let sink = self.sink;
        let mut packet = self.packet;

        let mut state = st.borrow_mut();
        let mut inner = sink.borrow_mut();
        if state.is_opened() {
            // handle client receive maximum
            if !inner.has_credit() {
                let (tx, rx) = inner.pool.waiters.channel();
                inner.waiters.push_back(tx);

                // do not borrow cross yield points
                drop(state);
                drop(inner);

                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }

                state = st.borrow_mut();
                inner = sink.borrow_mut();
            }

            // ack channel
            let (tx, rx) = inner.pool.queue.channel();

            // allocate packet id
            let idx = if self.id == 0 { inner.next_id() } else { self.id };
            if inner.inflight.contains_key(&idx) {
                return Err(SendPacketError::PacketIdInUse(idx));
            }
            inner.inflight.insert(idx, (tx, AckType::Unsubscribe));
            inner.inflight_order.push_back(idx);
            packet.packet_id = NonZeroU16::new(idx).unwrap();

            // send unsubscribe to client
            log::trace!("Sending unsubscribe packet {:#?}", packet);

            match state.send(codec::Packet::Unsubscribe(packet)) {
                Ok(_) => {
                    // do not borrow cross yield points
                    drop(state);
                    drop(inner);

                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(|pkt| pkt.unsubscribe())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}
