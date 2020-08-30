use std::num::{NonZeroU16, NonZeroU32};
use std::{cell::RefCell, collections::VecDeque, fmt, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{err, Either, Future};
use ntex::channel::{mpsc, pool};
use slab::Slab;

use crate::v5::error::{
    EncodeError, ProtocolError, PublishQos0Error, PublishQos1Error, SubscribeError,
};
use crate::{types::packet_type, types::QoS, v5::codec};

pub struct MqttSink(Rc<RefCell<MqttSinkInner>>);

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

pub(crate) enum Info {
    Written,
    Error(EncodeError),
}

pub(crate) struct MqttSinkPool {
    info: pool::Pool<Info>,
    queue: pool::Pool<Ack>,
    waiters: pool::Pool<()>,
}

impl Default for MqttSinkPool {
    fn default() -> Self {
        Self { info: pool::new(), queue: pool::new(), waiters: pool::new() }
    }
}

struct MqttSinkInner {
    cap: usize,
    sink: Option<mpsc::Sender<(codec::Packet, usize)>>,
    info: Slab<(pool::Sender<Info>, usize)>,
    queue: Slab<(pool::Sender<Ack>, AckType)>,
    queue_order: VecDeque<usize>,
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
        max_receive: usize,
        pool: Rc<MqttSinkPool>,
    ) -> Self {
        MqttSink(Rc::new(RefCell::new(MqttSinkInner {
            pool,
            cap: max_receive,
            sink: Some(sink),
            queue: Slab::with_capacity(8),
            queue_order: VecDeque::new(),
            waiters: VecDeque::new(),
            info: Slab::with_capacity(12),
        })))
    }

    /// Get client receive credit
    pub fn credit(&self) -> usize {
        let inner = self.0.borrow();
        inner.cap - inner.queue.len()
    }

    /// Close mqtt connection with default Disconnect message
    pub fn close(&self) {
        if let Some(sink) = self.0.borrow_mut().sink.take() {
            let _ = sink.send((codec::Packet::Disconnect(codec::Disconnect::default()), 0));
        }
    }

    /// Close mqtt connection
    pub fn close_with_reason(&self, pkt: codec::Disconnect) {
        if let Some(sink) = self.0.borrow_mut().sink.take() {
            let _ = sink.send((codec::Packet::Disconnect(pkt), 0));
        }
    }

    /// Send ping
    pub(super) fn ping(&self) -> bool {
        if let Some(sink) = self.0.borrow_mut().sink.take() {
            sink.send((codec::Packet::PingRequest, 0)).is_ok()
        } else {
            false
        }
    }

    /// Close mqtt connection, dont send disconnect message
    pub(super) fn drop_sink(&self) {
        let _ = self.0.borrow_mut().sink.take();
    }

    pub(super) fn pkt_written(&self, idx: usize) {
        let mut inner = self.0.borrow_mut();

        let idx = idx - 1;
        if inner.info.contains(idx) {
            let (tx, _) = inner.info.remove(idx);
            let _ = tx.send(Info::Written);
        } else {
            unreachable!("Internal: Can not get notification channel from queue");
        }
    }

    pub(super) fn pkt_encode_err(&self, idx: usize, err: EncodeError) {
        let mut inner = self.0.borrow_mut();

        let idx = idx - 1;
        if inner.info.contains(idx) {
            let (tx, idx) = inner.info.remove(idx);
            let _ = tx.send(Info::Error(err));

            // we have qos1 publish in queue, waiting for ack.
            // but publish packet is failed to encode,
            // so we have to cleanup queue
            if idx != 0 {
                for item in &mut inner.queue_order {
                    if *item == idx {
                        *item = 0;
                        inner.queue.remove(idx - 1);
                        if let Some(&0) = inner.queue_order.front() {
                            let _ = inner.queue_order.pop_front();
                        }
                        break;
                    }
                }
            }
        } else {
            unreachable!("Internal: Can not get encoder channel");
        }
    }

    pub(super) fn pkt_ack(&self, pkt: Ack) -> Result<(), ProtocolError> {
        let mut inner = self.0.borrow_mut();

        loop {
            // check ack order
            if let Some(idx) = inner.queue_order.pop_front() {
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
                    let idx = (pkt.packet_id() - 1) as usize;
                    if inner.queue.contains(idx) {
                        let (tx, tp) = inner.queue.remove(idx);
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
                        unreachable!("Internal: Can not get puublish ack channel")
                    }
                }
            } else {
                log::trace!("Unexpected PublishAck packet");
            }
            return Err(ProtocolError::PacketIdMismatch);
        }
    }

    /// Build publish packet
    pub fn publish(&self, topic: ByteString, payload: Bytes) -> PublishBuilder<'_> {
        PublishBuilder {
            packet: Some(codec::Publish {
                topic,
                payload,
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
                properties: codec::PublishProperties::default(),
            }),
            sink: self,
        }
    }

    /// Build subscribe packet
    pub fn subscribe(&self, id: Option<NonZeroU32>) -> SubscribeBuilder<'_> {
        SubscribeBuilder {
            packet: Some(codec::Subscribe {
                id,
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Vec::new(),
                topic_filters: Vec::new(),
            }),
            sink: self,
        }
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
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

    fn packet_id(&self) -> usize {
        match self {
            Ack::Publish(ref pkt) => pkt.packet_id.get() as usize,
            Ack::Subscribe(ref pkt) => pkt.packet_id.get() as usize,
            Ack::Unsubscribe(ref pkt) => pkt.packet_id.get() as usize,
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

pub struct PublishBuilder<'a> {
    sink: &'a MqttSink,
    packet: Option<codec::Publish>,
}

impl<'a> PublishBuilder<'a> {
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(&mut self, val: bool) -> &mut Self {
        if let Some(ref mut packet) = self.packet {
            packet.dup = val;
        }
        self
    }

    pub fn retain(&mut self) -> &mut Self {
        if let Some(ref mut packet) = self.packet {
            packet.retain = true;
        }
        self
    }

    pub fn properties<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut codec::PublishProperties),
    {
        if let Some(ref mut packet) = self.packet {
            f(&mut packet.properties);
        }
        self
    }

    /// Send publish packet with QoS 0
    pub fn send_at_most_once(&mut self) -> impl Future<Output = Result<(), PublishQos0Error>> {
        if let Some(packet) = self.packet.take() {
            let mut inner = self.sink.0.borrow_mut();

            // encoder notification channel
            let (info_tx, info_rx) = inner.pool.info.channel();
            let info_idx = inner.info.insert((info_tx, 0)) + 1;

            if let Some(ref sink) = inner.sink {
                log::trace!("Publish (QoS-0) to {:?}", packet.topic);
                if sink.send((codec::Packet::Publish(packet), info_idx)).is_ok() {
                    // wait notification from encoder
                    return Either::Left(async move {
                        let info = info_rx.await.map_err(|_| PublishQos0Error::Disconnected)?;
                        match info {
                            Info::Written => Ok(()),
                            Info::Error(err) => Err(PublishQos0Error::Encode(err)),
                        }
                    });
                }
            }
            log::error!("Mqtt sink is disconnected");
            Either::Right(err(PublishQos0Error::Disconnected))
        } else {
            panic!("PublishBuilder can be used only once.");
        }
    }

    /// Send publish packet with QoS 1
    pub fn send_at_least_once(
        &mut self,
    ) -> impl Future<Output = Result<codec::PublishAck, PublishQos1Error>> {
        if let Some(mut packet) = self.packet.take() {
            packet.qos = QoS::AtLeastOnce;

            let sink = self.sink.0.clone();

            async move {
                let mut inner = sink.borrow_mut();
                if inner.sink.is_some() {
                    // handle client receive maximum
                    if inner.cap - inner.queue.len() == 0 {
                        let (tx, rx) = inner.pool.waiters.channel();
                        inner.waiters.push_back(tx);

                        // do not borrow cross yield points
                        drop(inner);

                        if rx.await.is_err() {
                            return Err(PublishQos1Error::Disconnected);
                        }

                        inner = sink.borrow_mut();
                    }

                    // publish ack channel
                    let (tx, rx) = inner.pool.queue.channel();

                    // allocate packet id
                    let idx = inner.queue.insert((tx, AckType::Publish)) + 1;
                    if idx > u16::max_value() as usize {
                        return Err(PublishQos1Error::PacketIdNotAvailable);
                    }
                    inner.queue_order.push_back(idx);
                    packet.packet_id = NonZeroU16::new(idx as u16);

                    // encoder channel (item written/encoder error)
                    let (info_tx, info_rx) = inner.pool.info.channel();
                    let info_idx = inner.info.insert((info_tx, idx)) + 1;

                    // send publish to client
                    log::trace!("Publish (QoS1) to {:#?}", packet);

                    let send_result = inner
                        .sink
                        .as_ref()
                        .unwrap()
                        .send((codec::Packet::Publish(packet), info_idx));

                    if send_result.is_err() {
                        Err(PublishQos1Error::Disconnected)
                    } else {
                        // do not borrow cross yield points
                        drop(inner);

                        // wait notification from encoder
                        let info = info_rx.await.map_err(|_| PublishQos1Error::Disconnected)?;
                        match info {
                            Info::Written => (),
                            Info::Error(err) => return Err(PublishQos1Error::Encode(err)),
                        }

                        // wait ack from peer
                        rx.await.map_err(|_| PublishQos1Error::Disconnected).and_then(|pkt| {
                            let pkt = pkt.publish();
                            match pkt.reason_code {
                                codec::PublishAckReason::Success => Ok(pkt),
                                _ => Err(PublishQos1Error::Fail(pkt)),
                            }
                        })
                    }
                } else {
                    Err(PublishQos1Error::Disconnected)
                }
            }
        } else {
            panic!("PublishBuilder can be used only once.");
        }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder<'a> {
    sink: &'a MqttSink,
    packet: Option<codec::Subscribe>,
}

impl<'a> SubscribeBuilder<'a> {
    /// Add topic filter
    pub fn topic_filter(
        &mut self,
        filter: ByteString,
        opts: codec::SubscriptionOptions,
    ) -> &mut Self {
        if let Some(ref mut packet) = self.packet {
            packet.topic_filters.push((filter, opts));
        }
        self
    }

    /// Add user property
    pub fn property(&mut self, key: ByteString, value: ByteString) -> &mut Self {
        if let Some(ref mut packet) = self.packet {
            packet.user_properties.push((key, value));
        }
        self
    }

    /// Send subscribe packet
    ///
    /// Panics if none of topic filters are specified.
    pub fn send(
        &mut self,
    ) -> impl Future<Output = Result<codec::SubscribeAck, SubscribeError>> {
        if let Some(mut packet) = self.packet.take() {
            if packet.topic_filters.is_empty() {
                panic!("topic_filters is empty.");
            }
            let sink = self.sink.0.clone();

            async move {
                let mut inner = sink.borrow_mut();
                if inner.sink.is_some() {
                    // handle client receive maximum
                    if inner.cap - inner.queue.len() == 0 {
                        let (tx, rx) = inner.pool.waiters.channel();
                        inner.waiters.push_back(tx);

                        // do not borrow cross yield points
                        drop(inner);

                        if rx.await.is_err() {
                            return Err(SubscribeError::Disconnected);
                        }

                        inner = sink.borrow_mut();
                    }

                    // ack channel
                    let (tx, rx) = inner.pool.queue.channel();

                    // allocate packet id
                    let idx = inner.queue.insert((tx, AckType::Subscribe)) + 1;
                    if idx > u16::max_value() as usize {
                        return Err(SubscribeError::PacketIdNotAvailable);
                    }
                    inner.queue_order.push_back(idx);
                    packet.packet_id = NonZeroU16::new(idx as u16).unwrap();

                    // encoder channel (item written/encoder error)
                    let (info_tx, info_rx) = inner.pool.info.channel();
                    let info_idx = inner.info.insert((info_tx, idx)) + 1;

                    // send subscribe to client
                    log::trace!("Sending subscribe packet {:#?}", packet);

                    let send_result = inner
                        .sink
                        .as_ref()
                        .unwrap()
                        .send((codec::Packet::Subscribe(packet), info_idx));

                    if send_result.is_err() {
                        Err(SubscribeError::Disconnected)
                    } else {
                        // do not borrow cross yield points
                        drop(inner);

                        // wait notification from encoder
                        let info = info_rx.await.map_err(|_| SubscribeError::Disconnected)?;
                        match info {
                            Info::Written => (),
                            Info::Error(err) => return Err(SubscribeError::Encode(err)),
                        }

                        // wait ack from peer
                        rx.await
                            .map_err(|_| SubscribeError::Disconnected)
                            .map(|pkt| pkt.subscribe())
                    }
                } else {
                    Err(SubscribeError::Disconnected)
                }
            }
        } else {
            panic!("SubscribeBuilder can be used only once.");
        }
    }
}
