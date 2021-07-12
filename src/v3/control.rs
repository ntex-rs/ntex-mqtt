use ntex::util::ByteString;
use std::{marker::PhantomData, num::NonZeroU16};

use super::codec;
use crate::types::QoS;

#[derive(Debug)]
pub enum ControlMessage {
    /// Ping packet
    Ping(Ping),
    /// Disconnect packet
    Disconnect(Disconnect),
    /// Subscribe packet
    Subscribe(Subscribe),
    /// Unsubscribe packet
    Unsubscribe(Unsubscribe),
    /// Connection dropped
    Closed(Closed),
}

#[derive(Debug)]
pub struct ControlResult {
    pub(crate) result: ControlResultKind,
}

#[derive(Debug)]
pub(crate) enum ControlResultKind {
    Nothing,
    PublishAck(NonZeroU16),
    Ping,
    Disconnect,
    Subscribe(SubscribeResult),
    Unsubscribe(UnsubscribeResult),
    Closed,
}

impl ControlMessage {
    pub(crate) fn ping() -> Self {
        ControlMessage::Ping(Ping)
    }

    pub(crate) fn pkt_disconnect() -> Self {
        ControlMessage::Disconnect(Disconnect)
    }

    pub(crate) fn closed(is_error: bool) -> Self {
        ControlMessage::Closed(Closed::new(is_error))
    }

    pub fn disconnect(&self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
    }
}

#[derive(Debug)]
pub struct Ping;

impl Ping {
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Ping }
    }
}

#[derive(Debug)]
pub struct Disconnect;

impl Disconnect {
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
    }
}

/// Subscribe message
#[derive(Debug)]
pub struct Subscribe {
    packet_id: NonZeroU16,
    topics: Vec<(ByteString, QoS)>,
    codes: Vec<codec::SubscribeReturnCode>,
}

/// Result of a subscribe message
#[derive(Debug)]
pub(crate) struct SubscribeResult {
    pub(crate) codes: Vec<codec::SubscribeReturnCode>,
    pub(crate) packet_id: NonZeroU16,
}

impl Subscribe {
    pub(crate) fn new(packet_id: NonZeroU16, topics: Vec<(ByteString, QoS)>) -> Self {
        let mut codes = Vec::with_capacity(topics.len());
        (0..topics.len()).for_each(|_| codes.push(codec::SubscribeReturnCode::Failure));

        Self { packet_id, topics, codes }
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> SubscribeIter<'_> {
        SubscribeIter { subs: self as *const _ as *mut _, entry: 0, lt: PhantomData }
    }

    #[inline]
    /// convert subscription to a result
    pub fn ack(self) -> ControlResult {
        ControlResult {
            result: ControlResultKind::Subscribe(SubscribeResult {
                codes: self.codes,
                packet_id: self.packet_id,
            }),
        }
    }
}

impl<'a> IntoIterator for &'a mut Subscribe {
    type Item = Subscription<'a>;
    type IntoIter = SubscribeIter<'a>;

    fn into_iter(self) -> SubscribeIter<'a> {
        self.iter_mut()
    }
}

/// Iterator over subscription topics
pub struct SubscribeIter<'a> {
    subs: *mut Subscribe,
    entry: usize,
    lt: PhantomData<&'a mut Subscribe>,
}

impl<'a> SubscribeIter<'a> {
    fn next_unsafe(&mut self) -> Option<Subscription<'a>> {
        let subs = unsafe { &mut *self.subs };

        if self.entry < subs.topics.len() {
            let s = Subscription {
                topic: &subs.topics[self.entry].0,
                qos: subs.topics[self.entry].1,
                code: &mut subs.codes[self.entry],
            };
            self.entry += 1;
            Some(s)
        } else {
            None
        }
    }
}

impl<'a> Iterator for SubscribeIter<'a> {
    type Item = Subscription<'a>;

    #[inline]
    fn next(&mut self) -> Option<Subscription<'a>> {
        self.next_unsafe()
    }
}

/// Subscription topic
#[derive(Debug)]
pub struct Subscription<'a> {
    topic: &'a ByteString,
    qos: QoS,
    code: &'a mut codec::SubscribeReturnCode,
}

impl<'a> Subscription<'a> {
    #[inline]
    /// subscription topic
    pub fn topic(&self) -> &'a ByteString {
        self.topic
    }

    #[inline]
    /// the level of assurance for delivery of an Application Message.
    pub fn qos(&self) -> QoS {
        self.qos
    }

    #[inline]
    /// fail to subscribe to the topic
    pub fn fail(&mut self) {
        *self.code = codec::SubscribeReturnCode::Failure
    }

    #[inline]
    /// confirm subscription to a topic with specific qos
    pub fn confirm(&mut self, qos: QoS) {
        *self.code = codec::SubscribeReturnCode::Success(qos)
    }

    #[inline]
    #[doc(hidden)]
    /// confirm subscription to a topic with specific qos
    pub fn subscribe(&mut self, qos: QoS) {
        self.confirm(qos)
    }
}

/// Unsubscribe message
#[derive(Debug)]
pub struct Unsubscribe {
    packet_id: NonZeroU16,
    topics: Vec<ByteString>,
}

/// Result of a unsubscribe message
#[derive(Debug)]
pub(crate) struct UnsubscribeResult {
    pub(crate) packet_id: NonZeroU16,
}

impl Unsubscribe {
    pub(crate) fn new(packet_id: NonZeroU16, topics: Vec<ByteString>) -> Self {
        Self { packet_id, topics }
    }

    /// returns iterator over unsubscribe topics
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.topics.iter()
    }

    #[inline]
    /// convert packet to a result
    pub fn ack(self) -> ControlResult {
        ControlResult {
            result: ControlResultKind::Unsubscribe(UnsubscribeResult {
                packet_id: self.packet_id,
            }),
        }
    }
}

/// Connection closed message
#[derive(Debug)]
pub struct Closed {
    is_error: bool,
}

impl Closed {
    pub(crate) fn new(is_error: bool) -> Self {
        Self { is_error }
    }

    /// Returns error state on connection close
    pub fn is_error(&self) -> bool {
        self.is_error
    }

    #[inline]
    /// convert packet to a result
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Closed }
    }
}
