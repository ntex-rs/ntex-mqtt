use ntex::util::ByteString;
use std::{io, marker::PhantomData, num::NonZeroU16};

use super::codec;
use crate::{error, types::QoS};

#[derive(Debug)]
pub enum ControlMessage<E> {
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
    /// Service level error
    Error(Error<E>),
    /// Protocol level error
    ProtocolError(ProtocolError),
    /// Peer is gone
    PeerGone(PeerGone),
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

impl<E> ControlMessage<E> {
    /// Create a new PING `ControlMessage`.
    #[doc(hidden)]
    pub fn ping() -> Self {
        ControlMessage::Ping(Ping)
    }

    /// Create a new `ControlMessage` from SUBSCRIBE packet.
    #[doc(hidden)]
    pub fn subscribe(pkt: Subscribe) -> Self {
        ControlMessage::Subscribe(pkt)
    }

    /// Create a new `ControlMessage` from UNSUBSCRIBE packet.
    #[doc(hidden)]
    pub fn unsubscribe(pkt: Unsubscribe) -> Self {
        ControlMessage::Unsubscribe(pkt)
    }

    /// Create a new `ControlMessage` from DISCONNECT packet.
    #[doc(hidden)]
    pub fn remote_disconnect() -> Self {
        ControlMessage::Disconnect(Disconnect)
    }

    pub(super) fn closed() -> Self {
        ControlMessage::Closed(Closed)
    }

    pub(super) fn error(err: E) -> Self {
        ControlMessage::Error(Error::new(err))
    }

    pub(super) fn proto_error(err: error::ProtocolError) -> Self {
        ControlMessage::ProtocolError(ProtocolError::new(err))
    }

    /// Create a new `ControlMessage` from DISCONNECT packet.
    pub(super) fn peer_gone(err: Option<io::Error>) -> Self {
        ControlMessage::PeerGone(PeerGone(err))
    }

    /// Disconnects the client by sending DISCONNECT packet.
    pub fn disconnect(&self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Ping;

impl Ping {
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Ping }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Disconnect;

impl Disconnect {
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
    }
}

/// Service level error
#[derive(Debug)]
pub struct Error<E> {
    err: E,
}

impl<E> Error<E> {
    pub fn new(err: E) -> Self {
        Self { err }
    }

    #[inline]
    /// Returns reference to mqtt error
    pub fn get_ref(&self) -> &E {
        &self.err
    }

    #[inline]
    /// Ack service error, return disconnect packet and close connection.
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
    }

    #[inline]
    /// Ack service error, return disconnect packet and close connection.
    pub fn ack_and_error(self) -> (ControlResult, E) {
        (ControlResult { result: ControlResultKind::Disconnect }, self.err)
    }
}

/// Protocol level error
#[derive(Debug)]
pub struct ProtocolError {
    err: error::ProtocolError,
}

impl ProtocolError {
    pub fn new(err: error::ProtocolError) -> Self {
        Self { err }
    }

    #[inline]
    /// Returns reference to a protocol error
    pub fn get_ref(&self) -> &error::ProtocolError {
        &self.err
    }

    #[inline]
    /// Ack protocol error, return disconnect packet and close connection.
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
    }

    #[inline]
    /// Ack protocol error, return disconnect packet and close connection.
    pub fn ack_and_error(self) -> (ControlResult, error::ProtocolError) {
        (ControlResult { result: ControlResultKind::Disconnect }, self.err)
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
    /// Create a new `Subscribe` control message from packet id and
    /// a list of topics.
    #[doc(hidden)]
    pub fn new(packet_id: NonZeroU16, topics: Vec<(ByteString, QoS)>) -> Self {
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
    /// Create a new `Unsubscribe` control message from packet id and
    /// a list of topics.
    #[doc(hidden)]
    pub fn new(packet_id: NonZeroU16, topics: Vec<ByteString>) -> Self {
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
pub struct Closed;

impl Closed {
    #[inline]
    /// convert packet to a result
    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Closed }
    }
}

#[derive(Debug)]
pub struct PeerGone(pub(super) Option<io::Error>);

impl PeerGone {
    /// Returns error reference
    pub fn err(&self) -> Option<&io::Error> {
        self.0.as_ref()
    }

    /// Take error
    pub fn take(&mut self) -> Option<io::Error> {
        self.0.take()
    }

    pub fn ack(self) -> ControlResult {
        ControlResult { result: ControlResultKind::Nothing }
    }
}
