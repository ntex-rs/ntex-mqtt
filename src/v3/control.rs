use ntex_bytes::ByteString;
use std::{io, marker::PhantomData, num::NonZeroU16};

use super::codec;
use crate::{error, types::QoS};

/// Server control messages
#[derive(Debug)]
pub enum Control<E> {
    /// Publish release
    PublishRelease(PublishRelease),
    /// Ping packet
    Ping(Ping),
    /// Disconnect packet
    Disconnect(Disconnect),
    /// Subscribe packet
    Subscribe(Subscribe),
    /// Unsubscribe packet
    Unsubscribe(Unsubscribe),
    /// Write back-pressure is enabled/disabled
    WrBackpressure(WrBackpressure),
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
pub struct ControlAck {
    pub(crate) result: ControlAckKind,
}

#[derive(Debug)]
pub(crate) enum ControlAckKind {
    Nothing,
    PublishAck(NonZeroU16),
    PublishRelease(NonZeroU16),
    Ping,
    Disconnect,
    Subscribe(SubscribeResult),
    Unsubscribe(UnsubscribeResult),
    Closed,
}

impl<E> Control<E> {
    pub(crate) fn pubrel(packet_id: NonZeroU16) -> Self {
        Control::PublishRelease(PublishRelease { packet_id })
    }

    /// Create a new PING `Control` message.
    #[doc(hidden)]
    pub fn ping() -> Self {
        Control::Ping(Ping)
    }

    /// Create a new `Control` message from SUBSCRIBE packet.
    #[doc(hidden)]
    pub fn subscribe(pkt: Subscribe) -> Self {
        Control::Subscribe(pkt)
    }

    /// Create a new `Control` message from UNSUBSCRIBE packet.
    #[doc(hidden)]
    pub fn unsubscribe(pkt: Unsubscribe) -> Self {
        Control::Unsubscribe(pkt)
    }

    /// Create a new `Control` message from DISCONNECT packet.
    #[doc(hidden)]
    pub fn remote_disconnect() -> Self {
        Control::Disconnect(Disconnect)
    }

    pub(super) const fn closed() -> Self {
        Control::Closed(Closed)
    }

    pub(super) const fn wr_backpressure(enabled: bool) -> Self {
        Control::WrBackpressure(WrBackpressure(enabled))
    }

    pub(super) fn error(err: E) -> Self {
        Control::Error(Error::new(err))
    }

    pub(super) fn proto_error(err: error::ProtocolError) -> Self {
        Control::ProtocolError(ProtocolError::new(err))
    }

    /// Create a new `Control` message from DISCONNECT packet.
    pub(super) fn peer_gone(err: Option<io::Error>) -> Self {
        Control::PeerGone(PeerGone(err))
    }

    /// Disconnects the client by sending DISCONNECT packet.
    pub fn disconnect(&self) -> ControlAck {
        ControlAck { result: ControlAckKind::Disconnect }
    }

    /// Ack control message
    pub fn ack(self) -> ControlAck {
        match self {
            Control::PublishRelease(msg) => msg.ack(),
            Control::Ping(msg) => msg.ack(),
            Control::Disconnect(msg) => msg.ack(),
            Control::Subscribe(_) => {
                log::warn!("Subscribe is not supported");
                ControlAck { result: ControlAckKind::Disconnect }
            }
            Control::Unsubscribe(_) => {
                log::warn!("Unsubscribe is not supported");
                ControlAck { result: ControlAckKind::Disconnect }
            }
            Control::WrBackpressure(msg) => msg.ack(),
            Control::Closed(msg) => msg.ack(),
            Control::Error(msg) => msg.ack(),
            Control::ProtocolError(msg) => msg.ack(),
            Control::PeerGone(msg) => msg.ack(),
        }
    }
}

/// Publish release
#[derive(Copy, Clone, Debug)]
pub struct PublishRelease {
    pub packet_id: NonZeroU16,
}

impl PublishRelease {
    #[inline]
    /// Packet Identifier
    pub fn id(self) -> NonZeroU16 {
        self.packet_id
    }

    #[inline]
    /// convert packet to a result
    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::PublishRelease(self.packet_id) }
    }
}

pub(crate) struct PublishReleaseResult {
    pub packet_id: NonZeroU16,
}

#[derive(Copy, Clone, Debug)]
pub struct Ping;

impl Ping {
    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::Ping }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Disconnect;

impl Disconnect {
    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::Disconnect }
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
    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::Disconnect }
    }

    #[inline]
    /// Ack service error, return disconnect packet and close connection.
    pub fn ack_and_error(self) -> (ControlAck, E) {
        (ControlAck { result: ControlAckKind::Disconnect }, self.err)
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
    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::Disconnect }
    }

    #[inline]
    /// Ack protocol error, return disconnect packet and close connection.
    pub fn ack_and_error(self) -> (ControlAck, error::ProtocolError) {
        (ControlAck { result: ControlAckKind::Disconnect }, self.err)
    }
}

/// Subscribe message
#[derive(Debug)]
pub struct Subscribe {
    packet_id: NonZeroU16,
    packet_size: u32,
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
    pub fn new(
        packet_id: NonZeroU16,
        packet_size: u32,
        topics: Vec<(ByteString, QoS)>,
    ) -> Self {
        let mut codes = Vec::with_capacity(topics.len());
        (0..topics.len()).for_each(|_| codes.push(codec::SubscribeReturnCode::Failure));

        Self { packet_id, packet_size, topics, codes }
    }

    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> SubscribeIter<'_> {
        SubscribeIter { subs: self as *const _ as *mut _, entry: 0, lt: PhantomData }
    }

    #[inline]
    /// convert subscription to a result
    pub fn ack(self) -> ControlAck {
        ControlAck {
            result: ControlAckKind::Subscribe(SubscribeResult {
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
    packet_size: u32,
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
    pub fn new(packet_id: NonZeroU16, packet_size: u32, topics: Vec<ByteString>) -> Self {
        Self { packet_id, packet_size, topics }
    }

    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    /// returns iterator over unsubscribe topics
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.topics.iter()
    }

    #[inline]
    /// convert packet to a result
    pub fn ack(self) -> ControlAck {
        ControlAck {
            result: ControlAckKind::Unsubscribe(UnsubscribeResult {
                packet_id: self.packet_id,
            }),
        }
    }
}

/// Write back-pressure message
#[derive(Debug)]
pub struct WrBackpressure(bool);

impl WrBackpressure {
    #[inline]
    /// Is write back-pressure enabled
    pub fn enabled(&self) -> bool {
        self.0
    }

    #[inline]
    /// convert packet to a result
    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::Nothing }
    }
}

/// Connection closed message
#[derive(Debug)]
pub struct Closed;

impl Closed {
    #[inline]
    /// convert packet to a result
    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::Closed }
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

    pub fn ack(self) -> ControlAck {
        ControlAck { result: ControlAckKind::Nothing }
    }
}
