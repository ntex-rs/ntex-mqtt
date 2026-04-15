use std::{fmt, marker::PhantomData, num::NonZeroU16, ptr};

use ntex_bytes::ByteString;

use crate::{types::QoS, v3::codec};

/// MQTT protocol–related messages.
///
/// The control service is always called with these messages one at a time.
/// Unhandled messages are stored in a buffer.
#[derive(Debug)]
pub enum ProtocolMessage {
    /// Publish release
    PublishRelease(PublishRelease),
    /// Subscribe packet
    Subscribe(Subscribe),
    /// Unsubscribe packet
    Unsubscribe(Unsubscribe),
    /// Disconnect packet
    Disconnect(Disconnect),
    /// Ping packet from a client
    Ping(Ping),
}

#[derive(Debug)]
pub struct ProtocolMessageAck {
    pub(crate) result: ProtocolMessageKind,
}

#[derive(Debug)]
pub(crate) enum ProtocolMessageKind {
    Nothing,
    PublishAck(NonZeroU16),
    PublishRelease(NonZeroU16),
    Ping,
    Disconnect,
    Subscribe(SubscribeResult),
    Unsubscribe(UnsubscribeResult),
    Closed,
}

impl ProtocolMessage {
    pub(crate) fn pubrel(packet_id: NonZeroU16) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease { packet_id })
    }

    /// Create a new PING `Control` message.
    #[doc(hidden)]
    pub fn ping() -> Self {
        ProtocolMessage::Ping(Ping)
    }

    /// Create a new `Control` message from SUBSCRIBE packet.
    #[doc(hidden)]
    pub fn subscribe(pkt: Subscribe) -> Self {
        ProtocolMessage::Subscribe(pkt)
    }

    /// Create a new `Control` message from UNSUBSCRIBE packet.
    #[doc(hidden)]
    pub fn unsubscribe(pkt: Unsubscribe) -> Self {
        ProtocolMessage::Unsubscribe(pkt)
    }

    /// Create a new `Control` message from DISCONNECT packet.
    #[doc(hidden)]
    pub fn remote_disconnect() -> Self {
        ProtocolMessage::Disconnect(Disconnect)
    }

    #[inline]
    /// Disconnects the client by sending DISCONNECT packet.
    pub fn disconnect(&self) -> ProtocolMessageAck {
        ProtocolMessageAck { result: ProtocolMessageKind::Disconnect }
    }

    #[inline]
    /// Ack control message
    pub fn ack(self) -> ProtocolMessageAck {
        match self {
            ProtocolMessage::PublishRelease(msg) => msg.ack(),
            ProtocolMessage::Disconnect(msg) => msg.ack(),
            ProtocolMessage::Subscribe(_) => {
                log::warn!("Subscribe is not supported");
                ProtocolMessageAck { result: ProtocolMessageKind::Disconnect }
            }
            ProtocolMessage::Unsubscribe(_) => {
                log::warn!("Unsubscribe is not supported");
                ProtocolMessageAck { result: ProtocolMessageKind::Disconnect }
            }
            ProtocolMessage::Ping(msg) => msg.ack(),
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
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck { result: ProtocolMessageKind::PublishRelease(self.packet_id) }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Ping;

impl Ping {
    #[inline]
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck { result: ProtocolMessageKind::Ping }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Disconnect;

impl Disconnect {
    #[inline]
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck { result: ProtocolMessageKind::Disconnect }
    }
}

/// Subscribe message
#[derive(Debug, Clone)]
pub struct Subscribe {
    packet_id: NonZeroU16,
    packet_size: u32,
    topics: Vec<(ByteString, QoS)>,
    codes: Vec<codec::SubscribeReturnCode>,
}

/// Result of a subscribe message
#[derive(Debug, Clone)]
pub(crate) struct SubscribeResult {
    pub(crate) codes: Vec<codec::SubscribeReturnCode>,
    pub(crate) packet_id: NonZeroU16,
}

impl Subscribe {
    #[inline]
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

    #[inline]
    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> SubscribeIter<'_> {
        SubscribeIter { subs: ptr::from_ref(self).cast_mut(), entry: 0, lt: PhantomData }
    }

    #[inline]
    /// convert subscription to a result
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Subscribe(SubscribeResult {
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

impl fmt::Debug for SubscribeIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeIter").finish()
    }
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
        *self.code = codec::SubscribeReturnCode::Failure;
    }

    #[inline]
    /// confirm subscription to a topic with specific qos
    pub fn confirm(&mut self, qos: QoS) {
        *self.code = codec::SubscribeReturnCode::Success(qos);
    }

    #[inline]
    #[doc(hidden)]
    /// confirm subscription to a topic with specific qos
    pub fn subscribe(&mut self, qos: QoS) {
        self.confirm(qos);
    }
}

/// Unsubscribe message
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    packet_id: NonZeroU16,
    packet_size: u32,
    topics: Vec<ByteString>,
}

/// Result of a unsubscribe message
#[derive(Debug, Copy, Clone)]
pub(crate) struct UnsubscribeResult {
    pub(crate) packet_id: NonZeroU16,
}

impl Unsubscribe {
    #[inline]
    /// Create a new `Unsubscribe` control message from packet id and
    /// a list of topics.
    #[doc(hidden)]
    pub fn new(packet_id: NonZeroU16, packet_size: u32, topics: Vec<ByteString>) -> Self {
        Self { packet_id, packet_size, topics }
    }

    #[inline]
    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    #[inline]
    /// returns iterator over unsubscribe topics
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.topics.iter()
    }

    #[inline]
    /// Ack control message
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Unsubscribe(UnsubscribeResult {
                packet_id: self.packet_id,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use ntex_bytes::ByteString;

    use super::*;
    use crate::types::QoS;

    #[test]
    fn test_debug() {
        // SubscribeIter via Subscribe
        let mut sub = Subscribe::new(
            NonZeroU16::new(1).unwrap(),
            0,
            vec![(ByteString::from_static("a/b"), QoS::AtLeastOnce)],
        );
        let iter = sub.iter_mut();
        assert!(format!("{iter:?}").contains("SubscribeIter"));

        // Unsubscribe
        let unsub = Unsubscribe::new(
            NonZeroU16::new(2).unwrap(),
            0,
            vec![ByteString::from_static("a/b")],
        );
        assert!(format!("{unsub:?}").contains("Unsubscribe"));

        // Ping, Disconnect, WrBackpressure, Shutdown, PeerGone
        assert!(format!("{Ping:?}").contains("Ping"));
        assert!(format!("{Disconnect:?}").contains("Disconnect"));
        assert!(format!("{:?}", WrBackpressure(true)).contains("WrBackpressure"));
        assert!(format!("{Shutdown:?}").contains("Shutdown"));
        assert!(format!("{:?}", PeerGone(None)).contains("PeerGone"));
    }
}
