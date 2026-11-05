//! MQTT 3.1.1 server control messages

use std::{fmt, marker::PhantomData, num::NonZeroU16, ptr};

use ntex_bytes::ByteString;

use crate::{types::QoS, v3::codec};

/// Inbound protocol messages handled separately from publications.
///
/// The protocol service handles one message at a time, with up to 16 waiting
/// messages buffered by the server dispatcher.
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

/// Result of a control message handling
#[derive(Debug)]
pub struct ProtocolMessageAck {
    /// Dispatcher action produced by handling one protocol message.
    pub(crate) result: ProtocolMessageKind,
}

/// Internal acknowledgment actions interpreted by the protocol dispatcher.
#[derive(Debug)]
pub(crate) enum ProtocolMessageKind {
    /// Produce no response packet.
    Nothing,
    /// Reserved publish acknowledgment action; not produced by these message helpers.
    PublishAck(NonZeroU16),
    /// Finish an incoming `QoS 2` exchange with PUBCOMP.
    PublishRelease(NonZeroU16),
    /// Respond to PINGREQ with PINGRESP.
    Ping,
    /// Drop the current payload and close the connection.
    Disconnect,
    /// Release the subscription packet identifier and send SUBACK.
    Subscribe(SubscribeResult),
    /// Release the unsubscription packet identifier and send UNSUBACK.
    Unsubscribe(UnsubscribeResult),
}

impl ProtocolMessage {
    /// Create a release message for an incoming `QoS 2` publication awaiting completion.
    pub(crate) fn pubrel(packet_id: NonZeroU16) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease { packet_id })
    }

    /// Create a PINGREQ protocol message.
    #[doc(hidden)]
    pub fn ping() -> Self {
        ProtocolMessage::Ping(Ping)
    }

    /// Wrap a SUBSCRIBE request as a protocol message.
    #[doc(hidden)]
    pub fn subscribe(pkt: Subscribe) -> Self {
        ProtocolMessage::Subscribe(pkt)
    }

    /// Wrap an UNSUBSCRIBE request as a protocol message.
    #[doc(hidden)]
    pub fn unsubscribe(pkt: Unsubscribe) -> Self {
        ProtocolMessage::Unsubscribe(pkt)
    }

    /// Create a protocol message for a peer’s DISCONNECT packet.
    #[doc(hidden)]
    pub fn remote_disconnect() -> Self {
        ProtocolMessage::Disconnect(Disconnect)
    }

    #[inline]
    /// Close the connection.
    ///
    /// Pending payload is dropped. Server does not send `Disconnect` packet,
    /// MQTT v3 does not define server-side `Disconnect`.
    pub fn disconnect(&self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Disconnect,
        }
    }

    #[inline]
    /// Acknowledge this protocol message with its default action.
    ///
    /// For `Subscribe` and `Unsubscribe` messages, this method closes the connection,
    /// use [`Subscribe::ack`] and [`Unsubscribe::ack`] instead.
    pub fn ack(self) -> ProtocolMessageAck {
        match self {
            ProtocolMessage::PublishRelease(msg) => msg.ack(),
            ProtocolMessage::Disconnect(msg) => msg.ack(),
            ProtocolMessage::Subscribe(_) => {
                log::warn!("Subscribe is not supported");
                ProtocolMessageAck {
                    result: ProtocolMessageKind::Disconnect,
                }
            }
            ProtocolMessage::Unsubscribe(_) => {
                log::warn!("Unsubscribe is not supported");
                ProtocolMessageAck {
                    result: ProtocolMessageKind::Disconnect,
                }
            }
            ProtocolMessage::Ping(msg) => msg.ack(),
        }
    }
}

/// Publish release
#[derive(Copy, Clone, Debug)]
pub struct PublishRelease {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
}

impl PublishRelease {
    #[inline]
    /// Packet Identifier
    pub fn id(self) -> NonZeroU16 {
        self.packet_id
    }

    #[inline]
    /// Complete this release by requesting PUBCOMP for its packet identifier.
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::PublishRelease(self.packet_id),
        }
    }
}

/// PING request from the peer
#[derive(Copy, Clone, Debug)]
pub struct Ping;

impl Ping {
    #[inline]
    /// Ack ping message, sends `PingResponse` packet to the peer
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Ping,
        }
    }
}

/// DISCONNECT packet from the peer
#[derive(Copy, Clone, Debug)]
pub struct Disconnect;

impl Disconnect {
    #[inline]
    /// Ack disconnect message, closes the connection
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Disconnect,
        }
    }
}

/// SUBSCRIBE request with one mutable acknowledgment result per filter.
#[derive(Debug, Clone)]
pub struct Subscribe {
    /// Identifier echoed in SUBACK and reserved until handling completes.
    packet_id: NonZeroU16,
    /// SUBSCRIBE remaining length in bytes, excluding its fixed header.
    packet_size: u32,
    /// Requested topic filters and their maximum delivery qualities, in wire order.
    topics: Vec<(ByteString, QoS)>,
    /// Per-filter results, initially failures and updated through `Subscription` entries.
    codes: Vec<codec::SubscribeReturnCode>,
}

/// Result of a subscribe message
#[derive(Debug, Clone)]
pub(crate) struct SubscribeResult {
    /// Granted qualities or failures in the original topic-filter order.
    pub(crate) codes: Vec<codec::SubscribeReturnCode>,
    /// Identifier of the SUBSCRIBE packet being acknowledged.
    pub(crate) packet_id: NonZeroU16,
}

impl Subscribe {
    #[inline]
    /// Create a SUBSCRIBE request with all per-filter acknowledgment results set to failure.
    ///
    /// `packet_size` is the remaining length in bytes, excluding the fixed header.
    #[doc(hidden)]
    pub fn new(packet_id: NonZeroU16, packet_size: u32, topics: Vec<(ByteString, QoS)>) -> Self {
        let mut codes = Vec::with_capacity(topics.len());
        (0..topics.len()).for_each(|_| codes.push(codec::SubscribeReturnCode::Failure));

        Self {
            packet_id,
            packet_size,
            topics,
            codes,
        }
    }

    #[inline]
    /// Return the packet’s remaining length in bytes, excluding its fixed header.
    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    #[inline]
    /// Iterate over requested filters and mutable acknowledgment results.
    pub fn iter_mut(&mut self) -> SubscribeIter<'_> {
        SubscribeIter {
            subs: ptr::from_ref(self).cast_mut(),
            entry: 0,
            lt: PhantomData,
        }
    }

    #[inline]
    /// Request SUBACK with one result per topic filter, in request order.
    ///
    /// Filters not explicitly confirmed retain `SubscribeReturnCode::Failure`.
    /// This only constructs the acknowledgment; the application maintains subscriptions.
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
    /// Exclusively borrowed subscription storage; each result borrows a distinct entry.
    subs: *mut Subscribe,
    /// Index of the next topic and acknowledgment code to yield.
    entry: usize,
    /// Keeps the subscription exclusively borrowed for the iterator lifetime.
    lt: PhantomData<&'a mut Subscribe>,
}

impl fmt::Debug for SubscribeIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeIter").finish()
    }
}

impl<'a> SubscribeIter<'a> {
    /// Yield the next topic with its distinct mutable result slot, advancing the cursor.
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
    /// Borrowed topic filter requested by the client.
    topic: &'a ByteString,
    /// Maximum delivery quality requested for this filter.
    qos: QoS,
    /// Mutable SUBACK result slot, left as failure unless explicitly confirmed.
    code: &'a mut codec::SubscribeReturnCode,
}

impl<'a> Subscription<'a> {
    #[inline]
    /// Return the topic filter requested by the client.
    pub fn topic(&self) -> &'a ByteString {
        self.topic
    }

    #[inline]
    /// Return the maximum delivery quality requested by the client.
    pub fn qos(&self) -> QoS {
        self.qos
    }

    #[inline]
    /// Mark this topic filter as rejected in SUBACK.
    pub fn fail(&mut self) {
        *self.code = codec::SubscribeReturnCode::Failure;
    }

    #[inline]
    /// Grant this filter the supplied maximum delivery quality in SUBACK.
    ///
    /// The caller must not grant a quality higher than `qos` reports.
    pub fn confirm(&mut self, qos: QoS) {
        *self.code = codec::SubscribeReturnCode::Success(qos);
    }

    #[inline]
    #[doc(hidden)]
    /// Alias for `confirm`, recording the granted delivery quality.
    pub fn subscribe(&mut self, qos: QoS) {
        self.confirm(qos);
    }
}

/// UNSUBSCRIBE request exposing the filters the application should remove.
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    /// Identifier echoed in UNSUBACK after handling completes.
    packet_id: NonZeroU16,
    /// UNSUBSCRIBE remaining length in bytes, excluding its fixed header.
    packet_size: u32,
    /// Topic filters to remove, in the order sent by the client.
    topics: Vec<ByteString>,
}

/// Result of a unsubscribe message
#[derive(Debug, Copy, Clone)]
pub(crate) struct UnsubscribeResult {
    /// Identifier of the UNSUBSCRIBE packet being acknowledged.
    pub(crate) packet_id: NonZeroU16,
}

impl Unsubscribe {
    #[inline]
    /// Create an UNSUBSCRIBE request from its identifier and ordered topic filters.
    ///
    /// `packet_size` is the remaining length in bytes, excluding the fixed header.
    #[doc(hidden)]
    pub fn new(packet_id: NonZeroU16, packet_size: u32, topics: Vec<ByteString>) -> Self {
        Self {
            packet_id,
            packet_size,
            topics,
        }
    }

    #[inline]
    /// Return the packet’s remaining length in bytes, excluding its fixed header.
    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    #[inline]
    /// Iterate over the topic filters requested for removal.
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.topics.iter()
    }

    #[inline]
    /// Request UNSUBACK for this packet identifier.
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Unsubscribe(UnsubscribeResult {
                packet_id: self.packet_id,
            }),
        }
    }
}

/// Unit tests for protocol-message formatting.
#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use ntex_bytes::ByteString;

    use super::*;
    use crate::types::QoS;

    /// STATEMENT: Protocol-message debug formatting identifies the subscription iterator and basic message types.
    ///
    /// HOW:
    /// - Construct a subscription iterator and an unsubscription, then format them.
    /// - Assert their type names and the names of `Ping` and `Disconnect` appear in debug output.
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
    }
}
