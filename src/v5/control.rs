use std::{fmt, marker::PhantomData, ptr};

use ntex_bytes::ByteString;

use crate::{error, types::QoS, v5::codec};

/// MQTT protocol–related messages.
///
/// The protocol-message service is always called with these messages one at a time.
/// Up to 16 pending messages are buffered. Other types of messages may be
/// handled out of order.
#[derive(Debug)]
pub enum ProtocolMessage {
    /// `Auth` packet from a client
    Auth(Auth),
    /// `PublishRelease` packet from a client
    PublishRelease(PublishRelease),
    /// `Subscribe` packet from a client
    Subscribe(Subscribe),
    /// `Unsubscribe` packet from a client
    Unsubscribe(Unsubscribe),
    /// `Disconnect` packet from a client
    Disconnect(Disconnect),
    /// `Ping` packet from a client
    Ping(Ping),
}

/// Internal wire-response selection for a protocol-service result.
#[derive(Debug)]
pub(crate) enum Pkt {
    /// No response packet is sent.
    None,
    /// Disconnect response suppressed if a disconnect has already been sent.
    Disconnect(codec::Disconnect),
    /// Ordinary response returned to the ordered response queue.
    Packet(codec::Packet),
}

/// Protocol-service result containing a response packet and an optional close request.
#[derive(Debug)]
pub struct ProtocolMessageAck {
    /// Optional response packet produced by the protocol handler.
    pub(crate) packet: Pkt,
    /// Whether to close sink operations and fail any streamed payload after handling.
    pub(crate) disconnect: bool,
}

impl ProtocolMessage {
    /// Wraps an `AUTH` packet and its Remaining Length in bytes.
    #[doc(hidden)]
    pub fn auth(pkt: codec::Auth, size: u32) -> Self {
        ProtocolMessage::Auth(Auth { pkt, size })
    }

    /// Wraps a client `PUBREL` with its Remaining Length in bytes.
    pub(crate) fn pubrel(pkt: codec::PublishAck2, size: u32) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease::new(pkt, size))
    }

    /// Wraps a `SUBSCRIBE` packet and its Remaining Length in bytes.
    #[doc(hidden)]
    pub fn subscribe(pkt: codec::Subscribe, size: u32) -> Self {
        ProtocolMessage::Subscribe(Subscribe::new(pkt, size))
    }

    /// Wraps an `UNSUBSCRIBE` packet and its Remaining Length in bytes.
    #[doc(hidden)]
    pub fn unsubscribe(pkt: codec::Unsubscribe, size: u32) -> Self {
        ProtocolMessage::Unsubscribe(Unsubscribe::new(pkt, size))
    }

    /// Creates a message for an incoming `PINGREQ`.
    #[doc(hidden)]
    pub fn ping() -> Self {
        ProtocolMessage::Ping(Ping)
    }

    /// Wraps a received `DISCONNECT` and its Remaining Length in bytes.
    #[doc(hidden)]
    pub fn remote_disconnect(pkt: codec::Disconnect, size: u32) -> Self {
        ProtocolMessage::Disconnect(Disconnect(pkt, size))
    }

    /// Requests a normal disconnect, sending `DISCONNECT` unless one was already sent.
    pub fn disconnect(&self) -> ProtocolMessageAck {
        let pkt = codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::NormalDisconnection,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Vec::default(),
        };
        ProtocolMessageAck {
            packet: Pkt::Disconnect(pkt),
            disconnect: true,
        }
    }

    /// Requests disconnection with the supplied `DISCONNECT` packet.
    ///
    /// The dispatcher suppresses the packet if a disconnect was already sent.
    pub fn disconnect_with(&self, pkt: codec::Disconnect) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Disconnect(pkt),
            disconnect: true,
        }
    }

    /// Creates the default acknowledgement for this protocol message.
    ///
    /// Authentication is rejected with `ImplementationSpecificError`; unmodified
    /// subscriptions are rejected per topic with `UnspecifiedError`, while
    /// unsubscriptions succeed. A received disconnect closes without a reply.
    pub fn ack(self) -> ProtocolMessageAck {
        match self {
            ProtocolMessage::Auth(_) => super::disconnect(error::ERR_AUTH_NOT_SUP),
            ProtocolMessage::PublishRelease(msg) => msg.ack(),
            ProtocolMessage::Subscribe(msg) => msg.ack(),
            ProtocolMessage::Unsubscribe(msg) => msg.ack(),
            ProtocolMessage::Disconnect(msg) => msg.ack(),
            ProtocolMessage::Ping(msg) => msg.ack(),
        }
    }
}

/// Incoming authentication message requiring an application-provided response.
#[derive(Debug)]
pub struct Auth {
    /// Authentication packet received from the peer.
    pkt: codec::Auth,
    /// Packet Remaining Length in bytes, excluding the fixed header.
    size: u32,
}

impl Auth {
    /// Returns the received authentication packet.
    pub fn packet(&self) -> &codec::Auth {
        &self.pkt
    }

    /// Returns the packet's Remaining Length in bytes, excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.size
    }

    /// Responds with the supplied `AUTH` packet without closing the connection.
    pub fn ack(self, response: codec::Auth) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::from(response)),
            disconnect: false,
        }
    }
}

/// Incoming `PUBREL` with a configurable successful `PUBCOMP` response.
#[derive(Debug, Clone)]
pub struct PublishRelease {
    /// Client `PUBREL` packet completing the incoming `QoS 2` exchange.
    pkt: codec::PublishAck2,
    /// Pending `PUBCOMP`, initially successful with the same packet identifier.
    result: codec::PublishAck2,
    /// Packet Remaining Length in bytes, excluding the fixed header.
    size: u32,
}

impl PublishRelease {
    /// Prepares a successful `PUBCOMP` response to the received `PUBREL`.
    pub(crate) fn new(pkt: codec::PublishAck2, size: u32) -> Self {
        let packet_id = pkt.packet_id;
        Self {
            pkt,
            size,
            result: codec::PublishAck2 {
                packet_id,
                reason_code: codec::PublishAck2Reason::Success,
                properties: codec::UserProperties::default(),
                reason_string: None,
            },
        }
    }

    /// Returns reference to `PublishRelease` packet
    pub fn packet(&self) -> &codec::PublishAck2 {
        &self.pkt
    }

    /// Returns the packet's Remaining Length in bytes, excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.size
    }

    #[inline]
    #[must_use]
    /// Modifies user properties in the pending `PUBCOMP` response.
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.properties);
        self
    }

    #[inline]
    #[must_use]
    /// Sets diagnostic text in the pending `PUBCOMP` response.
    pub fn reason(mut self, reason: ByteString) -> Self {
        self.result.reason_string = Some(reason);
        self
    }

    /// Responds with `PUBCOMP` to complete the incoming `QoS 2` exchange.
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::PublishComplete(self.result)),
            disconnect: false,
        }
    }
}

/// Incoming `PINGREQ` keep-alive message.
#[derive(Debug, Copy, Clone)]
pub struct Ping;

impl Ping {
    /// Responds with a `PINGRESP` packet.
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::PingResponse),
            disconnect: false,
        }
    }
}

/// Received `DISCONNECT` packet and its Remaining Length in bytes.
#[derive(Debug, Clone)]
pub struct Disconnect(pub(crate) codec::Disconnect, pub(crate) u32);

impl Disconnect {
    /// Returns reference to disconnect packet
    pub fn packet(&self) -> &codec::Disconnect {
        &self.0
    }

    /// Returns the packet's Remaining Length in bytes, excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.1
    }

    /// Requests connection closure without sending a reply to the received disconnect.
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::None,
            disconnect: true,
        }
    }
}

/// Incoming subscription request with one configurable acknowledgement status per filter.
///
/// Updating statuses does not install subscriptions; the application manages its
/// subscriptions and confirms or rejects each filter before acknowledging.
#[derive(Debug, Clone)]
pub struct Subscribe {
    /// Client request containing the topic filters and subscription options.
    packet: codec::Subscribe,
    /// Pending response with one status per filter, initially `UnspecifiedError`.
    result: codec::SubscribeAck,
    /// Packet Remaining Length in bytes, excluding the fixed header.
    size: u32,
}

impl Subscribe {
    /// Prepares a `SUBACK` with `UnspecifiedError` for every requested topic filter.
    ///
    /// `size` is the incoming packet's Remaining Length in bytes.
    pub fn new(packet: codec::Subscribe, size: u32) -> Self {
        let mut status = Vec::with_capacity(packet.topic_filters.len());
        (0..packet.topic_filters.len())
            .for_each(|_| status.push(codec::SubscribeAckReason::UnspecifiedError));

        let result = codec::SubscribeAck {
            status,
            packet_id: packet.packet_id,
            properties: codec::UserProperties::default(),
            reason_string: None,
        };

        Self {
            packet,
            result,
            size,
        }
    }

    #[inline]
    #[must_use]
    /// Iterates over filters and options with mutable access to their `SUBACK` statuses.
    pub fn iter_mut(&mut self) -> SubscribeIter<'_> {
        SubscribeIter {
            subs: ptr::from_ref(self).cast_mut(),
            entry: 0,
            lt: PhantomData,
        }
    }

    #[inline]
    #[must_use]
    /// Sets the diagnostic reason string for the acknowledgement packet.
    pub fn ack_reason(mut self, reason: ByteString) -> Self {
        self.result.reason_string = Some(reason);
        self
    }

    #[inline]
    #[must_use]
    /// Modifies user properties for the acknowledgement packet.
    pub fn ack_properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.properties);
        self
    }

    #[inline]
    /// Responds with the configured `SUBACK`, leaving unconfirmed filters rejected.
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::SubscribeAck(self.result)),
            disconnect: false,
        }
    }

    #[inline]
    /// Returns reference to subscribe packet
    pub fn packet(&self) -> &codec::Subscribe {
        &self.packet
    }

    #[inline]
    /// Returns the packet's Remaining Length in bytes, excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.size
    }
}

impl<'a> IntoIterator for &'a mut Subscribe {
    type Item = Subscription<'a>;
    type IntoIter = SubscribeIter<'a>;

    fn into_iter(self) -> SubscribeIter<'a> {
        self.iter_mut()
    }
}

/// Iterator over requested filters, options, and mutable `SUBACK` statuses.
pub struct SubscribeIter<'a> {
    /// Exclusively borrowed request whose filter and status entries are yielded.
    subs: *mut Subscribe,
    /// Index of the next filter/status pair; only advances during iteration.
    entry: usize,
    /// Keeps the request exclusively borrowed for the lifetime of yielded entries.
    lt: PhantomData<&'a mut Subscribe>,
}

impl fmt::Debug for SubscribeIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeIter").finish()
    }
}

impl<'a> SubscribeIter<'a> {
    /// Yields the next filter, options, and mutable acknowledgement status using the stored pointer.
    fn next_unsafe(&mut self) -> Option<Subscription<'a>> {
        let subs = unsafe { &mut *self.subs };

        if self.entry < subs.packet.topic_filters.len() {
            let s = Subscription {
                topic: &subs.packet.topic_filters[self.entry].0,
                options: &subs.packet.topic_filters[self.entry].1,
                status: &mut subs.result.status[self.entry],
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

/// Borrowed subscription filter, requested options, and mutable acknowledgement status.
#[derive(Debug)]
pub struct Subscription<'a> {
    /// Topic filter from the incoming subscription request.
    topic: &'a ByteString,
    /// Requested delivery and retain options for this filter.
    options: &'a codec::SubscriptionOptions,
    /// This filter's response status, updated by confirmation or rejection.
    status: &'a mut codec::SubscribeAckReason,
}

impl<'a> Subscription<'a> {
    #[inline]
    /// Returns the requested topic filter.
    pub fn topic(&self) -> &'a ByteString {
        self.topic
    }

    #[inline]
    /// Returns the client's options for this topic filter.
    pub fn options(&self) -> &codec::SubscriptionOptions {
        self.options
    }

    #[inline]
    /// Sets this filter's `SUBACK` status to the supplied reason code.
    pub fn fail(&mut self, status: codec::SubscribeAckReason) {
        *self.status = status;
    }

    #[inline]
    /// Sets this filter's successful `SUBACK` status to the granted `QoS`.
    ///
    /// The application must install the subscription itself.
    pub fn confirm(&mut self, qos: QoS) {
        match qos {
            QoS::AtMostOnce => *self.status = codec::SubscribeAckReason::GrantedQos0,
            QoS::AtLeastOnce => *self.status = codec::SubscribeAckReason::GrantedQos1,
            QoS::ExactlyOnce => *self.status = codec::SubscribeAckReason::GrantedQos2,
        }
    }

    #[inline]
    #[doc(hidden)]
    /// Sets this filter's successful `SUBACK` status to the granted `QoS`.
    ///
    /// The application must install the subscription itself.
    pub fn subscribe(&mut self, qos: QoS) {
        self.confirm(qos);
    }
}

/// Incoming unsubscribe request with a configurable acknowledgement status per filter.
///
/// Status changes do not remove application subscriptions; the application manages
/// that state before acknowledging.
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    /// Client request containing the filters to remove.
    packet: codec::Unsubscribe,
    /// Pending response with one status per filter, initially `Success`.
    result: codec::UnsubscribeAck,
    /// Packet Remaining Length in bytes, excluding the fixed header.
    size: u32,
}

impl Unsubscribe {
    /// Prepares an `UNSUBACK` with `Success` for every requested topic filter.
    ///
    /// `size` is the incoming packet's Remaining Length in bytes.
    pub fn new(packet: codec::Unsubscribe, size: u32) -> Self {
        let mut status = Vec::with_capacity(packet.topic_filters.len());
        (0..packet.topic_filters.len())
            .for_each(|_| status.push(codec::UnsubscribeAckReason::Success));

        let result = codec::UnsubscribeAck {
            status,
            packet_id: packet.packet_id,
            properties: codec::UserProperties::default(),
            reason_string: None,
        };

        Self {
            packet,
            result,
            size,
        }
    }

    #[inline]
    /// Returns user properties from the incoming unsubscribe request.
    pub fn properties(&self) -> &codec::UserProperties {
        &self.packet.user_properties
    }

    #[inline]
    /// Iterates over the requested topic filters without changing response statuses.
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.packet.topic_filters.iter()
    }

    #[inline]
    /// Iterates over requested filters with mutable access to their `UNSUBACK` statuses.
    pub fn iter_mut(&mut self) -> UnsubscribeIter<'_> {
        UnsubscribeIter {
            subs: ptr::from_ref::<Unsubscribe>(self).cast_mut(),
            entry: 0,
            lt: PhantomData,
        }
    }

    #[inline]
    #[must_use]
    /// Sets the diagnostic reason string for the acknowledgement packet.
    pub fn ack_reason(mut self, reason: ByteString) -> Self {
        self.result.reason_string = Some(reason);
        self
    }

    #[inline]
    #[must_use]
    /// Modifies user properties for the acknowledgement packet.
    pub fn ack_properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.properties);
        self
    }

    #[inline]
    /// Responds with the configured `UNSUBACK`, leaving unchanged statuses successful.
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::UnsubscribeAck(self.result)),
            disconnect: false,
        }
    }

    #[inline]
    /// Returns reference to unsubscribe packet
    pub fn packet(&self) -> &codec::Unsubscribe {
        &self.packet
    }

    #[inline]
    /// Returns the packet's Remaining Length in bytes, excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.size
    }
}

impl<'a> IntoIterator for &'a mut Unsubscribe {
    type Item = UnsubscribeItem<'a>;
    type IntoIter = UnsubscribeIter<'a>;

    fn into_iter(self) -> UnsubscribeIter<'a> {
        self.iter_mut()
    }
}

/// Iterator over requested topic filters and mutable `UNSUBACK` statuses.
pub struct UnsubscribeIter<'a> {
    /// Exclusively borrowed request whose filter and status entries are yielded.
    subs: *mut Unsubscribe,
    /// Index of the next filter/status pair; only advances during iteration.
    entry: usize,
    /// Keeps the request exclusively borrowed for the lifetime of yielded entries.
    lt: PhantomData<&'a mut Unsubscribe>,
}

impl fmt::Debug for UnsubscribeIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnsubscribeIter").finish()
    }
}

impl<'a> UnsubscribeIter<'a> {
    /// Yields the next filter and mutable acknowledgement status using the stored pointer.
    fn next_unsafe(&mut self) -> Option<UnsubscribeItem<'a>> {
        let subs = unsafe { &mut *self.subs };

        if self.entry < subs.packet.topic_filters.len() {
            let s = UnsubscribeItem {
                topic: &subs.packet.topic_filters[self.entry],
                status: &mut subs.result.status[self.entry],
            };
            self.entry += 1;
            Some(s)
        } else {
            None
        }
    }
}

impl<'a> Iterator for UnsubscribeIter<'a> {
    type Item = UnsubscribeItem<'a>;

    #[inline]
    fn next(&mut self) -> Option<UnsubscribeItem<'a>> {
        self.next_unsafe()
    }
}

/// Borrowed unsubscribe filter and its mutable acknowledgement status.
#[derive(Debug)]
pub struct UnsubscribeItem<'a> {
    /// Topic filter from the incoming unsubscribe request.
    topic: &'a ByteString,
    /// This filter's response status, initially `Success`.
    status: &'a mut codec::UnsubscribeAckReason,
}

impl<'a> UnsubscribeItem<'a> {
    #[inline]
    /// Returns the topic filter requested for removal.
    pub fn topic(&self) -> &'a ByteString {
        self.topic
    }

    #[inline]
    /// Sets this filter's `UNSUBACK` status to the supplied reason code.
    pub fn fail(&mut self, status: codec::UnsubscribeAckReason) {
        *self.status = status;
    }

    #[inline]
    /// Marks this filter's `UNSUBACK` status as successful without removing application state.
    pub fn success(&mut self) {
        *self.status = codec::UnsubscribeAckReason::Success;
    }
}

/// Unit tests for protocol-message and iterator formatting.
#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use ntex_bytes::ByteString;

    use super::*;
    use crate::types::QoS;

    /// STATEMENT: Subscription iterators and ping messages identify their types in debug output.
    ///
    /// HOW:
    /// - Create single-filter subscribe and unsubscribe messages, then format their mutable iterators.
    /// - Check the iterator type names and the `Ping` name in the formatted output.
    #[test]
    fn test_debug() {
        // SubscribeIter via Subscribe
        let mut sub = Subscribe::new(
            codec::Subscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                id: None,
                user_properties: Vec::new(),
                topic_filters: vec![(
                    ByteString::from_static("a/b"),
                    codec::SubscriptionOptions {
                        qos: QoS::AtLeastOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: codec::RetainHandling::AtSubscribe,
                    },
                )],
            },
            0,
        );
        let iter = sub.iter_mut();
        assert!(format!("{iter:?}").contains("SubscribeIter"));

        // UnsubscribeIter via Unsubscribe
        let mut unsub = Unsubscribe::new(
            codec::Unsubscribe {
                packet_id: NonZeroU16::new(2).unwrap(),
                user_properties: Vec::new(),
                topic_filters: vec![ByteString::from_static("a/b")],
            },
            0,
        );
        let uiter = unsub.iter_mut();
        assert!(format!("{uiter:?}").contains("UnsubscribeIter"));

        // Ping
        assert!(format!("{Ping:?}").contains("Ping"));
    }
}
