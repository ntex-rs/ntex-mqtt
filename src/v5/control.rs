use std::{fmt, marker::PhantomData, ptr};

use ntex_bytes::ByteString;

use crate::{error, types::QoS, v5::codec};

/// MQTT protocol–related messages.
///
/// The control service is always called with these frames one at a time.
/// Unhandled messages are stored in a buffer. Other types of messages may be
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

#[derive(Debug)]
pub(crate) enum Pkt {
    None,
    Disconnect(codec::Disconnect),
    Packet(codec::Packet),
}

/// Protocol message handling result
#[derive(Debug)]
pub struct ProtocolMessageAck {
    pub(crate) packet: Pkt,
    pub(crate) disconnect: bool,
}

impl ProtocolMessage {
    /// Create a new message from AUTH packet.
    #[doc(hidden)]
    pub fn auth(pkt: codec::Auth, size: u32) -> Self {
        ProtocolMessage::Auth(Auth { pkt, size })
    }

    pub(crate) fn pubrel(pkt: codec::PublishAck2, size: u32) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease::new(pkt, size))
    }

    /// Create a new message from SUBSCRIBE packet.
    #[doc(hidden)]
    pub fn subscribe(pkt: codec::Subscribe, size: u32) -> Self {
        ProtocolMessage::Subscribe(Subscribe::new(pkt, size))
    }

    /// Create a new message from UNSUBSCRIBE packet.
    #[doc(hidden)]
    pub fn unsubscribe(pkt: codec::Unsubscribe, size: u32) -> Self {
        ProtocolMessage::Unsubscribe(Unsubscribe::new(pkt, size))
    }

    /// Create a new PING message.
    #[doc(hidden)]
    pub fn ping() -> Self {
        ProtocolMessage::Ping(Ping)
    }

    /// Create a new message from DISCONNECT packet.
    #[doc(hidden)]
    pub fn remote_disconnect(pkt: codec::Disconnect, size: u32) -> Self {
        ProtocolMessage::Disconnect(Disconnect(pkt, size))
    }

    /// Disconnects the client by sending DISCONNECT packet
    /// with `NormalDisconnection` reason code.
    pub fn disconnect(&self) -> ProtocolMessageAck {
        let pkt = codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::NormalDisconnection,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Vec::default(),
        };
        ProtocolMessageAck { packet: Pkt::Disconnect(pkt), disconnect: true }
    }

    /// Disconnects the client by sending DISCONNECT packet
    /// with provided reason code.
    pub fn disconnect_with(&self, pkt: codec::Disconnect) -> ProtocolMessageAck {
        ProtocolMessageAck { packet: Pkt::Disconnect(pkt), disconnect: true }
    }

    /// Ack control message
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

#[derive(Debug)]
pub struct Auth {
    pkt: codec::Auth,
    size: u32,
}

impl Auth {
    /// Returns reference to auth packet
    pub fn packet(&self) -> &codec::Auth {
        &self.pkt
    }

    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.size
    }

    pub fn ack(self, response: codec::Auth) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::Auth(response)),
            disconnect: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PublishRelease {
    pkt: codec::PublishAck2,
    result: codec::PublishAck2,
    size: u32,
}

impl PublishRelease {
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

    /// Returns reference to auth packet
    pub fn packet(&self) -> &codec::PublishAck2 {
        &self.pkt
    }

    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.size
    }

    #[inline]
    #[must_use]
    /// Update user properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.properties);
        self
    }

    #[inline]
    #[must_use]
    /// Set ack reason string
    pub fn reason(mut self, reason: ByteString) -> Self {
        self.result.reason_string = Some(reason);
        self
    }

    /// Ack publish release
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::PublishComplete(self.result)),
            disconnect: false,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Ping;

impl Ping {
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::PingResponse),
            disconnect: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Disconnect(pub(crate) codec::Disconnect, pub(crate) u32);

impl Disconnect {
    /// Returns reference to disconnect packet
    pub fn packet(&self) -> &codec::Disconnect {
        &self.0
    }

    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.1
    }

    /// Ack disconnect message
    pub fn ack(self) -> ProtocolMessageAck {
        ProtocolMessageAck { packet: Pkt::None, disconnect: true }
    }
}

/// `Subscribe` control message
#[derive(Debug, Clone)]
pub struct Subscribe {
    packet: codec::Subscribe,
    result: codec::SubscribeAck,
    size: u32,
}

impl Subscribe {
    /// Create a new `Subscribe` control message from a Subscribe
    /// packet
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

        Self { packet, result, size }
    }

    #[inline]
    #[must_use]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> SubscribeIter<'_> {
        SubscribeIter { subs: ptr::from_ref(self).cast_mut(), entry: 0, lt: PhantomData }
    }

    #[inline]
    #[must_use]
    /// Reason string for ack packet
    pub fn ack_reason(mut self, reason: ByteString) -> Self {
        self.result.reason_string = Some(reason);
        self
    }

    #[inline]
    #[must_use]
    /// Properties for ack packet
    pub fn ack_properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.properties);
        self
    }

    #[inline]
    /// Ack Subscribe packet
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
    /// Returns size of the packet
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

/// Subscription topic
#[derive(Debug)]
pub struct Subscription<'a> {
    topic: &'a ByteString,
    options: &'a codec::SubscriptionOptions,
    status: &'a mut codec::SubscribeAckReason,
}

impl<'a> Subscription<'a> {
    #[inline]
    /// subscription topic
    pub fn topic(&self) -> &'a ByteString {
        self.topic
    }

    #[inline]
    /// subscription options for current topic
    pub fn options(&self) -> &codec::SubscriptionOptions {
        self.options
    }

    #[inline]
    /// fail to subscribe to the topic
    pub fn fail(&mut self, status: codec::SubscribeAckReason) {
        *self.status = status;
    }

    #[inline]
    /// confirm subscription to a topic with specific qos
    pub fn confirm(&mut self, qos: QoS) {
        match qos {
            QoS::AtMostOnce => *self.status = codec::SubscribeAckReason::GrantedQos0,
            QoS::AtLeastOnce => *self.status = codec::SubscribeAckReason::GrantedQos1,
            QoS::ExactlyOnce => *self.status = codec::SubscribeAckReason::GrantedQos2,
        }
    }

    #[inline]
    #[doc(hidden)]
    /// confirm subscription to a topic with specific qos
    pub fn subscribe(&mut self, qos: QoS) {
        self.confirm(qos);
    }
}

/// `Unsubscribe` control message
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    packet: codec::Unsubscribe,
    result: codec::UnsubscribeAck,
    size: u32,
}

impl Unsubscribe {
    /// Create a new `Unsubscribe` control `CtlFrame` from an `Unsubscribe`
    /// packet
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

        Self { packet, result, size }
    }

    #[inline]
    /// Unsubscribe packet user properties
    pub fn properties(&self) -> &codec::UserProperties {
        &self.packet.user_properties
    }

    #[inline]
    /// returns iterator over unsubscribe topics
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.packet.topic_filters.iter()
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> UnsubscribeIter<'_> {
        UnsubscribeIter {
            subs: ptr::from_ref::<Unsubscribe>(self).cast_mut(),
            entry: 0,
            lt: PhantomData,
        }
    }

    #[inline]
    #[must_use]
    /// Reason string for ack packet
    pub fn ack_reason(mut self, reason: ByteString) -> Self {
        self.result.reason_string = Some(reason);
        self
    }

    #[inline]
    #[must_use]
    /// Properties for ack packet
    pub fn ack_properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.result.properties);
        self
    }

    #[inline]
    /// convert packet to a result
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
    /// Returns size of the packet
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

/// Iterator over topics to unsubscribe
pub struct UnsubscribeIter<'a> {
    subs: *mut Unsubscribe,
    entry: usize,
    lt: PhantomData<&'a mut Unsubscribe>,
}

impl fmt::Debug for UnsubscribeIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnsubscribeIter").finish()
    }
}

impl<'a> UnsubscribeIter<'a> {
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

/// Subscription topic
#[derive(Debug)]
pub struct UnsubscribeItem<'a> {
    topic: &'a ByteString,
    status: &'a mut codec::UnsubscribeAckReason,
}

impl<'a> UnsubscribeItem<'a> {
    #[inline]
    /// subscription topic
    pub fn topic(&self) -> &'a ByteString {
        self.topic
    }

    #[inline]
    /// fail to unsubscribe from the topic
    pub fn fail(&mut self, status: codec::UnsubscribeAckReason) {
        *self.status = status;
    }

    #[inline]
    /// unsubscribe from a topic
    pub fn success(&mut self) {
        *self.status = codec::UnsubscribeAckReason::Success;
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
