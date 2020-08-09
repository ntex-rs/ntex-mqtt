use std::marker::PhantomData;

use bytestring::ByteString;

use super::codec::{self, QoS};
use crate::error::MqttError;

pub enum ControlPacket<E> {
    Auth(Auth),
    Ping(Ping),
    Disconnect(Disconnect),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Closed(Closed),
    Error(Error<E>),
}

pub struct ControlResult {
    pub(crate) packet: Option<codec::Packet>,
    pub(crate) disconnect: bool,
}

impl<E> ControlPacket<E> {
    pub(super) fn ctl_auth(pkt: codec::Auth) -> Self {
        ControlPacket::Auth(Auth(pkt))
    }

    pub(super) fn ctl_ping() -> Self {
        ControlPacket::Ping(Ping)
    }

    pub(super) fn ctl_disconnect(pkt: codec::Disconnect) -> Self {
        ControlPacket::Disconnect(Disconnect(pkt))
    }

    pub(super) fn ctl_closed(is_error: bool) -> Self {
        ControlPacket::Closed(Closed::new(is_error))
    }

    pub(super) fn ctl_error(err: MqttError<E>) -> Self {
        ControlPacket::Error(Error { err })
    }

    pub fn disconnect(&self, pkt: codec::Disconnect) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }
}

pub struct Auth(codec::Auth);

impl Auth {
    /// Returns reference to dusconnect packet
    pub fn packet(&self) -> &codec::Auth {
        &self.0
    }

    pub fn ack(self) -> ControlResult {
        ControlResult { packet: None, disconnect: false }
    }
}

pub struct Ping;

impl Ping {
    pub fn ack(self) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::PingResponse), disconnect: false }
    }
}

pub struct Disconnect(codec::Disconnect);

impl Disconnect {
    /// Returns reference to dusconnect packet
    pub fn packet(&self) -> &codec::Disconnect {
        &self.0
    }

    /// Ack disconnect message
    pub fn ack(self) -> ControlResult {
        ControlResult { packet: None, disconnect: true }
    }
}

/// Subscribe message
pub struct Subscribe {
    packet: codec::Subscribe,
    result: codec::SubscribeAck,
}

impl Subscribe {
    pub(crate) fn create<E>(packet: codec::Subscribe) -> ControlPacket<E> {
        let mut status = Vec::with_capacity(packet.topic_filters.len());
        (0..packet.topic_filters.len())
            .for_each(|_| status.push(codec::SubscribeAckReason::UnspecifiedError));

        let result = codec::SubscribeAck {
            status,
            packet_id: packet.packet_id,
            properties: codec::UserProperties::default(),
            reason_string: None,
        };

        ControlPacket::Subscribe(Self { packet, result })
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> SubscribeIter {
        SubscribeIter { subs: self as *const _ as *mut _, entry: 0, lt: PhantomData }
    }

    #[inline]
    /// Ack Subscribe packet
    pub fn ack(self) -> ControlResult {
        ControlResult {
            packet: Some(codec::Packet::SubscribeAck(self.result)),
            disconnect: false,
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
pub struct Subscription<'a> {
    topic: &'a ByteString,
    options: &'a codec::SubscriptionOptions,
    status: &'a mut codec::SubscribeAckReason,
}

impl<'a> Subscription<'a> {
    #[inline]
    /// subscription topic
    pub fn topic(&self) -> &'a ByteString {
        &self.topic
    }

    #[inline]
    /// subscription options for current topic
    pub fn options(&self) -> &codec::SubscriptionOptions {
        self.options
    }

    #[inline]
    /// fail to subscribe to the topic
    pub fn fail(&mut self, status: codec::SubscribeAckReason) {
        *self.status = status
    }

    #[inline]
    /// subscribe to a topic with specific qos
    pub fn subscribe(&mut self, qos: QoS) {
        match qos {
            QoS::AtMostOnce => *self.status = codec::SubscribeAckReason::GrantedQos0,
            QoS::AtLeastOnce => *self.status = codec::SubscribeAckReason::GrantedQos1,
            QoS::ExactlyOnce => *self.status = codec::SubscribeAckReason::GrantedQos2,
        }
    }
}

/// Unsubscribe message
pub struct Unsubscribe {
    packet: codec::Unsubscribe,
    result: codec::UnsubscribeAck,
}

impl Unsubscribe {
    pub(crate) fn create<E>(packet: codec::Unsubscribe) -> ControlPacket<E> {
        let mut status = Vec::with_capacity(packet.topic_filters.len());
        (0..packet.topic_filters.len())
            .for_each(|_| status.push(codec::UnsubscribeAckReason::Success));

        let result = codec::UnsubscribeAck {
            status,
            packet_id: packet.packet_id,
            properties: codec::UserProperties::default(),
            reason_string: None,
        };

        ControlPacket::Unsubscribe(Self { packet, result })
    }

    /// Unsubscribe packet user properties
    pub fn properties(&self) -> &codec::UserProperties {
        &self.packet.user_properties
    }

    /// returns iterator over unsubscribe topics
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.packet.topic_filters.iter()
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> UnsubscribeIter {
        UnsubscribeIter { subs: self as *const _ as *mut _, entry: 0, lt: PhantomData }
    }

    #[inline]
    /// Reason stream for ack packet
    pub fn ack_reason(mut self, reason: ByteString) -> Self {
        self.result.reason_string = Some(reason);
        self
    }

    #[inline]
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
    pub fn ack(self) -> ControlResult {
        ControlResult {
            packet: Some(codec::Packet::UnsubscribeAck(self.result)),
            disconnect: false,
        }
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
pub struct UnsubscribeItem<'a> {
    topic: &'a ByteString,
    status: &'a mut codec::UnsubscribeAckReason,
}

impl<'a> UnsubscribeItem<'a> {
    #[inline]
    /// subscription topic
    pub fn topic(&self) -> &'a ByteString {
        &self.topic
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

/// Connection closed message
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
        ControlResult { packet: None, disconnect: false }
    }
}

/// Connection failed message
pub struct Error<E> {
    err: MqttError<E>,
}

impl<E> Error<E> {
    /// Returns reference to mqtt error
    pub fn err(&self) -> &MqttError<E> {
        &self.err
    }

    #[inline]
    /// convert packet to a result
    pub fn ack(self, pkt: codec::Disconnect) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }

    #[inline]
    /// convert packet to a result
    pub fn ack_default(self) -> ControlResult {
        ControlResult {
            packet: Some(codec::Packet::Disconnect(codec::Disconnect::default())),
            disconnect: true,
        }
    }
}
