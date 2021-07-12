use std::marker::PhantomData;

use ntex::util::ByteString;

use super::codec::{self, DisconnectReasonCode, QoS, UserProperties};
use crate::error;

/// Control plain messages
#[derive(Debug)]
pub enum ControlMessage<E> {
    Auth(Auth),
    Ping(Ping),
    Disconnect(Disconnect),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Closed(Closed),
    Error(Error<E>),
    ProtocolError(ProtocolError),
}

/// Control message handling result
#[derive(Debug)]
pub struct ControlResult {
    pub(crate) packet: Option<codec::Packet>,
    pub(crate) disconnect: bool,
}

impl<E> ControlMessage<E> {
    pub(super) fn auth(pkt: codec::Auth) -> Self {
        ControlMessage::Auth(Auth(pkt))
    }

    pub(super) fn ping() -> Self {
        ControlMessage::Ping(Ping)
    }

    pub(super) fn dis(pkt: codec::Disconnect) -> Self {
        ControlMessage::Disconnect(Disconnect(pkt))
    }

    pub(super) fn closed(is_error: bool) -> Self {
        ControlMessage::Closed(Closed::new(is_error))
    }

    pub(super) fn error(err: E) -> Self {
        ControlMessage::Error(Error::new(err))
    }

    pub(super) fn proto_error(err: error::ProtocolError) -> Self {
        ControlMessage::ProtocolError(ProtocolError::new(err))
    }

    pub fn disconnect(&self) -> ControlResult {
        let pkt = codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::NormalDisconnection,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        };
        ControlResult { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }

    pub fn disconnect_with(&self, pkt: codec::Disconnect) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }
}

#[derive(Debug)]
pub struct Auth(codec::Auth);

impl Auth {
    /// Returns reference to dusconnect packet
    pub fn packet(&self) -> &codec::Auth {
        &self.0
    }

    pub fn ack(self, response: codec::Auth) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::Auth(response)), disconnect: false }
    }
}

#[derive(Debug)]
pub struct Ping;

impl Ping {
    pub fn ack(self) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::PingResponse), disconnect: false }
    }
}

#[derive(Debug)]
pub struct Disconnect(pub(crate) codec::Disconnect);

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
#[derive(Debug)]
pub struct Subscribe {
    packet: codec::Subscribe,
    result: codec::SubscribeAck,
}

impl Subscribe {
    pub(crate) fn create<E>(packet: codec::Subscribe) -> ControlMessage<E> {
        let mut status = Vec::with_capacity(packet.topic_filters.len());
        (0..packet.topic_filters.len())
            .for_each(|_| status.push(codec::SubscribeAckReason::UnspecifiedError));

        let result = codec::SubscribeAck {
            status,
            packet_id: packet.packet_id,
            properties: codec::UserProperties::default(),
            reason_string: None,
        };

        ControlMessage::Subscribe(Self { packet, result })
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> SubscribeIter<'_> {
        SubscribeIter { subs: self as *const _ as *mut _, entry: 0, lt: PhantomData }
    }

    #[inline]
    /// Reason string for ack packet
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
    /// Ack Subscribe packet
    pub fn ack(self) -> ControlResult {
        ControlResult {
            packet: Some(codec::Packet::SubscribeAck(self.result)),
            disconnect: false,
        }
    }

    /// Returns reference to subscribe packet
    pub fn packet(&self) -> &codec::Subscribe {
        &self.packet
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
        *self.status = status
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
        self.confirm(qos)
    }
}

/// Unsubscribe message
#[derive(Debug)]
pub struct Unsubscribe {
    packet: codec::Unsubscribe,
    result: codec::UnsubscribeAck,
}

impl Unsubscribe {
    pub(crate) fn create<E>(packet: codec::Unsubscribe) -> ControlMessage<E> {
        let mut status = Vec::with_capacity(packet.topic_filters.len());
        (0..packet.topic_filters.len())
            .for_each(|_| status.push(codec::UnsubscribeAckReason::Success));

        let result = codec::UnsubscribeAck {
            status,
            packet_id: packet.packet_id,
            properties: codec::UserProperties::default(),
            reason_string: None,
        };

        ControlMessage::Unsubscribe(Self { packet, result })
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
    pub fn iter_mut(&mut self) -> UnsubscribeIter<'_> {
        UnsubscribeIter { subs: self as *const _ as *mut _, entry: 0, lt: PhantomData }
    }

    #[inline]
    /// Reason string for ack packet
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

    /// Returns reference to unsubscribe packet
    pub fn packet(&self) -> &codec::Unsubscribe {
        &self.packet
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
        ControlResult { packet: None, disconnect: false }
    }
}

/// Service level error
#[derive(Debug)]
pub struct Error<E> {
    err: E,
    pkt: codec::Disconnect,
}

impl<E> Error<E> {
    pub fn new(err: E) -> Self {
        Self {
            err,
            pkt: codec::Disconnect {
                session_expiry_interval_secs: None,
                server_reference: None,
                reason_string: None,
                user_properties: UserProperties::default(),
                reason_code: DisconnectReasonCode::ImplementationSpecificError,
            },
        }
    }

    #[inline]
    /// Returns reference to mqtt error
    pub fn get_err(&self) -> &E {
        &self.err
    }

    #[inline]
    /// Set reason string for disconnect packet
    pub fn reason_string(mut self, reason: ByteString) -> Self {
        self.pkt.reason_string = Some(reason);
        self
    }

    #[inline]
    /// Set server reference for disconnect packet
    pub fn server_reference(mut self, reference: ByteString) -> Self {
        self.pkt.server_reference = Some(reference);
        self
    }

    #[inline]
    /// Update disconnect packet properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.pkt.user_properties);
        self
    }

    #[inline]
    /// Ack service error, return disconnect packet and close connection.
    pub fn ack(mut self, reason: DisconnectReasonCode) -> ControlResult {
        self.pkt.reason_code = reason;
        ControlResult { packet: Some(codec::Packet::Disconnect(self.pkt)), disconnect: true }
    }

    #[inline]
    /// Ack service error, return disconnect packet and close connection.
    pub fn ack_with<F>(self, f: F) -> ControlResult
    where
        F: FnOnce(E, codec::Disconnect) -> codec::Disconnect,
    {
        let pkt = f(self.err, self.pkt);
        ControlResult { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }
}

/// Connection failed message
#[derive(Debug)]
pub struct ProtocolError {
    err: error::ProtocolError,
    pkt: codec::Disconnect,
}

impl ProtocolError {
    pub fn new(err: error::ProtocolError) -> Self {
        Self {
            pkt: codec::Disconnect {
                session_expiry_interval_secs: None,
                server_reference: None,
                reason_string: None,
                user_properties: UserProperties::default(),
                reason_code: match err {
                    error::ProtocolError::Decode(error::DecodeError::InvalidLength) => {
                        DisconnectReasonCode::MalformedPacket
                    }
                    error::ProtocolError::Decode(error::DecodeError::MaxSizeExceeded) => {
                        DisconnectReasonCode::PacketTooLarge
                    }
                    error::ProtocolError::Unexpected(_, _) => {
                        DisconnectReasonCode::ProtocolError
                    }
                    error::ProtocolError::ReceiveMaximumExceeded => {
                        DisconnectReasonCode::ReceiveMaximumExceeded
                    }
                    error::ProtocolError::KeepAliveTimeout => {
                        DisconnectReasonCode::KeepAliveTimeout
                    }
                    error::ProtocolError::UnknownTopicAlias => {
                        DisconnectReasonCode::TopicAliasInvalid
                    }
                    error::ProtocolError::Encode(_) => {
                        DisconnectReasonCode::ImplementationSpecificError
                    }
                    _ => DisconnectReasonCode::ImplementationSpecificError,
                },
            },
            err,
        }
    }

    #[inline]
    /// Returns reference to a protocol error
    pub fn get_ref(&self) -> &error::ProtocolError {
        &self.err
    }

    #[inline]
    /// Set reason code for disconnect packet
    pub fn reason_code(mut self, reason: DisconnectReasonCode) -> Self {
        self.pkt.reason_code = reason;
        self
    }

    #[inline]
    /// Set reason string for disconnect packet
    pub fn reason_string(mut self, reason: ByteString) -> Self {
        self.pkt.reason_string = Some(reason);
        self
    }

    #[inline]
    /// Set server reference for disconnect packet
    pub fn server_reference(mut self, reference: ByteString) -> Self {
        self.pkt.server_reference = Some(reference);
        self
    }

    #[inline]
    /// Update disconnect packet properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.pkt.user_properties);
        self
    }

    #[inline]
    /// Ack protocol error, return disconnect packet and close connection.
    pub fn ack(self) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::Disconnect(self.pkt)), disconnect: true }
    }
}
