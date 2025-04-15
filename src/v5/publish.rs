use std::{mem, num::NonZeroU16};

use ntex_bytes::{ByteString, Bytes};
use ntex_router::Path;

use super::codec;
use crate::payload::Payload;

/// Publish message
pub struct Publish {
    pkt: codec::Publish,
    pkt_size: u32,
    topic: Path<ByteString>,
    payload: Payload,
}

impl Publish {
    /// Create a new `Publish` message from a PUBLISH
    /// packet
    #[doc(hidden)]
    pub fn new(pkt: codec::Publish, payload: Payload, pkt_size: u32) -> Self {
        Self { topic: Path::new(pkt.topic.clone()), pkt, pkt_size, payload }
    }

    #[inline]
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(&self) -> bool {
        self.pkt.dup
    }

    #[inline]
    pub fn retain(&self) -> bool {
        self.pkt.retain
    }

    #[inline]
    /// the level of assurance for delivery of an Application Message.
    pub fn qos(&self) -> codec::QoS {
        self.pkt.qos
    }

    #[inline]
    /// the information channel to which payload data is published.
    pub fn publish_topic(&self) -> &str {
        &self.pkt.topic
    }

    #[inline]
    /// only present in PUBLISH Packets where the QoS level is 1 or 2.
    pub fn id(&self) -> Option<NonZeroU16> {
        self.pkt.packet_id
    }

    #[inline]
    pub fn topic(&self) -> &Path<ByteString> {
        &self.topic
    }

    #[inline]
    pub fn topic_mut(&mut self) -> &mut Path<ByteString> {
        &mut self.topic
    }

    #[inline]
    pub fn packet(&self) -> &codec::Publish {
        &self.pkt
    }

    #[inline]
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.pkt
    }

    #[inline]
    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.pkt_size
    }

    #[inline]
    /// Returns size of the payload
    pub fn payload_size(&self) -> usize {
        self.pkt.payload_size as usize
    }

    #[inline]
    /// Payload that is being published.
    pub async fn read(&self) -> Option<Result<Bytes, ()>> {
        self.payload.read().await
    }

    #[inline]
    /// Payload that is being published.
    pub async fn read_all(&self) -> Option<Result<Bytes, ()>> {
        self.payload.read_all().await
    }

    /// Replace packet'a payload with empty bytes, returns existing payload.
    pub fn take_payload(&mut self) -> Payload {
        mem::take(&mut self.payload)
    }

    /// Create acknowledgement for this packet
    pub fn ack(self) -> PublishAck {
        PublishAck {
            reason_code: codec::PublishAckReason::Success,
            properties: codec::UserProperties::default(),
            reason_string: None,
        }
    }

    pub(crate) fn into_inner(self) -> (codec::Publish, Payload) {
        (self.pkt, self.payload)
    }
}

impl std::fmt::Debug for Publish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.pkt.fmt(f)
    }
}

#[derive(Debug)]
/// Publish ack
pub struct PublishAck {
    pub(crate) reason_code: codec::PublishAckReason,
    pub(crate) properties: codec::UserProperties,
    pub(crate) reason_string: Option<ByteString>,
}

impl PublishAck {
    /// Create new `PublishAck` instance from a reason code.
    pub fn new(code: codec::PublishAckReason) -> Self {
        PublishAck {
            reason_code: code,
            properties: codec::UserProperties::default(),
            reason_string: None,
        }
    }

    /// Set acknowledgement's Reason Code
    #[inline]
    pub fn reason_code(mut self, reason_code: codec::PublishAckReason) -> Self {
        self.reason_code = reason_code;
        self
    }

    /// Update user properties
    #[inline]
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.properties);
        self
    }

    /// Set ack reason string
    #[inline]
    pub fn reason(mut self, reason: ByteString) -> Self {
        self.reason_string = Some(reason);
        self
    }
}
