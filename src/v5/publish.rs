use std::num::NonZeroU16;

use bytes::Bytes;
use bytestring::ByteString;
use ntex::router::Path;
use serde::de::DeserializeOwned;
use serde_json::Error as JsonError;

use super::codec;

/// Publish message
pub struct Publish {
    publish: codec::Publish,
    topic: Path<ByteString>,
}

impl Publish {
    pub(crate) fn new(publish: codec::Publish) -> Self {
        Self { topic: Path::new(publish.topic.clone()), publish }
    }

    #[inline]
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(&self) -> bool {
        self.publish.dup
    }

    #[inline]
    pub fn retain(&self) -> bool {
        self.publish.retain
    }

    #[inline]
    /// the level of assurance for delivery of an Application Message.
    pub fn qos(&self) -> codec::QoS {
        self.publish.qos
    }

    #[inline]
    /// the information channel to which payload data is published.
    pub fn publish_topic(&self) -> &str {
        &self.publish.topic
    }

    #[inline]
    /// only present in PUBLISH Packets where the QoS level is 1 or 2.
    pub fn id(&self) -> Option<NonZeroU16> {
        self.publish.packet_id
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
        &self.publish
    }

    #[inline]
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.publish
    }

    #[inline]
    /// the Application Message that is being published.
    pub fn payload(&self) -> &Bytes {
        &self.publish.payload
    }

    /// Extract Bytes from packet payload
    pub fn take_payload(&self) -> Bytes {
        self.publish.payload.clone()
    }

    /// Loads and parse `application/json` encoded body.
    pub fn json<T: DeserializeOwned>(&mut self) -> Result<T, JsonError> {
        serde_json::from_slice(&self.publish.payload)
    }

    /// Create acknowledgement for this packet
    pub fn ack(self) -> PublishAck {
        PublishAck {
            reason_code: codec::PublishAckReason::Success,
            properties: codec::UserProperties::default(),
            reason_string: None,
        }
    }

    pub(crate) fn into_inner(self) -> codec::Publish {
        self.publish
    }
}

impl std::fmt::Debug for Publish {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.publish.fmt(f)
    }
}

/// Publish ack
pub struct PublishAck {
    pub(crate) reason_code: codec::PublishAckReason,
    pub(crate) properties: codec::UserProperties,
    pub(crate) reason_string: Option<ByteString>,
}

impl PublishAck {
    /// Create new `PublishAck` instance
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
