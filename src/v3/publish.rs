use std::convert::TryFrom;
use std::num::NonZeroU16;

use bytes::Bytes;
use bytestring::ByteString;
use ntex::router::Path;
use serde::de::DeserializeOwned;
use serde_json::Error as JsonError;

use crate::v3::codec;

/// Publish message
pub struct Publish {
    publish: codec::Publish,
    topic: Path<ByteString>,
    query: Option<ByteString>,
}

impl Publish {
    pub(crate) fn new(publish: codec::Publish) -> Self {
        let (topic, query) = if let Some(pos) = publish.topic.find('?') {
            (
                ByteString::try_from(publish.topic.as_bytes().slice(0..pos)).unwrap(),
                Some(
                    ByteString::try_from(
                        publish.topic.as_bytes().slice(pos + 1..publish.topic.len()),
                    )
                    .unwrap(),
                ),
            )
        } else {
            (publish.topic.clone(), None)
        };
        let topic = Path::new(topic);
        Self { publish, topic, query }
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
    pub fn query(&self) -> &str {
        self.query.as_ref().map(|s| s.as_ref()).unwrap_or("")
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

    pub(super) fn into_inner(self) -> codec::Publish {
        self.publish
    }
}

impl std::fmt::Debug for Publish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.publish.fmt(f)
    }
}
