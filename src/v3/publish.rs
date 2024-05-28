use std::{mem, num::NonZeroU16};

use ntex_bytes::{ByteString, Bytes};
use ntex_router::Path;
use serde::de::DeserializeOwned;
use serde_json::Error as JsonError;

use crate::v3::codec;

#[derive(Clone)]
/// Publish message
pub struct Publish {
    pkt: codec::Publish,
    pkt_size: u32,
    topic: Path<ByteString>,
}

impl Publish {
    /// Create a new `Publish` message from a PUBLISH
    /// packet
    #[doc(hidden)]
    pub fn new(pkt: codec::Publish, pkt_size: u32) -> Self {
        Self { topic: Path::new(pkt.topic.clone()), pkt, pkt_size }
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
    /// Returns size of the publish
    pub fn packet_size(&self) -> u32 {
        self.pkt_size
    }

    #[inline]
    /// the Application Message that is being published.
    pub fn payload(&self) -> &Bytes {
        &self.pkt.payload
    }

    /// Replace packet'a payload with empty bytes, returns existing payload.
    pub fn take_payload(&mut self) -> Bytes {
        mem::take(&mut self.pkt.payload)
    }

    /// Loads and parse `application/json` encoded body.
    pub fn json<T: DeserializeOwned>(&mut self) -> Result<T, JsonError> {
        serde_json::from_slice(&self.pkt.payload)
    }

    pub(super) fn into_inner(self) -> codec::Publish {
        self.pkt
    }
}

impl std::fmt::Debug for Publish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.pkt.fmt(f)
    }
}
