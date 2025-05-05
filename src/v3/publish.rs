use std::{mem, num::NonZeroU16};

use ntex_bytes::{ByteString, Bytes};
use ntex_router::Path;
use serde::de::DeserializeOwned;
use serde_json::Error as JsonError;

use crate::{error::PayloadError, payload::Payload, v3::codec};

#[derive(Debug)]
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
    /// Returns size of the publish
    pub fn packet_size(&self) -> u32 {
        self.pkt_size
    }

    #[inline]
    /// Returns size of the payload
    pub fn payload_size(&self) -> usize {
        self.pkt.payload_size as usize
    }

    #[inline]
    /// Read next chunk of the published payload.
    pub async fn read(&self) -> Result<Option<Bytes>, PayloadError> {
        self.payload.read().await
    }

    #[inline]
    /// Read complete payload.
    pub async fn read_all(&self) -> Result<Bytes, PayloadError> {
        self.payload.read_all().await
    }

    /// Replace packet'a payload with empty bytes, returns existing payload.
    pub fn take_payload(&mut self) -> Payload {
        mem::take(&mut self.payload)
    }

    pub(super) fn into_inner(self) -> (codec::Publish, Payload, u32) {
        (self.pkt, self.payload, self.pkt_size)
    }
}
