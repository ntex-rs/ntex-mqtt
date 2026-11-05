use std::{mem, num::NonZeroU16};

use ntex_bytes::{ByteString, Bytes};
use ntex_router::Path;

use crate::{error::PayloadError, payload::Payload, v3::codec};

#[derive(Debug)]
/// Incoming publish metadata, routable topic path, and consumable payload receiver.
pub struct Publish {
    /// Decoded publish metadata, without its streamed payload.
    pkt: codec::Publish,
    /// PUBLISH remaining length in bytes, excluding its fixed header.
    pkt_size: u32,
    /// Independently parsed topic path and captures populated by the router.
    topic: Path<ByteString>,
    /// Payload receiver consumed through reads or transferred with `take_payload`.
    payload: Payload,
}

impl Publish {
    /// Wrap PUBLISH metadata and its payload receiver with an independent routing path.
    ///
    /// `pkt_size` is the remaining length in bytes, excluding the fixed header.
    #[doc(hidden)]
    pub fn new(pkt: codec::Publish, payload: Payload, pkt_size: u32) -> Self {
        Self {
            topic: Path::new(pkt.topic.clone()),
            pkt,
            pkt_size,
            payload,
        }
    }

    #[inline]
    /// Return whether this packet may be a redelivery of an earlier publication.
    pub fn dup(&self) -> bool {
        self.pkt.dup
    }

    #[inline]
    /// Return whether the sender requests retention for future subscribers.
    ///
    /// The application implements retained-message storage; this flag does not store data.
    pub fn retain(&self) -> bool {
        self.pkt.retain
    }

    #[inline]
    /// Return the publication’s delivery quality.
    pub fn qos(&self) -> codec::QoS {
        self.pkt.qos
    }

    #[inline]
    /// Return the topic name from the packet metadata.
    pub fn publish_topic(&self) -> &str {
        &self.pkt.topic
    }

    #[inline]
    /// Return the packet identifier, present for `QoS 1` and `QoS 2` publications.
    pub fn id(&self) -> Option<NonZeroU16> {
        self.pkt.packet_id
    }

    #[inline]
    /// Return the routing path and any captures populated during topic recognition.
    pub fn topic(&self) -> &Path<ByteString> {
        &self.topic
    }

    #[inline]
    /// Mutably access the routing path and its captures.
    ///
    /// This path is separate from the topic string in `packet`.
    pub fn topic_mut(&mut self) -> &mut Path<ByteString> {
        &mut self.topic
    }

    #[inline]
    /// Returns reference to the PUBLISH packet
    pub fn packet(&self) -> &codec::Publish {
        &self.pkt
    }

    #[inline]
    /// Mutably access PUBLISH metadata.
    ///
    /// Changing its topic does not update the independently parsed routing path.
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.pkt
    }

    #[inline]
    /// Return the PUBLISH remaining length in bytes, excluding its fixed header.
    pub fn packet_size(&self) -> u32 {
        self.pkt_size
    }

    #[inline]
    /// Return the declared payload size in bytes, including any bytes already consumed.
    pub fn payload_size(&self) -> usize {
        self.pkt.payload_size as usize
    }

    #[inline]
    /// Consume the next available payload chunk, waiting for more data if needed.
    ///
    /// Returns `None` after the payload is exhausted.
    pub async fn read(&self) -> Result<Option<Bytes>, PayloadError> {
        self.payload.read().await
    }

    #[inline]
    /// Consume and collect all remaining payload bytes.
    ///
    /// Bytes consumed by earlier reads are not included.
    pub async fn read_all(&self) -> Result<Bytes, PayloadError> {
        self.payload.read_all().await
    }

    /// Take the payload out of the message, leaving an empty `Payload` behind.
    pub fn take_payload(&mut self) -> Payload {
        mem::take(&mut self.payload)
    }

    /// Consume the message, returning its metadata, payload receiver, and remaining length.
    pub(super) fn into_inner(self) -> (codec::Publish, Payload, u32) {
        (self.pkt, self.payload, self.pkt_size)
    }
}
