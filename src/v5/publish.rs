use std::{mem, num::NonZeroU16};

use ntex_bytes::{ByteString, Bytes};
use ntex_router::Path;

use crate::{Ticket, error::PayloadError, payload::Payload, v5::codec};

#[derive(Debug)]
/// Incoming publish metadata, routing path, and payload receiver.
pub struct Publish {
    /// Decoded publish metadata, including the declared payload length.
    pkt: codec::Publish,
    /// PUBLISH Remaining Length in bytes, including the complete payload.
    pkt_size: u32,
    /// Independent topic path used for routing and captured parameters.
    topic: Path<ByteString>,
    /// Buffered or streamed payload receiver owned by this message.
    payload: Payload,
    /// Arrival ticket of a `QoS 1` or `QoS 2` publish.
    ticket: Option<Ticket>,
}

impl Publish {
    /// Wraps decoded publish metadata and its payload, using Remaining Length for `pkt_size`.
    #[doc(hidden)]
    pub fn new(pkt: codec::Publish, payload: Payload, pkt_size: u32) -> Self {
        Self {
            topic: Path::new(pkt.topic.clone()),
            pkt,
            pkt_size,
            payload,
            ticket: None,
        }
    }

    #[inline]
    /// Returns whether the client marked this publish as a possible redelivery.
    pub fn dup(&self) -> bool {
        self.pkt.dup
    }

    #[inline]
    /// Returns whether the client requested retaining this message for future subscribers.
    pub fn retain(&self) -> bool {
        self.pkt.retain
    }

    #[inline]
    /// Returns the publish delivery quality of service.
    pub fn qos(&self) -> codec::QoS {
        self.pkt.qos
    }

    #[inline]
    /// Returns the publish topic, after inbound topic-alias resolution.
    pub fn publish_topic(&self) -> &str {
        &self.pkt.topic
    }

    #[inline]
    /// Returns the packet identifier, present for `QoS 1` and `QoS 2` publishes.
    pub fn id(&self) -> Option<NonZeroU16> {
        self.pkt.packet_id
    }

    #[inline]
    /// Returns the routing path and any captured topic parameters.
    pub fn topic(&self) -> &Path<ByteString> {
        &self.topic
    }

    #[inline]
    /// Returns mutable access to the routing path without changing the packet topic.
    pub fn topic_mut(&mut self) -> &mut Path<ByteString> {
        &mut self.topic
    }

    #[inline]
    /// Returns reference to publish packet
    pub fn packet(&self) -> &codec::Publish {
        &self.pkt
    }

    #[inline]
    /// Returns mutable access to publish metadata.
    ///
    /// Changing the packet topic does not update the independent routing path; changing
    /// `payload_size` does not change the payload receiver.
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.pkt
    }

    #[inline]
    /// Returns Remaining Length in bytes, including the payload but excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.pkt_size
    }

    #[inline]
    /// Returns the declared payload length in bytes, including unread streamed chunks.
    pub fn payload_size(&self) -> usize {
        self.pkt.payload_size as usize
    }

    #[inline]
    /// Reads the next payload chunk, waiting for streamed data when necessary.
    ///
    /// Returns `None` at end of stream. Consuming chunks allows payload-buffer
    /// backpressure to clear once the low watermark is reached.
    pub async fn read(&self) -> Result<Option<Bytes>, PayloadError> {
        self.payload.read().await
    }

    #[inline]
    /// Collects all remaining payload bytes, waiting for the stream to end.
    ///
    /// Chunks already consumed by [`Self::read`] are not included. Returns
    /// [`PayloadError::Consumed`] if no unread chunk remains.
    pub async fn read_all(&self) -> Result<Bytes, PayloadError> {
        self.payload.read_all().await
    }

    /// Take the payload out of the message, leaving an empty `Payload` behind.
    pub fn take_payload(&mut self) -> Payload {
        mem::take(&mut self.payload)
    }

    #[inline]
    /// Arrival ticket of a `QoS 1` or `QoS 2` publish, `None` for `QoS 0` or once taken.
    ///
    /// Dropping the publish drops its ticket, which passes or skips its turn.
    pub fn ticket(&self) -> Option<&Ticket> {
        self.ticket.as_ref()
    }

    /// Take the arrival ticket out of the message.
    pub fn take_ticket(&mut self) -> Option<Ticket> {
        self.ticket.take()
    }

    /// Attaches the arrival ticket issued by the codec.
    pub(crate) fn with_ticket(mut self, ticket: Option<Ticket>) -> Self {
        self.ticket = ticket;
        self
    }

    /// Consumes this message and creates a successful publish acknowledgement.
    ///
    /// The dispatcher sends `PUBACK` for `QoS 1` or `PUBREC` for `QoS 2`; `QoS 0`
    /// does not produce a wire acknowledgement. Read or take a streamed payload
    /// before consuming the message to avoid dropping its receiver prematurely.
    pub fn ack(self) -> PublishAck {
        PublishAck {
            reason_code: codec::PublishAckReason::Success,
            properties: codec::UserProperties::default(),
            reason_string: None,
        }
    }

    /// Separates decoded metadata from its payload receiver, discarding routing state.
    pub(crate) fn into_inner(self) -> (codec::Publish, Payload) {
        (self.pkt, self.payload)
    }
}

#[derive(Debug)]
/// Application response used to construct a publish acknowledgement.
pub struct PublishAck {
    /// Reason used in `PUBACK` or `PUBREC` for this publish.
    pub(crate) reason_code: codec::PublishAckReason,
    /// User properties included in the acknowledgement.
    pub(crate) properties: codec::UserProperties,
    /// Optional diagnostic text included in the acknowledgement.
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

    #[inline]
    #[must_use]
    /// Sets the reason code for the publish acknowledgement.
    pub fn reason_code(mut self, reason_code: codec::PublishAckReason) -> Self {
        self.reason_code = reason_code;
        self
    }

    #[inline]
    #[must_use]
    /// Modifies user properties included in the publish acknowledgement.
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.properties);
        self
    }

    #[inline]
    #[must_use]
    /// Sets diagnostic text included in the publish acknowledgement.
    pub fn reason(mut self, reason: ByteString) -> Self {
        self.reason_string = Some(reason);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: Taking a publish's arrival ticket transfers ownership of its ordering turn independently of the publish.
    ///
    /// HOW:
    /// - Attach the first issued ticket to a publish and issue the next ticket.
    /// - Take the first ticket, then drop the publish and verify the next ticket still cannot run.
    /// - Drop the detached ticket and verify the next ticket acquires its turn.
    #[ntex::test]
    async fn test_taken_ticket_holds_turn() {
        let tickets = crate::ticket::Tickets::default();
        let mut publish = Publish::new(codec::Publish::default(), Payload::default(), 0)
            .with_ticket(tickets.issue(codec::QoS::AtLeastOnce));
        let next = tickets.issue(codec::QoS::AtLeastOnce).unwrap();
        let ticket = publish.take_ticket().unwrap();
        assert!(ticket.is_turn());
        assert!(publish.take_ticket().is_none());
        drop(publish);
        assert!(!next.is_turn());
        drop(ticket);
        assert!(next.is_turn());
    }
}
