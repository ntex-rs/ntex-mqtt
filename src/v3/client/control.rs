//! MQTT 3.1.1 client control messages

use std::num::NonZeroU16;

use ntex_bytes::Bytes;

use crate::payload::Payload;
pub use crate::v3::control::{Disconnect, Ping, ProtocolMessageAck, PublishRelease};
use crate::v3::{codec, control::ProtocolMessageKind, error};

/// Incoming protocol messages not consumed by client-side packet handling.
#[derive(Debug)]
pub enum ProtocolMessage {
    /// Incoming publication not handled by a topic route.
    Publish(Publish),
    /// Release step of an incoming `QoS` 2 publication acknowledged with `PUBREC`.
    PublishRelease(PublishRelease),
    /// Ping request; broker `PINGREQ` packets are rejected rather than delivered here.
    Ping(Ping),
}

impl ProtocolMessage {
    /// Wrap an unrouted publication with its payload and packet body size.
    pub(super) fn publish(pkt: codec::Publish, pl: Payload, size: u32) -> Self {
        ProtocolMessage::Publish(Publish(pkt, pl, size))
    }

    /// Wrap the release step of an incoming `QoS` 2 exchange.
    pub(super) fn pubrel(packet_id: NonZeroU16) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease { packet_id })
    }

    #[inline]
    /// Request a clean disconnect from the dispatcher.
    pub(super) fn disconnect() -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Disconnect,
        }
    }

    /// Build the default dispatcher response for this protocol message.
    pub fn ack(self) -> ProtocolMessageAck {
        match self {
            ProtocolMessage::Publish(msg) => msg.ack(),
            ProtocolMessage::PublishRelease(msg) => msg.ack(),
            ProtocolMessage::Ping(msg) => msg.ack(),
        }
    }
}

/// Incoming publication not handled by a topic route.
///
/// Read streamed payloads to completion before consuming this message; dropping an
/// unread streamed payload can close the connection.
#[derive(Debug)]
pub struct Publish(codec::Publish, Payload, u32);

impl Publish {
    #[inline]
    /// Returns reference to publish packet
    pub fn packet(&self) -> &codec::Publish {
        &self.0
    }

    #[inline]
    /// Returns mutable reference to publish packet
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.0
    }

    #[inline]
    /// Return the MQTT remaining length in bytes, excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.2
    }

    #[inline]
    /// Return the total declared payload size in bytes, including unread chunks.
    pub fn payload_size(&self) -> usize {
        self.0.payload_size as usize
    }

    #[inline]
    /// Read the next unread payload chunk, or `None` when the payload is complete.
    pub async fn read(&self) -> Result<Option<Bytes>, error::PayloadError> {
        self.1.read().await
    }

    #[inline]
    /// Collect all remaining unread payload chunks into a single buffer.
    ///
    /// Returns `PayloadError::Consumed` if the payload has already been fully read.
    pub async fn read_all(&self) -> Result<Bytes, error::PayloadError> {
        self.1.read_all().await
    }

    #[inline]
    /// Build an acknowledgement for the dispatcher to send.
    ///
    /// The dispatcher sends `PublishAck` for `QoS` 1 and `PublishReceived` for `QoS` 2.
    /// No packet is sent for `QoS` 0.
    pub fn ack(self) -> ProtocolMessageAck {
        if let Some(id) = self.0.packet_id {
            ProtocolMessageAck {
                result: ProtocolMessageKind::PublishAck(id),
            }
        } else {
            ProtocolMessageAck {
                result: ProtocolMessageKind::Nothing,
            }
        }
    }

    #[inline]
    /// Return the acknowledgement and publish header, dropping unread payload.
    ///
    /// See [`Publish::ack`].
    pub fn into_inner(self) -> (ProtocolMessageAck, codec::Publish) {
        if let Some(id) = self.0.packet_id {
            (
                ProtocolMessageAck {
                    result: ProtocolMessageKind::PublishAck(id),
                },
                self.0,
            )
        } else {
            (
                ProtocolMessageAck {
                    result: ProtocolMessageKind::Nothing,
                },
                self.0,
            )
        }
    }
}
