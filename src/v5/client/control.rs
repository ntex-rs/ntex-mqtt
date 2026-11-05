//! MQTT 5 client protocol messages and acknowledgement builders.
use ntex_bytes::{ByteString, Bytes};

use crate::{error, payload::Payload, types::QoS, v5::codec, v5::control::Pkt};

pub use crate::v5::control::{Disconnect, Ping, ProtocolMessageAck, PublishRelease};

/// Incoming protocol messages not consumed by client-side packet handling.
#[derive(Debug)]
pub enum ProtocolMessage {
    /// Incoming publication not handled by a topic route.
    Publish(Publish),
    /// Release step of an incoming `QoS` 2 publication acknowledged with `PUBREC`.
    PublishRelease(PublishRelease),
    /// `Disconnect` packet from a server
    Disconnect(Disconnect),
    /// Ping request; broker `PINGREQ` packets are rejected rather than delivered here.
    Ping(Ping),
}

impl ProtocolMessage {
    /// Wrap an unrouted publication with its payload and packet body size.
    pub(super) fn publish(pkt: codec::Publish, pl: Payload, size: u32) -> Self {
        ProtocolMessage::Publish(Publish(pkt, pl, size))
    }

    /// Wrap the release step of an incoming `QoS` 2 exchange.
    pub(super) fn pubrel(pkt: codec::PublishAck2, size: u32) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease::new(pkt, size))
    }

    /// Preserve the broker disconnect packet and its body size.
    pub(super) fn dis(pkt: codec::Disconnect, size: u32) -> Self {
        ProtocolMessage::Disconnect(Disconnect(pkt, size))
    }

    /// Build a dispatcher response that sends the provided `Disconnect` and closes the connection.
    pub fn disconnect(&self, pkt: codec::Disconnect) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::from(pkt)),
            disconnect: true,
        }
    }

    /// Build the default dispatcher response for this protocol message.
    ///
    /// An unrouted publication causes a disconnect because publishing is unsupported
    /// by this default response; use its `Publish::ack` method to accept it instead.
    pub fn ack(self) -> ProtocolMessageAck {
        match self {
            ProtocolMessage::Publish(_) => crate::v5::disconnect(error::ERR_PUB_NOT_SUP),
            ProtocolMessage::PublishRelease(msg) => msg.ack(),
            ProtocolMessage::Disconnect(msg) => msg.ack(),
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
    /// Consume a `QoS` 0 publication without sending an acknowledgement.
    ///
    /// This does not inspect the packet `QoS`; use only for `QoS` 0 publications.
    pub fn ack_qos0(self) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::None,
            disconnect: false,
        }
    }

    #[inline]
    /// Build a publish acknowledgement with the provided reason code.
    ///
    /// The dispatcher sends `PublishAck` for `QoS` 1 and `PublishReceived` for `QoS` 2.
    /// No packet is sent for `QoS` 0.
    pub fn ack(self, reason_code: codec::PublishAckReason) -> ProtocolMessageAck {
        self.ack_with(reason_code, codec::UserProperties::new(), None)
    }

    #[inline]
    /// Build a publish acknowledgement with reason code, properties, and reason string.
    ///
    /// See [`Publish::ack`].
    pub fn ack_with(
        self,
        reason_code: codec::PublishAckReason,
        properties: codec::UserProperties,
        reason_string: Option<ByteString>,
    ) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: ack_packet(&self.0, reason_code, properties, reason_string),
            disconnect: false,
        }
    }

    /// Return the acknowledgement and publish header, dropping unread payload.
    ///
    /// See [`Publish::ack`].
    pub fn into_inner(
        self,
        reason_code: codec::PublishAckReason,
    ) -> (ProtocolMessageAck, codec::Publish) {
        (
            ProtocolMessageAck {
                packet: ack_packet(&self.0, reason_code, codec::UserProperties::new(), None),
                disconnect: false,
            },
            self.0,
        )
    }
}

/// Build the `QoS`-specific acknowledgement, or no packet for `QoS` 0.
fn ack_packet(
    pkt: &codec::Publish,
    reason_code: codec::PublishAckReason,
    properties: codec::UserProperties,
    reason_string: Option<ByteString>,
) -> Pkt {
    pkt.packet_id.map_or(Pkt::None, |packet_id| {
        let ack = codec::PublishAck {
            packet_id,
            reason_code,
            properties,
            reason_string,
        };
        Pkt::Packet(if pkt.qos == QoS::ExactlyOnce {
            codec::Packet::PublishReceived(ack)
        } else {
            codec::Packet::PublishAck(ack)
        })
    })
}
