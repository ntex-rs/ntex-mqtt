use ntex_bytes::{ByteString, Bytes};

use crate::{error, payload::Payload, v5::codec, v5::control::Pkt};

pub use crate::v5::control::{Disconnect, Ping, ProtocolMessageAck, PublishRelease};

/// MQTT protocol–related messages
#[derive(Debug)]
pub enum ProtocolMessage {
    /// Unhandled `Publish` packet
    Publish(Publish),
    /// `PublishRelease` packet from a server
    PublishRelease(PublishRelease),
    /// `Disconnect` packet from a server
    Disconnect(Disconnect),
    /// `Ping` packet from a server
    Ping(Ping),
}

impl ProtocolMessage {
    pub(super) fn publish(pkt: codec::Publish, pl: Payload, size: u32) -> Self {
        ProtocolMessage::Publish(Publish(pkt, pl, size))
    }

    pub(super) fn pubrel(pkt: codec::PublishAck2, size: u32) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease::new(pkt, size))
    }

    pub(super) fn dis(pkt: codec::Disconnect, size: u32) -> Self {
        ProtocolMessage::Disconnect(Disconnect(pkt, size))
    }

    pub fn disconnect(&self, pkt: codec::Disconnect) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: Pkt::Packet(codec::Packet::Disconnect(pkt)),
            disconnect: true,
        }
    }

    /// Ack control message
    pub fn ack(self) -> ProtocolMessageAck {
        match self {
            ProtocolMessage::Publish(_) => crate::v5::disconnect(error::ERR_PUB_NOT_SUP),
            ProtocolMessage::PublishRelease(msg) => msg.ack(),
            ProtocolMessage::Disconnect(msg) => msg.ack(),
            ProtocolMessage::Ping(msg) => msg.ack(),
        }
    }
}

#[derive(Debug)]
pub struct Publish(codec::Publish, Payload, u32);

impl Publish {
    #[inline]
    /// Returns reference to publish packet
    pub fn packet(&self) -> &codec::Publish {
        &self.0
    }

    #[inline]
    /// Returns reference to publish packet
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.0
    }

    #[inline]
    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.2
    }

    #[inline]
    /// Returns size of the payload
    pub fn payload_size(&self) -> usize {
        self.0.payload_size as usize
    }

    #[inline]
    /// Read next chunk of the published payload.
    pub async fn read(&self) -> Result<Option<Bytes>, error::PayloadError> {
        self.1.read().await
    }

    #[inline]
    /// Read complete payload.
    pub async fn read_all(&self) -> Result<Bytes, error::PayloadError> {
        self.1.read_all().await
    }

    #[inline]
    pub fn ack_qos0(self) -> ProtocolMessageAck {
        ProtocolMessageAck { packet: Pkt::None, disconnect: false }
    }

    #[inline]
    pub fn ack(self, reason_code: codec::PublishAckReason) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: self.0.packet_id.map_or(Pkt::None, |packet_id| {
                Pkt::Packet(codec::Packet::PublishAck(codec::PublishAck {
                    packet_id,
                    reason_code,
                    properties: codec::UserProperties::new(),
                    reason_string: None,
                }))
            }),
            disconnect: false,
        }
    }

    #[inline]
    pub fn ack_with(
        self,
        reason_code: codec::PublishAckReason,
        properties: codec::UserProperties,
        reason_string: Option<ByteString>,
    ) -> ProtocolMessageAck {
        ProtocolMessageAck {
            packet: self.0.packet_id.map_or(Pkt::None, |packet_id| {
                Pkt::Packet(codec::Packet::PublishAck(codec::PublishAck {
                    packet_id,
                    reason_code,
                    properties,
                    reason_string,
                }))
            }),
            disconnect: false,
        }
    }

    pub fn into_inner(
        self,
        reason_code: codec::PublishAckReason,
    ) -> (ProtocolMessageAck, codec::Publish) {
        (
            ProtocolMessageAck {
                packet: self.0.packet_id.map_or(Pkt::None, |packet_id| {
                    Pkt::Packet(codec::Packet::PublishAck(codec::PublishAck {
                        packet_id,
                        reason_code,
                        properties: codec::UserProperties::new(),
                        reason_string: None,
                    }))
                }),
                disconnect: false,
            },
            self.0,
        )
    }
}
