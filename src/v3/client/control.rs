//! MQTT 3.1.1 client control messages

use std::num::NonZeroU16;

use ntex_bytes::Bytes;

use crate::payload::Payload;
pub use crate::v3::control::{Disconnect, Ping, ProtocolMessageAck, PublishRelease};
use crate::v3::{codec, control::ProtocolMessageKind, error};

/// MQTT protocol–related messages.
#[derive(Debug)]
pub enum ProtocolMessage {
    /// Unhandled publish packet
    Publish(Publish),
    /// `PublishRelease` packet from the server
    PublishRelease(PublishRelease),
    /// Ping packet from the server
    Ping(Ping),
}

impl ProtocolMessage {
    pub(super) fn publish(pkt: codec::Publish, pl: Payload, size: u32) -> Self {
        ProtocolMessage::Publish(Publish(pkt, pl, size))
    }

    pub(super) fn pubrel(packet_id: NonZeroU16) -> Self {
        ProtocolMessage::PublishRelease(PublishRelease { packet_id })
    }

    #[inline]
    /// Initiate clean disconnect
    pub(super) fn disconnect() -> ProtocolMessageAck {
        ProtocolMessageAck {
            result: ProtocolMessageKind::Disconnect,
        }
    }

    /// Ack control message
    pub fn ack(self) -> ProtocolMessageAck {
        match self {
            ProtocolMessage::Publish(msg) => msg.ack(),
            ProtocolMessage::PublishRelease(msg) => msg.ack(),
            ProtocolMessage::Ping(msg) => msg.ack(),
        }
    }
}

/// Unhandled publish packet from the server
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
    /// Ack publish message
    ///
    /// Sends `PublishAck` packet for `QoS 1` publish and `PublishReceived` packet
    /// for `QoS 2` publish.
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
    /// Ack publish message and return the publish packet, payload is dropped
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
