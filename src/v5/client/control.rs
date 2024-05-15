use std::io;

use ntex::util::ByteString;

use crate::{error, v5::codec};

pub use crate::v5::control::{Closed, ControlAck, Disconnect, Error, ProtocolError};

/// Client control messages
#[derive(Debug)]
pub enum Control<E> {
    /// Unhandled publish packet
    Publish(Publish),
    /// Disconnect packet
    Disconnect(Disconnect),
    /// Application level error from resources and control services
    Error(Error<E>),
    /// Protocol level error
    ProtocolError(ProtocolError),
    /// Connection closed
    Closed(Closed),
    /// Peer is gone
    PeerGone(PeerGone),
}

impl<E> Control<E> {
    pub(super) fn publish(pkt: codec::Publish, size: u32) -> Self {
        Control::Publish(Publish(pkt, size))
    }

    pub(super) fn dis(pkt: codec::Disconnect, size: u32) -> Self {
        Control::Disconnect(Disconnect(pkt, size))
    }

    pub(super) const fn closed() -> Self {
        Control::Closed(Closed)
    }

    pub(super) fn error(err: E) -> Self {
        Control::Error(Error::new(err))
    }

    pub(super) fn proto_error(err: error::ProtocolError) -> Self {
        Control::ProtocolError(ProtocolError::new(err))
    }

    pub(super) fn peer_gone(err: Option<io::Error>) -> Self {
        Control::PeerGone(PeerGone(err))
    }

    pub fn disconnect(&self, pkt: codec::Disconnect) -> ControlAck {
        ControlAck { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }

    /// Ack control message
    pub fn ack(self) -> ControlAck {
        match self {
            Control::Publish(_) => {
                crate::v5::disconnect("Publish control message is not supported")
            }
            Control::Disconnect(msg) => msg.ack(),
            Control::Closed(msg) => msg.ack(),
            Control::Error(_) => {
                crate::v5::disconnect("Error control message is not supported")
            }
            Control::ProtocolError(msg) => msg.ack(),
            Control::PeerGone(msg) => msg.ack(),
        }
    }
}

#[derive(Debug)]
pub struct Publish(codec::Publish, u32);

impl Publish {
    /// Returns reference to publish packet
    pub fn packet(&self) -> &codec::Publish {
        &self.0
    }

    /// Returns reference to publish packet
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.0
    }

    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.1
    }

    pub fn ack_qos0(self) -> ControlAck {
        ControlAck { packet: None, disconnect: false }
    }

    pub fn ack(self, reason_code: codec::PublishAckReason) -> ControlAck {
        ControlAck {
            packet: self.0.packet_id.map(|packet_id| {
                codec::Packet::PublishAck(codec::PublishAck {
                    packet_id,
                    reason_code,
                    properties: codec::UserProperties::new(),
                    reason_string: None,
                })
            }),
            disconnect: false,
        }
    }

    pub fn ack_with(
        self,
        reason_code: codec::PublishAckReason,
        properties: codec::UserProperties,
        reason_string: Option<ByteString>,
    ) -> ControlAck {
        ControlAck {
            packet: self.0.packet_id.map(|packet_id| {
                codec::Packet::PublishAck(codec::PublishAck {
                    packet_id,
                    reason_code,
                    properties,
                    reason_string,
                })
            }),
            disconnect: false,
        }
    }

    pub fn into_inner(
        self,
        reason_code: codec::PublishAckReason,
    ) -> (ControlAck, codec::Publish) {
        (
            ControlAck {
                packet: self.0.packet_id.map(|packet_id| {
                    codec::Packet::PublishAck(codec::PublishAck {
                        packet_id,
                        reason_code,
                        properties: codec::UserProperties::new(),
                        reason_string: None,
                    })
                }),
                disconnect: false,
            },
            self.0,
        )
    }
}

#[derive(Debug)]
pub struct PeerGone(Option<io::Error>);

impl PeerGone {
    /// Returns error reference
    pub fn error(&self) -> Option<&io::Error> {
        self.0.as_ref()
    }

    /// Ack PeerGone message
    pub fn ack(self) -> ControlAck {
        ControlAck { packet: None, disconnect: true }
    }
}
