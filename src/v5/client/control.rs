use ntex::util::ByteString;

use crate::{error, v5::codec};

pub use crate::v5::control::{Closed, ControlResult, Disconnect, Error, ProtocolError};

pub enum ControlMessage<E> {
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
}

impl<E> ControlMessage<E> {
    pub(super) fn publish(pkt: codec::Publish) -> Self {
        ControlMessage::Publish(Publish(pkt))
    }

    pub(super) fn dis(pkt: codec::Disconnect) -> Self {
        ControlMessage::Disconnect(Disconnect(pkt))
    }

    pub(super) fn closed(is_error: bool) -> Self {
        ControlMessage::Closed(Closed::new(is_error))
    }

    pub(super) fn error(err: E) -> Self {
        ControlMessage::Error(Error::new(err))
    }

    pub(super) fn proto_error(err: error::ProtocolError) -> Self {
        ControlMessage::ProtocolError(ProtocolError::new(err))
    }

    pub fn disconnect(&self, pkt: codec::Disconnect) -> ControlResult {
        ControlResult { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }
}

pub struct Publish(codec::Publish);

impl Publish {
    /// Returns reference to publish packet
    pub fn packet(&self) -> &codec::Publish {
        &self.0
    }

    /// Returns reference to publish packet
    pub fn packet_mut(&mut self) -> &mut codec::Publish {
        &mut self.0
    }

    pub fn ack_qos0(self) -> ControlResult {
        ControlResult { packet: None, disconnect: false }
    }

    pub fn ack(self, reason_code: codec::PublishAckReason) -> ControlResult {
        ControlResult {
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
    ) -> ControlResult {
        ControlResult {
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
}
