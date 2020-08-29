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

    pub fn ack(self, response: Option<codec::PublishAck>) -> ControlResult {
        ControlResult { packet: response.map(codec::Packet::PublishAck), disconnect: false }
    }
}
