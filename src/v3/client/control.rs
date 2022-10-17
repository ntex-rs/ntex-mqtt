use std::io;

pub use crate::v3::control::{
    Closed, ControlResult, Disconnect, Error, PeerGone, ProtocolError,
};
use crate::v3::{codec, control::ControlResultKind, error};

#[derive(Debug)]
pub enum ControlMessage<E> {
    /// Unhandled publish packet
    Publish(Publish),
    /// Connection closed
    Closed(Closed),
    /// Application level error from resources and control services
    Error(Error<E>),
    /// Protocol level error
    ProtocolError(ProtocolError),
    /// Peer is gone
    PeerGone(PeerGone),
}

impl<E> ControlMessage<E> {
    pub(super) fn publish(pkt: codec::Publish) -> Self {
        ControlMessage::Publish(Publish(pkt))
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

    pub(super) fn peer_gone(err: Option<io::Error>) -> Self {
        ControlMessage::PeerGone(PeerGone(err))
    }

    /// Initiate clean disconnect
    pub fn disconnect(&self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
    }
}

#[derive(Debug)]
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

    pub fn ack(self) -> ControlResult {
        if let Some(id) = self.0.packet_id {
            ControlResult { result: ControlResultKind::PublishAck(id) }
        } else {
            ControlResult { result: ControlResultKind::Nothing }
        }
    }

    pub fn into_inner(self) -> (ControlResult, codec::Publish) {
        if let Some(id) = self.0.packet_id {
            (ControlResult { result: ControlResultKind::PublishAck(id) }, self.0)
        } else {
            (ControlResult { result: ControlResultKind::Nothing }, self.0)
        }
    }
}
