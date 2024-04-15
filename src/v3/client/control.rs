use std::io;

pub use crate::v3::control::{Closed, ControlAck, Disconnect, Error, PeerGone, ProtocolError};
use crate::v3::{codec, control::ControlAckKind, error};

#[derive(Debug)]
pub enum Control<E> {
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

impl<E> Control<E> {
    pub(super) fn publish(pkt: codec::Publish) -> Self {
        Control::Publish(Publish(pkt))
    }

    pub(super) fn closed() -> Self {
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

    /// Initiate clean disconnect
    pub fn disconnect(&self) -> ControlAck {
        ControlAck { result: ControlAckKind::Disconnect }
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

    pub fn ack(self) -> ControlAck {
        if let Some(id) = self.0.packet_id {
            ControlAck { result: ControlAckKind::PublishAck(id) }
        } else {
            ControlAck { result: ControlAckKind::Nothing }
        }
    }

    pub fn into_inner(self) -> (ControlAck, codec::Publish) {
        if let Some(id) = self.0.packet_id {
            (ControlAck { result: ControlAckKind::PublishAck(id) }, self.0)
        } else {
            (ControlAck { result: ControlAckKind::Nothing }, self.0)
        }
    }
}
