use std::io;

use ntex_bytes::Bytes;

use crate::payload::Payload;
pub use crate::v3::control::{Closed, ControlAck, Disconnect, Error, PeerGone, ProtocolError};
use crate::v3::{codec, control::ControlAckKind, error};

/// Client control messages
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
    pub(super) fn publish(pkt: codec::Publish, pl: Payload, size: u32) -> Self {
        Control::Publish(Publish(pkt, pl, size))
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

    /// Ack control message
    pub fn ack(self) -> ControlAck {
        match self {
            Control::Publish(msg) => msg.ack(),
            Control::Closed(msg) => msg.ack(),
            Control::Error(msg) => msg.ack(),
            Control::ProtocolError(msg) => msg.ack(),
            Control::PeerGone(msg) => msg.ack(),
        }
    }
}

#[derive(Debug)]
pub struct Publish(codec::Publish, Payload, u32);

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
        self.2
    }

    #[inline]
    /// Returns size of the payload
    pub fn payload_size(&self) -> usize {
        self.0.payload_size as usize
    }

    #[inline]
    /// Payload that is being published.
    pub async fn read(&self) -> Option<Result<Bytes, ()>> {
        self.1.read().await
    }

    #[inline]
    /// Payload that is being published.
    pub async fn read_all(&self) -> Option<Result<Bytes, ()>> {
        self.1.read_all().await
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
