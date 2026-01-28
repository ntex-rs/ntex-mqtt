use std::{io, num::NonZeroU16};

use ntex_bytes::Bytes;

use crate::payload::Payload;
pub use crate::v3::control::{
    ControlAck, CtlReason, Disconnect, Error, PeerGone, ProtocolError, PublishRelease, Shutdown,
};
use crate::v3::{codec, control::ControlAckKind, error};

/// Client control messages
#[derive(Debug)]
pub enum Control<E> {
    /// MQTT protocol–related messages.
    Protocol(CtlFrame),

    /// Dispatcher is preparing for shutdown.
    ///
    /// The control service will receive this message only once. The next message
    /// after `Disconnect` is `Shutdown`.
    Stop(CtlReason<E>),
    /// Underlying pipeline is shutting down.
    ///
    /// This is the last message the control service receives.
    Shutdown(Shutdown),
}

/// MQTT protocol–related messages
#[derive(Debug)]
pub enum CtlFrame {
    /// Unhandled publish packet
    Publish(Publish),
    /// PublishRelease packet from a client
    PublishRelease(PublishRelease),
}

impl<E> Control<E> {
    pub(super) fn publish(pkt: codec::Publish, pl: Payload, size: u32) -> Self {
        Control::Protocol(CtlFrame::Publish(Publish(pkt, pl, size)))
    }

    pub(super) fn pubrel(packet_id: NonZeroU16) -> Self {
        Control::Protocol(CtlFrame::PublishRelease(PublishRelease { packet_id }))
    }

    pub(super) fn shutdown() -> Self {
        Control::Shutdown(Shutdown)
    }

    pub(super) fn error(err: E) -> Self {
        Control::Stop(CtlReason::Error(Error::new(err)))
    }

    pub(super) fn proto_error(err: error::ProtocolError) -> Self {
        Control::Stop(CtlReason::ProtocolError(ProtocolError::new(err)))
    }

    pub(super) fn peer_gone(err: Option<io::Error>) -> Self {
        Control::Stop(CtlReason::PeerGone(PeerGone(err)))
    }

    #[inline]
    /// Initiate clean disconnect
    pub fn disconnect(&self) -> ControlAck {
        ControlAck { result: ControlAckKind::Disconnect }
    }

    /// Ack control message
    pub fn ack(self) -> ControlAck {
        match self {
            Control::Protocol(CtlFrame::Publish(msg)) => msg.ack(),
            Control::Protocol(CtlFrame::PublishRelease(msg)) => msg.ack(),
            Control::Stop(CtlReason::Error(msg)) => msg.ack(),
            Control::Stop(CtlReason::ProtocolError(msg)) => msg.ack(),
            Control::Stop(CtlReason::PeerGone(msg)) => msg.ack(),
            Control::Shutdown(msg) => msg.ack(),
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
    pub fn ack(self) -> ControlAck {
        if let Some(id) = self.0.packet_id {
            ControlAck { result: ControlAckKind::PublishAck(id) }
        } else {
            ControlAck { result: ControlAckKind::Nothing }
        }
    }

    #[inline]
    pub fn into_inner(self) -> (ControlAck, codec::Publish) {
        if let Some(id) = self.0.packet_id {
            (ControlAck { result: ControlAckKind::PublishAck(id) }, self.0)
        } else {
            (ControlAck { result: ControlAckKind::Nothing }, self.0)
        }
    }
}
