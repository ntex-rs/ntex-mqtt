use std::io;

use ntex_bytes::{ByteString, Bytes};

use crate::{error, payload::Payload, v5::codec};

pub use crate::v5::control::{
    ControlAck, CtlReason, Disconnect, Error, PeerGone, ProtocolError, PublishRelease, Shutdown,
};

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
    /// Disconnect packet from a client
    Disconnect(Disconnect),
}

impl<E> Control<E> {
    pub(super) fn publish(pkt: codec::Publish, pl: Payload, size: u32) -> Self {
        Control::Protocol(CtlFrame::Publish(Publish(pkt, pl, size)))
    }

    pub(super) fn pubrel(pkt: codec::PublishAck2, size: u32) -> Self {
        Control::Protocol(CtlFrame::PublishRelease(PublishRelease::new(pkt, size)))
    }

    pub(super) fn dis(pkt: codec::Disconnect, size: u32) -> Self {
        Control::Protocol(CtlFrame::Disconnect(Disconnect(pkt, size)))
    }

    pub(super) const fn shutdown() -> Self {
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

    pub fn disconnect(&self, pkt: codec::Disconnect) -> ControlAck {
        ControlAck { packet: Some(codec::Packet::Disconnect(pkt)), disconnect: true }
    }

    /// Ack control message
    pub fn ack(self) -> ControlAck {
        match self {
            Control::Protocol(CtlFrame::Publish(_)) => {
                crate::v5::disconnect(error::ERR_PUB_NOT_SUP)
            }
            Control::Protocol(CtlFrame::PublishRelease(msg)) => msg.ack(),
            Control::Protocol(CtlFrame::Disconnect(msg)) => msg.ack(),
            Control::Shutdown(msg) => msg.ack(),
            Control::Stop(CtlReason::Error(_)) => crate::v5::disconnect(error::ERR_CTL_NOT_SUP),
            Control::Stop(CtlReason::ProtocolError(msg)) => msg.ack(),
            Control::Stop(CtlReason::PeerGone(msg)) => msg.ack(),
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
    pub fn ack_qos0(self) -> ControlAck {
        ControlAck { packet: None, disconnect: false }
    }

    #[inline]
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

    #[inline]
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
