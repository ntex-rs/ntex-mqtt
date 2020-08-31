pub use crate::v3::control::{Closed, ControlResult, Disconnect};
use crate::v3::{codec, control::ControlResultKind};

pub enum ControlMessage {
    /// Unhandled publish packet
    Publish(Publish),
    /// Disconnect packet
    Disconnect(Disconnect),
    /// Connection closed
    Closed(Closed),
}

impl ControlMessage {
    pub(super) fn publish(pkt: codec::Publish) -> Self {
        ControlMessage::Publish(Publish(pkt))
    }

    pub(super) fn dis() -> Self {
        ControlMessage::Disconnect(Disconnect)
    }

    pub(super) fn closed(is_error: bool) -> Self {
        ControlMessage::Closed(Closed::new(is_error))
    }

    pub fn disconnect(&self) -> ControlResult {
        ControlResult { result: ControlResultKind::Disconnect }
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

    pub fn ack(self) -> ControlResult {
        if let Some(id) = self.0.packet_id {
            ControlResult { result: ControlResultKind::PublishAck(id) }
        } else {
            ControlResult { result: ControlResultKind::Nothing }
        }
    }
}
