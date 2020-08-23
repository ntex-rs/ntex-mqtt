use super::sink::MqttSink;
use crate::{error, v5::codec};

pub enum ControlMessage {
    /// Unhandled publish packet
    Publish(Publish),
    /// Disconnect packet
    Disconnect(Disconnect),
    /// Protocol level error
    ProtocolError(ProtocolError),
    Closed,
}

impl ControlMessage {
    pub(super) fn protocol_error(err: error::ProtocolError, sink: &MqttSink) -> Self {
        ControlMessage::ProtocolError(ProtocolError { err, sink: sink.clone() })
    }
}

pub struct Publish {
    sink: MqttSink,
    pkt: codec::Publish,
}

impl Publish {
    /// Returns reference to publish packet
    pub fn packet(&self) -> &codec::Publish {
        &self.pkt
    }
}

pub struct Disconnect {
    sink: MqttSink,
    pkt: codec::Disconnect,
}

impl Disconnect {
    /// Returns reference to dusconnect packet
    pub fn packet(&self) -> &codec::Disconnect {
        &self.pkt
    }
}

pub struct ProtocolError {
    sink: MqttSink,
    err: error::ProtocolError,
}
