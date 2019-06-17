use std::io;

use mqtt_codec;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug)]
pub enum MqttError<E> {
    Service(E),
    Protocol(mqtt_codec::ParseError),
    UnexpectedPacket(mqtt_codec::Packet),
    Disconnected,
    /// "SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets MUST contain a non-zero 16-bit Packet Identifier [MQTT-2.3.1-1]."
    PacketIdRequired,
    KeepAliveTimeout,
    ConstructError,
    InternalError,
    Io(io::Error),
}

impl<E> From<mqtt_codec::ParseError> for MqttError<E> {
    fn from(err: mqtt_codec::ParseError) -> Self {
        MqttError::Protocol(err)
    }
}

impl<E> From<mqtt_codec::Packet> for MqttError<E> {
    fn from(err: mqtt_codec::Packet) -> Self {
        MqttError::UnexpectedPacket(err)
    }
}

impl<E> From<io::Error> for MqttError<E> {
    fn from(err: io::Error) -> Self {
        MqttError::Io(err)
    }
}
