use derive_more::From;
use std::io;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug)]
pub enum MqttError<E> {
    /// Message handler service error
    Service(E),
    /// Mqtt parse error
    Decode(DecodeError),
    /// Mqtt encode error
    Encode(EncodeError),
    /// Unexpected packet
    Unexpected(crate::codec3::Packet, &'static str),
    /// "SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets MUST contain a non-zero 16-bit Packet Identifier [MQTT-2.3.1-1]."
    PacketIdRequired,
    /// Multiple in-flight publish packet with same package_id
    DuplicatedPacketId,
    /// Keep alive timeout
    KeepAliveTimeout,
    /// Handshake timeout
    HandshakeTimeout,
    /// Peer disconnect
    Disconnected,
    /// Unexpected io error
    Io(io::Error),
}

impl<E> From<DecodeError> for MqttError<E> {
    fn from(err: DecodeError) -> Self {
        MqttError::Decode(err)
    }
}

impl<E> From<EncodeError> for MqttError<E> {
    fn from(err: EncodeError) -> Self {
        MqttError::Encode(err)
    }
}

impl<E> From<io::Error> for MqttError<E> {
    fn from(err: io::Error) -> Self {
        MqttError::Io(err)
    }
}

#[derive(Debug, From)]
pub enum DecodeError {
    InvalidProtocol,
    InvalidLength,
    MalformedPacket,
    UnsupportedProtocolLevel,
    ConnectReservedFlagSet,
    ConnAckReservedFlagSet,
    InvalidClientId,
    UnsupportedPacketType,
    // MQTT v3 only
    PacketIdRequired,
    MaxSizeExceeded,
    IoError(io::Error),
    Utf8Error(std::str::Utf8Error),
}

#[derive(Debug, From)]
pub enum EncodeError {
    InvalidLength,
    MalformedPacket,
    PacketIdRequired,
    IoError(io::Error),
}

impl PartialEq for DecodeError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DecodeError::InvalidProtocol, DecodeError::InvalidProtocol) => true,
            (DecodeError::InvalidLength, DecodeError::InvalidLength) => true,
            (DecodeError::UnsupportedProtocolLevel, DecodeError::UnsupportedProtocolLevel) => {
                true
            }
            (DecodeError::ConnectReservedFlagSet, DecodeError::ConnectReservedFlagSet) => true,
            (DecodeError::ConnAckReservedFlagSet, DecodeError::ConnAckReservedFlagSet) => true,
            (DecodeError::InvalidClientId, DecodeError::InvalidClientId) => true,
            (DecodeError::UnsupportedPacketType, DecodeError::UnsupportedPacketType) => true,
            (DecodeError::PacketIdRequired, DecodeError::PacketIdRequired) => true,
            (DecodeError::MaxSizeExceeded, DecodeError::MaxSizeExceeded) => true,
            (DecodeError::MalformedPacket, DecodeError::MalformedPacket) => true,
            (DecodeError::IoError(_), _) => false,
            (DecodeError::Utf8Error(_), _) => false,
            _ => false,
        }
    }
}
