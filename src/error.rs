use std::io;

use ntex::util::Either;

use crate::types::QoS;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug, thiserror::Error)]
pub enum MqttError<E> {
    /// Publish handler service error
    #[error("Service error")]
    Service(E),
    /// Protocol error
    #[error("Mqtt protocol error: {}", _0)]
    Protocol(ProtocolError),
    /// Handshake timeout
    #[error("Handshake timeout")]
    HandshakeTimeout,
    /// Peer disconnect
    #[error("Peer is disconnected, error: {:?}", _0)]
    Disconnected(Option<io::Error>),
    /// Server error
    #[error("Server error: {}", _0)]
    ServerError(&'static str),
}

/// Protocol level errors
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// Mqtt parse error
    #[error("Decode error: {:?}", _0)]
    Decode(#[from] DecodeError),
    /// Mqtt encode error
    #[error("Encode error: {:?}", _0)]
    Encode(#[from] EncodeError),
    /// Unexpected packet
    #[error("Unexpected packet {:?}, {}", _0, _1)]
    Unexpected(u8, &'static str),
    /// Packet id of publish ack packet does not match of send publish packet
    #[error("Packet id of publish ack packet does not match of send publish packet")]
    PacketIdMismatch,
    /// Peer sent publish with higher qos than configured
    #[error("Max allowed QoS level is violated {:?}", _0)]
    MaxQoSViolated(QoS),
    /// Topic alias is greater than max topic alias
    #[error("Topic alias is greater than max topic alias")]
    MaxTopicAlias,
    /// Number of in-flight messages exceeded
    #[error("Number of in-flight messages exceeded")]
    ReceiveMaximumExceeded,
    /// Unknown topic alias
    #[error("Unknown topic alias")]
    UnknownTopicAlias,
    /// Keep alive timeout
    #[error("Keep alive timeout")]
    KeepAliveTimeout,
}

impl<E> From<ProtocolError> for MqttError<E> {
    fn from(err: ProtocolError) -> Self {
        MqttError::Protocol(err)
    }
}

impl<E> From<io::Error> for MqttError<E> {
    fn from(err: io::Error) -> Self {
        MqttError::Disconnected(Some(err))
    }
}

impl<E> From<Either<io::Error, io::Error>> for MqttError<E> {
    fn from(err: Either<io::Error, io::Error>) -> Self {
        MqttError::Disconnected(Some(err.into_inner()))
    }
}

impl<E> From<Either<DecodeError, io::Error>> for MqttError<E> {
    fn from(err: Either<DecodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => MqttError::Protocol(ProtocolError::Decode(err)),
            Either::Right(err) => MqttError::Disconnected(Some(err)),
        }
    }
}

impl<E> From<Either<EncodeError, io::Error>> for MqttError<E> {
    fn from(err: Either<EncodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => MqttError::Protocol(ProtocolError::Encode(err)),
            Either::Right(err) => MqttError::Disconnected(Some(err)),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum DecodeError {
    #[error("Invalid protocol")]
    InvalidProtocol,
    #[error("Invalid length")]
    InvalidLength,
    #[error("Malformed packet")]
    MalformedPacket,
    #[error("Unsupported protocol level")]
    UnsupportedProtocolLevel,
    #[error("Connect frame's reserved flag is set")]
    ConnectReservedFlagSet,
    #[error("ConnectAck frame's reserved flag is set")]
    ConnAckReservedFlagSet,
    #[error("Invalid client id")]
    InvalidClientId,
    #[error("Unsupported packet type")]
    UnsupportedPacketType,
    // MQTT v3 only
    #[error("Packet id is required")]
    PacketIdRequired,
    #[error("Max size exceeded")]
    MaxSizeExceeded,
    #[error("utf8 error")]
    Utf8Error,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum EncodeError {
    #[error("Invalid length")]
    InvalidLength,
    #[error("Malformed packet")]
    MalformedPacket,
    #[error("Packet id is required")]
    PacketIdRequired,
    #[error("Unsupported version")]
    UnsupportedVersion,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, thiserror::Error)]
pub enum SendPacketError {
    /// Encoder error
    #[error("Encoding error {:?}", _0)]
    Encode(#[from] EncodeError),
    /// Provided packet id is in use
    #[error("Provided packet id is in use")]
    PacketIdInUse(u16),
    /// Peer disconnected
    #[error("Peer is disconnected")]
    Disconnected,
}
