use std::{error, fmt, io};

use derive_more::{Display, From};
use ntex::util::Either;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug, Display)]
pub enum MqttError<E> {
    /// Publish handler service error
    #[display(fmt = "Service error")]
    Service(E),
    /// Protocol error
    #[display(fmt = "Mqtt protocol error: {}", _0)]
    Protocol(ProtocolError),
    /// Handshake timeout
    HandshakeTimeout,
    /// Peer disconnect
    #[display(fmt = "Peer is disconnected, error: {:?}", _0)]
    Disconnected(Option<io::Error>),
    /// Server error
    #[display(fmt = "Server error: {}", _0)]
    ServerError(&'static str),
}

impl<E: fmt::Debug> error::Error for MqttError<E> {}

/// Protocol level errors
#[derive(Debug, Display, From)]
pub enum ProtocolError {
    /// Mqtt parse error
    #[display(fmt = "Decode error: {:?}", _0)]
    Decode(DecodeError),
    /// Mqtt encode error
    #[display(fmt = "Encode error: {:?}", _0)]
    Encode(EncodeError),
    /// Unexpected packet
    #[display(fmt = "Unexpected packet {:?}, {}", _0, _1)]
    Unexpected(u8, &'static str),
    /// Packet id of publish ack packet does not match of send publish packet
    #[display(fmt = "Packet id of publish ack packet does not match of send publish packet")]
    PacketIdMismatch,
    /// Topic alias is greater than max topic alias
    #[display(fmt = "Topic alias is greater than max topic alias")]
    MaxTopicAlias,
    /// Number of in-flight messages exceeded
    #[display(fmt = "Number of in-flight messages exceeded")]
    ReceiveMaximumExceeded,
    /// Unknown topic alias
    #[display(fmt = "Unknown topic alias")]
    UnknownTopicAlias,
    /// Keep alive timeout
    #[display(fmt = "Keep alive timeout")]
    KeepAliveTimeout,
}

impl error::Error for ProtocolError {}

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

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash)]
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
    Utf8Error,
}

impl error::Error for DecodeError {}

#[derive(Copy, Clone, Debug, Display, PartialEq, Eq, Hash)]
pub enum EncodeError {
    InvalidLength,
    MalformedPacket,
    PacketIdRequired,
    UnsupportedVersion,
}

impl error::Error for EncodeError {}

#[derive(Debug, Display, PartialEq, Eq, Copy, Clone)]
pub enum SendPacketError {
    /// Encoder error
    Encode(EncodeError),
    /// Provided packet id is in use
    #[display(fmt = "Provided packet id is in use")]
    PacketIdInUse(u16),
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
}

impl error::Error for SendPacketError {}
