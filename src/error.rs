use derive_more::From;
use either::Either;
use std::io;

use super::framed::DispatcherError;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug)]
pub enum MqttError<E> {
    /// Publish handler service error
    Service(E),
    /// Protocol error
    Protocol(ProtocolError),
    /// Handshake timeout
    HandshakeTimeout,
    /// Peer disconnect
    Disconnected,
    /// Protocol specific unhandled error (for v3.1.1 only)
    V3ProtocolError,
}

/// Protocol level errors
#[derive(Debug)]
pub enum ProtocolError {
    /// Mqtt parse error
    Decode(DecodeError),
    /// Mqtt encode error
    Encode(EncodeError),
    /// Unexpected packet
    Unexpected(u8, &'static str),
    /// Packet id of publish ack packet does not match of send publish packet
    PacketIdMismatch,
    /// Topic alias is greater than max topic alias
    MaxTopicAlias,
    /// Number of in-flight messages exceeded
    ReceiveMaximumExceeded,
    /// Unknown topic alias
    UnknownTopicAlias,
    /// Keep alive timeout
    KeepAliveTimeout,
    /// Unexpected io error
    Io(io::Error),
}

impl<E> From<Either<E, ProtocolError>> for MqttError<E> {
    fn from(err: Either<E, ProtocolError>) -> Self {
        match err {
            Either::Left(e) => MqttError::Service(e),
            Either::Right(e) => MqttError::Protocol(e),
        }
    }
}

impl<E> From<Either<DecodeError, io::Error>> for MqttError<E> {
    fn from(err: Either<DecodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => MqttError::Protocol(ProtocolError::Decode(err)),
            Either::Right(err) => MqttError::Protocol(ProtocolError::Io(err)),
        }
    }
}

impl<E> From<Either<EncodeError, io::Error>> for MqttError<E> {
    fn from(err: Either<EncodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => MqttError::Protocol(ProtocolError::Encode(err)),
            Either::Right(err) => MqttError::Protocol(ProtocolError::Io(err)),
        }
    }
}

impl From<DispatcherError<crate::v3::codec::Codec>> for ProtocolError {
    fn from(err: DispatcherError<crate::v3::codec::Codec>) -> Self {
        match err {
            DispatcherError::KeepAlive => ProtocolError::KeepAliveTimeout,
            DispatcherError::Encoder(err) => ProtocolError::Encode(err),
            DispatcherError::Decoder(err) => ProtocolError::Decode(err),
            DispatcherError::Io(err) => ProtocolError::Io(err),
        }
    }
}

impl From<DispatcherError<crate::v5::codec::Codec>> for ProtocolError {
    fn from(err: DispatcherError<crate::v5::codec::Codec>) -> Self {
        match err {
            DispatcherError::KeepAlive => ProtocolError::KeepAliveTimeout,
            DispatcherError::Encoder(err) => ProtocolError::Encode(err),
            DispatcherError::Decoder(err) => ProtocolError::Decode(err),
            DispatcherError::Io(err) => ProtocolError::Io(err),
        }
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
    Utf8Error(std::str::Utf8Error),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EncodeError {
    InvalidLength,
    MalformedPacket,
    PacketIdRequired,
    UnsupportedVersion,
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
            (DecodeError::Utf8Error(_), _) => false,
            _ => false,
        }
    }
}
