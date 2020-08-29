use derive_more::{Display, From};
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

/// Errors which can occur when attempting to handle mqtt client connection.
#[derive(Debug, Display, From)]
pub enum ClientError {
    /// Connect negotiation failed
    #[display(fmt = "Connect ack failed: {:?}", _0)]
    Ack(super::v5::codec::ConnectAck),
    /// Protocol error
    #[display(fmt = "Protocol error: {:?}", _0)]
    Protocol(ProtocolError),
    /// Handshake timeout
    #[display(fmt = "Handshake timeout")]
    HandshakeTimeout,
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
    /// Connect error
    #[display(fmt = "Connect error: {}", _0)]
    Connect(ntex::connect::ConnectError),
}

impl std::error::Error for ClientError {}

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
    /// Unexpected io error
    #[display(fmt = "Unexpected io error: {}", _0)]
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

impl From<Either<EncodeError, io::Error>> for ClientError {
    fn from(err: Either<EncodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(ProtocolError::Encode(err)),
            Either::Right(err) => ClientError::Protocol(ProtocolError::Io(err)),
        }
    }
}

impl From<Either<DecodeError, io::Error>> for ProtocolError {
    fn from(err: Either<DecodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => ProtocolError::Decode(err),
            Either::Right(err) => ProtocolError::Io(err),
        }
    }
}

impl From<DispatcherError<crate::v3::codec::Codec>> for ProtocolError {
    fn from(err: DispatcherError<crate::v3::codec::Codec>) -> Self {
        match err {
            DispatcherError::KeepAlive => ProtocolError::KeepAliveTimeout,
            DispatcherError::Decoder(err) => ProtocolError::Decode(err),
            DispatcherError::Encoder(_, err) => ProtocolError::Encode(err),
            DispatcherError::EncoderWritten(_) => panic!("Internal error"),
            DispatcherError::Io(err) => ProtocolError::Io(err),
        }
    }
}

impl From<DispatcherError<crate::v5::codec::Codec>> for ProtocolError {
    fn from(err: DispatcherError<crate::v5::codec::Codec>) -> Self {
        match err {
            DispatcherError::KeepAlive => ProtocolError::KeepAliveTimeout,
            DispatcherError::Decoder(err) => ProtocolError::Decode(err),
            DispatcherError::Encoder(_, err) => ProtocolError::Encode(err),
            DispatcherError::EncoderWritten(_) => panic!("Internal error"),
            DispatcherError::Io(err) => ProtocolError::Io(err),
        }
    }
}

#[derive(Debug, Display, From)]
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

#[derive(Copy, Clone, Debug, Display, PartialEq, Eq, Hash)]
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
