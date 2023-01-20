use std::{fmt, io, num::NonZeroU16};

use ntex::util::Either;

use crate::v5::codec::DisconnectReasonCode;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug, thiserror::Error)]
pub enum MqttError<E> {
    /// Publish handler service error
    #[error("Service error")]
    Service(E),
    /// Protocol error
    #[error("Mqtt protocol error: {}", _0)]
    Protocol(#[from] ProtocolError),
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
    /// MQTT decoding error
    #[error("Decoding error: {0:?}")]
    Decode(#[from] DecodeError),
    /// MQTT encoding error
    #[error("Encoding error: {0:?}")]
    Encode(#[from] EncodeError),
    // /// Packet id of publish ack packet does not match of send publish packet
    // #[error("Packet id of publish ack packet does not match of send publish packet")]
    // PacketIdMismatch,
    /// Peer violated MQTT protocol specification
    #[error("Protocol violation: {0}")]
    ProtocolViolation(#[from] ProtocolViolationError),
    /// Keep alive timeout
    #[error("Keep Alive timeout")]
    KeepAliveTimeout,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolViolationError {
    #[error("{message}")]
    Custom { reason: DisconnectReasonCode, message: &'static str },
    #[error("{message}; received packet with type `{packet_type:b}`")]
    UnexpectedPacket { packet_type: u8, message: &'static str },
}
impl ProtocolViolationError {
    pub(crate) fn impl_specific(message: &'static str) -> Self {
        ProtocolViolationError::Custom {
            reason: DisconnectReasonCode::ImplementationSpecificError,
            message,
        }
    }
    pub(crate) fn generic(message: &'static str) -> Self {
        Self::Custom { reason: DisconnectReasonCode::ProtocolError, message }
    }
    pub(crate) fn new(reason: DisconnectReasonCode, message: &'static str) -> Self {
        Self::Custom { reason, message }
    }

    pub(crate) fn reason(&self) -> DisconnectReasonCode {
        match self {
            ProtocolViolationError::Custom { reason, .. } => *reason,
            ProtocolViolationError::UnexpectedPacket { .. } => {
                DisconnectReasonCode::ProtocolError
            }
        }
    }
}

impl ProtocolError {
    pub(crate) fn unexpected_packet(packet_type: u8, message: &'static str) -> ProtocolError {
        Self::ProtocolViolation(ProtocolViolationError::UnexpectedPacket {
            packet_type,
            message,
        })
    }
    pub(crate) fn packet_id_mismatch() -> Self {
        Self::ProtocolViolation(ProtocolViolationError::generic(
            "Packet id of PUBACK packet does not match that of send publish packet [MQTT-4.6.0-2]"
        ))
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
    PacketIdInUse(NonZeroU16),
    /// Peer disconnected
    #[error("Peer is disconnected")]
    Disconnected,
}

/// Errors which can occur when attempting to handle mqtt client connection.
#[derive(Debug, thiserror::Error)]
pub enum ClientError<T: fmt::Debug> {
    /// Connect negotiation failed
    #[error("Connect ack failed: {:?}", _0)]
    Ack(T),
    /// Protocol error
    #[error("Protocol error: {:?}", _0)]
    Protocol(#[from] ProtocolError),
    /// Handshake timeout
    #[error("Handshake timeout")]
    HandshakeTimeout,
    /// Peer disconnected
    #[error("Peer disconnected")]
    Disconnected(Option<std::io::Error>),
    /// Connect error
    #[error("Connect error: {}", _0)]
    Connect(#[from] ntex::connect::ConnectError),
}

impl<T: fmt::Debug> From<Either<EncodeError, std::io::Error>> for ClientError<T> {
    fn from(err: Either<EncodeError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(ProtocolError::Encode(err)),
            Either::Right(err) => ClientError::Disconnected(Some(err)),
        }
    }
}

impl<T: fmt::Debug> From<Either<DecodeError, std::io::Error>> for ClientError<T> {
    fn from(err: Either<DecodeError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(ProtocolError::Decode(err)),
            Either::Right(err) => ClientError::Disconnected(Some(err)),
        }
    }
}
