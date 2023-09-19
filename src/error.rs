use std::{fmt, io, num::NonZeroU16};

use ntex::util::Either;

use crate::v5::codec::DisconnectReasonCode;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug, thiserror::Error)]
pub enum MqttError<E> {
    /// Publish handler service error
    #[error("Service error")]
    Service(E),
    /// Handshake error
    #[error("Mqtt handshake error: {}", _0)]
    Handshake(#[from] HandshakeError<E>),
}

/// Errors which can occur during mqtt connection handshake.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError<E> {
    /// Handshake service error
    #[error("Handshake service error")]
    Service(E),
    /// Protocol error
    #[error("Mqtt protocol error: {}", _0)]
    Protocol(#[from] ProtocolError),
    /// Handshake timeout
    #[error("Handshake timeout")]
    Timeout,
    /// Peer disconnect
    #[error("Peer is disconnected, error: {:?}", _0)]
    Disconnected(Option<io::Error>),
    /// Server error
    #[error("Server error: {}", _0)]
    Server(&'static str),
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
    /// Peer violated MQTT protocol specification
    #[error("Protocol violation: {0}")]
    ProtocolViolation(#[from] ProtocolViolationError),
    /// Keep alive timeout
    #[error("Keep Alive timeout")]
    KeepAliveTimeout,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ProtocolViolationError {
    inner: ViolationInner,
}

#[derive(Debug, thiserror::Error)]
enum ViolationInner {
    #[error("{message}")]
    Common { reason: DisconnectReasonCode, message: &'static str },
    #[error("{message}; received packet with type `{packet_type:b}`")]
    UnexpectedPacket { packet_type: u8, message: &'static str },
}

impl ProtocolViolationError {
    pub(crate) fn reason(&self) -> DisconnectReasonCode {
        match self.inner {
            ViolationInner::Common { reason, .. } => reason,
            ViolationInner::UnexpectedPacket { .. } => DisconnectReasonCode::ProtocolError,
        }
    }
}

impl ProtocolError {
    pub(crate) fn violation(reason: DisconnectReasonCode, message: &'static str) -> Self {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::Common { reason, message },
        })
    }
    pub(crate) fn generic_violation(message: &'static str) -> Self {
        Self::violation(DisconnectReasonCode::ProtocolError, message)
    }

    pub(crate) fn unexpected_packet(packet_type: u8, message: &'static str) -> ProtocolError {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::UnexpectedPacket { packet_type, message },
        })
    }
    pub(crate) fn packet_id_mismatch() -> Self {
        Self::generic_violation(
            "Packet id of PUBACK packet does not match expected next value according to sending order of PUBLISH packets [MQTT-4.6.0-2]"
        )
    }
}

impl<E> From<io::Error> for MqttError<E> {
    fn from(err: io::Error) -> Self {
        MqttError::Handshake(HandshakeError::Disconnected(Some(err)))
    }
}

impl<E> From<Either<io::Error, io::Error>> for MqttError<E> {
    fn from(err: Either<io::Error, io::Error>) -> Self {
        MqttError::Handshake(HandshakeError::Disconnected(Some(err.into_inner())))
    }
}

impl<E> From<Either<DecodeError, io::Error>> for HandshakeError<E> {
    fn from(err: Either<DecodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => HandshakeError::Protocol(ProtocolError::Decode(err)),
            Either::Right(err) => HandshakeError::Disconnected(Some(err)),
        }
    }
}

impl<E> From<Either<EncodeError, io::Error>> for MqttError<E> {
    fn from(err: Either<EncodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => {
                MqttError::Handshake(HandshakeError::Protocol(ProtocolError::Encode(err)))
            }
            Either::Right(err) => MqttError::Handshake(HandshakeError::Disconnected(Some(err))),
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
    #[error("Packet is bigger than peer's Maximum Packet Size")]
    OverMaxPacketSize,
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
