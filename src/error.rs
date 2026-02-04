use std::{fmt, io, num::NonZeroU16};

use ntex_util::future::Either;

use crate::v5::codec::DisconnectReasonCode;

pub(crate) const ERR_PUB_NOT_SUP: &str = "Publish control message is not supported";
pub(crate) const ERR_AUTH_NOT_SUP: &str = "Auth control message is not supported";
pub(crate) const ERR_CTL_NOT_SUP: &str = "Error control message is not supported";

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
}

/// Errors related to payload processing
#[derive(Copy, Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PayloadError {
    /// Protocol error
    #[error("{0}")]
    Protocol(#[from] ProtocolError),
    /// Service error
    #[error("Service error")]
    Service,
    /// Payload is consumed
    #[error("Payload is consumed")]
    Consumed,
    /// Peer is disconnected
    #[error("Peer is disconnected")]
    Disconnected,
}

/// Protocol level errors
#[derive(Debug, Copy, Clone, PartialEq, Eq, thiserror::Error)]
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
    /// Read frame timeout
    #[error("Read frame timeout")]
    ReadTimeout,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, thiserror::Error)]
#[error(transparent)]
pub struct ProtocolViolationError {
    inner: ViolationInner,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, thiserror::Error)]
enum ViolationInner {
    #[error("{message}")]
    Common { reason: DisconnectReasonCode, message: &'static str },
    #[error("{message}; received packet with type `{packet_type:b}`")]
    UnexpectedPacket { packet_type: u8, message: &'static str },
}

impl ProtocolViolationError {
    /// Protocol violation reason code
    pub fn reason(&self) -> DisconnectReasonCode {
        match self.inner {
            ViolationInner::Common { reason, .. } => reason,
            ViolationInner::UnexpectedPacket { .. } => DisconnectReasonCode::ProtocolError,
        }
    }

    /// Protocol violation reason message
    pub fn message(&self) -> &'static str {
        match self.inner {
            ViolationInner::Common { message, .. }
            | ViolationInner::UnexpectedPacket { message, .. } => message,
        }
    }
}

impl ProtocolError {
    pub(crate) fn violation(reason: DisconnectReasonCode, message: &'static str) -> Self {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::Common { reason, message },
        })
    }
    pub fn generic_violation(message: &'static str) -> Self {
        Self::violation(DisconnectReasonCode::ProtocolError, message)
    }

    pub(crate) fn unexpected_packet(packet_type: u8, message: &'static str) -> ProtocolError {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::UnexpectedPacket { packet_type, message },
        })
    }
    pub(crate) fn packet_id_mismatch() -> Self {
        Self::generic_violation(
            "Packet id of PUBACK packet does not match expected next value according to sending order of PUBLISH packets [MQTT-4.6.0-2]",
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

impl<E> From<EncodeError> for MqttError<E> {
    fn from(err: EncodeError) -> Self {
        MqttError::Handshake(HandshakeError::Protocol(ProtocolError::Encode(err)))
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
    #[error("Unexpected payload")]
    UnexpectedPayload,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum EncodeError {
    #[error("Packet is bigger than peer's Maximum Packet Size")]
    OverMaxPacketSize,
    #[error("Streaming payload is bigger than Publish packet definition")]
    OverPublishSize,
    #[error("Streaming payload is incomplete")]
    PublishIncomplete,
    #[error("Invalid length")]
    InvalidLength,
    #[error("Malformed packet")]
    MalformedPacket,
    #[error("Packet id is required")]
    PacketIdRequired,
    #[error("Unexpected payload")]
    UnexpectedPayload,
    #[error("Publish packet is not completed, expect payload")]
    ExpectPayload,
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
    /// Unexpected release publish
    #[error("Unexpected publish release")]
    UnexpectedRelease,
    /// Streaming has been cancelled
    #[error("Streaming has been cancelled")]
    StreamingCancelled,
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
    Connect(#[from] ntex_net::connect::ConnectError),
}

impl<T: fmt::Debug> From<EncodeError> for ClientError<T> {
    fn from(err: EncodeError) -> Self {
        ClientError::Protocol(ProtocolError::Encode(err))
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
