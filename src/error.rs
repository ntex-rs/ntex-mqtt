use std::{fmt, io, num::NonZeroU16};

use ntex_util::future::Either;

use crate::v5::codec::DisconnectReasonCode;

pub(crate) const ERR_PUB_NOT_SUP: &str = "Publish control message is not supported";
pub(crate) const ERR_AUTH_NOT_SUP: &str = "Auth control message is not supported";

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

/// Errors related to protocol dispatcher
#[derive(Debug, thiserror::Error)]
pub enum DispatcherError<E> {
    /// Publish handler service error
    #[error("Service error")]
    Service(E),
    /// Protocol violations error
    #[error("Protocol violations error: {}", _0)]
    Protocol(#[from] ProtocolError),
}

impl<E> From<SpecViolation> for DispatcherError<E> {
    fn from(spec: SpecViolation) -> Self {
        DispatcherError::Protocol(ProtocolError::spec(spec))
    }
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
    pub(crate) inner: ViolationInner,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ViolationInner {
    #[error("{0}")]
    Spec(SpecViolation),
    #[error("{message}")]
    Common { reason: DisconnectReasonCode, message: &'static str },
    #[error("{message}; received packet with type `{packet_type:b}`")]
    UnexpectedPacket { packet_type: u8, message: &'static str },
}

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpecViolation {
    #[error("[MQTT-2.2.1-3] PUBLISH received with packet id that is already in use")]
    PacketId_2_2_1_3_Pub,
    #[error("[MQTT-2.2.1-3] SUBSCRIBE received with packet id that is already in use")]
    PacketId_2_2_1_3_Sub,
    #[error("[MQTT-2.2.1-3] UNSUBSCRIBE received with packet id that is already in use")]
    PacketId_2_2_1_3_Unsub,
    #[error("[MQTT-3.1.2-26] Topic alias is greater than max allowed")]
    Connect_3_1_2_26,
    #[error(
        "[MQTT-3.2.2-11] PUBLISH packet at a QoS level exceeding the Maximum QoS level specified in CONNACK"
    )]
    Connack_3_2_2_11,
    #[error("[MQTT-3.2.2-14] RETAIN is not supported")]
    Connack_3_2_2_14,
    #[error("[MQTT-3.2.2-17] Topic alias is greater than max allowed")]
    Connack_3_2_2_17,
    #[error("[MQTT-3.2.2-3.12] Subscription Identifiers are not supported")]
    Connack_3_2_2_3_12,
    #[error("[MQTT-3.3.2-2] PUBLISH packet's topic name contains wildcard character")]
    Pub_3_3_2_2,
    #[error("[MQTT-3.3.4-7] Number of in-flight messages exceeds set maximum")]
    Pub_3_3_4_7,
    #[error("[MQTT-3.3.4-9] Number of in-flight messages exceeds set maximum")]
    Pub_3_3_4_9,
    #[error("[MQTT-4.7.1-*] Topic filter is malformed")]
    Subs_4_7_1,
    #[error(
        "[MQTT-3.14.2-*] The Session Expiry Interval must not be set on DISCONNECT by Server"
    )]
    Disconnect_3_14_2_21,
    #[error("[MQTT-3.14.2-*] Non-Zero Session Expiry Interval is set on DISCONNECT")]
    Disconnect_3_14_2_22,
}

impl SpecViolation {
    const fn reason(self) -> DisconnectReasonCode {
        match self {
            SpecViolation::Pub_3_3_4_7 | SpecViolation::Pub_3_3_4_9 => {
                DisconnectReasonCode::ReceiveMaximumExceeded
            }
            SpecViolation::Connack_3_2_2_11 => DisconnectReasonCode::QosNotSupported,
            SpecViolation::Connack_3_2_2_14 => DisconnectReasonCode::RetainNotSupported,
            SpecViolation::Connack_3_2_2_3_12 => {
                DisconnectReasonCode::SubscriptionIdentifiersNotSupported
            }
            SpecViolation::PacketId_2_2_1_3_Pub
            | SpecViolation::PacketId_2_2_1_3_Sub
            | SpecViolation::PacketId_2_2_1_3_Unsub
            | SpecViolation::Connect_3_1_2_26
            | SpecViolation::Pub_3_3_2_2
            | SpecViolation::Subs_4_7_1
            | SpecViolation::Connack_3_2_2_17
            | SpecViolation::Disconnect_3_14_2_21
            | SpecViolation::Disconnect_3_14_2_22 => DisconnectReasonCode::ProtocolError,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            SpecViolation::PacketId_2_2_1_3_Pub => {
                "[MQTT-2.2.1-3] PUBLISH received with packet id that is already in use"
            }
            SpecViolation::PacketId_2_2_1_3_Sub => {
                "[MQTT-2.2.1-3] SUBSCRIBE received with packet id that is already in use"
            }
            SpecViolation::PacketId_2_2_1_3_Unsub => {
                "[MQTT-2.2.1-3] UNSUBSCRIBE received with packet id that is already in use"
            }
            SpecViolation::Connect_3_1_2_26 => {
                "[MQTT-3.1.2-26] Topic alias is greater than max allowed"
            }
            SpecViolation::Connack_3_2_2_11 => {
                "[MQTT-3.2.2-11] PUBLISH packet at a QoS level exceeding the Maximum QoS level specified in CONNACK"
            }
            SpecViolation::Connack_3_2_2_14 => "[MQTT-3.2.2-14] RETAIN is not supported",
            SpecViolation::Connack_3_2_2_17 => {
                "[MQTT-3.2.2-17] Topic alias is greater than max allowed"
            }
            SpecViolation::Connack_3_2_2_3_12 => {
                "[MQTT-3.2.2-3.12] Subscription Identifiers are not supported"
            }
            SpecViolation::Pub_3_3_2_2 => {
                "[MQTT-3.3.2-2] PUBLISH packet's topic name contains wildcard character"
            }
            SpecViolation::Pub_3_3_4_7 => {
                "[MQTT-3.3.4-7] Number of in-flight messages exceeds set maximum"
            }
            SpecViolation::Pub_3_3_4_9 => {
                "[MQTT-3.3.4-9] Number of in-flight messages exceeds set maximum"
            }
            SpecViolation::Subs_4_7_1 => "[MQTT-4.7.1-*] Topic filter is malformed",
            SpecViolation::Disconnect_3_14_2_21 => {
                "[MQTT-3.14.2-*] The Session Expiry Interval must not be set on DISCONNECT by Server"
            }
            SpecViolation::Disconnect_3_14_2_22 => {
                "[MQTT-3.14.2-*] Non-Zero Session Expiry Interval is set on DISCONNECT"
            }
        }
    }
}

impl ProtocolViolationError {
    /// Protocol violation reason code
    pub const fn reason(&self) -> DisconnectReasonCode {
        match self.inner {
            ViolationInner::Spec(err) => err.reason(),
            ViolationInner::Common { reason, .. } => reason,
            ViolationInner::UnexpectedPacket { .. } => DisconnectReasonCode::ProtocolError,
        }
    }

    /// Protocol violation reason message
    pub const fn message(&self) -> &'static str {
        match self.inner {
            ViolationInner::Common { message, .. }
            | ViolationInner::UnexpectedPacket { message, .. } => message,
            ViolationInner::Spec(err) => err.as_str(),
        }
    }
}

impl ProtocolError {
    pub(crate) fn violation(reason: DisconnectReasonCode, message: &'static str) -> Self {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::Common { reason, message },
        })
    }

    pub fn spec(err: SpecViolation) -> Self {
        Self::ProtocolViolation(ProtocolViolationError { inner: ViolationInner::Spec(err) })
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
