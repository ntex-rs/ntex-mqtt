use std::{fmt, io, num::NonZeroU16};

use ntex_error::Failure;
use ntex_util::future::Either;

use crate::v5::codec::DisconnectReasonCode;

pub(crate) const ERR_PUB_NOT_SUP: &str = "Publish control message is not supported";
pub(crate) const ERR_AUTH_NOT_SUP: &str = "Auth control message is not supported";

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug, thiserror::Error)]
pub enum MqttError<E> {
    /// Application service error (handshake, publish or control service)
    #[error("Service error")]
    Service(E),
    /// Connect error
    #[error("Mqtt connect error: {}", _0)]
    Connect(
        #[from]
        #[source]
        MqttConnectError<E>,
    ),
    /// Handler initialization error
    #[error("Mqtt handler initialization error: {}", _0)]
    HandlerInit(
        #[from]
        #[source]
        Failure,
    ),
}

/// Errors which can occur during mqtt connection handshake.
#[derive(Debug, thiserror::Error)]
pub enum MqttConnectError<E> {
    /// Handshake service error
    #[error("Connect service error")]
    Service(E),
    /// Protocol error
    #[error("Mqtt protocol error: {}", _0)]
    Protocol(#[from] MqttProtocolError),
    /// Connect timeout
    #[error("Connect timeout")]
    Timeout,
    /// Peer disconnect
    #[error("Peer is disconnected, error: {:?}", _0)]
    Disconnected(Option<io::Error>),
}

/// Errors related to protocol dispatcher
#[derive(Debug, thiserror::Error)]
pub enum DispatcherError<E> {
    /// Application service error (publish or control service)
    #[error("Service error")]
    Service(E),
    /// Protocol violations error
    #[error("Protocol violations error: {}", _0)]
    Protocol(#[from] MqttProtocolError),
}

impl<E> From<SpecViolation> for DispatcherError<E> {
    fn from(spec: SpecViolation) -> Self {
        DispatcherError::Protocol(MqttProtocolError::spec(spec))
    }
}

/// Errors related to payload processing
#[derive(Copy, Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PayloadError {
    /// Protocol error
    #[error("{0}")]
    Protocol(#[from] MqttProtocolError),
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
pub enum MqttProtocolError {
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
    /// Write backpressure timeout
    #[error("Write timeout")]
    WriteTimeout,
}

/// Protocol violation error
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
    Common {
        reason: DisconnectReasonCode,
        message: &'static str,
    },
    #[error("{message}; received packet with type `{packet_type:08b}`")]
    UnexpectedPacket {
        packet_type: u8,
        message: &'static str,
    },
}

/// Mqtt specification violations
#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpecViolation {
    /// PUBLISH is received with a packet id that is already in use
    #[error("[MQTT-2.2.1-3] PUBLISH received with packet id that is already in use")]
    PacketId_2_2_1_3_Pub,
    /// SUBSCRIBE is received with a packet id that is already in use
    #[error("[MQTT-2.2.1-3] SUBSCRIBE received with packet id that is already in use")]
    PacketId_2_2_1_3_Sub,
    /// UNSUBSCRIBE is received with a packet id that is already in use
    #[error("[MQTT-2.2.1-3] UNSUBSCRIBE received with packet id that is already in use")]
    PacketId_2_2_1_3_Unsub,
    /// Topic alias is greater than the Topic Alias Maximum sent in CONNECT
    #[error("[MQTT-3.1.2-26] Topic alias is greater than max allowed")]
    Connect_3_1_2_26,
    /// PUBLISH is received with a `QoS` greater than the Maximum `QoS` sent in CONNACK
    #[error(
        "[MQTT-3.2.2-11] PUBLISH packet at a QoS level exceeding the Maximum QoS level specified in CONNACK"
    )]
    Connack_3_2_2_11,
    /// PUBLISH is received with the RETAIN flag set while retain is not supported
    #[error("[MQTT-3.2.2-14] RETAIN is not supported")]
    Connack_3_2_2_14,
    /// Topic alias is greater than the Topic Alias Maximum sent in CONNACK
    #[error("[MQTT-3.2.2-17] Topic alias is greater than max allowed")]
    Connack_3_2_2_17,
    /// Subscription Identifier is used while it is not supported
    #[error("[MQTT-3.2.2-3.12] Subscription Identifiers are not supported")]
    Connack_3_2_2_3_12,
    /// PUBLISH topic name contains a wildcard character
    #[error("[MQTT-3.3.2-2] PUBLISH packet's topic name contains wildcard character")]
    Pub_3_3_2_2,
    /// PUBLISH Response Topic contains a wildcard character
    #[error("[MQTT-3.3.2-14] PUBLISH packet's Response Topic contains wildcard character")]
    Pub_3_3_2_14,
    /// PUBLISH sent by a client contains a Subscription Identifier
    #[error("[MQTT-3.3.4-6] PUBLISH packet sent by Client contains a Subscription Identifier")]
    Pub_3_3_4_6,
    /// Number of in-flight messages received exceeds the Receive Maximum sent by the server
    #[error("[MQTT-3.3.4-7] Number of in-flight messages exceeds set maximum")]
    Pub_3_3_4_7,
    /// Number of in-flight messages received exceeds the Receive Maximum sent by the client
    #[error("[MQTT-3.3.4-9] Number of in-flight messages exceeds set maximum")]
    Pub_3_3_4_9,
    /// Subscription topic filter is malformed
    #[error("[MQTT-4.7.1-*] Topic filter is malformed")]
    Subs_4_7_1,
    /// Shared subscription topic filter is malformed
    #[error("[MQTT-4.8.2-*] Shared Subscription Topic Filter is malformed")]
    Subs_4_8_2,
    /// No Local option is set on a shared subscription
    #[error("[MQTT-3.8.3-4] No Local is set on a Shared Subscription")]
    Subs_3_8_3_4,
    /// CONNECT Will Topic is empty
    #[error("[MQTT-4.7.3-1] CONNECT packet's Will Topic is empty")]
    Will_4_7_3_1,
    /// CONNECT Will Topic contains a wildcard character
    #[error("[MQTT-4.7.0-1] CONNECT packet's Will Topic contains wildcard character")]
    Will_4_7_0_1,
    /// CONNECT Will Response Topic contains a wildcard character
    #[error("[MQTT-3.3.2-14] CONNECT packet's Will Response Topic contains wildcard character")]
    Will_3_3_2_14,
    /// DISCONNECT sent by the server contains a Session Expiry Interval
    #[error("[MQTT-3.14.2-*] The Session Expiry Interval must not be set on DISCONNECT by Server")]
    Disconnect_3_14_2_21,
    /// DISCONNECT contains a non-zero Session Expiry Interval while the session expiry
    /// interval of the CONNECT packet was zero
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
            // Topic Alias greater than the Topic Alias Maximum is a Protocol Error, the receiver
            // uses DISCONNECT with Reason Code 0x94 (MQTT 5.0, 3.3.2.3.4)
            SpecViolation::Connect_3_1_2_26 | SpecViolation::Connack_3_2_2_17 => {
                DisconnectReasonCode::TopicAliasInvalid
            }
            SpecViolation::PacketId_2_2_1_3_Pub
            | SpecViolation::PacketId_2_2_1_3_Sub
            | SpecViolation::PacketId_2_2_1_3_Unsub
            | SpecViolation::Pub_3_3_2_2
            | SpecViolation::Pub_3_3_2_14
            | SpecViolation::Pub_3_3_4_6
            | SpecViolation::Subs_4_7_1
            | SpecViolation::Subs_4_8_2
            | SpecViolation::Subs_3_8_3_4
            | SpecViolation::Will_4_7_3_1
            | SpecViolation::Will_4_7_0_1
            | SpecViolation::Will_3_3_2_14
            | SpecViolation::Disconnect_3_14_2_21
            | SpecViolation::Disconnect_3_14_2_22 => DisconnectReasonCode::ProtocolError,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
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
            SpecViolation::Pub_3_3_2_14 => {
                "[MQTT-3.3.2-14] PUBLISH packet's Response Topic contains wildcard character"
            }
            SpecViolation::Pub_3_3_4_6 => {
                "[MQTT-3.3.4-6] PUBLISH packet sent by Client contains a Subscription Identifier"
            }
            SpecViolation::Pub_3_3_4_7 => {
                "[MQTT-3.3.4-7] Number of in-flight messages exceeds set maximum"
            }
            SpecViolation::Pub_3_3_4_9 => {
                "[MQTT-3.3.4-9] Number of in-flight messages exceeds set maximum"
            }
            SpecViolation::Subs_4_7_1 => "[MQTT-4.7.1-*] Topic filter is malformed",
            SpecViolation::Subs_4_8_2 => {
                "[MQTT-4.8.2-*] Shared Subscription Topic Filter is malformed"
            }
            SpecViolation::Subs_3_8_3_4 => {
                "[MQTT-3.8.3-4] No Local is set on a Shared Subscription"
            }
            SpecViolation::Will_4_7_3_1 => "[MQTT-4.7.3-1] CONNECT packet's Will Topic is empty",
            SpecViolation::Will_4_7_0_1 => {
                "[MQTT-4.7.0-1] CONNECT packet's Will Topic contains wildcard character"
            }
            SpecViolation::Will_3_3_2_14 => {
                "[MQTT-3.3.2-14] CONNECT packet's Will Response Topic contains wildcard character"
            }
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

impl MqttProtocolError {
    pub(crate) fn violation(reason: DisconnectReasonCode, message: &'static str) -> Self {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::Common { reason, message },
        })
    }

    /// Create protocol violation error from a specification violation
    pub fn spec(err: SpecViolation) -> Self {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::Spec(err),
        })
    }

    /// Create generic protocol violation error with the `ProtocolError` reason code
    pub fn generic_violation(message: &'static str) -> Self {
        Self::violation(DisconnectReasonCode::ProtocolError, message)
    }

    pub(crate) fn unexpected_packet(packet_type: u8, message: &'static str) -> MqttProtocolError {
        Self::ProtocolViolation(ProtocolViolationError {
            inner: ViolationInner::UnexpectedPacket {
                packet_type,
                message,
            },
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
        MqttError::Connect(MqttConnectError::Disconnected(Some(err)))
    }
}

impl<E> From<Either<io::Error, io::Error>> for MqttError<E> {
    fn from(err: Either<io::Error, io::Error>) -> Self {
        MqttError::Connect(MqttConnectError::Disconnected(Some(err.into_inner())))
    }
}

impl<E> From<EncodeError> for MqttError<E> {
    fn from(err: EncodeError) -> Self {
        MqttError::Connect(MqttConnectError::Protocol(MqttProtocolError::Encode(err)))
    }
}

impl<E> From<Either<DecodeError, io::Error>> for MqttConnectError<E> {
    fn from(err: Either<DecodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => MqttConnectError::Protocol(MqttProtocolError::Decode(err)),
            Either::Right(err) => MqttConnectError::Disconnected(Some(err)),
        }
    }
}

/// Errors which can occur during packet decoding
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum DecodeError {
    /// CONNECT packet's protocol name is not `MQTT`
    #[error("Invalid protocol")]
    InvalidProtocol,
    /// Packet's length does not match its content
    #[error("Invalid length")]
    InvalidLength,
    /// Packet cannot be parsed according to the protocol specification
    #[error("Malformed packet")]
    MalformedPacket,
    /// CONNECT packet's protocol level is not supported
    #[error("Unsupported protocol level")]
    UnsupportedProtocolLevel,
    /// CONNECT packet's reserved flag is set
    #[error("Connect frame's reserved flag is set")]
    ConnectReservedFlagSet,
    /// CONNACK packet's reserved flags are set
    #[error("ConnectAck frame's reserved flag is set")]
    ConnAckReservedFlagSet,
    /// CONNECT packet's client id is not valid
    #[error("Invalid client id")]
    InvalidClientId,
    /// Packet type is not known or not supported
    #[error("Unsupported packet type")]
    UnsupportedPacketType,
    // MQTT v3 only
    /// Packet id is missing for a packet that requires it
    #[error("Packet id is required")]
    PacketIdRequired,
    /// Packet is bigger than the configured maximum packet size
    #[error("Max size exceeded size:{size} max-size:{max_size}")]
    MaxSizeExceeded {
        /// Size of the received packet
        size: u32,
        /// Configured maximum packet size
        max_size: u32,
    },
    /// String field does not contain valid utf-8 data
    #[error("utf8 error")]
    Utf8Error,
    /// Packet contains more data than expected
    #[error("Unexpected payload")]
    UnexpectedPayload,
}

/// Errors which can occur during packet encoding
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum EncodeError {
    /// Packet is bigger than the Maximum Packet Size advertised by the peer
    #[error("Packet is bigger than peer's Maximum Packet Size")]
    OverMaxPacketSize,
    /// More payload chunks are sent than declared by the publish packet
    #[error("Streaming payload is bigger than Publish packet definition")]
    OverPublishSize,
    /// Streaming publish is completed before all declared payload is sent
    #[error("Streaming payload is incomplete")]
    PublishIncomplete,
    /// Packet's length does not match its content
    #[error("Invalid length")]
    InvalidLength,
    /// Packet cannot be encoded according to the protocol specification
    #[error("Malformed packet")]
    MalformedPacket,
    /// Packet id is missing for a packet that requires it
    #[error("Packet id is required")]
    PacketIdRequired,
    /// Payload is set for a packet that does not allow it
    #[error("Unexpected payload")]
    UnexpectedPayload,
    /// Another packet is sent while a streaming publish expects payload chunks
    #[error("Publish packet is not completed, expect payload")]
    ExpectPayload,
    /// Packet cannot be encoded for the negotiated protocol version
    #[error("Unsupported version")]
    UnsupportedVersion,
}

/// Errors which can occur when sending a packet
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
    /// The packet cannot be sent by this side of the connection, a server
    /// does not send SUBSCRIBE and UNSUBSCRIBE packets
    #[error("Packet is not allowed to be sent by the server")]
    NotAllowed,
}

/// Errors which can occur when attempting to handle mqtt client connection.
#[derive(Debug, thiserror::Error)]
pub enum MqttClientError<T: fmt::Debug> {
    /// Connect negotiation failed
    #[error("Connect ack failed: {:?}", _0)]
    Ack(T),
    /// Protocol error
    #[error("Protocol error: {:?}", _0)]
    Protocol(#[from] MqttProtocolError),
    /// Connect timeout
    #[error("Connect timeout")]
    ConnectTimeout,
    /// Peer disconnected
    #[error("Peer disconnected")]
    Disconnected(Option<std::io::Error>),
    /// Connect error
    #[error("Connect error: {}", _0)]
    Connect(#[from] ntex_net::connect::ConnectError),
}

impl<T: Clone + fmt::Debug> Clone for MqttClientError<T> {
    fn clone(&self) -> Self {
        match self {
            MqttClientError::Ack(e) => MqttClientError::Ack(e.clone()),
            MqttClientError::Protocol(e) => MqttClientError::Protocol(*e),
            MqttClientError::ConnectTimeout => MqttClientError::ConnectTimeout,
            MqttClientError::Disconnected(_) => MqttClientError::Disconnected(None),
            MqttClientError::Connect(e) => MqttClientError::Connect(e.clone()),
        }
    }
}

impl<T: fmt::Debug> From<EncodeError> for MqttClientError<T> {
    fn from(err: EncodeError) -> Self {
        MqttClientError::Protocol(MqttProtocolError::Encode(err))
    }
}

impl<T: fmt::Debug> From<Either<DecodeError, std::io::Error>> for MqttClientError<T> {
    fn from(err: Either<DecodeError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => MqttClientError::Protocol(MqttProtocolError::Decode(err)),
            Either::Right(err) => MqttClientError::Disconnected(Some(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn test_spec_violation_reason_and_message() {
        let err = MqttProtocolError::spec(SpecViolation::Connack_3_2_2_11);
        let MqttProtocolError::ProtocolViolation(violation) = err else {
            panic!("expected protocol violation");
        };

        assert_eq!(violation.reason(), DisconnectReasonCode::QosNotSupported);
        assert_eq!(
            violation.message(),
            "[MQTT-3.2.2-11] PUBLISH packet at a QoS level exceeding the Maximum QoS level specified in CONNACK"
        );
    }

    #[test]
    fn test_topic_alias_violation_reason() {
        // Topic Alias greater than the maximum uses 0x94 (MQTT 5.0, 3.3.2.3.4)
        for spec in [
            SpecViolation::Connect_3_1_2_26,
            SpecViolation::Connack_3_2_2_17,
        ] {
            let MqttProtocolError::ProtocolViolation(violation) = MqttProtocolError::spec(spec)
            else {
                panic!("expected protocol violation");
            };
            assert_eq!(violation.reason(), DisconnectReasonCode::TopicAliasInvalid);
        }
    }

    #[test]
    fn test_generic_violation_reason_and_message() {
        let err = MqttProtocolError::generic_violation("broken");
        let MqttProtocolError::ProtocolViolation(violation) = err else {
            panic!("expected protocol violation");
        };

        assert_eq!(violation.reason(), DisconnectReasonCode::ProtocolError);
        assert_eq!(violation.message(), "broken");
    }

    #[test]
    fn test_unexpected_packet_reason_and_message() {
        let err = MqttProtocolError::unexpected_packet(0b0011_0000, "unexpected");
        let MqttProtocolError::ProtocolViolation(violation) = err else {
            panic!("expected protocol violation");
        };

        assert_eq!(violation.reason(), DisconnectReasonCode::ProtocolError);
        assert_eq!(violation.message(), "unexpected");
        assert_eq!(
            err.to_string(),
            "Protocol violation: unexpected; received packet with type `00110000`"
        );
    }

    #[test]
    fn test_mqtt_error_from_io_and_encode() {
        let io_err = io::Error::other("io");
        let err: MqttError<()> = io_err.into();
        match err {
            MqttError::Connect(MqttConnectError::Disconnected(Some(err))) => {
                assert_eq!(err.kind(), io::ErrorKind::Other);
            }
            _ => panic!("expected disconnected handshake error"),
        }

        let err: MqttError<()> = EncodeError::MalformedPacket.into();
        assert!(matches!(
            err,
            MqttError::Connect(MqttConnectError::Protocol(MqttProtocolError::Encode(
                EncodeError::MalformedPacket
            )))
        ));
    }

    #[test]
    fn test_connect_error_from_decode_or_io() {
        let err: MqttConnectError<()> = Either::Left(DecodeError::MalformedPacket).into();
        assert!(matches!(
            err,
            MqttConnectError::Protocol(MqttProtocolError::Decode(DecodeError::MalformedPacket))
        ));

        let err: MqttConnectError<()> = Either::Right(io::Error::other("peer")).into();
        match err {
            MqttConnectError::Disconnected(Some(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::Other);
            }
            _ => panic!("expected disconnected handshake error"),
        }
    }

    #[test]
    fn test_client_error_from_decode_or_io_and_encode() {
        let err: MqttClientError<()> = Either::Left(DecodeError::InvalidLength).into();
        assert!(matches!(
            err,
            MqttClientError::Protocol(MqttProtocolError::Decode(DecodeError::InvalidLength))
        ));

        let err: MqttClientError<()> = Either::Right(io::Error::other("peer")).into();
        match err {
            MqttClientError::Disconnected(Some(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::Other);
            }
            _ => panic!("expected disconnected client error"),
        }

        let err: MqttClientError<()> = EncodeError::UnexpectedPayload.into();
        assert!(matches!(
            err,
            MqttClientError::Protocol(MqttProtocolError::Encode(EncodeError::UnexpectedPayload))
        ));
    }
}
