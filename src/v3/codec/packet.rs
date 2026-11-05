use std::num::NonZeroU16;

use ntex_bytes::{ByteString, Bytes};

use crate::types::{QoS, packet_type};

prim_enum! {
    /// Connect Return Code
    pub enum ConnectAckReason {
        /// Connection accepted
        ConnectionAccepted = 0,
        /// Connection Refused, unacceptable protocol version
        UnacceptableProtocolVersion = 1,
        /// Connection Refused, identifier rejected
        IdentifierRejected = 2,
        /// Connection Refused, Server unavailable
        ServiceUnavailable = 3,
        /// Connection Refused, bad user name or password
        BadUserNameOrPassword = 4,
        /// Connection Refused, not authorized
        NotAuthorized = 5,
        /// Reserved
        Reserved = 6
    }
}

impl ConnectAckReason {
    /// Returns a human readable description of the return code
    pub fn reason(self) -> &'static str {
        match self {
            ConnectAckReason::ConnectionAccepted => "Connection Accepted",
            ConnectAckReason::UnacceptableProtocolVersion => {
                "Connection Refused, unacceptable protocol version"
            }
            ConnectAckReason::IdentifierRejected => "Connection Refused, identifier rejected",
            ConnectAckReason::ServiceUnavailable => "Connection Refused, Server unavailable",
            ConnectAckReason::BadUserNameOrPassword => {
                "Connection Refused, bad user name or password"
            }
            ConnectAckReason::NotAuthorized => "Connection Refused, not authorized",
            ConnectAckReason::Reserved => "Connection Refused",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// Publication metadata and bytes supplied by CONNECT for an unexpected disconnect.
pub struct LastWill {
    /// Delivery quality requested for publishing the will.
    pub qos: QoS,
    /// Whether the will publication requests retained-message storage.
    pub retain: bool,
    /// Topic name on which to publish the will; subscription wildcards are not allowed.
    pub topic: ByteString,
    /// Will payload, limited to 65,535 bytes by the CONNECT binary-field format.
    pub message: Bytes,
}

#[derive(Default, Debug, PartialEq, Eq, Clone)]
/// CONNECT fields used to establish and authenticate an MQTT 3.1.1 connection.
pub struct Connect {
    /// Whether to start a clean session rather than resume stored session state.
    pub clean_session: bool,
    /// Client keep-alive interval in seconds; zero disables the protocol keep-alive request.
    pub keep_alive: u16,
    /// Optional will publication for the server to handle after an unexpected disconnect.
    pub last_will: Option<LastWill>,
    /// Client identifier; an empty identifier requires `clean_session` to be `true`.
    pub client_id: ByteString,
    /// Optional authentication username, encoded as an MQTT UTF-8 string.
    pub username: Option<ByteString>,
    /// Optional authentication bytes; requires a username and at most 65,535 bytes.
    pub password: Option<Bytes>,
}

impl Connect {
    #[must_use]
    /// Set `client_id` value
    pub fn client_id<T>(mut self, client_id: T) -> Self
    where
        ByteString: From<T>,
    {
        self.client_id = client_id.into();
        self
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// PUBLISH header metadata, with payload bytes streamed separately by the codec.
pub struct Publish {
    /// Whether this may be a redelivery; must be `false` for `QoS 0`.
    pub dup: bool,
    /// Whether the publication requests retained-message storage for future subscribers.
    pub retain: bool,
    /// Delivery quality determining the acknowledgment exchange and identifier requirement.
    pub qos: QoS,
    /// Nonempty topic name without subscription wildcards.
    pub topic: ByteString,
    /// Required for `QoS 1` and `QoS 2`; absent for `QoS 0`.
    pub packet_id: Option<NonZeroU16>,
    /// Declared payload length in bytes, excluding all fixed and variable headers.
    pub payload_size: u32,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
/// CONNACK result sent in response to CONNECT.
pub struct ConnectAck {
    /// Connection acceptance or refusal reason reported by the server.
    pub return_code: ConnectAckReason,
    /// Whether an existing session was resumed; only valid for an accepted connection.
    pub session_present: bool,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
/// Result for one requested filter in a SUBACK packet.
pub enum SubscribeReturnCode {
    /// Subscription accepted with the granted maximum `QoS`
    Success(QoS),
    /// Subscription failed
    Failure,
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// MQTT control packets other than PUBLISH, whose payload uses separate codec items.
pub enum Packet {
    /// Client request to connect to Server
    Connect(Box<Connect>),
    /// Connect acknowledgment
    ConnectAck(ConnectAck),
    /// Publish acknowledgment
    PublishAck {
        /// Packet Identifier
        packet_id: NonZeroU16,
    },
    /// Publish received (assured delivery part 1)
    PublishReceived {
        /// Packet Identifier
        packet_id: NonZeroU16,
    },
    /// Publish release (assured delivery part 2)
    PublishRelease {
        /// Packet Identifier
        packet_id: NonZeroU16,
    },
    /// Publish complete (assured delivery part 3)
    PublishComplete {
        /// Packet Identifier
        packet_id: NonZeroU16,
    },
    /// Client subscribe request
    Subscribe {
        /// Packet Identifier
        packet_id: NonZeroU16,
        /// Nonempty ordered filters and requested maximum delivery qualities.
        topic_filters: Vec<(ByteString, QoS)>,
    },
    /// Subscribe acknowledgment
    SubscribeAck {
        /// Packet Identifier
        packet_id: NonZeroU16,
        /// Per-filter results in the same order as the corresponding SUBSCRIBE request.
        status: Vec<SubscribeReturnCode>,
    },
    /// Unsubscribe request
    Unsubscribe {
        /// Packet Identifier
        packet_id: NonZeroU16,
        /// Nonempty ordered list of subscription filters to remove.
        topic_filters: Vec<ByteString>,
    },
    /// Unsubscribe acknowledgment
    UnsubscribeAck {
        /// Packet Identifier
        packet_id: NonZeroU16,
    },
    /// PING request
    PingRequest,
    /// PING response
    PingResponse,
    /// Client is disconnecting
    Disconnect,
}

impl From<Connect> for Packet {
    fn from(val: Connect) -> Packet {
        Packet::Connect(Box::new(val))
    }
}

impl Packet {
    /// Returns the MQTT control packet type value
    pub fn packet_type(&self) -> u8 {
        match self {
            Packet::Connect(_) => packet_type::CONNECT,
            Packet::ConnectAck { .. } => packet_type::CONNACK,
            Packet::PublishAck { .. } => packet_type::PUBACK,
            Packet::PublishReceived { .. } => packet_type::PUBREC,
            Packet::PublishRelease { .. } => packet_type::PUBREL,
            Packet::PublishComplete { .. } => packet_type::PUBCOMP,
            Packet::Subscribe { .. } => packet_type::SUBSCRIBE,
            Packet::SubscribeAck { .. } => packet_type::SUBACK,
            Packet::Unsubscribe { .. } => packet_type::UNSUBSCRIBE,
            Packet::UnsubscribeAck { .. } => packet_type::UNSUBACK,
            Packet::PingRequest => packet_type::PINGREQ,
            Packet::PingResponse => packet_type::PINGRESP,
            Packet::Disconnect => packet_type::DISCONNECT,
        }
    }
}

/// Unit tests for packet type values and connection return descriptions.
#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: Every connection acknowledgment reason has its expected human-readable description.
    ///
    /// HOW:
    /// - Call `ConnectAckReason::reason` for each accepted, refused, and reserved variant.
    /// - Compare every returned description with its expected string.
    #[test]
    fn test_ack_reason() {
        assert_eq!(
            ConnectAckReason::ConnectionAccepted.reason(),
            "Connection Accepted"
        );
        assert_eq!(
            ConnectAckReason::UnacceptableProtocolVersion.reason(),
            "Connection Refused, unacceptable protocol version"
        );
        assert_eq!(
            ConnectAckReason::IdentifierRejected.reason(),
            "Connection Refused, identifier rejected"
        );
        assert_eq!(
            ConnectAckReason::ServiceUnavailable.reason(),
            "Connection Refused, Server unavailable"
        );
        assert_eq!(
            ConnectAckReason::BadUserNameOrPassword.reason(),
            "Connection Refused, bad user name or password"
        );
        assert_eq!(
            ConnectAckReason::NotAuthorized.reason(),
            "Connection Refused, not authorized"
        );
        assert_eq!(ConnectAckReason::Reserved.reason(), "Connection Refused");
    }

    /// STATEMENT: Each control packet reports its MQTT fixed-header type byte.
    ///
    /// HOW:
    /// - Construct every `Packet` variant with representative identifiers and empty topic/result lists.
    /// - Compare `packet_type` with the corresponding protocol constant.
    #[test]
    fn test_packet_type() {
        let packet_id = NonZeroU16::new(1).unwrap();
        for (pkt, ty) in [
            (Connect::default().into(), packet_type::CONNECT),
            (
                Packet::ConnectAck(ConnectAck {
                    return_code: ConnectAckReason::ConnectionAccepted,
                    session_present: false,
                }),
                packet_type::CONNACK,
            ),
            (Packet::PublishAck { packet_id }, packet_type::PUBACK),
            (Packet::PublishReceived { packet_id }, packet_type::PUBREC),
            (Packet::PublishRelease { packet_id }, packet_type::PUBREL),
            (Packet::PublishComplete { packet_id }, packet_type::PUBCOMP),
            (
                Packet::Subscribe {
                    packet_id,
                    topic_filters: Vec::new(),
                },
                packet_type::SUBSCRIBE,
            ),
            (
                Packet::SubscribeAck {
                    packet_id,
                    status: Vec::new(),
                },
                packet_type::SUBACK,
            ),
            (
                Packet::Unsubscribe {
                    packet_id,
                    topic_filters: Vec::new(),
                },
                packet_type::UNSUBSCRIBE,
            ),
            (Packet::UnsubscribeAck { packet_id }, packet_type::UNSUBACK),
            (Packet::PingRequest, packet_type::PINGREQ),
            (Packet::PingResponse, packet_type::PINGRESP),
            (Packet::Disconnect, packet_type::DISCONNECT),
        ] {
            assert_eq!(pkt.packet_type(), ty, "{pkt:?}");
        }
    }
}
