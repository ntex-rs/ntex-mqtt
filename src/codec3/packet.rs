use bytes::Bytes;
use bytestring::ByteString;
use std::num::NonZeroU16;

use crate::types::QoS;

prim_enum! {
    /// Connect Return Code
    pub enum ConnectCode {
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

impl ConnectCode {
    pub fn reason(self) -> &'static str {
        match self {
            ConnectCode::ConnectionAccepted => "Connection Accepted",
            ConnectCode::UnacceptableProtocolVersion => {
                "Connection Refused, unacceptable protocol version"
            }
            ConnectCode::IdentifierRejected => "Connection Refused, identifier rejected",
            ConnectCode::ServiceUnavailable => "Connection Refused, Server unavailable",
            ConnectCode::BadUserNameOrPassword => {
                "Connection Refused, bad user name or password"
            }
            ConnectCode::NotAuthorized => "Connection Refused, not authorized",
            _ => "Connection Refused",
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
/// Connection Will
pub struct LastWill {
    /// the QoS level to be used when publishing the Will Message.
    pub qos: QoS,
    /// the Will Message is to be Retained when it is published.
    pub retain: bool,
    /// the Will Topic
    pub topic: ByteString,
    /// defines the Application Message that is to be published to the Will Topic
    pub message: Bytes,
}

#[derive(Debug, PartialEq, Clone)]
/// Connect packet content
pub struct Connect {
    /// the handling of the Session state.
    pub clean_session: bool,
    /// a time interval measured in seconds.
    pub keep_alive: u16,
    /// Will Message be stored on the Server and associated with the Network Connection.
    pub last_will: Option<LastWill>,
    /// identifies the Client to the Server.
    pub client_id: ByteString,
    /// username can be used by the Server for authentication and authorization.
    pub username: Option<ByteString>,
    /// password can be used by the Server for authentication and authorization.
    pub password: Option<Bytes>,
}

#[derive(Debug, PartialEq, Clone)]
/// Publish message
pub struct Publish {
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub dup: bool,
    pub retain: bool,
    /// the level of assurance for delivery of an Application Message.
    pub qos: QoS,
    /// the information channel to which payload data is published.
    pub topic: ByteString,
    /// only present in PUBLISH Packets where the QoS level is 1 or 2.
    pub packet_id: Option<NonZeroU16>,
    /// the Application Message that is being published.
    pub payload: Bytes,
}

#[derive(Debug, PartialEq, Copy, Clone)]
/// Subscribe Return Code
pub enum SubscribeReturnCode {
    Success(QoS),
    Failure,
}

#[derive(Debug, PartialEq, Clone)]
/// MQTT Control Packets
pub enum Packet {
    /// Client request to connect to Server
    Connect(Connect),

    /// Connect acknowledgment
    ConnectAck {
        /// enables a Client to establish whether the Client and Server have a consistent view
        /// about whether there is already stored Session state.
        session_present: bool,
        return_code: ConnectCode,
    },

    /// Publish message
    Publish(Publish),

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
        /// the list of Topic Filters and QoS to which the Client wants to subscribe.
        topic_filters: Vec<(ByteString, QoS)>,
    },
    /// Subscribe acknowledgment
    SubscribeAck {
        packet_id: NonZeroU16,
        /// corresponds to a Topic Filter in the SUBSCRIBE Packet being acknowledged.
        status: Vec<SubscribeReturnCode>,
    },

    /// Unsubscribe request
    Unsubscribe {
        /// Packet Identifier
        packet_id: NonZeroU16,
        /// the list of Topic Filters that the Client wishes to unsubscribe from.
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
        Packet::Connect(val)
    }
}

impl From<Publish> for Packet {
    fn from(val: Publish) -> Packet {
        Packet::Publish(val)
    }
}

pub(super) mod packet_type {
    pub const CONNECT: u8 = 0b0001_0000;
    pub const CONNACK: u8 = 0b0010_0000;
    pub const PUBLISH_START: u8 = 0b0011_0000;
    pub const PUBLISH_END: u8 = 0b0011_1111;
    pub const PUBACK: u8 = 0b0100_0000;
    pub const PUBREC: u8 = 0b0101_0000;
    pub const PUBREL: u8 = 0b0110_0010;
    pub const PUBCOMP: u8 = 0b0111_0000;
    pub const SUBSCRIBE: u8 = 0b1000_0010;
    pub const SUBACK: u8 = 0b1001_0000;
    pub const UNSUBSCRIBE: u8 = 0b1010_0010;
    pub const UNSUBACK: u8 = 0b1011_0000;
    pub const PINGREQ: u8 = 0b1100_0000;
    pub const PINGRESP: u8 = 0b1101_0000;
    pub const DISCONNECT: u8 = 0b1110_0000;
}
