use bytes::Bytes;
use proto::{Protocol, QoS};
use string::String;

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
/// Connect Return Code
pub enum ConnectReturnCode {
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
    Reserved = 6,
}

const_enum!(ConnectReturnCode: u8);

impl ConnectReturnCode {
    pub fn reason(&self) -> &'static str {
        match *self {
            ConnectReturnCode::ConnectionAccepted => "Connection Accepted",
            ConnectReturnCode::UnacceptableProtocolVersion => {
                "Connection Refused, unacceptable protocol version"
            }
            ConnectReturnCode::IdentifierRejected => "Connection Refused, identifier rejected",
            ConnectReturnCode::ServiceUnavailable => "Connection Refused, Server unavailable",
            ConnectReturnCode::BadUserNameOrPassword => {
                "Connection Refused, bad user name or password"
            }
            ConnectReturnCode::NotAuthorized => "Connection Refused, not authorized",
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
    pub topic: String<Bytes>,
    /// defines the Application Message that is to be published to the Will Topic
    pub message: Bytes,
}

#[derive(Debug, PartialEq, Clone)]
/// Connect packet content
pub struct Connect {
    pub protocol: Protocol,
    /// the handling of the Session state.
    pub clean_session: bool,
    /// a time interval measured in seconds.
    pub keep_alive: u16,
    /// Will Message be stored on the Server and associated with the Network Connection.
    pub last_will: Option<LastWill>,
    /// identifies the Client to the Server.
    pub client_id: String<Bytes>,
    /// username can be used by the Server for authentication and authorization.
    pub username: Option<String<Bytes>>,
    /// password can be used by the Server for authentication and authorization.
    pub password: Option<Bytes>,
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
    Connect { connect: Box<Connect> },
    /// Connect acknowledgment
    ConnectAck {
        /// enables a Client to establish whether the Client and Server have a consistent view
        /// about whether there is already stored Session state.
        session_present: bool,
        return_code: ConnectReturnCode,
    },
    /// Publish message
    Publish {
        /// this might be re-delivery of an earlier attempt to send the Packet.
        dup: bool,
        retain: bool,
        /// the level of assurance for delivery of an Application Message.
        qos: QoS,
        /// the information channel to which payload data is published.
        topic: String<Bytes>,
        /// only present in PUBLISH Packets where the QoS level is 1 or 2.
        packet_id: Option<u16>,
        /// the Application Message that is being published.
        payload: Bytes,
    },
    /// Publish acknowledgment
    PublishAck {
        /// Packet Identifier
        packet_id: u16,
    },
    /// Publish received (assured delivery part 1)
    PublishReceived {
        /// Packet Identifier
        packet_id: u16,
    },
    /// Publish release (assured delivery part 2)
    PublishRelease {
        /// Packet Identifier
        packet_id: u16,
    },
    /// Publish complete (assured delivery part 3)
    PublishComplete {
        /// Packet Identifier
        packet_id: u16,
    },
    /// Client subscribe request
    Subscribe {
        /// Packet Identifier
        packet_id: u16,
        /// the list of Topic Filters and QoS to which the Client wants to subscribe.
        topic_filters: Vec<(String<Bytes>, QoS)>,
    },
    /// Subscribe acknowledgment
    SubscribeAck {
        packet_id: u16,
        /// corresponds to a Topic Filter in the SUBSCRIBE Packet being acknowledged.
        status: Vec<SubscribeReturnCode>,
    },
    /// Unsubscribe request
    Unsubscribe {
        /// Packet Identifier
        packet_id: u16,
        /// the list of Topic Filters that the Client wishes to unsubscribe from.
        topic_filters: Vec<String<Bytes>>,
    },
    /// Unsubscribe acknowledgment
    UnsubscribeAck {
        /// Packet Identifier
        packet_id: u16,
    },
    /// PING request
    PingRequest,
    /// PING response
    PingResponse,
    /// Client is disconnecting
    Disconnect,
}

impl Packet {
    #[inline]
    /// MQTT Control Packet type
    pub fn packet_type(&self) -> u8 {
        match *self {
            Packet::Connect { .. } => CONNECT,
            Packet::ConnectAck { .. } => CONNACK,
            Packet::Publish { .. } => PUBLISH,
            Packet::PublishAck { .. } => PUBACK,
            Packet::PublishReceived { .. } => PUBREC,
            Packet::PublishRelease { .. } => PUBREL,
            Packet::PublishComplete { .. } => PUBCOMP,
            Packet::Subscribe { .. } => SUBSCRIBE,
            Packet::SubscribeAck { .. } => SUBACK,
            Packet::Unsubscribe { .. } => UNSUBSCRIBE,
            Packet::UnsubscribeAck { .. } => UNSUBACK,
            Packet::PingRequest => PINGREQ,
            Packet::PingResponse => PINGRESP,
            Packet::Disconnect => DISCONNECT,
        }
    }

    /// Flags specific to each MQTT Control Packet type
    pub fn packet_flags(&self) -> u8 {
        match *self {
            Packet::Publish {
                dup, qos, retain, ..
            } => {
                let mut b = qos.into();

                b <<= 1;

                if dup {
                    b |= 0b1000;
                }

                if retain {
                    b |= 0b0001;
                }

                b
            }
            Packet::PublishRelease { .. }
            | Packet::Subscribe { .. }
            | Packet::Unsubscribe { .. } => 0b0010,
            _ => 0,
        }
    }
}

pub const CONNECT: u8 = 1;
pub const CONNACK: u8 = 2;
pub const PUBLISH: u8 = 3;
pub const PUBACK: u8 = 4;
pub const PUBREC: u8 = 5;
pub const PUBREL: u8 = 6;
pub const PUBCOMP: u8 = 7;
pub const SUBSCRIBE: u8 = 8;
pub const SUBACK: u8 = 9;
pub const UNSUBSCRIBE: u8 = 10;
pub const UNSUBACK: u8 = 11;
pub const PINGREQ: u8 = 12;
pub const PINGRESP: u8 = 13;
pub const DISCONNECT: u8 = 14;
