use std::mem;
use std::convert::From;

macro_rules! const_enum {
    ($name:ty : $repr:ty) => {
        impl From<$repr> for $name {
            fn from(u: $repr) -> Self {
                unsafe { mem::transmute(u) }
            }
        }

        impl Into<$repr> for $name {
            fn into(self) -> $repr {
                unsafe { mem::transmute(self) }
            }
        }
    }
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
/// Quality of Service levels
pub enum QoS {
    /// At most once delivery
    ///
    /// The message is delivered according to the capabilities of the underlying network.
    /// No response is sent by the receiver and no retry is performed by the sender.
    /// The message arrives at the receiver either once or not at all.
    AtMostOnce = 0,
    /// At least once delivery
    ///
    /// This quality of service ensures that the message arrives at the receiver at least once.
    /// A QoS 1 PUBLISH Packet has a Packet Identifier in its variable header
    /// and is acknowledged by a PUBACK Packet.
    AtLeastOnce = 1,
    /// Exactly once delivery
    ///
    /// This is the highest quality of service,
    /// for use when neither loss nor duplication of messages are acceptable.
    /// There is an increased overhead associated with this quality of service.
    ExactlyOnce = 2,
}

const_enum!(QoS: u8);

bitflags! {
    pub flags ConnectFlags: u8 {
        const USERNAME      = 0b10000000,
        const PASSWORD      = 0b01000000,
        const WILL_RETAIN   = 0b00100000,
        const WILL_QOS      = 0b00011000,
        const WILL          = 0b00000100,
        const CLEAN_SESSION = 0b00000010,
    }
}

pub const WILL_QOS_SHIFT: u8 = 3;

bitflags! {
    pub flags ConnectAckFlags: u8 {
        const SESSION_PRESENT = 0b00000001,
    }
}

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

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FixedHeader {
    /// MQTT Control Packet type
    pub packet_type: u8,
    /// Flags specific to each MQTT Control Packet type
    pub packet_flags: u8,
    /// the number of bytes remaining within the current packet,
    /// including data in the variable header and the payload.
    pub remaining_length: usize,
}

#[derive(Debug, Eq, PartialEq, Clone)]
/// Connection Will
pub struct ConnectionWill<'a> {
    /// the QoS level to be used when publishing the Will Message.
    pub qos: QoS,
    /// the Will Message is to be Retained when it is published.
    pub retain: bool,
    /// the Will Topic
    pub topic: &'a str,
    /// defines the Application Message that is to be published to the Will Topic
    pub message: &'a str,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
/// Subscribe Return Code
pub enum SubscribeReturnCode {
    Success(QoS),
    Failure,
}

#[derive(Debug, Eq, PartialEq, Clone)]
/// MQTT Control Packets
pub enum Packet<'a> {
    /// Client request to connect to Server
    Connect {
        /// the handling of the Session state.
        clean_session: bool,
        /// a time interval measured in seconds.
        keep_alive: u16,
        /// Will Message be stored on the Server and associated with the Network Connection.
        will: Option<ConnectionWill<'a>>,
        /// identifies the Client to the Server.
        client_id: &'a [u8],
        /// username can be used by the Server for authentication and authorization.
        username: Option<&'a str>,
        /// password can be used by the Server for authentication and authorization.
        password: Option<&'a [u8]>,
    },
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
        topic: &'a str,
        /// only present in PUBLISH Packets where the QoS level is 1 or 2.
        packet_id: Option<u16>,
        /// the Application Message that is being published.
        payload: &'a [u8],
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
        topic_filters: Vec<(&'a str, QoS)>,
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
        topic_filters: Vec<&'a str>,
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

impl<'a> Packet<'a> {
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
            Packet::Publish { dup, qos, retain, .. } => {
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
            Packet::PublishRelease { .. } |
            Packet::Subscribe { .. } |
            Packet::Unsubscribe { .. } => 0b0010,
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
