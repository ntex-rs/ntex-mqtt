use std::mem;
use std::convert::From;

macro_rules! const_enum {
    ($name:ty : $repr:ty) => {
        impl From<$repr> for $name {
            fn from(u: u8) -> Self {
                unsafe { mem::transmute(u) }
            }
        }

        impl Into<$repr> for $name {
            fn into(self) -> u8 {
                unsafe { mem::transmute(self) }
            }
        }
    }
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
    Reserved = 3,
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
pub enum ConnectReturnCode {
    /// Connection accepted
    ConnectionAccepted = 1,
    /// Connection Refused, unacceptable protocol version
    UnacceptableProtocolVersion = 2,
    /// Connection Refused, identifier rejected
    IdentifierRejected = 3,
    /// Connection Refused, Server unavailable
    ServiceUnavailable = 4,
    /// Connection Refused, bad user name or password
    BadUserNameOrPassword = 5,
    /// Connection Refused, not authorized
    NotAuthorized = 6,
}

const_enum!(ConnectReturnCode: u8);

#[derive(Debug, Eq, PartialEq)]
pub struct FixedHeader {
    pub packet_type: ControlType,
    pub packet_flags: u8,
    pub remaining_length: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnectionWill<'a> {
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a str,
    pub message: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Packet<'a> {
    /// Reserved
    Reserved,
    /// Client request to connect to Server
    Connect {
        clean_session: bool,
        keep_alive: u16,
        will: Option<ConnectionWill<'a>>,
        client_id: &'a str,
        username: Option<&'a str>,
        password: Option<&'a str>,
    },
    /// Connect acknowledgment
    ConnectAck {
        session_present: bool,
        return_code: ConnectReturnCode,
    },
    /// Publish message
    Publish {
        dup: bool,
        retain: bool,
        qos: QoS,
        topic: &'a str,
        packet_id: u16,
    },
    /// Publish acknowledgment
    PublishAck { packet_id: u16 },
    /// Publish received (assured delivery part 1)
    PublishReceived { packet_id: u16 },
    /// Publish release (assured delivery part 2)
    PublishRelease { packet_id: u16 },
    /// Publish complete (assured delivery part 3)
    PublishComplete { packet_id: u16 },
    /// Client subscribe request
    Subscribe,
    /// Subscribe acknowledgment
    SubscribeAck,
    /// Unsubscribe request
    Unsubscribe,
    /// Unsubscribe acknowledgment
    UnsubscribeAck,
    /// PING request
    PingRequest,
    /// PING response
    PingResponse,
    /// Client is disconnecting
    Disconnect,
}

mod control_type {
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
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ControlType {
    /// Client request to connect to Server
    Connect = control_type::CONNECT,

    /// Connect acknowledgment
    ConnectAck = control_type::CONNACK,

    /// Publish message
    Publish = control_type::PUBLISH,

    /// Publish acknowledgment
    PublishAck = control_type::PUBACK,

    /// Publish received (assured delivery part 1)
    PublishReceived = control_type::PUBREC,

    /// Publish release (assured delivery part 2)
    PublishRelease = control_type::PUBREL,

    /// Publish complete (assured delivery part 3)
    PublishComplete = control_type::PUBCOMP,

    /// Client subscribe request
    Subscribe = control_type::SUBSCRIBE,

    /// Subscribe acknowledgment
    SubscribeAck = control_type::SUBACK,

    /// Unsubscribe request
    Unsubscribe = control_type::UNSUBSCRIBE,

    /// Unsubscribe acknowledgment
    UnsubscribeAck = control_type::UNSUBACK,

    /// PING request
    PingRequest = control_type::PINGREQ,

    /// PING response
    PingResponse = control_type::PINGRESP,

    /// Client is disconnecting
    Disconnect = control_type::DISCONNECT,
}

const_enum!(ControlType: u8);
