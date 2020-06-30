//! MQTT v3.1.1 Protocol codec

mod codec;
mod decode;
mod encode;
mod packet;

pub use self::codec::Codec;
pub use self::packet::{Connect, ConnectCode, LastWill, Packet, Publish, SubscribeReturnCode};
pub use crate::topic::{Level, Topic, TopicError};
pub use crate::types::QoS;

bitflags::bitflags! {
    pub struct ConnectFlags: u8 {
        const USERNAME      = 0b1000_0000;
        const PASSWORD      = 0b0100_0000;
        const WILL_RETAIN   = 0b0010_0000;
        const WILL_QOS      = 0b0001_1000;
        const WILL          = 0b0000_0100;
        const CLEAN_SESSION = 0b0000_0010;
    }
}

pub const WILL_QOS_SHIFT: u8 = 3;

bitflags::bitflags! {
    pub struct ConnectAckFlags: u8 {
        const SESSION_PRESENT = 0b0000_0001;
    }
}
