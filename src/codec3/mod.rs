//! MQTT v3.1.1 Protocol codec

#[macro_use]
mod utils;
mod codec;
mod decode;
mod encode;
mod packet;

pub use self::codec::Codec;
pub use self::packet::{Connect, ConnectCode, LastWill, Packet, Publish, SubscribeReturnCode};
pub use crate::topic::{Level, Topic, TopicError};

pub const MQTT_LEVEL: u8 = 4;

/// Max possible packet size
pub const MAX_PACKET_SIZE: u32 = 0xF_FF_FF_FF;

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

prim_enum! {
    /// Quality of Service
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
        ExactlyOnce = 2
    }
}
