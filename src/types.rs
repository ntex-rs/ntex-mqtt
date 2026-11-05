/// Protocol name encoded in a CONNECT variable header.
pub(crate) const MQTT: &[u8] = b"MQTT";
/// Protocol level identifying MQTT v3.1.1.
pub(crate) const MQTT_LEVEL_3: u8 = 4;
/// Protocol level identifying MQTT v5.
pub(crate) const MQTT_LEVEL_5: u8 = 5;
/// Bit offset of the Will quality-of-service field in CONNECT flags.
pub(crate) const WILL_QOS_SHIFT: u8 = 3;

/// Maximum representable MQTT Remaining Length in bytes (256 MiB minus one).
pub(crate) const MAX_PACKET_SIZE: u32 = 0xF_FF_FF_FF;

/// Max read buffer space reserved ahead for a non-publish packet,
/// larger packets grow the buffer as data arrives
pub(crate) const MAX_FRAME_RESERVE: usize = 8192;

prim_enum! {
    /// MQTT delivery quality-of-service levels.
    #[derive(serde::Serialize, serde::Deserialize, PartialOrd, Ord, Hash)]
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
        /// A `QoS` 1 PUBLISH Packet has a Packet Identifier in its variable header
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

bitflags::bitflags! {
    /// CONNECT packet flags
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ConnectFlags: u8 {
        /// User Name is present in the payload
        const USERNAME    = 0b1000_0000;
        /// Password is present in the payload
        const PASSWORD    = 0b0100_0000;
        /// Will Message must be retained by the server
        const WILL_RETAIN = 0b0010_0000;
        /// Quality of Service of the Will Message
        const WILL_QOS    = 0b0001_1000;
        /// Will Message is present in the payload
        const WILL        = 0b0000_0100;
        /// Request a clean session in v3.1.1 or a clean start in v5.
        const CLEAN_START = 0b0000_0010;
    }
}

bitflags::bitflags! {
    /// CONNACK packet flags
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ConnectAckFlags: u8 {
        /// The server has an existing session state for the client
        const SESSION_PRESENT = 0b0000_0001;
    }
}

/// Fixed-header type bytes, including required reserved flag values.
pub(super) mod packet_type {
    /// CONNECT fixed-header byte.
    pub(crate) const CONNECT: u8 = 0b0001_0000;
    /// CONNACK fixed-header byte.
    pub(crate) const CONNACK: u8 = 0b0010_0000;
    /// Lowest PUBLISH fixed-header byte, with all publish flags clear.
    pub(crate) const PUBLISH_START: u8 = 0b0011_0000;
    /// Highest PUBLISH fixed-header byte, including every flag combination.
    pub(crate) const PUBLISH_END: u8 = 0b0011_1111;
    /// PUBACK fixed-header byte.
    pub(crate) const PUBACK: u8 = 0b0100_0000;
    /// PUBREC fixed-header byte.
    pub(crate) const PUBREC: u8 = 0b0101_0000;
    /// PUBREL fixed-header byte, including its required reserved flag.
    pub(crate) const PUBREL: u8 = 0b0110_0010;
    /// PUBCOMP fixed-header byte.
    pub(crate) const PUBCOMP: u8 = 0b0111_0000;
    /// SUBSCRIBE fixed-header byte, including its required reserved flag.
    pub(crate) const SUBSCRIBE: u8 = 0b1000_0010;
    /// SUBACK fixed-header byte.
    pub(crate) const SUBACK: u8 = 0b1001_0000;
    /// UNSUBSCRIBE fixed-header byte, including its required reserved flag.
    pub(crate) const UNSUBSCRIBE: u8 = 0b1010_0010;
    /// UNSUBACK fixed-header byte.
    pub(crate) const UNSUBACK: u8 = 0b1011_0000;
    /// PINGREQ fixed-header byte.
    pub(crate) const PINGREQ: u8 = 0b1100_0000;
    /// PINGRESP fixed-header byte.
    pub(crate) const PINGRESP: u8 = 0b1101_0000;
    /// DISCONNECT fixed-header byte.
    pub(crate) const DISCONNECT: u8 = 0b1110_0000;
    /// MQTT v5 AUTH fixed-header byte.
    pub(crate) const AUTH: u8 = 0b1111_0000;

    /// Check whether a fixed-header byte carries the PUBLISH packet type.
    pub(crate) const fn is_publish(packet_type: u8) -> bool {
        matches!(packet_type, PUBLISH_START..=PUBLISH_END)
    }
}

/// Decoded fixed header describing the remaining variable header and payload.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct FixedHeader {
    /// Fixed Header byte
    pub(crate) first_byte: u8,
    /// the number of bytes remaining within the current packet,
    /// including data in the variable header and the payload.
    pub(crate) remaining_length: u32,
}
