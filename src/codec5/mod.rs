//! MQTT v5 Protocol codec

use bytestring::ByteString;

mod codec;
mod decode;
mod encode;
mod packet;

pub use self::codec::Codec;
pub use self::packet::*;

pub const MQTT_LEVEL: u8 = 5;

/// Max possible packet size
pub const MAX_PACKET_SIZE: u32 = 0xF_FF_FF_FF;

pub(crate) type UserProperty = (ByteString, ByteString);
pub(crate) type UserProperties = Vec<UserProperty>;
