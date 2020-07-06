//! MQTT v5 Protocol codec

use bytestring::ByteString;

mod codec;
mod decode;
mod encode;
mod packet;

pub use self::codec::Codec;
pub use self::packet::*;

pub type UserProperty = (ByteString, ByteString);
pub type UserProperties = Vec<UserProperty>;
