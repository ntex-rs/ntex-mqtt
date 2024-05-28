//! MQTT v5 Protocol codec

use ntex_bytes::ByteString;

#[allow(clippy::module_inception)]
mod codec;
mod decode;
mod encode;
mod packet;

pub use self::codec::Codec;
pub(crate) use self::encode::EncodeLtd;
pub use self::packet::*;

pub type UserProperty = (ByteString, ByteString);
pub type UserProperties = Vec<UserProperty>;
