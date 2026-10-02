use std::num::NonZeroU16;

use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes};

use super::ack_props;
use crate::error::{DecodeError, EncodeError};
use crate::utils::{Decode, Encode};
use crate::v5::codec::{UserProperties, encode};

const HEADER_LEN: u32 = 2 + 1; // packet id + reason code

/// PUBACK/PUBREC message content
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PublishAck {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// PUBACK/PUBREC Reason Code.
    pub reason_code: PublishAckReason,
    /// User Property pairs, additional diagnostic or other information.
    pub properties: UserProperties,
    /// Reason String property, a human readable string designed for diagnostics.
    pub reason_string: Option<ByteString>,
}

/// PUBREL/PUBCOMP message content
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PublishAck2 {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// PUBREL/PUBCOMP Reason Code.
    pub reason_code: PublishAck2Reason,
    /// User Property pairs, additional diagnostic or other information.
    pub properties: UserProperties,
    /// Reason String property, a human readable string designed for diagnostics.
    pub reason_string: Option<ByteString>,
}

prim_enum! {
    /// PUBACK / PUBREC reason codes
    pub enum PublishAckReason {
        /// The message is accepted.
        Success = 0,
        /// The message is accepted but there are no subscribers, sent only by the Server.
        NoMatchingSubscribers = 16,
        /// The receiver does not accept the publish but does not want to reveal the reason,
        /// or none of the other reason codes apply.
        UnspecifiedError = 128,
        /// The PUBLISH is valid but the receiver is not willing to accept it.
        ImplementationSpecificError = 131,
        /// The PUBLISH is not authorized.
        NotAuthorized = 135,
        /// The Topic Name is not malformed, but is not accepted by this Client or Server.
        TopicNameInvalid = 144,
        /// The Packet Identifier is already in use.
        PacketIdentifierInUse = 145,
        /// An implementation or administrative imposed limit has been exceeded.
        QuotaExceeded = 151,
        /// The payload format does not match the specified Payload Format Indicator.
        PayloadFormatInvalid = 153
    }
}

prim_enum! {
    /// PUBREL / PUBCOMP reason codes
    pub enum PublishAck2Reason {
        /// Message released (PUBREL) or Packet Identifier released (PUBCOMP).
        Success = 0,
        /// The Packet Identifier is not known.
        PacketIdNotFound = 146
    }
}

impl PublishAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;

        let ack = if src.has_remaining() {
            let reason_code = src.get_u8().try_into()?;
            if src.has_remaining() {
                let (properties, reason_string) = ack_props::decode(src)?;
                ensure!(!src.has_remaining(), DecodeError::InvalidLength); // no data should be left in src
                Self {
                    packet_id,
                    reason_code,
                    properties,
                    reason_string,
                }
            } else {
                Self {
                    packet_id,
                    reason_code,
                    ..Default::default()
                }
            }
        } else {
            Self {
                packet_id,
                ..Default::default()
            }
        };

        Ok(ack)
    }
}

impl Default for PublishAck {
    fn default() -> Self {
        Self {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: PublishAckReason::Success,
            properties: UserProperties::default(),
            reason_string: None,
        }
    }
}

impl PublishAck2 {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let ack = if src.has_remaining() {
            let reason_code = src.get_u8().try_into()?;
            if src.has_remaining() {
                let (properties, reason_string) = ack_props::decode(src)?;
                ensure!(!src.has_remaining(), DecodeError::InvalidLength); // no data should be left in src
                Self {
                    packet_id,
                    reason_code,
                    properties,
                    reason_string,
                }
            } else {
                Self {
                    packet_id,
                    reason_code,
                    ..Default::default()
                }
            }
        } else {
            Self {
                packet_id,
                ..Default::default()
            }
        };

        Ok(ack)
    }
}

impl Default for PublishAck2 {
    fn default() -> Self {
        Self {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: PublishAck2Reason::Success,
            properties: UserProperties::default(),
            reason_string: None,
        }
    }
}

impl encode::EncodeLtd for PublishAck {
    fn encoded_size(&self, limit: u32) -> usize {
        let prop_len = ack_props::encoded_size(
            &self.properties,
            &self.reason_string,
            encode::reduce_limit(limit, HEADER_LEN as usize),
        );
        HEADER_LEN as usize + prop_len
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        self.packet_id.get().encode(buf)?;
        buf.put_u8(self.reason_code.into());
        ack_props::encode(
            &self.properties,
            &self.reason_string,
            buf,
            size - HEADER_LEN,
        )?;
        Ok(())
    }
}

impl encode::EncodeLtd for PublishAck2 {
    fn encoded_size(&self, limit: u32) -> usize {
        const HEADER_LEN: u32 = 2 + 1; // fixed header + packet id + reason code
        let prop_len = ack_props::encoded_size(
            &self.properties,
            &self.reason_string,
            encode::reduce_limit(limit, HEADER_LEN as usize),
        );
        HEADER_LEN as usize + prop_len
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        self.packet_id.get().encode(buf)?;
        buf.put_u8(self.reason_code.into());
        ack_props::encode(&self.properties, &self.reason_string, buf, size - 3)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(b"\xFF\xFF\x00\x00", 65535, PublishAckReason::Success, vec![], None; "success_empty")]
    #[test_case(b"\x00\x01", 1, PublishAckReason::Success, vec![], None; "success_no_reason")]
    #[test_case(b"\x01\x01\x00", 257, PublishAckReason::Success, vec![], None; "success_no_prop_len")]
    #[test_case(b"\x00\x01\x87", 1, PublishAckReason::NotAuthorized, vec![], None; "no_success_no_prop_len")]
    #[test_case(b"\x00\x01\x83\x00", 1, PublishAckReason::ImplementationSpecificError, vec![], None; "no_success_min")]
    #[test_case(b"\x00\xFF\x80\x0D\x26\x00\x01a\x00\x01b\x1F\x00\x03123", 255, PublishAckReason::UnspecifiedError, vec![("a", "b")], Some("123"); "all_out")]
    fn puback_decode_success(
        input: &'static [u8],
        packet_id: u16,
        reason_code: PublishAckReason,
        properties: Vec<(&'static str, &'static str)>,
        reason_string: Option<&'static str>,
    ) {
        let mut input = input.into();
        let result = PublishAck::decode(&mut input);
        assert_eq!(
            result,
            Ok(PublishAck {
                packet_id: packet_id.try_into().unwrap(),
                reason_code,
                properties: properties
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
                reason_string: reason_string.map(Into::into)
            })
        );
        assert_eq!(input.len(), 0);
    }

    #[test_case(b"\x00\x00", DecodeError::MalformedPacket; "packet_id_zero")]
    #[test_case(b"\x00\x01\x00\x01", DecodeError::InvalidLength; "properties_promised")]
    fn puback_decode_must_fail(input: &'static [u8], error: DecodeError) {
        let mut input = input.into();
        let result = PublishAck::decode(&mut input);
        assert_eq!(result, Err(error));
    }
}
