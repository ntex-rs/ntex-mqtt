use ntex::util::{Buf, BufMut, ByteString, Bytes, BytesMut};
use std::{convert::TryInto, num::NonZeroU16};

use super::ack_props;
use crate::error::{DecodeError, EncodeError};
use crate::utils::{Decode, Encode};
use crate::v5::codec::{encode::*, UserProperties};

const HEADER_LEN: u32 = 2 + 1; // packet id + reason code

/// PUBACK/PUBREC message content
#[derive(Debug, PartialEq, Clone)]
pub struct PublishAck {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    pub reason_code: PublishAckReason,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
}

/// PUBREL/PUBCOMP message content
#[derive(Debug, PartialEq, Clone)]
pub struct PublishAck2 {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    pub reason_code: PublishAck2Reason,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
}

prim_enum! {
    /// PUBACK / PUBREC reason codes
    pub enum PublishAckReason {
        Success = 0,
        NoMatchingSubscribers = 16,
        UnspecifiedError = 128,
        ImplementationSpecificError = 131,
        NotAuthorized = 135,
        TopicNameInvalid = 144,
        PacketIdentifierInUse = 145,
        QuotaExceeded = 151,
        PayloadFormatInvalid = 153
    }
}

prim_enum! {
    /// PUBREL / PUBCOMP reason codes
    pub enum PublishAck2Reason {
        Success = 0,
        PacketIdNotFound = 146
    }
}

impl PublishAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let (reason_code, properties, reason_string) = if src.has_remaining() {
            let reason_code = src.get_u8().try_into()?;
            let (properties, reason_string) = ack_props::decode(src)?;
            ensure!(!src.has_remaining(), DecodeError::InvalidLength); // no bytes should be left
            (reason_code, properties, reason_string)
        } else {
            (PublishAckReason::Success, UserProperties::default(), None)
        };

        Ok(Self { packet_id, reason_code, properties, reason_string })
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
        let (reason_code, properties, reason_string) = if src.has_remaining() {
            let reason_code = src.get_u8().try_into()?;
            let (properties, reason_string) = ack_props::decode(src)?;
            ensure!(!src.has_remaining(), DecodeError::InvalidLength); // no bytes should be left
            (reason_code, properties, reason_string)
        } else {
            (PublishAck2Reason::Success, UserProperties::default(), None)
        };

        Ok(Self { packet_id, reason_code, properties, reason_string })
    }
}

impl EncodeLtd for PublishAck {
    fn encoded_size(&self, limit: u32) -> usize {
        let prop_len = ack_props::encoded_size(
            &self.properties,
            &self.reason_string,
            limit - HEADER_LEN - 4,
        ); // limit - HEADER_LEN - len(packet_len.max())
        HEADER_LEN as usize + prop_len
    }

    fn encode(&self, buf: &mut BytesMut, size: u32) -> Result<(), EncodeError> {
        self.packet_id.get().encode(buf)?;
        buf.put_u8(self.reason_code.into());
        ack_props::encode(&self.properties, &self.reason_string, buf, size - HEADER_LEN)?;
        Ok(())
    }
}

impl EncodeLtd for PublishAck2 {
    fn encoded_size(&self, limit: u32) -> usize {
        const HEADER_LEN: u32 = 2 + 1; // fixed header + packet id + reason code
        let prop_len = ack_props::encoded_size(
            &self.properties,
            &self.reason_string,
            limit - HEADER_LEN - 4,
        ); // limit - HEADER_LEN - prop_len.max()
        HEADER_LEN as usize + prop_len
    }

    fn encode(&self, buf: &mut BytesMut, size: u32) -> Result<(), EncodeError> {
        self.packet_id.get().encode(buf)?;
        buf.put_u8(self.reason_code.into());
        ack_props::encode(&self.properties, &self.reason_string, buf, size - 3)?;
        Ok(())
    }
}
