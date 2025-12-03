use std::{num::NonZeroU16, num::NonZeroU32};

use ntex_bytes::{Buf, BufMut, ByteString, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError};
use crate::types::{QoS, packet_type};
use crate::utils::{self, Decode, Encode, Property, write_variable_length};
use crate::v5::codec::{UserProperties, encode::*, property_type as pt};

/// PUBLISH message
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Publish {
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub dup: bool,
    pub retain: bool,
    /// the level of assurance for delivery of an Application Message.
    pub qos: QoS,
    /// only present in PUBLISH Packets where the QoS level is 1 or 2.
    pub packet_id: Option<NonZeroU16>,
    pub topic: ByteString,
    pub payload_size: u32,
    pub properties: PublishProperties,
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct PublishProperties {
    pub topic_alias: Option<NonZeroU16>,
    pub correlation_data: Option<Bytes>,
    pub message_expiry_interval: Option<NonZeroU32>,
    pub content_type: Option<ByteString>,
    pub user_properties: UserProperties,
    pub is_utf8_payload: bool,
    pub response_topic: Option<ByteString>,
    pub subscription_ids: Vec<NonZeroU32>,
}

impl Publish {
    pub(crate) fn decode(
        src: &mut Bytes,
        packet_flags: u8,
        payload_size: u32,
    ) -> Result<Self, DecodeError> {
        let topic = ByteString::decode(src)?;
        let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?;
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(NonZeroU16::decode(src)?) // packet id = 0 encountered
        };
        let properties = parse_publish_properties(src)?;

        Ok(Self {
            qos,
            topic,
            packet_id,
            properties,
            payload_size,
            dup: (packet_flags & 0b1000) == 0b1000,
            retain: (packet_flags & 0b0001) == 0b0001,
        })
    }

    pub(crate) fn packet_header_size(
        src: &BytesMut,
        packet_flags: u8,
    ) -> Result<Option<u32>, DecodeError> {
        if src.remaining() < 2 {
            return Ok(None);
        }

        // topic len
        let mut len = u16::from_be_bytes([src[0], src[1]]) as u32 + 2;

        // packet-id len
        let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?;
        if qos != QoS::AtMostOnce {
            len += 2; // len of u16
        }
        if src.remaining() < len as usize {
            return Ok(None);
        }

        // properties len
        if let Some((prop_len, pos)) = utils::decode_variable_length(&src[len as usize..])? {
            Ok(Some(len + prop_len + pos as u32))
        } else {
            Ok(None)
        }
    }
}

fn parse_publish_properties(src: &mut Bytes) -> Result<PublishProperties, DecodeError> {
    let prop_src = &mut utils::take_properties(src)?;

    let mut message_expiry_interval = None;
    let mut topic_alias = None;
    let mut content_type = None;
    let mut correlation_data = None;
    let mut subscription_ids = Vec::new();
    let mut response_topic = None;
    let mut is_utf8_payload = None;
    let mut user_props = Vec::new();

    while prop_src.has_remaining() {
        match prop_src.get_u8() {
            pt::UTF8_PAYLOAD => is_utf8_payload.read_value(prop_src)?,
            pt::MSG_EXPIRY_INT => message_expiry_interval.read_value(prop_src)?,
            pt::CONTENT_TYPE => content_type.read_value(prop_src)?,
            pt::RESP_TOPIC => response_topic.read_value(prop_src)?,
            pt::CORR_DATA => correlation_data.read_value(prop_src)?,
            pt::SUB_ID => {
                let id = utils::decode_variable_length_cursor(prop_src)?;
                subscription_ids.push(NonZeroU32::new(id).ok_or(DecodeError::MalformedPacket)?);
            }
            pt::TOPIC_ALIAS => topic_alias.read_value(prop_src)?,
            pt::USER => user_props.push(<(ByteString, ByteString)>::decode(prop_src)?),
            _ => return Err(DecodeError::MalformedPacket),
        }
    }

    Ok(PublishProperties {
        message_expiry_interval,
        topic_alias,
        content_type,
        correlation_data,
        subscription_ids,
        response_topic,
        is_utf8_payload: is_utf8_payload.unwrap_or(false),
        user_properties: user_props,
    })
}

impl EncodeLtd for Publish {
    fn encoded_size(&self, _limit: u32) -> usize {
        let packet_id_size = if self.qos == QoS::AtMostOnce { 0 } else { 2 };
        self.topic.encoded_size()
            + packet_id_size
            + self.properties.encoded_size(_limit)
            + self.payload_size as usize
    }

    fn encode(&self, buf: &mut BytesMut, size: u32) -> Result<(), EncodeError> {
        // publish fixed headers
        buf.put_u8(
            packet_type::PUBLISH_START
                | (u8::from(self.qos) << 1)
                | ((self.dup as u8) << 3)
                | (self.retain as u8),
        );
        utils::write_variable_length(size, buf);

        // publish headers
        let start_len = buf.len();

        self.topic.encode(buf)?;
        if self.qos == QoS::AtMostOnce {
            if self.packet_id.is_some() {
                return Err(EncodeError::MalformedPacket); // packet id must not be set
            }
        } else {
            self.packet_id.ok_or(EncodeError::PacketIdRequired)?.encode(buf)?;
        }
        self.properties
            .encode(buf, size - (buf.len() - start_len + self.payload_size as usize) as u32)?;

        Ok(())
    }
}

impl EncodeLtd for PublishProperties {
    fn encoded_size(&self, _limit: u32) -> usize {
        let prop_len = encoded_property_size(&self.topic_alias)
            + encoded_property_size(&self.correlation_data)
            + encoded_property_size(&self.message_expiry_interval)
            + encoded_property_size(&self.content_type)
            + encoded_property_size_default(&self.is_utf8_payload, false)
            + encoded_property_size(&self.response_topic)
            + self
                .subscription_ids
                .iter()
                .fold(0, |acc, id| acc + 1 + var_int_len(id.get() as usize) as usize)
            + self.user_properties.encoded_size();
        prop_len + var_int_len(prop_len) as usize
    }

    fn encode(&self, buf: &mut BytesMut, size: u32) -> Result<(), EncodeError> {
        let prop_len = var_int_len_from_size(size);
        utils::write_variable_length(prop_len, buf);
        encode_property(&self.topic_alias, pt::TOPIC_ALIAS, buf)?;
        encode_property(&self.correlation_data, pt::CORR_DATA, buf)?;
        encode_property(&self.message_expiry_interval, pt::MSG_EXPIRY_INT, buf)?;
        encode_property(&self.content_type, pt::CONTENT_TYPE, buf)?;
        encode_property_default(&self.is_utf8_payload, false, pt::UTF8_PAYLOAD, buf)?;
        encode_property(&self.response_topic, pt::RESP_TOPIC, buf)?;
        for sub_id in self.subscription_ids.iter() {
            buf.put_u8(pt::SUB_ID);
            write_variable_length(sub_id.get(), buf);
        }
        self.user_properties.encode(buf)
    }
}
