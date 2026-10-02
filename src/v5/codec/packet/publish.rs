use std::{num::NonZeroU16, num::NonZeroU32};

use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError};
use crate::types::{QoS, packet_type};
use crate::utils::{self, Decode, Encode, Property, write_variable_length};
use crate::v5::codec::{UserProperties, encode, property_type as pt};

/// PUBLISH message
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Publish {
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub dup: bool,
    /// the Server must store the Application Message and deliver it to future
    /// matching subscribers.
    pub retain: bool,
    /// the level of assurance for delivery of an Application Message.
    pub qos: QoS,
    /// only present in PUBLISH Packets where the `QoS` level is 1 or 2.
    pub packet_id: Option<NonZeroU16>,
    /// the Topic Name the Application Message is published to.
    pub topic: ByteString,
    /// size of the Application Message payload, in bytes.
    pub payload_size: u32,
    /// PUBLISH properties.
    pub properties: PublishProperties,
}

/// PUBLISH message properties
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct PublishProperties {
    /// Topic Alias property, an integer value used in place of the Topic Name.
    pub topic_alias: Option<NonZeroU16>,
    /// Correlation Data property, used by the sender of the request to identify the response.
    pub correlation_data: Option<Bytes>,
    /// Message Expiry Interval in seconds, 0 is a valid value (MQTT 5.0, 3.3.2.3.3)
    pub message_expiry_interval: Option<u32>,
    /// Content Type property, describes the content of the Application Message.
    pub content_type: Option<ByteString>,
    /// User Property pairs, additional diagnostic or other information.
    pub user_properties: UserProperties,
    /// Payload Format Indicator property, `true` if the payload is UTF-8 encoded
    /// character data.
    pub is_utf8_payload: bool,
    /// Response Topic property, the topic name for a response message.
    pub response_topic: Option<ByteString>,
    /// Subscription Identifier properties of the subscriptions matching this message.
    pub subscription_ids: Vec<NonZeroU32>,
}

impl Default for Publish {
    fn default() -> Publish {
        Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
            topic: ByteString::new(),
            payload_size: 0,
            properties: PublishProperties::default(),
        }
    }
}

impl Publish {
    pub(crate) fn decode(
        src: &mut Bytes,
        packet_flags: u8,
        payload_size: u32,
    ) -> Result<Self, DecodeError> {
        let topic = ByteString::decode(src)?;
        let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?;
        let dup = (packet_flags & 0b1000) == 0b1000;
        // DUP flag must be 0 for QoS 0 messages, [MQTT-3.3.1-2] (MQTT 5.0, 3.3.1.1)
        ensure!(!dup || qos != QoS::AtMostOnce, DecodeError::MalformedPacket);
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(NonZeroU16::decode(src)?) // packet id = 0 encountered
        };
        let properties = parse_publish_properties(src)?;
        // topic name must be at least one character long, [MQTT-4.7.3-1] (MQTT 5.0, 4.7.3),
        // unless a topic alias is used (MQTT 5.0, 3.3.2.3.4)
        ensure!(
            !topic.is_empty() || properties.topic_alias.is_some(),
            DecodeError::MalformedPacket
        );

        Ok(Self {
            qos,
            topic,
            packet_id,
            properties,
            payload_size,
            dup,
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
        let mut len = u32::from(u16::from_be_bytes([src[0], src[1]])) + 2;

        // packet-id len
        let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?;
        if qos != QoS::AtMostOnce {
            len += 2; // len of u16
        }
        if src.remaining() < len as usize {
            return Ok(None);
        }

        // properties len
        if let Some((prop_len, pos)) = utils::decode_variable_length_v5(&src[len as usize..])? {
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
                let id = utils::decode_variable_length_cursor_v5(prop_src)?;
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

impl encode::EncodeLtd for Publish {
    fn encoded_size(&self, limit: u32) -> usize {
        let packet_id_size = if self.qos == QoS::AtMostOnce { 0 } else { 2 };
        self.topic.encoded_size()
            + packet_id_size
            + self.properties.encoded_size(limit)
            + self.payload_size as usize
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        // publish fixed headers
        buf.put_u8(
            packet_type::PUBLISH_START
                | (u8::from(self.qos) << 1)
                | (u8::from(self.dup) << 3)
                | u8::from(self.retain),
        );
        utils::write_variable_length(size, buf);

        // publish headers
        let start_len = buf.len();

        self.topic.encode(buf)?;
        if self.qos != QoS::AtMostOnce {
            self.packet_id
                .ok_or(EncodeError::PacketIdRequired)?
                .encode(buf)?;
        }
        self.properties.encode(
            buf,
            size - (buf.len() - start_len + self.payload_size as usize) as u32,
        )?;

        Ok(())
    }
}

impl encode::EncodeLtd for PublishProperties {
    fn encoded_size(&self, _limit: u32) -> usize {
        let prop_len = encode::encoded_property_size(&self.topic_alias)
            + encode::encoded_property_size(&self.correlation_data)
            + encode::encoded_property_size(&self.message_expiry_interval)
            + encode::encoded_property_size(&self.content_type)
            + encode::encoded_property_size_default(&self.is_utf8_payload, false)
            + encode::encoded_property_size(&self.response_topic)
            + self.subscription_ids.iter().fold(0, |acc, id| {
                acc + 1 + encode::var_int_len(id.get() as usize) as usize
            })
            + self.user_properties.encoded_size();
        prop_len + encode::var_int_len(prop_len) as usize
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        let prop_len = encode::var_int_len_from_size(size);
        utils::write_variable_length(prop_len, buf);
        encode::encode_property(&self.topic_alias, pt::TOPIC_ALIAS, buf)?;
        encode::encode_property(&self.correlation_data, pt::CORR_DATA, buf)?;
        encode::encode_property(&self.message_expiry_interval, pt::MSG_EXPIRY_INT, buf)?;
        encode::encode_property(&self.content_type, pt::CONTENT_TYPE, buf)?;
        encode::encode_property_default(&self.is_utf8_payload, false, pt::UTF8_PAYLOAD, buf)?;
        encode::encode_property(&self.response_topic, pt::RESP_TOPIC, buf)?;
        for sub_id in &self.subscription_ids {
            buf.put_u8(pt::SUB_ID);
            write_variable_length(sub_id.get(), buf);
        }
        self.user_properties.encode(buf)
    }
}
