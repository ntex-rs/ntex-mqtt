use std::{num::NonZeroU16, num::NonZeroU32};

use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError};
use crate::types::{QoS, packet_type};
use crate::utils::{self, Decode, Encode, Property, write_variable_length};
use crate::v5::codec::{UserProperties, encode, property_type as pt};

/// Publish metadata whose payload is transferred separately through the codec.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Publish {
    /// Whether this may be a redelivery; must be `false` at `QoS::AtMostOnce`.
    pub dup: bool,
    /// RETAIN flag, requesting storage when sent to the server.
    ///
    /// For server-to-client delivery, its meaning also depends on the subscription's
    /// retain options and whether the message is a stored retained message.
    pub retain: bool,
    /// Delivery assurance level for the application message.
    pub qos: QoS,
    /// Required at `QoS::AtLeastOnce` or `QoS::ExactlyOnce`; absent at `QoS::AtMostOnce`.
    pub packet_id: Option<NonZeroU16>,
    /// Destination topic name, which may be empty only when a topic alias is present.
    pub topic: ByteString,
    /// Total payload byte count, including all chunks sent or received separately from this header.
    pub payload_size: u32,
    /// PUBLISH properties.
    pub properties: PublishProperties,
}

/// PUBLISH message properties
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct PublishProperties {
    /// Topic alias identifying a connection-local topic mapping.
    ///
    /// A nonempty topic establishes the mapping; an empty topic uses an existing one.
    /// The codec carries the alias but does not resolve or validate its connection state.
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
    /// Decodes publish metadata from the header and properties, with a known payload byte count.
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

    /// Determines the header-plus-properties byte count, or waits for enough bytes to determine it.
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

/// Consumes the publish property block, rejecting unknown or duplicate single-use properties.
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

#[cfg(test)]
mod tests {
    use ntex_codec::{Decoder, Encoder};
    use test_case::test_case;

    use super::*;
    use crate::v5::codec::{Codec, Decoded, Encoded};

    /// STATEMENT: Publish properties preserve binary data, repeated identifiers, and duplicate user-property keys.
    ///
    /// HOW:
    /// - Round-trip a retained, duplicate QoS-one publish containing every property and binary payload.
    /// - Reject a one-byte-too-small inbound limit without consuming input, then accept the exact frame size.
    /// - Compare the complete metadata and payload.
    /// - Check repeated properties retain their order and the frame leaves no buffered bytes.
    #[test]
    fn test_publish_properties_roundtrip() {
        let payload = Bytes::from_static(b"\x00data\xff");
        let publish = Publish {
            dup: true,
            retain: true,
            qos: QoS::AtLeastOnce,
            packet_id: NonZeroU16::new(7),
            topic: "events/status".into(),
            payload_size: payload.len() as u32,
            properties: PublishProperties {
                topic_alias: NonZeroU16::new(3),
                correlation_data: Some(Bytes::from_static(b"\x00\xff")),
                message_expiry_interval: Some(0),
                content_type: Some("application/octet-stream".into()),
                user_properties: vec![("key".into(), "one".into()), ("key".into(), "two".into())],
                is_utf8_payload: false,
                response_topic: Some("response/topic".into()),
                subscription_ids: vec![
                    NonZeroU32::MIN,
                    NonZeroU32::new(268_435_455).unwrap(),
                    NonZeroU32::MIN,
                ],
            },
        };
        for is_utf8_payload in [false, true] {
            let mut publish = publish.clone();
            publish.properties.is_utf8_payload = is_utf8_payload;
            let payload = if is_utf8_payload {
                Bytes::from_static(b"data")
            } else {
                payload.clone()
            };
            publish.payload_size = payload.len() as u32;
            let mut output = BytePages::default();
            Codec::new()
                .encode(
                    Encoded::Publish(publish.clone(), Some(payload.clone())),
                    &mut output,
                )
                .unwrap();
            let output = output.freeze();
            let mut input = BytesMut::from(output.as_ref());
            let codec = Codec::new();
            codec.set_max_inbound_size(output.len() as u32 - 1);
            assert_eq!(
                codec.decode(&mut input),
                Err(DecodeError::MaxSizeExceeded {
                    size: output.len() as u32,
                    max_size: output.len() as u32 - 1,
                })
            );
            assert_eq!(input.as_ref(), output.as_ref());
            codec.set_max_inbound_size(output.len() as u32);
            let Some(Decoded::Publish(decoded, decoded_payload, _, _)) =
                codec.decode(&mut input).unwrap()
            else {
                panic!("expected a complete publish");
            };
            assert_eq!(decoded, publish);
            assert_eq!(decoded_payload, payload);
            assert!(input.is_empty());
        }
    }

    /// STATEMENT: Publish properties reject unknown identifiers, repeated singleton values, and malformed values.
    ///
    /// HOW:
    /// - Place each malformed property block after a valid topic in a publish header.
    /// - Decode it and compare with the expected malformed-packet or truncated-field error.
    #[test_case(b"\x7f", DecodeError::MalformedPacket; "unknown")]
    #[test_case(b"\x01\x01\x01\x00", DecodeError::MalformedPacket; "duplicate_format")]
    #[test_case(b"\x01\x02", DecodeError::MalformedPacket; "invalid_format")]
    #[test_case(b"\x02\x00\x00\x00\x01\x02\x00\x00\x00\x02", DecodeError::MalformedPacket; "duplicate_expiry")]
    #[test_case(b"\x03\x00\x00\x03\x00\x00", DecodeError::MalformedPacket; "duplicate_content_type")]
    #[test_case(b"\x08\x00\x01a\x08\x00\x01b", DecodeError::MalformedPacket; "duplicate_response_topic")]
    #[test_case(b"\x09\x00\x00\x09\x00\x00", DecodeError::MalformedPacket; "duplicate_correlation_data")]
    #[test_case(b"\x23\x00\x01\x23\x00\x02", DecodeError::MalformedPacket; "duplicate_alias")]
    #[test_case(b"\x23\x00\x00", DecodeError::MalformedPacket; "zero_alias")]
    #[test_case(b"\x0b\x00", DecodeError::MalformedPacket; "zero_subscription_id")]
    #[test_case(b"\x03\x00\x02a", DecodeError::InvalidLength; "truncated_content_type")]
    #[test_case(b"\x09\x00\x02a", DecodeError::InvalidLength; "truncated_correlation_data")]
    fn test_malformed_publish_properties(properties: &'static [u8], error: DecodeError) {
        let mut input = BytesMut::from(&b"\x00\x01a"[..]);
        input.extend_from_slice(&[properties.len() as u8]);
        input.extend_from_slice(properties);
        assert_eq!(Publish::decode(&mut input.freeze(), 0x30, 0), Err(error));
    }
}
