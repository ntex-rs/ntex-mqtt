#![allow(clippy::ref_option, clippy::needless_pass_by_value)]
use std::num::NonZeroU32;

use ntex_bytes::{BufMut, BytePages, ByteString, Bytes};

use super::packet::{Packet, Publish, property_type as pt};
use super::{UserProperties, UserProperty};
use crate::error::EncodeError;
use crate::types::{MAX_PACKET_SIZE, QoS, packet_type};
use crate::utils::{
    Encode, is_valid_bin, is_valid_str, is_valid_topic_name, write_variable_length,
};

fn is_valid_opt_str(s: &Option<ByteString>) -> bool {
    s.as_deref().is_none_or(is_valid_str)
}

fn is_valid_opt_bin(b: &Option<Bytes>) -> bool {
    b.as_deref().is_none_or(is_valid_bin)
}

/// Response Topic is used as the Topic Name of the response message, it must not
/// contain wildcard characters, [MQTT-3.3.2-14] (MQTT 5.0, 3.3.2.3.5, 3.1.3.2.6)
fn is_valid_response_topic(topic: &Option<ByteString>) -> bool {
    topic.as_deref().is_none_or(is_valid_topic_name)
}

/// Shared Subscription Topic Filters start with `$share/` (MQTT 5.0, 4.8.2)
fn is_shared_filter(filter: &str) -> bool {
    filter.starts_with("$share/")
}

fn is_valid_props(props: &UserProperties) -> bool {
    props
        .iter()
        .all(|(k, v)| is_valid_str(k) && is_valid_str(v))
}

/// Subscription Identifier is a Variable Byte Integer in the range 1 to 268,435,455,
/// (MQTT 5.0, 3.3.2.3.8, 3.8.2.1.2, 1.5.5)
fn is_valid_sub_id(id: NonZeroU32) -> bool {
    id.get() <= MAX_PACKET_SIZE
}

/// Checks the rules a sender must follow before anything gets written,
/// an invalid packet must not leave partial data in the write buffer.
///
/// Strings, including user properties, must not include U+0000,
/// [MQTT-1.5.4-2] (MQTT 5.0, 1.5.4), strings and binary data are limited
/// to 65,535 bytes (MQTT 5.0, 1.5.4, 1.5.6).
pub(super) fn validate(packet: &Packet) -> Result<(), EncodeError> {
    let valid = match packet {
        Packet::Connect(connect) => {
            is_valid_str(&connect.client_id)
                && is_valid_opt_str(&connect.username)
                && is_valid_opt_bin(&connect.password)
                && is_valid_opt_str(&connect.auth_method)
                && is_valid_opt_bin(&connect.auth_data)
                // Authentication Data requires Authentication Method (MQTT 5.0, 3.1.2.11.10)
                && (connect.auth_data.is_none() || connect.auth_method.is_some())
                && is_valid_props(&connect.user_properties)
                // Will Topic is a topic name, [MQTT-4.7.3-1], [MQTT-4.7.0-1] (MQTT 5.0, 4.7)
                && connect.last_will.as_ref().is_none_or(|will| {
                    is_valid_topic_name(&will.topic)
                        && is_valid_bin(&will.message)
                        && is_valid_opt_bin(&will.correlation_data)
                        && is_valid_opt_str(&will.content_type)
                        && is_valid_response_topic(&will.response_topic)
                        && is_valid_props(&will.user_properties)
                })
        }
        // Maximum Packet Size of zero is a Protocol Error (MQTT 5.0, 3.2.2.3.6)
        Packet::ConnectAck(ack) => {
            ack.max_packet_size != Some(0)
                && is_valid_opt_str(&ack.assigned_client_id)
                && is_valid_opt_str(&ack.response_info)
                && is_valid_opt_str(&ack.server_reference)
                && is_valid_opt_str(&ack.auth_method)
                && is_valid_opt_bin(&ack.auth_data)
                && is_valid_opt_str(&ack.reason_string)
                && is_valid_props(&ack.user_properties)
        }
        Packet::PublishAck(ack) | Packet::PublishReceived(ack) => {
            is_valid_opt_str(&ack.reason_string) && is_valid_props(&ack.properties)
        }
        Packet::PublishRelease(ack) | Packet::PublishComplete(ack) => {
            is_valid_opt_str(&ack.reason_string) && is_valid_props(&ack.properties)
        }
        // [MQTT-3.8.3-2] at least one topic filter is required (5.0, 3.8.3)
        // [MQTT-4.7.3-1] topic filters must be at least one character long (5.0, 4.7.3)
        // [MQTT-3.8.3-4] No Local must not be set on a Shared Subscription (5.0, 3.8.3.1)
        Packet::Subscribe(sub) => {
            !sub.topic_filters.is_empty()
                && sub.topic_filters.iter().all(|(f, opts)| {
                    !f.is_empty() && is_valid_str(f) && !(opts.no_local && is_shared_filter(f))
                })
                && sub.id.is_none_or(is_valid_sub_id)
                && is_valid_props(&sub.user_properties)
        }
        Packet::SubscribeAck(ack) => {
            is_valid_opt_str(&ack.reason_string) && is_valid_props(&ack.properties)
        }
        // [MQTT-3.10.3-2] at least one topic filter is required (5.0, 3.10.3)
        // [MQTT-4.7.3-1] topic filters must be at least one character long (5.0, 4.7.3)
        Packet::Unsubscribe(unsub) => {
            !unsub.topic_filters.is_empty()
                && unsub
                    .topic_filters
                    .iter()
                    .all(|f| !f.is_empty() && is_valid_str(f))
                && is_valid_props(&unsub.user_properties)
        }
        Packet::UnsubscribeAck(ack) => {
            is_valid_opt_str(&ack.reason_string) && is_valid_props(&ack.properties)
        }
        Packet::Disconnect(disconnect) => {
            is_valid_opt_str(&disconnect.server_reference)
                && is_valid_opt_str(&disconnect.reason_string)
                && is_valid_props(&disconnect.user_properties)
        }
        // Authentication Method is required, unless AUTH is encoded with
        // a Remaining Length of 0 (MQTT 5.0, 3.15.2.1, 3.15.2.2.2)
        Packet::Auth(auth) => {
            (auth.auth_method.is_some() || auth.is_short_form())
                && is_valid_opt_str(&auth.auth_method)
                && is_valid_opt_bin(&auth.auth_data)
                && is_valid_opt_str(&auth.reason_string)
                && is_valid_props(&auth.user_properties)
        }
        Packet::PingRequest | Packet::PingResponse => true,
    };
    ensure!(valid, EncodeError::MalformedPacket);
    Ok(())
}

/// Checks the rules a sender must follow before anything gets written.
pub(super) fn validate_publish(publish: &Publish) -> Result<(), EncodeError> {
    // topic name must not contain wildcards, [MQTT-3.3.2-2] (MQTT 5.0, 3.3.2.1),
    // it must not be empty, [MQTT-4.7.3-1] (MQTT 5.0, 4.7.3),
    // unless a topic alias is used (MQTT 5.0, 3.3.2.3.4)
    ensure!(
        is_valid_topic_name(&publish.topic)
            || (publish.topic.is_empty() && publish.properties.topic_alias.is_some()),
        EncodeError::MalformedPacket
    );
    // strings must not include U+0000, [MQTT-1.5.4-2] (MQTT 5.0, 1.5.4),
    // strings and binary data are limited to 65,535 bytes (MQTT 5.0, 1.5.4, 1.5.6)
    ensure!(
        is_valid_opt_bin(&publish.properties.correlation_data)
            && is_valid_opt_str(&publish.properties.content_type)
            && is_valid_response_topic(&publish.properties.response_topic)
            && is_valid_props(&publish.properties.user_properties),
        EncodeError::MalformedPacket
    );
    ensure!(
        publish
            .properties
            .subscription_ids
            .iter()
            .all(|id| is_valid_sub_id(*id)),
        EncodeError::MalformedPacket
    );
    if publish.qos == QoS::AtMostOnce {
        // DUP flag must be 0 for QoS 0 messages, [MQTT-3.3.1-2] (MQTT 5.0, 3.3.1.1)
        ensure!(!publish.dup, EncodeError::MalformedPacket);
        // QoS 0 PUBLISH must not contain a Packet Identifier, [MQTT-2.2.1-2] (MQTT 5.0, 2.2.1)
        ensure!(publish.packet_id.is_none(), EncodeError::MalformedPacket);
    } else {
        ensure!(publish.packet_id.is_some(), EncodeError::PacketIdRequired);
    }
    Ok(())
}

pub(crate) trait EncodeLtd {
    fn encoded_size(&self, limit: u32) -> usize;

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError>;
}

impl EncodeLtd for Packet {
    fn encoded_size(&self, limit: u32) -> usize {
        // limit -= 5; // fixed header = 1, var_len(remaining.max_value()) = 4
        match self {
            Packet::Connect(connect) => connect.encoded_size(limit),
            Packet::ConnectAck(ack) => ack.encoded_size(limit),
            Packet::PublishAck(ack) | Packet::PublishReceived(ack) => ack.encoded_size(limit),
            Packet::PublishRelease(ack) | Packet::PublishComplete(ack) => ack.encoded_size(limit),
            Packet::Subscribe(sub) => sub.encoded_size(limit),
            Packet::SubscribeAck(ack) => ack.encoded_size(limit),
            Packet::Unsubscribe(unsub) => unsub.encoded_size(limit),
            Packet::UnsubscribeAck(ack) => ack.encoded_size(limit),
            Packet::PingRequest | Packet::PingResponse => 0,
            Packet::Disconnect(disconnect) => disconnect.encoded_size(limit),
            Packet::Auth(auth) => auth.encoded_size(limit),
        }
    }

    fn encode(&self, buf: &mut BytePages, check_size: u32) -> Result<(), EncodeError> {
        match self {
            Packet::Connect(connect) => {
                buf.put_u8(packet_type::CONNECT);
                write_variable_length(check_size, buf);
                connect.encode(buf, check_size)
            }
            Packet::ConnectAck(ack) => {
                buf.put_u8(packet_type::CONNACK);
                write_variable_length(check_size, buf);
                ack.encode(buf, check_size)
            }
            Packet::PublishAck(ack) => {
                buf.put_u8(packet_type::PUBACK);
                write_variable_length(check_size, buf);
                ack.encode(buf, check_size)
            }
            Packet::PublishReceived(ack) => {
                buf.put_u8(packet_type::PUBREC);
                write_variable_length(check_size, buf);
                ack.encode(buf, check_size)
            }
            Packet::PublishRelease(ack) => {
                buf.put_u8(packet_type::PUBREL);
                write_variable_length(check_size, buf);
                ack.encode(buf, check_size)
            }
            Packet::PublishComplete(ack) => {
                buf.put_u8(packet_type::PUBCOMP);
                write_variable_length(check_size, buf);
                ack.encode(buf, check_size)
            }
            Packet::Subscribe(sub) => {
                buf.put_u8(packet_type::SUBSCRIBE);
                write_variable_length(check_size, buf);
                sub.encode(buf, check_size)
            }
            Packet::SubscribeAck(ack) => {
                buf.put_u8(packet_type::SUBACK);
                write_variable_length(check_size, buf);
                ack.encode(buf, check_size)
            }
            Packet::Unsubscribe(unsub) => {
                buf.put_u8(packet_type::UNSUBSCRIBE);
                write_variable_length(check_size, buf);
                unsub.encode(buf, check_size)
            }
            Packet::UnsubscribeAck(ack) => {
                buf.put_u8(packet_type::UNSUBACK);
                write_variable_length(check_size, buf);
                ack.encode(buf, check_size)
            }
            Packet::PingRequest => {
                buf.put_slice(&[packet_type::PINGREQ, 0]);
                Ok(())
            }
            Packet::PingResponse => {
                buf.put_slice(&[packet_type::PINGRESP, 0]);
                Ok(())
            }
            Packet::Disconnect(disconnect) => {
                buf.put_u8(packet_type::DISCONNECT);
                write_variable_length(check_size, buf);
                disconnect.encode(buf, check_size)
            }
            Packet::Auth(auth) => {
                buf.put_u8(packet_type::AUTH);
                write_variable_length(check_size, buf);
                auth.encode(buf, check_size)
            }
        }
    }
}

pub(crate) fn encoded_size_opt_props(
    user_props: &[UserProperty],
    reason_str: &Option<ByteString>,
    mut limit: u32,
) -> usize {
    let mut len = 0;
    for up in user_props {
        let prop_len = 1 + up.encoded_size(); // prop type byte + key.len() + val.len()
        if prop_len > limit as usize {
            return len;
        }
        limit -= prop_len as u32;
        len += prop_len;
    }

    if let Some(reason) = reason_str {
        let reason_len = 1 + reason.encoded_size(); // string length is checked by `validate`
        if reason_len <= limit as usize {
            len += reason_len;
        }
    }

    len
}

pub(crate) fn encode_opt_props(
    user_props: &[UserProperty],
    reason_str: &Option<ByteString>,
    buf: &mut BytePages,
    mut size: u32,
) -> Result<(), EncodeError> {
    for up in user_props {
        let prop_len = 1 + up.0.encoded_size() + up.1.encoded_size(); // prop_type.len() + key.len() + val.len()
        if prop_len > size as usize {
            return Ok(());
        }
        buf.put_u8(pt::USER);
        up.encode(buf)?;
        size -= prop_len as u32; // safe: checked it's less already
    }

    if let Some(reason) = reason_str
        && reason.len() < size as usize
    {
        buf.put_u8(pt::REASON_STRING);
        reason.encode(buf)?;
    }

    // todo: debug_assert remaining is 0

    Ok(())
}

pub(super) fn encoded_property_size<T: Encode>(v: &Option<T>) -> usize {
    v.as_ref().map_or(0, |v| 1 + v.encoded_size()) // 1 - property type byte
}

pub(super) fn encoded_property_size_default<T: Encode + PartialEq>(v: &T, default: T) -> usize {
    if *v == default {
        0
    } else {
        1 + v.encoded_size() // 1 - property type byte
    }
}

pub(super) fn encode_property<T: Encode>(
    v: &Option<T>,
    prop_type: u8,
    buf: &mut BytePages,
) -> Result<(), EncodeError> {
    if let Some(v) = v {
        buf.put_u8(prop_type);
        v.encode(buf)
    } else {
        Ok(())
    }
}

pub(super) fn encode_property_default<T: Encode + PartialEq>(
    v: &T,
    default: T,
    prop_type: u8,
    buf: &mut BytePages,
) -> Result<(), EncodeError> {
    if *v == default {
        Ok(())
    } else {
        buf.put_u8(prop_type);
        v.encode(buf)
    }
}

/// Calculates length of variable length integer based on its value
pub(crate) fn var_int_len(val: usize) -> u32 {
    #[cfg(target_pointer_width = "16")]
    panic!("16-bit platforms are not supported");
    #[cfg(target_pointer_width = "32")]
    const MAP: [u32; 33] = [
        5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1,
        1, 1, 1,
    ];
    #[cfg(target_pointer_width = "64")]
    const MAP: [u32; 65] = [
        10, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5,
        5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1,
        1, 1, 1, 1, 1,
    ];
    let zeros = val.leading_zeros();
    unsafe { *MAP.get_unchecked(zeros as usize) } // safety: zeros will never be more than 65 by definition.
}

/// Calculates length of variable length integer based on its value
pub(crate) fn var_int_len_u32(val: u32) -> u32 {
    const MAP: [u32; 33] = [
        5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1,
        1, 1, 1,
    ];
    let zeros = val.leading_zeros();
    unsafe { *MAP.get_unchecked(zeros as usize) } // safety: zeros will never be more than 32 by definition.
}

/// Calculates `len` from `var_int_len(len) + len` value
pub(crate) fn var_int_len_from_size(val: u32) -> u32 {
    let over_size = var_int_len_u32(val);
    let res = val - over_size + 1;
    val - var_int_len_u32(res)
}

impl Encode for UserProperties {
    fn encoded_size(&self) -> usize {
        let mut len = 0;
        for prop in self {
            len += 1 + prop.encoded_size();
        }
        len
    }
    fn encode(&self, buf: &mut BytePages) -> Result<(), EncodeError> {
        for prop in self {
            buf.put_u8(pt::USER);
            prop.encode(buf)?;
        }
        Ok(())
    }
}

pub(super) fn reduce_limit(limit: u32, reduction: usize) -> u32 {
    if reduction > limit as usize {
        return 0;
    }
    limit - (reduction as u32) // safe: by now we're sure `reduction` fits in u32
}

#[cfg(test)]
mod tests {
    use ntex_bytes::Bytes;
    use std::num::{NonZeroU16, NonZeroU32};

    use super::*;
    use crate::types::{MAX_PACKET_SIZE, QoS};
    use crate::v5::codec::*;

    fn packet_id(v: u16) -> NonZeroU16 {
        NonZeroU16::new(v).unwrap()
    }

    #[test]
    fn test_encode_fixed_header() {
        let mut v = BytePages::default();
        let p = Packet::PingRequest;

        assert_eq!(p.encoded_size(MAX_PACKET_SIZE), 0);
        p.encode(&mut v, 0).unwrap();
        assert_eq!(&v.freeze()[..2], b"\xc0\x00".as_ref());

        let mut v = BytePages::default();

        let p = Publish {
            dup: true,
            retain: true,
            qos: QoS::ExactlyOnce,
            topic: ByteString::from_static("topic"),
            packet_id: Some(packet_id(0x4321)),
            payload_size: 255,
            properties: PublishProperties::default(),
        };

        //assert_eq!(p.encoded_size(MAX_PACKET_SIZE), 265);
        p.encode(&mut v, 265).unwrap();
        assert_eq!(&v.freeze()[..3], b"\x3d\x89\x02".as_ref());
    }

    fn assert_encode_packet(packet: &Packet, expected: &[u8]) {
        let mut v = BytePages::default();
        packet
            .encode(&mut v, packet.encoded_size(1024) as u32)
            .unwrap();
        assert_eq!(expected.len(), v.len());
        assert_eq!(expected, &v.freeze()[..]);
    }

    fn assert_encode_publish(packet: &Publish, pl: &[u8], expected: &[u8]) {
        let mut v = BytePages::default();
        packet
            .encode(&mut v, packet.encoded_size(1024) as u32)
            .unwrap();
        v.extend_from_slice(pl);
        assert_eq!(expected.len(), v.len());
        assert_eq!(expected, &v.freeze()[..]);
    }

    #[test]
    fn test_encode_connect_packets() {
        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: None,
                username: Some(ByteString::from_static("user")),
                password: Some(Bytes::from_static(b"pass")),
                session_expiry_interval_secs: 0,
                auth_method: None,
                auth_data: None,
                request_problem_info: true,
                request_response_info: false,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: vec![],
                max_packet_size: None,
            })),
            &b"\x10\x1E\x00\x04MQTT\x05\xC0\x00\x3C\x00\x00\
\x0512345\x00\x04user\x00\x04pass"[..],
        );

        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: ByteString::from_static("topic"),
                    message: Bytes::from_static(b"message"),
                    will_delay_interval_sec: None,
                    correlation_data: None,
                    message_expiry_interval: None,
                    content_type: None,
                    user_properties: vec![],
                    is_utf8_payload: None,
                    response_topic: None,
                }),
                username: None,
                password: None,
                session_expiry_interval_secs: 0,
                auth_method: None,
                auth_data: None,
                request_problem_info: true,
                request_response_info: false,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: vec![],
                max_packet_size: None,
            })),
            &b"\x10\x23\x00\x04MQTT\x05\x14\x00\x3C\x00\x00\
\x0512345\x00\x00\x05topic\x00\x07message"[..],
        );

        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: true,
                    topic: ByteString::from_static("topic"),
                    message: Bytes::from_static(b"message"),
                    will_delay_interval_sec: Some(5),
                    correlation_data: Some(Bytes::from_static(b"correlationData")),
                    message_expiry_interval: NonZeroU32::new(7),
                    content_type: Some(ByteString::from_static("contentType")),
                    user_properties: vec![
                        (ByteString::from_static("name"), ByteString::from_static("value"))
                    ],
                    is_utf8_payload: Some(true),
                    response_topic: Some(ByteString::from_static("responseTopic")),
                }),
                username: None,
                password: None,
                session_expiry_interval_secs: 0,
                auth_method: None,
                auth_data: None,
                request_problem_info: true,
                request_response_info: false,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: vec![],
                max_packet_size: None,
            })),
            &b"\x10\x6D\x00\x04MQTT\x05\x34\x00\x3C\x00\x00\
\x0512345\x4A\x18\0\0\0\x05\x01\x01\x02\0\0\0\x07\x03\0\x0bcontentType\x08\x00\x0dresponseTopic\x09\0\x0fcorrelationData\x26\0\x04name\0\x05value\x00\x05topic\x00\x07message"[..],
        );

        assert_encode_packet(
            &Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::NormalDisconnection,
                session_expiry_interval_secs: None,
                server_reference: None,
                reason_string: None,
                user_properties: vec![],
            }),
            b"\xe0\x02\x00\x00",
        );
    }

    #[test]
    fn test_encode_publish_packets() {
        assert_encode_publish(
            &Publish {
                dup: true,
                retain: true,
                qos: QoS::ExactlyOnce,
                topic: ByteString::from_static("topic"),
                packet_id: Some(packet_id(0x4321)),
                payload_size: 4,
                properties: PublishProperties::default(),
            },
            b"data",
            b"\x3d\x0E\x00\x05topic\x43\x21\x00data",
        );

        assert_encode_publish(
            &Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                topic: ByteString::from_static("topic"),
                packet_id: None,
                payload_size: 4,
                properties: PublishProperties::default(),
            },
            b"data",
            b"\x30\x0c\x00\x05topic\x00data",
        );

        assert_encode_publish(
            &Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                topic: ByteString::from_static("topic"),
                packet_id: None,
                payload_size: 4,
                properties: PublishProperties {
                    subscription_ids: vec![NonZeroU32::new(1).unwrap()],
                    ..Default::default()
                },
            },
            b"data",
            b"\x30\x0e\x00\x05topic\x02\x0b\x01data",
        );
    }

    #[test]
    fn test_encode_subscribe_packets() {
        assert_encode_packet(
            &Packet::Subscribe(Subscribe {
                packet_id: packet_id(0x1234),
                id: None,
                user_properties: Vec::new(),
                topic_filters: vec![
                    (
                        ByteString::from_static("test"),
                        SubscriptionOptions {
                            qos: QoS::AtLeastOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: RetainHandling::AtSubscribe,
                        },
                    ),
                    (
                        ByteString::from_static("filter"),
                        SubscriptionOptions {
                            qos: QoS::ExactlyOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: RetainHandling::AtSubscribe,
                        },
                    ),
                ],
            }),
            b"\x82\x13\x12\x34\x00\x00\x04test\x01\x00\x06filter\x02",
        );

        assert_encode_packet(
            &Packet::Subscribe(Subscribe {
                packet_id: packet_id(0x1234),
                id: Some(NonZeroU32::new(1).unwrap()),
                user_properties: Vec::new(),
                topic_filters: vec![
                    (
                        ByteString::from_static("test"),
                        SubscriptionOptions {
                            qos: QoS::AtLeastOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: RetainHandling::AtSubscribe,
                        },
                    ),
                    (
                        ByteString::from_static("filter"),
                        SubscriptionOptions {
                            qos: QoS::ExactlyOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: RetainHandling::AtSubscribe,
                        },
                    ),
                ],
            }),
            b"\x82\x15\x12\x34\x02\x0b\x01\x00\x04test\x01\x00\x06filter\x02",
        );

        assert_encode_packet(
            &Packet::SubscribeAck(SubscribeAck {
                packet_id: packet_id(0x1234),
                properties: UserProperties::default(),
                reason_string: None,
                status: vec![
                    SubscribeAckReason::GrantedQos1,
                    SubscribeAckReason::UnspecifiedError,
                    SubscribeAckReason::GrantedQos2,
                ],
            }),
            b"\x90\x06\x12\x34\x00\x01\x80\x02",
        );

        assert_encode_packet(
            &Packet::Unsubscribe(Unsubscribe {
                packet_id: packet_id(0x1234),
                topic_filters: vec![
                    ByteString::from_static("test"),
                    ByteString::from_static("filter"),
                ],
                user_properties: Vec::new(),
            }),
            b"\xa2\x11\x12\x34\x00\x00\x04test\x00\x06filter",
        );

        assert_encode_packet(
            &Packet::UnsubscribeAck(UnsubscribeAck {
                packet_id: packet_id(0x4321),
                properties: UserProperties::default(),
                reason_string: None,
                status: vec![
                    UnsubscribeAckReason::Success,
                    UnsubscribeAckReason::NotAuthorized,
                ],
            }),
            b"\xb0\x05\x43\x21\x00\x00\x87",
        );
    }

    #[test]
    fn test_encode_ping_packets() {
        assert_encode_packet(&Packet::PingRequest, b"\xc0\x00");
        assert_encode_packet(&Packet::PingResponse, b"\xd0\x00");
    }
}
