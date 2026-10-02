use std::num::NonZeroU16;

use ntex_bytes::{Buf, ByteString, Bytes, BytesMut};

use crate::error::DecodeError;
use crate::types::{MQTT, MQTT_LEVEL_3, QoS, WILL_QOS_SHIFT, packet_type};
use crate::utils::Decode;

use super::packet::{
    Connect, ConnectAck, ConnectAckReason, LastWill, Packet, Publish, SubscribeReturnCode,
};
use super::{ConnectAckFlags, ConnectFlags};

pub(crate) fn decode_packet(mut src: Bytes, first_byte: u8) -> Result<Packet, DecodeError> {
    match first_byte {
        packet_type::CONNECT => decode_connect_packet(&mut src),
        packet_type::CONNACK => decode_connect_ack_packet(&mut src),
        packet_type::PUBACK => decode_ack(src, |packet_id| Packet::PublishAck { packet_id }),
        packet_type::PUBREC => decode_ack(src, |packet_id| Packet::PublishReceived { packet_id }),
        packet_type::PUBREL => decode_ack(src, |packet_id| Packet::PublishRelease { packet_id }),
        packet_type::PUBCOMP => decode_ack(src, |packet_id| Packet::PublishComplete { packet_id }),
        packet_type::SUBSCRIBE => decode_subscribe_packet(&mut src),
        packet_type::SUBACK => decode_subscribe_ack_packet(&mut src),
        packet_type::UNSUBSCRIBE => decode_unsubscribe_packet(&mut src),
        packet_type::UNSUBACK => decode_ack(src, |packet_id| Packet::UnsubscribeAck { packet_id }),
        packet_type::PINGREQ => decode_empty(&src, Packet::PingRequest),
        packet_type::PINGRESP => decode_empty(&src, Packet::PingResponse),
        packet_type::DISCONNECT => decode_empty(&src, Packet::Disconnect),
        _ => Err(DecodeError::UnsupportedPacketType),
    }
}

/// PINGREQ, PINGRESP and DISCONNECT have no variable header and no payload
/// (MQTT 3.1.1, 3.12.2 - 3.14.3)
fn decode_empty(src: &Bytes, pkt: Packet) -> Result<Packet, DecodeError> {
    ensure!(src.is_empty(), DecodeError::InvalidLength);
    Ok(pkt)
}

#[inline]
fn decode_ack(mut src: Bytes, f: impl Fn(NonZeroU16) -> Packet) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(&mut src)?;
    ensure!(!src.has_remaining(), DecodeError::InvalidLength);
    Ok(f(packet_id))
}

fn decode_connect_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    ensure!(src.remaining() >= 10, DecodeError::InvalidLength);
    let len = src.get_u16();

    ensure!(
        len == 4 && &src.as_ref()[0..4] == MQTT,
        DecodeError::InvalidProtocol
    );
    src.advance(4);

    let level = src.get_u8();
    ensure!(level == MQTT_LEVEL_3, DecodeError::UnsupportedProtocolLevel);

    let flags = ConnectFlags::from_bits(src.get_u8()).ok_or(DecodeError::ConnectReservedFlagSet)?;

    // Will QoS and Will Retain must be 0 if Will Flag is 0,
    // [MQTT-3.1.2-11], [MQTT-3.1.2-13], [MQTT-3.1.2-15] (MQTT 3.1.1, 3.1.2.5 - 3.1.2.7)
    ensure!(
        flags.contains(ConnectFlags::WILL)
            || !flags.intersects(ConnectFlags::WILL_QOS | ConnectFlags::WILL_RETAIN),
        DecodeError::MalformedPacket
    );
    // Password Flag must be 0 if User Name Flag is 0, [MQTT-3.1.2-22] (MQTT 3.1.1, 3.1.2.9)
    ensure!(
        flags.contains(ConnectFlags::USERNAME) || !flags.contains(ConnectFlags::PASSWORD),
        DecodeError::MalformedPacket
    );

    let keep_alive = u16::decode(src)?;
    let client_id = ByteString::decode(src)?;

    ensure!(
        !client_id.is_empty() || flags.contains(ConnectFlags::CLEAN_START),
        DecodeError::InvalidClientId
    );

    let last_will = if flags.contains(ConnectFlags::WILL) {
        let topic = ByteString::decode(src)?;
        let message = Bytes::decode(src)?;
        Some(LastWill {
            // Will QoS 3 is rejected, [MQTT-3.1.2-14] (MQTT 3.1.1, 3.1.2.6)
            qos: QoS::try_from((flags & ConnectFlags::WILL_QOS).bits() >> WILL_QOS_SHIFT)?,
            retain: flags.contains(ConnectFlags::WILL_RETAIN),
            topic,
            message,
        })
    } else {
        None
    };
    let username = if flags.contains(ConnectFlags::USERNAME) {
        Some(ByteString::decode(src)?)
    } else {
        None
    };
    let password = if flags.contains(ConnectFlags::PASSWORD) {
        Some(Bytes::decode(src)?)
    } else {
        None
    };
    // payload contains only the fields selected by the flags, [MQTT-3.1.3-1] (MQTT 3.1.1, 3.1.3)
    ensure!(!src.has_remaining(), DecodeError::InvalidLength);

    Ok(Connect {
        clean_session: flags.contains(ConnectFlags::CLEAN_START),
        keep_alive,
        client_id,
        last_will,
        username,
        password,
    }
    .into())
}

fn decode_connect_ack_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    ensure!(src.remaining() >= 2, DecodeError::InvalidLength);
    let flags =
        ConnectAckFlags::from_bits(src.get_u8()).ok_or(DecodeError::ConnAckReservedFlagSet)?;

    let return_code: ConnectAckReason = src.get_u8().try_into()?;
    // remaining length of CONNACK is 2 (MQTT 3.1.1, 3.2.1)
    ensure!(!src.has_remaining(), DecodeError::InvalidLength);

    let session_present = flags.contains(ConnectAckFlags::SESSION_PRESENT);
    // Session Present must be 0 with a non-zero return code, [MQTT-3.2.2-4] (MQTT 3.1.1, 3.2.2.2)
    ensure!(
        !session_present || return_code == ConnectAckReason::ConnectionAccepted,
        DecodeError::MalformedPacket
    );

    Ok(Packet::ConnectAck(ConnectAck {
        return_code,
        session_present,
    }))
}

pub(super) fn decode_publish_packet(
    src: &mut Bytes,
    packet_flags: u8,
    payload_size: u32,
) -> Result<Publish, DecodeError> {
    let topic = ByteString::decode(src)?;
    // topic name must be at least one character long, [MQTT-4.7.3-1] (MQTT 3.1.1, 4.7.3)
    ensure!(!topic.is_empty(), DecodeError::MalformedPacket);
    let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?;
    let dup = (packet_flags & 0b1000) == 0b1000;
    // DUP flag must be 0 for QoS 0 messages, [MQTT-3.3.1-2] (MQTT 3.1.1, 3.3.1.1)
    ensure!(!dup || qos != QoS::AtMostOnce, DecodeError::MalformedPacket);
    let packet_id = if qos == QoS::AtMostOnce {
        None
    } else {
        Some(NonZeroU16::decode(src)?) // packet id = 0 encountered
    };

    Ok(Publish {
        qos,
        topic,
        packet_id,
        payload_size,
        dup,
        retain: (packet_flags & 0b0001) == 0b0001,
    })
}

pub(super) fn publish_size(src: &BytesMut, flags: u8) -> Result<Option<u32>, DecodeError> {
    // topic len
    if src.remaining() < 2 {
        return Ok(None);
    }
    let mut len = u32::from(u16::from_be_bytes([src[0], src[1]])) + 2;

    // packet-id len
    let qos = QoS::try_from((flags & 0b0110) >> 1)?;
    if qos != QoS::AtMostOnce {
        len += 2; // len of u16
    }
    Ok(Some(len))
}

fn decode_subscribe_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut topic_filters = Vec::new();
    while src.has_remaining() {
        let topic = ByteString::decode(src)?;
        ensure!(src.remaining() >= 1, DecodeError::InvalidLength);
        // [MQTT-3.8.3-4] reserved bits of requested QoS must be zero,
        // QoS must be 0, 1 or 2 (3.1.1, 3.8.3.1)
        let qos = src.get_u8().try_into()?;
        topic_filters.push((topic, qos));
    }
    // [MQTT-3.8.3-3] at least one topic filter is required (3.1.1, 3.8.3)
    ensure!(!topic_filters.is_empty(), DecodeError::MalformedPacket);

    Ok(Packet::Subscribe {
        packet_id,
        topic_filters,
    })
}

fn decode_subscribe_ack_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut status = Vec::with_capacity(src.len());
    for code in src.as_ref() {
        status.push(if *code == 0x80 {
            SubscribeReturnCode::Failure
        } else {
            SubscribeReturnCode::Success(QoS::try_from(*code)?)
        });
    }
    Ok(Packet::SubscribeAck { packet_id, status })
}

fn decode_unsubscribe_packet(src: &mut Bytes) -> Result<Packet, DecodeError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut topic_filters = Vec::new();
    while src.remaining() > 0 {
        topic_filters.push(ByteString::decode(src)?);
    }
    // [MQTT-3.10.3-2] at least one topic filter is required (3.1.1, 3.10.3)
    ensure!(!topic_filters.is_empty(), DecodeError::MalformedPacket);

    Ok(Packet::Unsubscribe {
        packet_id,
        topic_filters,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::decode_variable_length;

    macro_rules! assert_decode_packet (
        ($bytes:expr, $res:expr) => {{
            let first_byte = $bytes.as_ref()[0];
            let (_len, consumed) = decode_variable_length(&$bytes[1..]).unwrap().unwrap();
            let cur = Bytes::from_static(&$bytes[consumed + 1..]);
            assert_eq!(decode_packet(cur, first_byte), Ok($res));
        }};
    );

    macro_rules! assert_decode_publish (
        ($bytes:expr, $res:expr, $pl:expr) => {{
            let first_byte = $bytes.as_ref()[0];
            let (_len, consumed) = decode_variable_length(&$bytes[1..]).unwrap().unwrap();
            let mut cur = Bytes::from_static(&$bytes[consumed + 1..]);
            assert_eq!(decode_publish_packet(&mut cur, first_byte, $pl.len() as u32), Ok($res));
            assert_eq!(cur, $pl);
        }};
    );

    fn packet_id(v: u16) -> NonZeroU16 {
        NonZeroU16::new(v).unwrap()
    }

    #[test]
    fn test_decode_connect_flags() {
        // will qos/retain without will flag, password without user name
        for flags in *b"\x08\x10\x18\x20\x40" {
            let mut buf = b"\x00\x04MQTT\x04\x00\x00\x3C\x00\x0512345\x00\x04pass".to_vec();
            buf[7] = flags;
            assert_eq!(
                decode_connect_packet(&mut Bytes::from(buf)),
                Err(DecodeError::MalformedPacket),
                "flags: {flags:#x}"
            );
        }
        // will qos 3
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x04\x1C\x00\x3C\x00\x0512345\x00\x05topic\x00\x07message"
            )),
            Err(DecodeError::MalformedPacket),
        );
    }

    #[test]
    fn test_decode_connect_packets() {
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x04\xC0\x00\x3C\x00\x0512345\x00\x04user\x00\x04pass"
            )),
            Ok(Packet::Connect(Box::new(Connect {
                clean_session: false,
                keep_alive: 60,
                client_id: ByteString::try_from(Bytes::from_static(b"12345")).unwrap(),
                last_will: None,
                username: Some(ByteString::try_from(Bytes::from_static(b"user")).unwrap()),
                password: Some(Bytes::from(&b"pass"[..])),
            })))
        );

        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x04\x14\x00\x3C\x00\x0512345\x00\x05topic\x00\x07message"
            )),
            Ok(Packet::Connect(Box::new(Connect {
                clean_session: false,
                keep_alive: 60,
                client_id: ByteString::try_from(Bytes::from_static(b"12345")).unwrap(),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: ByteString::try_from(Bytes::from_static(b"topic")).unwrap(),
                    message: Bytes::from(&b"message"[..]),
                }),
                username: None,
                password: None,
            })))
        );

        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(b"\x00\x02MQ00000000000000000000")),
            Err(DecodeError::InvalidProtocol),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(b"\x00\x10MQ00000000000000000000")),
            Err(DecodeError::InvalidProtocol),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(b"\x00\x04MQAA00000000000000000000")),
            Err(DecodeError::InvalidProtocol),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x0300000000000000000000"
            )),
            Err(DecodeError::UnsupportedProtocolLevel),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x04\xff00000000000000000000"
            )),
            Err(DecodeError::ConnectReservedFlagSet)
        );

        assert_eq!(
            decode_connect_ack_packet(&mut Bytes::from_static(b"\x01\x00")),
            Ok(Packet::ConnectAck(ConnectAck {
                session_present: true,
                return_code: ConnectAckReason::ConnectionAccepted
            }))
        );
        assert_eq!(
            decode_connect_ack_packet(&mut Bytes::from_static(b"\x00\x04")),
            Ok(Packet::ConnectAck(ConnectAck {
                session_present: false,
                return_code: ConnectAckReason::BadUserNameOrPassword
            }))
        );
        // [MQTT-3.2.2-4] Session Present with a non-zero return code
        for code in 1..=5u8 {
            assert_eq!(
                decode_connect_ack_packet(&mut Bytes::from(vec![1, code])),
                Err(DecodeError::MalformedPacket)
            );
        }

        assert_eq!(
            decode_connect_ack_packet(&mut Bytes::from_static(b"\x03\x04")),
            Err(DecodeError::ConnAckReservedFlagSet)
        );

        assert_decode_packet!(
            b"\x20\x02\x00\x04",
            Packet::ConnectAck(ConnectAck {
                session_present: false,
                return_code: ConnectAckReason::BadUserNameOrPassword,
            })
        );

        assert_decode_packet!(b"\xe0\x00", Packet::Disconnect);
    }

    #[test]
    fn test_decode_publish_empty_topic() {
        assert_eq!(
            decode_publish_packet(&mut Bytes::from_static(b"\x00\x00data"), 0x30, 4),
            Err(DecodeError::MalformedPacket)
        );
        assert_eq!(
            decode_publish_packet(&mut Bytes::from_static(b"\x00\x00\x00\x01data"), 0x32, 4),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn test_decode_publish_dup_qos0() {
        // DUP flag must be 0 for QoS 0 messages, [MQTT-3.3.1-2]
        assert_eq!(
            decode_publish_packet(&mut Bytes::from_static(b"\x00\x01tdata"), 0x38, 4),
            Err(DecodeError::MalformedPacket)
        );
        let codec = crate::v3::codec::Codec::new();
        let mut buf = BytesMut::from(&b"\x38\x07\x00\x01tdata"[..]);
        assert!(matches!(
            ntex_codec::Decoder::decode(&codec, &mut buf),
            Err(DecodeError::MalformedPacket)
        ));
        assert!(
            decode_publish_packet(&mut Bytes::from_static(b"\x00\x01t\x00\x01data"), 0x3a, 4)
                .is_ok_and(|p| p.dup)
        );
    }

    #[test]
    fn test_decode_null_char() {
        assert_eq!(
            decode_publish_packet(&mut Bytes::from_static(b"\x00\x03a\x00bdata"), 0x30, 4),
            Err(DecodeError::MalformedPacket)
        );
        assert_eq!(
            decode_packet(Bytes::from_static(b"\x00\x01\x00\x03a\x00b\x00"), 0x82),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn test_decode_publish_packets() {
        //assert_eq!(
        //    decode_publish_packet(b"\x00\x05topic\x12\x34"),
        //    Done(&b""[..], ("topic".to_owned(), 0x1234))
        //);

        assert_decode_publish!(
            b"\x3d\x0D\x00\x05topic\x43\x21data",
            Publish {
                dup: true,
                retain: true,
                qos: QoS::ExactlyOnce,
                topic: ByteString::try_from(Bytes::from_static(b"topic")).unwrap(),
                packet_id: Some(packet_id(0x4321)),
                payload_size: 4,
            },
            Bytes::from_static(b"data")
        );
        assert_decode_publish!(
            b"\x30\x0b\x00\x05topicdata",
            Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                topic: ByteString::try_from(Bytes::from_static(b"topic")).unwrap(),
                packet_id: None,
                payload_size: 4,
            },
            Bytes::from_static(b"data")
        );

        assert_decode_packet!(
            b"\x40\x02\x43\x21",
            Packet::PublishAck {
                packet_id: packet_id(0x4321)
            }
        );
        assert_decode_packet!(
            b"\x50\x02\x43\x21",
            Packet::PublishReceived {
                packet_id: packet_id(0x4321)
            }
        );
        assert_decode_packet!(
            b"\x62\x02\x43\x21",
            Packet::PublishRelease {
                packet_id: packet_id(0x4321)
            }
        );
        assert_decode_packet!(
            b"\x70\x02\x43\x21",
            Packet::PublishComplete {
                packet_id: packet_id(0x4321)
            }
        );
    }

    #[test]
    fn test_decode_empty_subscribe_packets() {
        assert_eq!(
            decode_packet(Bytes::from_static(b"\x12\x34"), packet_type::SUBSCRIBE),
            Err(DecodeError::MalformedPacket)
        );
        assert_eq!(
            decode_packet(Bytes::from_static(b"\x12\x34"), packet_type::UNSUBSCRIBE),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn test_decode_subscribe_packets() {
        let p = Packet::Subscribe {
            packet_id: packet_id(0x1234),
            topic_filters: vec![
                (
                    ByteString::try_from(Bytes::from_static(b"test")).unwrap(),
                    QoS::AtLeastOnce,
                ),
                (
                    ByteString::try_from(Bytes::from_static(b"filter")).unwrap(),
                    QoS::ExactlyOnce,
                ),
            ],
        };

        assert_eq!(
            decode_subscribe_packet(&mut Bytes::from_static(
                b"\x12\x34\x00\x04test\x01\x00\x06filter\x02"
            )),
            Ok(p.clone())
        );
        assert_decode_packet!(b"\x82\x12\x12\x34\x00\x04test\x01\x00\x06filter\x02", p);

        // reserved bits of requested QoS are set, or QoS is 3
        for opts in [0b0000_0101, 0b1000_0001, 0b0100_0000, 0b0000_0011] {
            let mut src = BytesMut::from(&b"\x12\x34\x00\x04test"[..]);
            src.extend_from_slice(&[opts]);
            assert_eq!(
                decode_subscribe_packet(&mut src.freeze()),
                Err(DecodeError::MalformedPacket)
            );
        }

        let p = Packet::SubscribeAck {
            packet_id: packet_id(0x1234),
            status: vec![
                SubscribeReturnCode::Success(QoS::AtLeastOnce),
                SubscribeReturnCode::Failure,
                SubscribeReturnCode::Success(QoS::ExactlyOnce),
            ],
        };

        assert_eq!(
            decode_subscribe_ack_packet(&mut Bytes::from_static(b"\x12\x34\x01\x80\x02")),
            Ok(p.clone())
        );
        assert_decode_packet!(b"\x90\x05\x12\x34\x01\x80\x02", p);

        let p = Packet::Unsubscribe {
            packet_id: packet_id(0x1234),
            topic_filters: vec![
                ByteString::try_from(Bytes::from_static(b"test")).unwrap(),
                ByteString::try_from(Bytes::from_static(b"filter")).unwrap(),
            ],
        };

        assert_eq!(
            decode_unsubscribe_packet(&mut Bytes::from_static(
                b"\x12\x34\x00\x04test\x00\x06filter"
            )),
            Ok(p.clone())
        );
        assert_decode_packet!(b"\xa2\x10\x12\x34\x00\x04test\x00\x06filter", p);

        assert_decode_packet!(
            b"\xb0\x02\x43\x21",
            Packet::UnsubscribeAck {
                packet_id: packet_id(0x4321)
            }
        );
    }

    #[test]
    fn test_decode_trailing_bytes() {
        let cases: [(u8, &[u8]); 5] = [
            (
                packet_type::CONNECT,
                b"\x00\x04MQTT\x04\x02\x00\x3C\x00\x0512345\x00",
            ),
            (packet_type::CONNACK, b"\x00\x00\x00"),
            (packet_type::PINGREQ, b"\x00"),
            (packet_type::PINGRESP, b"\x00"),
            (packet_type::DISCONNECT, b"\x00"),
        ];
        for (first_byte, src) in cases {
            assert_eq!(
                decode_packet(Bytes::copy_from_slice(src), first_byte),
                Err(DecodeError::InvalidLength),
                "packet type: {first_byte:#x}"
            );
        }
    }

    #[test]
    fn test_decode_ping_packets() {
        assert_decode_packet!(b"\xc0\x00", Packet::PingRequest);
        assert_decode_packet!(b"\xd0\x00", Packet::PingResponse);
    }

    #[test]
    fn test_decode_non_minimal_remaining_length() {
        // MQTT 3.1.1 does not require the minimal encoding (2.2.3)
        let codec = crate::v3::codec::Codec::new();
        let mut buf = BytesMut::from(&b"\xc0\x80\x00"[..]);
        let res = ntex_codec::Decoder::decode(&codec, &mut buf);
        assert!(
            matches!(
                res,
                Ok(Some(super::super::Decoded::Packet(Packet::PingRequest, _)))
            ),
            "{res:?}"
        );
        assert!(buf.is_empty());
    }
}
