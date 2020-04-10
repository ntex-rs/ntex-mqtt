use super::{ConnectAckFlags, ConnectFlags, WILL_QOS_SHIFT};
use crate::codec3::error::ParseError;
use crate::codec3::packet::*;
use crate::codec3::*;
use bytes::{Buf, Bytes};
use bytestring::ByteString;
use std::convert::{TryFrom, TryInto};
use std::num::NonZeroU16;

pub(crate) fn decode_packet(mut src: Bytes, first_byte: u8) -> Result<Packet, ParseError> {
    match first_byte {
        packet_type::CONNECT => decode_connect_packet(&mut src),
        packet_type::CONNACK => decode_connect_ack_packet(&mut src),
        packet_type::PUBLISH_START..=packet_type::PUBLISH_END => {
            decode_publish_packet(&mut src, first_byte & 0b0000_1111)
        }
        packet_type::PUBACK => decode_ack(src, |packet_id| Packet::PublishAck { packet_id }),
        packet_type::PUBREC => {
            decode_ack(src, |packet_id| Packet::PublishReceived { packet_id })
        }
        packet_type::PUBREL => {
            decode_ack(src, |packet_id| Packet::PublishRelease { packet_id })
        }
        packet_type::PUBCOMP => {
            decode_ack(src, |packet_id| Packet::PublishComplete { packet_id })
        }
        packet_type::SUBSCRIBE => decode_subscribe_packet(&mut src),
        packet_type::SUBACK => decode_subscribe_ack_packet(&mut src),
        packet_type::UNSUBSCRIBE => decode_unsubscribe_packet(&mut src),
        packet_type::UNSUBACK => {
            decode_ack(src, |packet_id| Packet::UnsubscribeAck { packet_id })
        }
        packet_type::PINGREQ => Ok(Packet::PingRequest),
        packet_type::PINGRESP => Ok(Packet::PingResponse),
        packet_type::DISCONNECT => Ok(Packet::Disconnect),
        _ => Err(ParseError::UnsupportedPacketType),
    }
}

pub(crate) trait Decode: Sized {
    fn decode(src: &mut Bytes) -> Result<Self, ParseError>;
}

/// Decodes variable length and returns tuple of (length, bytes consumed)
pub fn decode_variable_length(src: &[u8]) -> Result<Option<(usize, usize)>, ParseError> {
    if let Some((len, consumed, more)) = src
        .iter()
        .enumerate()
        .scan((0, true), |state, (idx, x)| {
            if !state.1 || idx > 3 {
                return None;
            }
            state.0 += ((x & 0x7F) as usize) << (idx * 7);
            state.1 = x & 0x80 != 0;
            Some((state.0, idx + 1, state.1))
        })
        .last()
    {
        ensure!(!more || consumed < 4, ParseError::InvalidLength);
        return Ok(Some((len, consumed)));
    }

    Ok(None)
}

#[inline]
fn decode_ack(mut src: Bytes, f: impl Fn(NonZeroU16) -> Packet) -> Result<Packet, ParseError> {
    let packet_id = NonZeroU16::decode(&mut src)?;
    ensure!(!src.has_remaining(), ParseError::InvalidLength);
    Ok(f(packet_id))
}

fn decode_connect_packet(src: &mut Bytes) -> Result<Packet, ParseError> {
    ensure!(src.remaining() >= 10, ParseError::InvalidLength);
    let len = src.get_u16();

    ensure!(
        len == 4 && &src.bytes()[0..4] == b"MQTT",
        ParseError::InvalidProtocol
    );
    src.advance(4);

    let level = src.get_u8();
    ensure!(level == MQTT_LEVEL, ParseError::UnsupportedProtocolLevel);

    let flags = ConnectFlags::from_bits(src.get_u8())
        .ok_or_else(|| ParseError::ConnectReservedFlagSet)?;

    let keep_alive = u16::decode(src)?;
    let client_id = ByteString::decode(src)?;

    ensure!(
        !client_id.is_empty() || flags.contains(ConnectFlags::CLEAN_SESSION),
        ParseError::InvalidClientId
    );

    let last_will = if flags.contains(ConnectFlags::WILL) {
        let topic = ByteString::decode(src)?;
        let message = Bytes::decode(src)?;
        Some(LastWill {
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
    Ok(Packet::Connect(Connect {
        clean_session: flags.contains(ConnectFlags::CLEAN_SESSION),
        keep_alive,
        client_id,
        last_will,
        username,
        password,
    }))
}

fn decode_connect_ack_packet(src: &mut Bytes) -> Result<Packet, ParseError> {
    ensure!(src.remaining() >= 2, ParseError::InvalidLength);
    let flags = ConnectAckFlags::from_bits(src.get_u8())
        .ok_or_else(|| ParseError::ConnAckReservedFlagSet)?;

    let return_code = src.get_u8().try_into()?;
    Ok(Packet::ConnectAck {
        session_present: flags.contains(ConnectAckFlags::SESSION_PRESENT),
        return_code,
    })
}

fn decode_publish_packet(src: &mut Bytes, packet_flags: u8) -> Result<Packet, ParseError> {
    let topic = ByteString::decode(src)?;
    let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?;
    let packet_id = if qos == QoS::AtMostOnce {
        None
    } else {
        Some(NonZeroU16::decode(src)?) // packet id = 0 encountered
    };

    Ok(Packet::Publish(Publish {
        dup: (packet_flags & 0b1000) == 0b1000,
        qos,
        retain: (packet_flags & 0b0001) == 0b0001,
        topic,
        packet_id,
        payload: src.split_off(0),
    }))
}

fn decode_subscribe_packet(src: &mut Bytes) -> Result<Packet, ParseError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut topic_filters = Vec::new();
    while src.has_remaining() {
        let topic = ByteString::decode(src)?;
        ensure!(src.remaining() >= 1, ParseError::InvalidLength);
        let qos = (src.get_u8() & 0b0000_0011).try_into()?;
        topic_filters.push((topic, qos));
    }

    Ok(Packet::Subscribe {
        packet_id,
        topic_filters,
    })
}

fn decode_subscribe_ack_packet(src: &mut Bytes) -> Result<Packet, ParseError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut status = Vec::with_capacity(src.len());
    for code in src.as_ref().iter() {
        status.push(if *code == 0x80 {
            SubscribeReturnCode::Failure
        } else {
            SubscribeReturnCode::Success(QoS::try_from(*code)?)
        });
    }
    Ok(Packet::SubscribeAck { packet_id, status })
}

fn decode_unsubscribe_packet(src: &mut Bytes) -> Result<Packet, ParseError> {
    let packet_id = NonZeroU16::decode(src)?;
    let mut topic_filters = Vec::new();
    while src.remaining() > 0 {
        topic_filters.push(ByteString::decode(src)?);
    }
    Ok(Packet::Unsubscribe {
        packet_id,
        topic_filters,
    })
}

impl Decode for u16 {
    fn decode(src: &mut Bytes) -> Result<Self, ParseError> {
        ensure!(src.remaining() >= 2, ParseError::InvalidLength);
        Ok(src.get_u16())
    }
}

impl Decode for NonZeroU16 {
    fn decode(src: &mut Bytes) -> Result<Self, ParseError> {
        Ok(NonZeroU16::new(u16::decode(src)?).ok_or(ParseError::MalformedPacket)?)
    }
}

impl Decode for Bytes {
    fn decode(src: &mut Bytes) -> Result<Self, ParseError> {
        let len = u16::decode(src)? as usize;
        ensure!(src.remaining() >= len, ParseError::InvalidLength);
        Ok(src.split_to(len))
    }
}

impl Decode for ByteString {
    fn decode(src: &mut Bytes) -> Result<Self, ParseError> {
        let bytes = Bytes::decode(src)?;
        Ok(ByteString::try_from(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_decode_packet (
        ($bytes:expr, $res:expr) => {{
            let first_byte = $bytes.as_ref()[0];
            let (_len, consumed) = decode_variable_length(&$bytes[1..]).unwrap().unwrap();
            let cur = Bytes::from_static(&$bytes[consumed + 1..]);
            assert_eq!(decode_packet(cur, first_byte), Ok($res));
        }};
    );

    fn packet_id(v: u16) -> NonZeroU16 {
        NonZeroU16::new(v).unwrap()
    }

    #[test]
    fn test_decode_variable_length() {
        macro_rules! assert_variable_length (
            ($bytes:expr, $res:expr) => {{
                assert_eq!(decode_variable_length($bytes), Ok(Some($res)));
            }};

            ($bytes:expr, $res:expr, $rest:expr) => {{
                assert_eq!(decode_variable_length($bytes), Ok(Some($res)));
            }};
        );

        assert_variable_length!(b"\x7f\x7f", (127, 1), b"\x7f");

        //assert_eq!(decode_variable_length(b"\xff\xff\xff"), Ok(None));
        assert_eq!(
            decode_variable_length(b"\xff\xff\xff\xff\xff\xff"),
            Err(ParseError::InvalidLength)
        );

        assert_variable_length!(b"\x00", (0, 1));
        assert_variable_length!(b"\x7f", (127, 1));
        assert_variable_length!(b"\x80\x01", (128, 2));
        assert_variable_length!(b"\xff\x7f", (16383, 2));
        assert_variable_length!(b"\x80\x80\x01", (16384, 3));
        assert_variable_length!(b"\xff\xff\x7f", (2097151, 3));
        assert_variable_length!(b"\x80\x80\x80\x01", (2097152, 4));
        assert_variable_length!(b"\xff\xff\xff\x7f", (268435455, 4));
    }

    #[test]
    fn test_decode_connect_packets() {
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x04\xC0\x00\x3C\x00\x0512345\x00\x04user\x00\x04pass"
            )),
            Ok(Packet::Connect(Connect {
                clean_session: false,
                keep_alive: 60,
                client_id: ByteString::try_from(Bytes::from_static(b"12345")).unwrap(),
                last_will: None,
                username: Some(ByteString::try_from(Bytes::from_static(b"user")).unwrap()),
                password: Some(Bytes::from(&b"pass"[..])),
            }))
        );

        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x04\x14\x00\x3C\x00\x0512345\x00\x05topic\x00\x07message"
            )),
            Ok(Packet::Connect(Connect {
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
            }))
        );

        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(b"\x00\x02MQ00000000000000000000")),
            Err(ParseError::InvalidProtocol),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(b"\x00\x10MQ00000000000000000000")),
            Err(ParseError::InvalidProtocol),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(b"\x00\x04MQAA00000000000000000000")),
            Err(ParseError::InvalidProtocol),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x0300000000000000000000"
            )),
            Err(ParseError::UnsupportedProtocolLevel),
        );
        assert_eq!(
            decode_connect_packet(&mut Bytes::from_static(
                b"\x00\x04MQTT\x04\xff00000000000000000000"
            )),
            Err(ParseError::ConnectReservedFlagSet)
        );

        assert_eq!(
            decode_connect_ack_packet(&mut Bytes::from_static(b"\x01\x04")),
            Ok(Packet::ConnectAck {
                session_present: true,
                return_code: ConnectCode::BadUserNameOrPassword
            })
        );

        assert_eq!(
            decode_connect_ack_packet(&mut Bytes::from_static(b"\x03\x04")),
            Err(ParseError::ConnAckReservedFlagSet)
        );

        assert_decode_packet!(
            b"\x20\x02\x01\x04",
            Packet::ConnectAck {
                session_present: true,
                return_code: ConnectCode::BadUserNameOrPassword,
            }
        );

        assert_decode_packet!(b"\xe0\x00", Packet::Disconnect);
    }

    #[test]
    fn test_decode_publish_packets() {
        //assert_eq!(
        //    decode_publish_packet(b"\x00\x05topic\x12\x34"),
        //    Done(&b""[..], ("topic".to_owned(), 0x1234))
        //);

        assert_decode_packet!(
            b"\x3d\x0D\x00\x05topic\x43\x21data",
            Packet::Publish(Publish {
                dup: true,
                retain: true,
                qos: QoS::ExactlyOnce,
                topic: ByteString::try_from(Bytes::from_static(b"topic")).unwrap(),
                packet_id: Some(packet_id(0x4321)),
                payload: Bytes::from_static(b"data"),
            })
        );
        assert_decode_packet!(
            b"\x30\x0b\x00\x05topicdata",
            Packet::Publish(Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                topic: ByteString::try_from(Bytes::from_static(b"topic")).unwrap(),
                packet_id: None,
                payload: Bytes::from_static(b"data"),
            })
        );

        assert_decode_packet!(
            b"\x40\x02\x43\x21",
            Packet::PublishAck {
                packet_id: packet_id(0x4321),
            }
        );
        assert_decode_packet!(
            b"\x50\x02\x43\x21",
            Packet::PublishReceived {
                packet_id: packet_id(0x4321),
            }
        );
        assert_decode_packet!(
            b"\x62\x02\x43\x21",
            Packet::PublishRelease {
                packet_id: packet_id(0x4321),
            }
        );
        assert_decode_packet!(
            b"\x70\x02\x43\x21",
            Packet::PublishComplete {
                packet_id: packet_id(0x4321),
            }
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
                packet_id: packet_id(0x4321),
            }
        );
    }

    #[test]
    fn test_decode_ping_packets() {
        assert_decode_packet!(b"\xc0\x00", Packet::PingRequest);
        assert_decode_packet!(b"\xd0\x00", Packet::PingResponse);
    }
}
