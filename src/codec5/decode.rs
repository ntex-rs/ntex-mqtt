use bytes::buf::ext::{BufExt, Take};
use bytes::{Buf, Bytes};
use bytestring::ByteString;
use std::convert::TryFrom;
use std::io::Cursor;
use std::num::{NonZeroU16, NonZeroU32};

use super::{packet::*, UserProperty};
use crate::error::DecodeError;

pub(super) fn decode_packet(mut src: Bytes, first_byte: u8) -> Result<Packet, DecodeError> {
    match first_byte {
        packet_type::PUBLISH_START..=packet_type::PUBLISH_END => Ok(Packet::Publish(
            Publish::decode(src, first_byte & 0b0000_1111)?,
        )),
        packet_type::PUBACK => Ok(Packet::PublishAck(PublishAck::decode(&mut src)?)),
        packet_type::PINGREQ => Ok(Packet::PingRequest),
        packet_type::PINGRESP => Ok(Packet::PingResponse),
        packet_type::SUBSCRIBE => Ok(Packet::Subscribe(Subscribe::decode(&mut src)?)),
        packet_type::SUBACK => Ok(Packet::SubscribeAck(SubscribeAck::decode(&mut src)?)),
        packet_type::UNSUBSCRIBE => Ok(Packet::Unsubscribe(Unsubscribe::decode(&mut src)?)),
        packet_type::UNSUBACK => Ok(Packet::UnsubscribeAck(UnsubscribeAck::decode(&mut src)?)),
        packet_type::CONNECT => Ok(Packet::Connect(Connect::decode(&mut src)?)),
        packet_type::CONNACK => Ok(Packet::ConnectAck(ConnectAck::decode(&mut src)?)),
        packet_type::DISCONNECT => Ok(Packet::Disconnect(Disconnect::decode(&mut src)?)),
        packet_type::AUTH => Ok(Packet::Auth(Auth::decode(&mut src)?)),
        packet_type::PUBREC => Ok(Packet::PublishReceived(PublishAck::decode(&mut src)?)),
        packet_type::PUBREL => Ok(Packet::PublishRelease(PublishAck2::decode(&mut src)?)),
        packet_type::PUBCOMP => Ok(Packet::PublishComplete(PublishAck2::decode(&mut src)?)),
        _ => Err(DecodeError::UnsupportedPacketType),
    }
}

pub(super) trait ByteBuf: Buf {
    fn inner_mut(&mut self) -> &mut Bytes;
}

pub(super) trait Decode: Sized {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError>;
}

pub(super) trait Property {
    fn read_value<B: ByteBuf>(&mut self, src: &mut B) -> Result<(), DecodeError>;
}

impl ByteBuf for Bytes {
    fn inner_mut(&mut self) -> &mut Bytes {
        self
    }
}

impl ByteBuf for Take<&mut Bytes> {
    fn inner_mut(&mut self) -> &mut Bytes {
        self.get_mut()
    }
}

impl<T: Decode> Property for Option<T> {
    fn read_value<B: ByteBuf>(&mut self, src: &mut B) -> Result<(), DecodeError> {
        ensure!(self.is_none(), DecodeError::MalformedPacket); // property is set twice while not allowed
        *self = Some(T::decode(src)?);
        Ok(())
    }
}

impl<T: Decode> Property for Vec<T> {
    fn read_value<B: ByteBuf>(&mut self, src: &mut B) -> Result<(), DecodeError> {
        self.push(T::decode(src)?);
        Ok(())
    }
}

impl Decode for bool {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        ensure!(src.has_remaining(), DecodeError::InvalidLength); // expected more data within the field
        let v = src.get_u8();
        ensure!(v <= 0x1, DecodeError::MalformedPacket); // value is invalid
        Ok(v == 0x1)
    }
}

impl Decode for u16 {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        ensure!(src.remaining() >= 2, DecodeError::InvalidLength);
        Ok(src.get_u16())
    }
}

impl Decode for u32 {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        ensure!(src.remaining() >= 4, DecodeError::InvalidLength); // expected more data within the field
        let val = src.get_u32();
        Ok(val)
    }
}

impl Decode for NonZeroU32 {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        let val = NonZeroU32::new(u32::decode(src)?).ok_or(DecodeError::MalformedPacket)?;
        Ok(val)
    }
}

impl Decode for NonZeroU16 {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        Ok(NonZeroU16::new(u16::decode(src)?).ok_or(DecodeError::MalformedPacket)?)
    }
}

impl Decode for Bytes {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        let len = u16::decode(src)? as usize;
        ensure!(src.remaining() >= len, DecodeError::InvalidLength);
        Ok(src.inner_mut().split_to(len))
    }
}

impl Decode for ByteString {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        let bytes = Bytes::decode(src)?;
        Ok(ByteString::try_from(bytes)?)
    }
}

impl Decode for UserProperty {
    fn decode<B: ByteBuf>(src: &mut B) -> Result<Self, DecodeError> {
        let key = ByteString::decode(src)?;
        let val = ByteString::decode(src)?;
        Ok((key, val))
    }
}

pub fn decode_variable_length(src: &[u8]) -> Result<Option<(u32, usize)>, DecodeError> {
    let mut cur = Cursor::new(src);
    match decode_variable_length_cursor(&mut cur) {
        Ok(len) => Ok(Some((len, cur.position() as usize))),
        Err(DecodeError::MalformedPacket) => Ok(None),
        Err(e) => Err(e),
    }
}

#[allow(clippy::cast_lossless)] // safe: allow cast through `as` because it is type-safe
pub fn decode_variable_length_cursor<B: Buf>(src: &mut B) -> Result<u32, DecodeError> {
    let mut shift: u32 = 0;
    let mut len: u32 = 0;
    loop {
        ensure!(src.has_remaining(), DecodeError::MalformedPacket);
        let val = src.get_u8();
        len += ((val & 0b0111_1111u8) as u32) << shift;
        if val & 0b1000_0000 == 0 {
            return Ok(len);
        } else {
            ensure!(shift < 21, DecodeError::InvalidLength);
            shift += 7;
        }
    }
}

pub(crate) fn take_properties(src: &mut Bytes) -> Result<Take<&mut Bytes>, DecodeError> {
    let prop_len = decode_variable_length_cursor(src)?;
    ensure!(
        src.remaining() >= prop_len as usize,
        DecodeError::InvalidLength
    );

    Ok(src.take(prop_len as usize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec5::*;
    use crate::types::QoS;
    use bytestring::ByteString;

    fn packet_id(v: u16) -> NonZeroU16 {
        NonZeroU16::new(v).unwrap()
    }

    fn assert_decode_packet<B: AsRef<[u8]>>(bytes: B, res: Packet) {
        let bytes = bytes.as_ref();
        let fixed = bytes[0];
        let (_len, consumed) = decode_variable_length(&bytes[1..]).unwrap().unwrap();
        let cur = Bytes::copy_from_slice(&bytes[consumed + 1..]);
        let mut tmp = bytes::BytesMut::with_capacity(4096);
        ntex_codec::Encoder::encode(
            &mut crate::codec5::codec::Codec::new(),
            res.clone(),
            &mut tmp,
        )
        .unwrap();
        let decoded = decode_packet(cur, fixed);
        let res = Ok(res);
        if decoded != res {
            panic!("decoded packet does not match expectations.\nexpected: {:?}\nactual: {:?}\nencoding output for expected: {:X?}", res, decoded, tmp.as_ref());
        }
        //assert_eq!(, Ok(res));
    }

    #[test]
    fn test_decode_variable_length() {
        fn assert_variable_length<B: AsRef<[u8]> + 'static>(bytes: B, res: (u32, usize)) {
            assert_eq!(decode_variable_length(bytes.as_ref()), Ok(Some(res)));
        }

        assert_variable_length(b"\x7f\x7f", (127, 1));

        assert_eq!(decode_variable_length(b"\xff\xff\xff"), Ok(None));

        assert_eq!(
            decode_variable_length(b"\xff\xff\xff\xff\xff\xff"),
            Err(DecodeError::InvalidLength)
        );

        assert_variable_length(b"\x00", (0, 1));
        assert_variable_length(b"\x7f", (127, 1));
        assert_variable_length(b"\x80\x01", (128, 2));
        assert_variable_length(b"\xff\x7f", (16383, 2));
        assert_variable_length(b"\x80\x80\x01", (16384, 3));
        assert_variable_length(b"\xff\xff\x7f", (2_097_151, 3));
        assert_variable_length(b"\x80\x80\x80\x01", (2_097_152, 4));
        assert_variable_length(b"\xff\xff\xff\x7f", (268_435_455, 4));
    }

    #[test]
    fn test_decode_connect_packets() {
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(
                b"\x00\x04MQTT\x05\xC0\x00\x3C\x00\x00\x0512345\x00\x04user\x00\x04pass"
            )),
            Ok(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: None,
                username: Some(ByteString::from_static("user")),
                password: Some(Bytes::from_static(&b"pass"[..])),
                session_expiry_interval_secs: None,
                auth_method: None,
                auth_data: None,
                request_problem_info: None,
                request_response_info: None,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: Vec::new(),
                max_packet_size: None,
            })
        );

        assert_eq!(
            Connect::decode(&mut Bytes::from_static(
                b"\x00\x04MQTT\x05\x14\x00\x3C\x00\x00\x0512345\x00\x00\x05topic\x00\x07message"
            )),
            Ok(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: ByteString::from_static("topic"),
                    message: Bytes::from_static(&b"message"[..]),
                    will_delay_interval_sec: None,
                    correlation_data: None,
                    message_expiry_interval: None,
                    content_type: None,
                    user_properties: Vec::new(),
                    is_utf8_payload: None,
                    response_topic: None,
                }),
                username: None,
                password: None,
                session_expiry_interval_secs: None,
                auth_method: None,
                auth_data: None,
                request_problem_info: None,
                request_response_info: None,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: Vec::new(),
                max_packet_size: None,
            })
        );

        assert_eq!(
            Connect::decode(&mut Bytes::from_static(b"\x00\x02MQ00000000000000000000")),
            Err(DecodeError::InvalidProtocol),
        );
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(b"\x00\x04MQAA00000000000000000000")),
            Err(DecodeError::InvalidProtocol),
        );
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(
                b"\x00\x04MQTT\x0300000000000000000000"
            )),
            Err(DecodeError::UnsupportedProtocolLevel),
        );
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(
                b"\x00\x04MQTT\x05\xff00000000000000000000"
            )),
            Err(DecodeError::ConnectReservedFlagSet)
        );

        assert_eq!(
            ConnectAck::decode(&mut Bytes::from_static(b"\x01\x86\x00")),
            Ok(ConnectAck {
                session_present: true,
                reason_code: ConnectAckReasonCode::BadUserNameOrPassword,
                ..ConnectAck::default()
            })
        );

        assert_eq!(
            ConnectAck::decode(&mut Bytes::from_static(b"\x03\x86\x00")),
            Err(DecodeError::ConnAckReservedFlagSet)
        );

        assert_decode_packet(
            b"\x20\x03\x01\x86\x00",
            Packet::ConnectAck(ConnectAck {
                session_present: true,
                reason_code: ConnectAckReasonCode::BadUserNameOrPassword,
                ..ConnectAck::default()
            }),
        );

        assert_decode_packet([0b1110_0000, 0], Packet::Disconnect(Disconnect::default()));
    }

    fn default_test_publish() -> Publish {
        Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::default(),
            packet_id: Some(packet_id(1)),
            payload: Bytes::new(),
            properties: PublishProperties::default(),
        }
    }

    #[test]
    fn test_decode_publish_packets() {
        //assert_eq!(
        //    decode_publish_packet(b"\x00\x05topic\x12\x34"),
        //    Done(&b""[..], ("topic".to_owned(), 0x1234))
        //);

        assert_decode_packet(
            b"\x3d\x0E\x00\x05topic\x43\x21\x00data",
            Packet::Publish(Publish {
                dup: true,
                retain: true,
                qos: QoS::ExactlyOnce,
                topic: ByteString::from_static("topic"),
                packet_id: Some(packet_id(0x4321)),
                payload: Bytes::from_static(b"data"),
                ..default_test_publish()
            }),
        );
        assert_decode_packet(
            b"\x30\x0C\x00\x05topic\x00data",
            Packet::Publish(Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                topic: ByteString::from_static("topic"),
                packet_id: None,
                payload: Bytes::from_static(b"data"),
                ..default_test_publish()
            }),
        );

        assert_decode_packet(
            b"\x40\x02\x43\x21",
            Packet::PublishAck(PublishAck {
                packet_id: packet_id(0x4321),
                reason_code: PublishAckReasonCode::Success,
                properties: UserProperties::default(),
                reason_string: None,
            }),
        );
        assert_decode_packet(
            b"\x50\x02\x43\x21",
            Packet::PublishReceived(PublishAck {
                packet_id: packet_id(0x4321),
                reason_code: PublishAckReasonCode::Success,
                properties: UserProperties::default(),
                reason_string: None,
            }),
        );
        assert_decode_packet(
            b"\x62\x02\x43\x21",
            Packet::PublishRelease(PublishAck2 {
                packet_id: packet_id(0x4321),
                reason_code: PublishAck2ReasonCode::Success,
                properties: UserProperties::default(),
                reason_string: None,
            }),
        );
        assert_decode_packet(
            b"\x70\x02\x43\x21",
            Packet::PublishComplete(PublishAck2 {
                packet_id: packet_id(0x4321),
                reason_code: PublishAck2ReasonCode::Success,
                properties: UserProperties::default(),
                reason_string: None,
            }),
        );
    }

    #[test]
    fn test_decode_subscribe_packets() {
        let p = Packet::Subscribe(Subscribe {
            packet_id: packet_id(0x1234),
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
            id: None,
            user_properties: Vec::new(),
        });

        assert_decode_packet(b"\x82\x13\x12\x34\x00\x00\x04test\x01\x00\x06filter\x02", p);

        let p = Packet::SubscribeAck(SubscribeAck {
            packet_id: packet_id(0x1234),
            status: vec![
                SubscribeAckReasonCode::GrantedQos1,
                SubscribeAckReasonCode::UnspecifiedError,
                SubscribeAckReasonCode::GrantedQos2,
            ],
            properties: UserProperties::default(),
            reason_string: None,
        });

        assert_decode_packet(b"\x90\x05\x12\x34\x00\x01\x80\x02", p);

        let p = Packet::Unsubscribe(Unsubscribe {
            packet_id: packet_id(0x1234),
            topic_filters: vec![
                ByteString::from_static("test"),
                ByteString::from_static("filter"),
            ],
            user_properties: UserProperties::default(),
        });

        assert_eq!(
            Packet::Unsubscribe(
                Unsubscribe::decode(&mut Bytes::from_static(
                    b"\x12\x34\x00\x00\x04test\x00\x06filter"
                ))
                .unwrap()
            ),
            p.clone()
        );
        assert_decode_packet(b"\xa2\x11\x12\x34\x00\x00\x04test\x00\x06filter", p);

        assert_decode_packet(
            b"\xb0\x03\x43\x21\x00",
            Packet::UnsubscribeAck(UnsubscribeAck {
                packet_id: packet_id(0x4321),
                properties: UserProperties::default(),
                reason_string: None,
                status: vec![],
            }),
        );
    }

    #[test]
    fn test_decode_ping_packets() {
        assert_decode_packet(b"\xc0\x00", Packet::PingRequest);
        assert_decode_packet(b"\xd0\x00", Packet::PingResponse);
    }
}
