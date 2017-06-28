use nom::{be_u8, be_u16, IResult, Needed, ErrorKind, IError};
use nom::IResult::{Done, Incomplete, Error};
use bytes::Bytes;

use proto::*;
use packet::*;

pub const INVALID_PROTOCOL: u32 = 0x0001;
pub const UNSUPPORT_LEVEL: u32 = 0x0002;
pub const RESERVED_FLAG: u32 = 0x0003;
pub const INVALID_CLIENT_ID: u32 = 0x0004;
pub const INVALID_LENGTH: u32 = 0x0005;
pub const UNSUPPORT_PACKET_TYPE: u32 = 0x0100;

macro_rules! error_if (
  ($i:expr, $cond:expr, $code:expr) => (
    {
      if $cond {
        IResult::Error(error_code!(ErrorKind::Custom($code)))
      } else {
        IResult::Done($i, ())
      }
    }
  );
  ($i:expr, $cond:expr, $err:expr) => (
    error!($i, $cond, $err);
  );
);

pub fn decode_variable_length_usize(i: &[u8]) -> IResult<&[u8], usize> {
    let n = if i.len() > 4 { 4 } else { i.len() };
    let pos = i[..n].iter().position(|b| (b & 0x80) == 0);

    match pos {
        Some(idx) => {
            Done(&i[idx + 1..],
                 i[..idx + 1]
                     .iter().enumerate()
                     .fold(0usize, |acc, (i, b)| {
                         acc + (((b & 0x7F) as usize) << (i * 7))
                     }))
        }
        _ => {
            if n < 4 {
                Incomplete(Needed::Unknown)
            } else {
                Error(error_position!(ErrorKind::Custom(INVALID_LENGTH), i))
            }
        }
    }
}

named!(pub decode_length_bytes<Bytes>,
    do_parse!(
        len: be_u16
         >> s: map!(take!(len), Bytes::from)
         >> (s)
    ));
named!(pub decode_utf8_str<String>, map_res!(length_bytes!(be_u16), |r: &[u8]| String::from_utf8(r.to_vec())));

named!(pub decode_fixed_header<FixedHeader>, do_parse!(
    b0: bits!( pair!( take_bits!( u8, 4 ), take_bits!( u8, 4 ) ) ) >>
    remaining_length: decode_variable_length_usize >>
    (
        FixedHeader {
            packet_type: b0.0,
            packet_flags: b0.1,
            remaining_length: remaining_length,
        }
    )
));

macro_rules! is_flag_set {
    ($flags:expr, $flag:expr) => (($flags & $flag.bits()) == $flag.bits())
}

named!(pub decode_connect_header<Packet>, do_parse!(
    length: be_u16 >>
    error_if!(length != 4, INVALID_PROTOCOL) >>

    proto: take!(4) >>
    error_if!(proto != b"MQTT", INVALID_PROTOCOL) >>

    level: be_u8 >>
    error_if!(level != DEFAULT_MQTT_LEVEL, UNSUPPORT_LEVEL) >>

    flags: be_u8 >>
    error_if!((flags & 0x01) != 0, RESERVED_FLAG) >>

    keep_alive: be_u16 >>
    client_id: decode_utf8_str >>
    error_if!(client_id.is_empty() && !is_flag_set!(flags, CLEAN_SESSION), INVALID_CLIENT_ID) >>

    topic: cond!(is_flag_set!(flags, WILL), decode_utf8_str) >>
    message: cond!(is_flag_set!(flags, WILL), decode_length_bytes) >>
    username: cond!(is_flag_set!(flags, USERNAME), decode_utf8_str) >>
    password: cond!(is_flag_set!(flags, PASSWORD), decode_length_bytes) >>
    (
        Packet::Connect {
            connect: Box::new(Connect {
                protocol: Protocol::MQTT(level),
                clean_session: is_flag_set!(flags, CLEAN_SESSION),
                keep_alive: keep_alive,
                client_id: client_id,
                last_will: if is_flag_set!(flags, WILL) { Some(LastWill{
                    qos: QoS::from((flags & WILL_QOS.bits()) >> WILL_QOS_SHIFT),
                    retain: is_flag_set!(flags, WILL_RETAIN),
                    topic: topic.unwrap(),
                    message: message.unwrap(),
                }) } else { None },
                username: username,
                password: password,
            })
        }
    )
));

named!(pub decode_connect_ack_header<(ConnectAckFlags, ConnectReturnCode)>, do_parse!(
    flags: be_u8 >>
    error_if!((flags & 0b11111110) != 0, RESERVED_FLAG) >>

    return_code: be_u8 >>
    (
        ConnectAckFlags::from_bits_truncate(flags),
        ConnectReturnCode::from(return_code)
    )
));

named!(pub decode_publish_header<(String, u16)>, pair!(decode_utf8_str, be_u16));

named!(pub decode_subscribe_header<Packet>, do_parse!(
    packet_id: be_u16 >>
    topic_filters: many1!(pair!(decode_utf8_str, be_u8)) >>
    (
        Packet::Subscribe {
            packet_id: packet_id,
            topic_filters: topic_filters.iter()
                                        .map(|&(ref filter, flags)| (filter.to_owned(), QoS::from(flags & 0x03))) // todo: revisit String ref changes and this .to_owned() call
                                        .collect(),
        }
    )
));

named!(pub decode_subscribe_ack_header<Packet>, do_parse!(
    packet_id: be_u16 >>
    return_codes: many1!(be_u8) >>
    (
        Packet::SubscribeAck {
            packet_id: packet_id,
            status: return_codes.iter()
                                .map(|&return_code| if return_code == 0x80 {
                                    SubscribeReturnCode::Failure
                                } else {
                                    SubscribeReturnCode::Success(QoS::from(return_code & 0x03))
                                })
                                .collect(),
        }
    )
));

named!(pub decode_unsubscribe_header<Packet>, do_parse!(
    packet_id: be_u16 >>
    topic_filters: many1!(decode_utf8_str) >>
    (
        Packet::Unsubscribe {
            packet_id: packet_id,
            topic_filters: topic_filters,
        }
    )
));


fn decode_variable_header<'a>(i: &[u8], fixed_header: FixedHeader) -> IResult<&[u8], (Packet, usize)> {
    match fixed_header.packet_type {
        CONNECT => decode_connect_header(i).map(|p| (p, 0)),
        CONNACK => {
            decode_connect_ack_header(i).map(|(flags, return_code)| {
                (Packet::ConnectAck {
                    session_present: is_flag_set!(flags.bits(), SESSION_PRESENT),
                    return_code: return_code,
                }, 0)
            })
        }
        PUBLISH => {
            let dup = (fixed_header.packet_flags & 0b1000) == 0b1000;
            let qos = QoS::from((fixed_header.packet_flags & 0b0110) >> 1);
            let retain = (fixed_header.packet_flags & 0b0001) == 0b0001;

            let result = match qos {
                QoS::AtLeastOnce | QoS::ExactlyOnce => {
                    decode_publish_header(i).map(|(topic, packet_id)| (topic, Some(packet_id)))
                }
                _ => decode_utf8_str(i).map(|topic| (topic, None)),
            };

            match result {
                Done(i, (topic, packet_id)) => {
                    let size = if i.len() <= 31 { 0 } else { i.len() };
                    Done(Default::default(),
                         (Packet::Publish {
                             dup: dup,
                             retain: retain,
                             qos: qos,
                             topic: topic,
                             packet_id: packet_id,
                             payload: if size == 0 { i.into() } else {Bytes::new()},
                         }, size))
                }
                Error(err) => Error(err),
                Incomplete(needed) => Incomplete(needed),
            }
        }
        PUBACK => be_u16(i).map(|packet_id| (Packet::PublishAck { packet_id: packet_id }, 0)),
        PUBREC => be_u16(i).map(|packet_id| (Packet::PublishReceived { packet_id: packet_id }, 0)),
        PUBREL => be_u16(i).map(|packet_id| (Packet::PublishRelease { packet_id: packet_id }, 0)),
        PUBCOMP => be_u16(i).map(|packet_id| (Packet::PublishComplete { packet_id: packet_id }, 0)),
        SUBSCRIBE => decode_subscribe_header(i).map(|p| (p, 0)),
        SUBACK => decode_subscribe_ack_header(i).map(|p| (p, 0)),
        UNSUBSCRIBE => decode_unsubscribe_header(i).map(|p| (p, 0)),
        UNSUBACK => be_u16(i).map(|packet_id| (Packet::UnsubscribeAck { packet_id: packet_id }, 0)),

        PINGREQ => Done(i, (Packet::PingRequest, 0)),
        PINGRESP => Done(i, (Packet::PingResponse, 0)),
        DISCONNECT => Done(i, (Packet::Disconnect, 0)),
        _ => {
            let err_code = UNSUPPORT_PACKET_TYPE + (fixed_header.packet_type as u32);

            Error(error_position!(ErrorKind::Custom(err_code), i))
        }
    }
}

named!(pub decode_packet<(Packet, usize)>, do_parse!(
    fixed_header: decode_fixed_header >>
    packet: flat_map!(
        take!(fixed_header.remaining_length),
        apply!(decode_variable_header, fixed_header)
    ) >>
    ( packet )
));

/// Extends `AsRef<[u8]>` with methods for reading packet.
///
/// ```
/// use mqtt::{ReadPacketExt, Packet};
///
/// assert_eq!(b"\xd0\x00".read_packet().unwrap(), Packet::PingResponse);
/// ```
pub trait ReadPacketExt: AsRef<[u8]> {
    #[inline]
    /// Read packet from the underlying reader.
    fn read_packet(&self) -> Result<Packet, IError> {
        decode_packet(self.as_ref())
            .map(|decoded| {
                // match decoded {
                //     (Packet:::Publish { }, size) if size > 0 => P
                // }
                decoded.0
            })
            .to_full_result()
    }
}

impl<T: AsRef<[u8]>> ReadPacketExt for T {}

/// Read packet from the underlying `&[u8]`.
///
/// ```
/// use mqtt::{read_packet, Packet};
///
/// assert_eq!(read_packet(b"\xc0\x00\xd0\x00").unwrap(), (&b"\xd0\x00"[..], Packet::PingRequest));
/// ```
pub fn read_packet(i: &[u8]) -> Result<(&[u8], Packet, usize), IError> {
    match decode_packet(i) {
        Done(rest, (packet, size)) => Ok((rest, packet, size)),
        Error(e) => Err(IError::Error(e)),
        Incomplete(n) => Err(IError::Incomplete(n)),
    }
}

#[cfg(test)]
mod tests {
    //extern crate env_logger;

    use nom::{Needed, ErrorKind};
    use nom::IResult::{Done, Incomplete, Error};

    use proto::*;
    use packet::*;
    use super::*;

    #[test]
    fn test_decode_variable_length() {
        macro_rules! assert_variable_length (
            ($bytes:expr, $res:expr) => {{
                assert_eq!(decode_variable_length_usize($bytes), Done(&b""[..], $res));
            }};

            ($bytes:expr, $res:expr, $rest:expr) => {{
                assert_eq!(decode_variable_length_usize($bytes), Done(&$rest[..], $res));
            }};
        );

        assert_variable_length!(b"\x7f\x7f", 127, b"\x7f");

        assert_eq!(decode_variable_length_usize(b"\xff\xff\xff"),
               Incomplete(Needed::Unknown));
        assert_eq!(decode_variable_length_usize(b"\xff\xff\xff\xff\xff\xff"),
               Error(ErrorKind::Custom(INVALID_LENGTH)));

        assert_variable_length!(b"\x00", 0);
        assert_variable_length!(b"\x7f", 127);
        assert_variable_length!(b"\x80\x01", 128);
        assert_variable_length!(b"\xff\x7f", 16383);
        assert_variable_length!(b"\x80\x80\x01", 16384);
        assert_variable_length!(b"\xff\xff\x7f", 2097151);
        assert_variable_length!(b"\x80\x80\x80\x01", 2097152);
        assert_variable_length!(b"\xff\xff\xff\x7f", 268435455);
    }

    #[test]
    fn test_decode_fixed_header() {
        assert_eq!(decode_fixed_header(b"\x20\x7f"),
               Done(&b""[..],
                    FixedHeader {
                        packet_type: CONNACK,
                        packet_flags: 0,
                        remaining_length: 127,
                    }));

        assert_eq!(decode_fixed_header(b"\x3C\x82\x7f"),
               Done(&b""[..],
                    FixedHeader {
                        packet_type: PUBLISH,
                        packet_flags: 0x0C,
                        remaining_length: 16258,
                    }));

        assert_eq!(decode_fixed_header(b"\x20"), Incomplete(Needed::Unknown));
    }

    #[test]
    fn test_decode_connect_packets() {
        assert_eq!(decode_connect_header(
        b"\x00\x04MQTT\x04\xC0\x00\x3C\x00\x0512345\x00\x04user\x00\x04pass"),
        Done(&b""[..], Packet::Connect {
            protocol: Protocol::MQTT(4),
            clean_session: false,
            keep_alive: 60,
            client_id: "12345".to_owned(),
            last_will: None,
            username: Some("user".to_owned()),
            password: Some(Bytes::from(&b"pass"[..])),
        }));

        assert_eq!(decode_connect_header(
        b"\x00\x04MQTT\x04\x14\x00\x3C\x00\x0512345\x00\x05topic\x00\x07message"),
        Done(&b""[..], Packet::Connect {
            protocol: Protocol::MQTT(4),
            clean_session: false,
            keep_alive: 60,
            client_id: "12345".to_owned(),
            last_will: Some(LastWill{
                qos: QoS::ExactlyOnce,
                retain: false,
                topic: "topic".to_owned(),
                message: Bytes::from(&b"message"[..]),
            }),
            username: None,
            password: None,
        }));

        assert_eq!(decode_connect_header(b"\x00\x02MQ"),
               Error(ErrorKind::Custom(INVALID_PROTOCOL)));
        assert_eq!(decode_connect_header(b"\x00\x04MQAA"),
               Error(ErrorKind::Custom(INVALID_PROTOCOL)));
        assert_eq!(decode_connect_header(b"\x00\x04MQTT\x03"),
               Error(ErrorKind::Custom(UNSUPPORT_LEVEL)));
        assert_eq!(decode_connect_header(b"\x00\x04MQTT\x04\xff"),
               Error(ErrorKind::Custom(RESERVED_FLAG)));

        assert_eq!(decode_connect_ack_header(b"\x01\x04"),
               Done(&b""[..],
                    (SESSION_PRESENT, ConnectReturnCode::BadUserNameOrPassword)));

        assert_eq!(decode_connect_ack_header(b"\x03\x04"),
               Error(ErrorKind::Custom(RESERVED_FLAG)));

        assert_eq!(decode_packet(b"\x20\x02\x01\x04"),
               Done(&b""[..],
                    Packet::ConnectAck {
                        session_present: true,
                        return_code: ConnectReturnCode::BadUserNameOrPassword,
                    }));

        assert_eq!(decode_packet(b"\xe0\x00"),
               Done(&b""[..], Packet::Disconnect));
    }

    #[test]
    fn test_decode_publish_packets() {
        assert_eq!(decode_publish_header(b"\x00\x05topic\x12\x34"),
               Done(&b""[..], ("topic".to_owned(), 0x1234)));

        assert_eq!(decode_packet(b"\x3d\x0D\x00\x05topic\x43\x21data"),
               Done(&b""[..],
                    Packet::Publish {
                        dup: true,
                        retain: true,
                        qos: QoS::ExactlyOnce,
                        topic: "topic".to_owned(),
                        packet_id: Some(0x4321),
                        payload: PayloadPromise::from(&b"data"[..]),
                    }));
        assert_eq!(decode_packet(b"\x30\x0b\x00\x05topicdata"),
               Done(&b""[..],
                    Packet::Publish {
                        dup: false,
                        retain: false,
                        qos: QoS::AtMostOnce,
                        topic: "topic".to_owned(),
                        packet_id: None,
                        payload: PayloadPromise::from(&b"data"[..]),
                    }));

        assert_eq!(decode_packet(b"\x40\x02\x43\x21"),
               Done(&b""[..], Packet::PublishAck { packet_id: 0x4321 }));
        assert_eq!(decode_packet(b"\x50\x02\x43\x21"),
               Done(&b""[..], Packet::PublishReceived { packet_id: 0x4321 }));
        assert_eq!(decode_packet(b"\x60\x02\x43\x21"),
               Done(&b""[..], Packet::PublishRelease { packet_id: 0x4321 }));
        assert_eq!(decode_packet(b"\x70\x02\x43\x21"),
               Done(&b""[..], Packet::PublishComplete { packet_id: 0x4321 }));
    }

    #[test]
    fn test_decode_subscribe_packets() {
        let p = Packet::Subscribe {
            packet_id: 0x1234,
            topic_filters: vec![("test".to_owned(), QoS::AtLeastOnce), ("filter".to_owned(), QoS::ExactlyOnce)],
        };

        assert_eq!(decode_subscribe_header(b"\x12\x34\x00\x04test\x01\x00\x06filter\x02"),
               Done(&b""[..], p.clone()));
        assert_eq!(decode_packet(b"\x82\x12\x12\x34\x00\x04test\x01\x00\x06filter\x02"),
               Done(&b""[..], p));

        let p = Packet::SubscribeAck {
            packet_id: 0x1234,
            status: vec![SubscribeReturnCode::Success(QoS::AtLeastOnce),
                     SubscribeReturnCode::Failure,
                     SubscribeReturnCode::Success(QoS::ExactlyOnce)],
        };

        assert_eq!(decode_subscribe_ack_header(b"\x12\x34\x01\x80\x02"),
               Done(&b""[..], p.clone()));

        assert_eq!(decode_packet(b"\x90\x05\x12\x34\x01\x80\x02"),
               Done(&b""[..], p));

        let p = Packet::Unsubscribe {
            packet_id: 0x1234,
            topic_filters: vec!["test".to_owned(), "filter".to_owned()],
        };

        assert_eq!(decode_unsubscribe_header(b"\x12\x34\x00\x04test\x00\x06filter"),
               Done(&b""[..], p.clone()));
        assert_eq!(decode_packet(b"\xa2\x10\x12\x34\x00\x04test\x00\x06filter"),
               Done(&b""[..], p));

        assert_eq!(decode_packet(b"\xb0\x02\x43\x21"),
               Done(&b""[..], Packet::UnsubscribeAck { packet_id: 0x4321 }));
    }

    #[test]
    fn test_decode_ping_packets() {
        assert_eq!(decode_packet(b"\xc0\x00"),
               Done(&b""[..], Packet::PingRequest));
        assert_eq!(decode_packet(b"\xd0\x00"),
               Done(&b""[..], Packet::PingResponse));
    }
}
