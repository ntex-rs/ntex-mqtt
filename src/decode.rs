use std::str;

use nom::{be_u8, be_u16, IResult, Needed, ErrorKind, IError};
use nom::IResult::{Done, Incomplete, Error};

use error::*;
use packet::*;

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
                     .iter()
                     .fold(0, |acc, b| (acc << 7) + (b & 0x7F) as usize))
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

named!(pub decode_length_bytes, length_bytes!(be_u16));
named!(pub decode_utf8_str<&str>, map_res!(length_bytes!(be_u16), str::from_utf8));

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
    error_if!(level != 4, UNSUPPORT_LEVEL) >>

    flags: be_u8 >>
    error_if!((flags & 0x01) != 0, RESERVED_FLAG) >>

    keep_alive: be_u16 >>
    client_id: decode_length_bytes >>
    error_if!(client_id.is_empty() && !is_flag_set!(flags, CLEAN_SESSION), INVALID_CLIENT_ID) >>

    topic: cond!(is_flag_set!(flags, WILL), decode_utf8_str) >>
    message: cond!(is_flag_set!(flags, WILL), decode_utf8_str) >>
    username: cond!(is_flag_set!(flags, USERNAME), decode_utf8_str) >>
    password: cond!(is_flag_set!(flags, PASSWORD), decode_length_bytes) >>
    (
        Packet::Connect {
            clean_session: is_flag_set!(flags, CLEAN_SESSION),
            keep_alive: keep_alive,
            client_id: client_id,
            will: if is_flag_set!(flags, WILL) { Some(ConnectionWill{
                qos: QoS::from((flags & WILL_QOS.bits()) >> WILL_QOS_SHIFT),
                retain: is_flag_set!(flags, WILL_RETAIN),
                topic: topic.unwrap(),
                message: message.unwrap(),
            }) } else { None },
            username: username,
            password: password,
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

named!(pub decode_publish_header<(&str, u16)>, pair!(decode_utf8_str, be_u16));

named!(pub decode_subscribe_header<Packet>, do_parse!(
    packet_id: be_u16 >>
    topic_filters: many1!(pair!(decode_utf8_str, be_u8)) >>
    (
        Packet::Subscribe {
            packet_id: packet_id,
            topic_filters: topic_filters.iter()
                                        .map(|&(filter, flags)| (filter, QoS::from(flags & 0x03)))
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


fn decode_variable_header<'a>(i: &[u8], fixed_header: FixedHeader) -> IResult<&[u8], Packet> {
    match fixed_header.packet_type {
        CONNECT => decode_connect_header(i),
        CONNACK => {
            decode_connect_ack_header(i).map(|(flags, return_code)| {
                Packet::ConnectAck {
                    session_present: is_flag_set!(flags.bits(), SESSION_PRESENT),
                    return_code: return_code,
                }
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
                    Done(Default::default(),
                         Packet::Publish {
                             dup: dup,
                             retain: retain,
                             qos: qos,
                             topic: topic,
                             packet_id: packet_id,
                             payload: i,
                         })
                }
                Error(err) => Error(err),
                Incomplete(needed) => Incomplete(needed),
            }
        }
        PUBACK => be_u16(i).map(|packet_id| Packet::PublishAck { packet_id: packet_id }),
        PUBREC => be_u16(i).map(|packet_id| Packet::PublishReceived { packet_id: packet_id }),
        PUBREL => be_u16(i).map(|packet_id| Packet::PublishRelease { packet_id: packet_id }),
        PUBCOMP => be_u16(i).map(|packet_id| Packet::PublishComplete { packet_id: packet_id }),
        SUBSCRIBE => decode_subscribe_header(i),
        SUBACK => decode_subscribe_ack_header(i),
        UNSUBSCRIBE => decode_unsubscribe_header(i),
        UNSUBACK => be_u16(i).map(|packet_id| Packet::UnsubscribeAck { packet_id: packet_id }),

        PINGREQ => Done(i, Packet::PingRequest),
        PINGRESP => Done(i, Packet::PingResponse),
        DISCONNECT => Done(i, Packet::Disconnect),
        _ => {
            let err_code = UNSUPPORT_PACKET_TYPE + (fixed_header.packet_type as u32);

            Error(error_position!(ErrorKind::Custom(err_code), i))
        }
    }
}

named!(pub decode_packet<Packet>, do_parse!(
    fixed_header: decode_fixed_header >>
    packet: flat_map!(
        take!(fixed_header.remaining_length),
        apply!(decode_variable_header, fixed_header)
    ) >>
    ( packet )
));

/// Extends `AsRef<[u8]>` with methods for reading packet.
pub trait ReadPacketExt: AsRef<[u8]> {
    #[inline]
    /// Read packet from the underlying reader.
    fn read_packet(&self) -> Result<Packet, IError> {
        decode_packet(self.as_ref()).to_full_result()
    }
}

impl<T: AsRef<[u8]>> ReadPacketExt for T {}
