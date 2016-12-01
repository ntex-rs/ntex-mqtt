use std::str;
use std::convert::Into;

use nom::{be_u8, be_u16, IResult, Needed, ErrorKind};
use nom::IResult::{Done, Incomplete, Error};

use packet::*;

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
                Error(error_position!(ErrorKind::Digit, i))
            }
        }
    }
}

named!(pub decode_utf8_str<&str>, do_parse!(
    len: be_u16 >>
    buf: take!(len) >>
    (
        unsafe { str::from_utf8_unchecked(buf) }
    )
));

named!(pub decode_fixed_header<FixedHeader>, do_parse!(
    b0: bits!( pair!( take_bits!( u8, 4 ), take_bits!( u8, 4 ) ) ) >>
    remaining_length: decode_variable_length_usize >>
    (
        FixedHeader{
            packet_type: ControlType::from(b0.0),
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
    proto: take!(4) >>
    level: be_u8 >>
    flags: be_u8 >>
    keep_alive: be_u16 >>
    client_id: decode_utf8_str >>
    topic: cond!(is_flag_set!(flags, WILL), decode_utf8_str) >>
    message: cond!(is_flag_set!(flags, WILL), decode_utf8_str) >>
    username: cond!(is_flag_set!(flags, USERNAME), decode_utf8_str) >>
    password: cond!(is_flag_set!(flags, PASSWORD), decode_utf8_str) >>
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
    return_code: be_u8 >>
    ( (ConnectAckFlags::from_bits_truncate(flags), ConnectReturnCode::from(return_code)) )
));

named!(pub decode_publish_header<(&str, u16)>, do_parse!(
    topic: decode_utf8_str >>
    packet_id: be_u16 >>
    ( (topic, packet_id) )
));

fn decode_variable_header<'a>(i: &[u8], fixed_header: FixedHeader) -> IResult<&[u8], Packet> {
    match fixed_header.packet_type {
        ControlType::Connect => decode_connect_header(i),
        ControlType::ConnectAck => {
            decode_connect_ack_header(i).map(|(flags, return_code)| {
                Packet::ConnectAck {
                    session_present: is_flag_set!(flags.bits(), SESSION_PRESENT),
                    return_code: return_code,
                }
            })
        }
        ControlType::Publish => {
            decode_publish_header(i).map(|(topic, packet_id)| {
                Packet::Publish {
                    dup: (fixed_header.packet_flags & 0b1000) == 0b1000,
                    qos: QoS::from((fixed_header.packet_flags & 0b0110) >> 1),
                    retain: (fixed_header.packet_flags & 0b0001) == 0b0001,
                    topic: topic,
                    packet_id: packet_id,
                }
            })
        }
        ControlType::PublishAck => {
            be_u16(i).map(|packet_id| Packet::PublishAck { packet_id: packet_id })
        }
        ControlType::PublishReceived => {
            be_u16(i).map(|packet_id| Packet::PublishReceived { packet_id: packet_id })
        }
        ControlType::PublishRelease => {
            be_u16(i).map(|packet_id| Packet::PublishRelease { packet_id: packet_id })
        }
        ControlType::PublishComplete => {
            be_u16(i).map(|packet_id| Packet::PublishComplete { packet_id: packet_id })
        }

        ControlType::PingRequest => Done(i, Packet::PingRequest),
        ControlType::PingResponse => Done(i, Packet::PingResponse),
        ControlType::Disconnect => Done(i, Packet::Disconnect),

        _ => {
            let t: u8 = fixed_header.packet_type.into();
            let b = ((t as u32) << 4) + fixed_header.packet_flags as u32;

            Error(error_position!(ErrorKind::Custom(b), i))
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
