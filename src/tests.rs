extern crate env_logger;

use nom::{Needed, ErrorKind};
use nom::IResult::{Done, Incomplete, Error};

use packet::*;
use decode::*;

#[test]
fn test_decode_variable_length_encoding_usize() {
    assert_eq!(decode_variable_length_usize(b"\x7f\x7f"),
               Done(&b"\x7f"[..], 127));
    assert_eq!(decode_variable_length_usize(b"\x83\x7f"),
               Done(&b""[..], (3 << 7) + 127));
    assert_eq!(decode_variable_length_usize(b"\x83\x82\x81\x7f"),
               Done(&b""[..], ((((((3 << 7) + 2) << 7) + 1) << 7) + 127)));
    assert_eq!(decode_variable_length_usize(b"\xff\xff\xff\x7f"),
               Done(&b""[..],
                    ((((((0x7f << 7) + 0x7f) << 7) + 0x7f) << 7) + 127)));

    assert_eq!(decode_variable_length_usize(b"\xff\xff\xff"),
               Incomplete(Needed::Unknown));
    assert_eq!(decode_variable_length_usize(b"\xff\xff\xff\xff\xff\xff"),
               Error(ErrorKind::Digit));
}

#[test]
fn test_decode_fixed_header() {
    assert_eq!(decode_fixed_header(b"\x20\x7f"),
               Done(&b""[..],
                    FixedHeader {
                        packet_type: ControlType::ConnectAck,
                        packet_flags: 0,
                        remaining_length: 127,
                    }));

    assert_eq!(decode_fixed_header(b"\x3C\x82\x7f"),
               Done(&b""[..],
                    FixedHeader {
                        packet_type: ControlType::Publish,
                        packet_flags: 0x0C,
                        remaining_length: (2 << 7) + 0x7F,
                    }));
}

#[test]
fn test_decode_connect_packets() {
    assert_eq!(decode_connect_header(
        b"\x00\x04MQTT\x04\xC0\x00\x3C\x00\x0512345\x00\x04user\x00\x04pass"),
        Done(&b""[..], Packet::Connect {
            clean_session: false,
            keep_alive: 60,
            client_id: "12345",
            will: None,
            username: Some("user"),
            password: Some("pass"),
        }));

    assert_eq!(decode_connect_header(
        b"\x00\x04MQTT\x04\x14\x00\x3C\x00\x0512345\x00\x05topic\x00\x07message"),
        Done(&b""[..], Packet::Connect {
            clean_session: false,
            keep_alive: 60,
            client_id: "12345",
            will: Some(ConnectionWill{
                qos: QoS::ExactlyOnce,
                retain: false,
                topic: "topic",
                message: "message",
            }),
            username: None,
            password: None,
        }));

    assert_eq!(decode_connect_ack_header(b"\x01\x04"),
               Done(&b""[..],
                    (SESSION_PRESENT, ConnectReturnCode::ServiceUnavailable)));

    assert_eq!(decode_packet(b"\x20\x02\x01\x04"),
               Done(&b""[..],
                    Packet::ConnectAck {
                        session_present: true,
                        return_code: ConnectReturnCode::ServiceUnavailable,
                    }));

    assert_eq!(decode_packet(b"\xe0\x00"),
               Done(&b""[..], Packet::Disconnect));
}

#[test]
fn test_decode_publish_packets() {
    assert_eq!(decode_publish_header(b"\x00\x05topic\x12\x34"),
               Done(&b""[..], ("topic", 0x1234)));

    assert_eq!(decode_packet(b"\x3d\x09\x00\x05topic\x43\x21"),
               Done(&b""[..],
                    Packet::Publish {
                        dup: true,
                        retain: true,
                        qos: QoS::ExactlyOnce,
                        topic: "topic",
                        packet_id: 0x4321,
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
fn test_decode_ping_packets() {
    assert_eq!(decode_packet(b"\xc0\x00"),
               Done(&b""[..], Packet::PingRequest));
    assert_eq!(decode_packet(b"\xd0\x00"),
               Done(&b""[..], Packet::PingResponse));
}
