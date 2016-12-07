extern crate env_logger;

use nom::{Needed, ErrorKind};
use nom::IResult::{Done, Incomplete, Error};

use proto::*;
use packet::*;
use decode::*;
use encode::*;

#[test]
fn test_decode_variable_length() {
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
               Error(ErrorKind::Custom(INVALID_LENGTH)));
}

#[test]
fn test_encode_variable_length() {
    let mut v = Vec::new();

    assert_eq!(v.write_variable_length(123).unwrap(), 1);
    assert_eq!(v, &[123]);

    v.clear();

    assert_eq!(v.write_variable_length(129).unwrap(), 2);
    assert_eq!(v, b"\x81\x01");

    v.clear();

    assert_eq!(v.write_variable_length(16383).unwrap(), 2);
    assert_eq!(v, b"\xff\x7f");

    v.clear();

    assert_eq!(v.write_variable_length(2097151).unwrap(), 3);
    assert_eq!(v, b"\xff\xff\x7f");

    v.clear();

    assert_eq!(v.write_variable_length(268435455).unwrap(), 4);
    assert_eq!(v, b"\xff\xff\xff\x7f");

    assert!(v.write_variable_length(MAX_VARIABLE_LENGTH + 1).is_err())
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
                        remaining_length: (2 << 7) + 0x7F,
                    }));

    assert_eq!(decode_fixed_header(b"\x20"), Incomplete(Needed::Unknown));
}

#[test]
fn test_encode_fixed_header() {
    let mut v = Vec::new();
    let p = Packet::PingRequest;

    assert_eq!(v.calc_content_size(&p), 0);
    assert_eq!(v.write_fixed_header(&p).unwrap(), 2);
    assert_eq!(v, b"\xc0\x00");

    v.clear();

    let p = Packet::Publish {
        dup: true,
        retain: true,
        qos: QoS::ExactlyOnce,
        topic: "topic",
        packet_id: Some(0x4321),
        payload: &(0..255).map(|b| b).collect::<Vec<u8>>(),
    };

    assert_eq!(v.calc_content_size(&p), 264);
    assert_eq!(v.write_fixed_header(&p).unwrap(), 3);
    assert_eq!(v, b"\x3d\x88\x02");
}

#[test]
fn test_decode_connect_packets() {
    assert_eq!(decode_connect_header(
        b"\x00\x04MQTT\x04\xC0\x00\x3C\x00\x0512345\x00\x04user\x00\x04pass"),
        Done(&b""[..], Packet::Connect {
            clean_session: false,
            keep_alive: 60,
            client_id: b"12345",
            will: None,
            username: Some("user"),
            password: Some(b"pass"),
        }));

    assert_eq!(decode_connect_header(
        b"\x00\x04MQTT\x04\x14\x00\x3C\x00\x0512345\x00\x05topic\x00\x07message"),
        Done(&b""[..], Packet::Connect {
            clean_session: false,
            keep_alive: 60,
            client_id: b"12345",
            will: Some(ConnectionWill{
                qos: QoS::ExactlyOnce,
                retain: false,
                topic: "topic",
                message: "message",
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

macro_rules! assert_packet {
    ($p:expr, $data:expr) => {
        let mut v = Vec::new();
        assert_eq!(v.write_packet(&$p).unwrap(), $data.len());
        assert_eq!(v, $data);
        assert_eq!(read_packet($data).unwrap(), (&b""[..], $p));
    }
}

#[test]
fn test_encode_connect_packets() {
    assert_packet!(Packet::Connect {
                       clean_session: false,
                       keep_alive: 60,
                       client_id: b"12345",
                       will: None,
                       username: Some("user"),
                       password: Some(b"pass"),
                   },
                   &b"\x10\x1D\x00\x04MQTT\x04\xC0\x00\x3C\x00\
\x0512345\x00\x04user\x00\x04pass"[..]);

    assert_packet!(Packet::Connect {
                       clean_session: false,
                       keep_alive: 60,
                       client_id: b"12345",
                       will: Some(ConnectionWill {
                           qos: QoS::ExactlyOnce,
                           retain: false,
                           topic: "topic",
                           message: "message",
                       }),
                       username: None,
                       password: None,
                   },
                   &b"\x10\x21\x00\x04MQTT\x04\x14\x00\x3C\x00\
\x0512345\x00\x05topic\x00\x07message"[..]);

    assert_packet!(Packet::Disconnect, b"\xe0\x00");
}

#[test]
fn test_decode_publish_packets() {
    assert_eq!(decode_publish_header(b"\x00\x05topic\x12\x34"),
               Done(&b""[..], ("topic", 0x1234)));

    assert_eq!(decode_packet(b"\x3d\x0D\x00\x05topic\x43\x21data"),
               Done(&b""[..],
                    Packet::Publish {
                        dup: true,
                        retain: true,
                        qos: QoS::ExactlyOnce,
                        topic: "topic",
                        packet_id: Some(0x4321),
                        payload: b"data",
                    }));
    assert_eq!(decode_packet(b"\x30\x0b\x00\x05topicdata"),
               Done(&b""[..],
                    Packet::Publish {
                        dup: false,
                        retain: false,
                        qos: QoS::AtMostOnce,
                        topic: "topic",
                        packet_id: None,
                        payload: b"data",
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
fn test_encode_publish_packets() {
    assert_packet!(Packet::Publish {
                       dup: true,
                       retain: true,
                       qos: QoS::ExactlyOnce,
                       topic: "topic",
                       packet_id: Some(0x4321),
                       payload: b"data",
                   },
                   b"\x3d\x0D\x00\x05topic\x43\x21data");

    assert_packet!(Packet::Publish {
                       dup: false,
                       retain: false,
                       qos: QoS::AtMostOnce,
                       topic: "topic",
                       packet_id: None,
                       payload: b"data",
                   },
                   b"\x30\x0b\x00\x05topicdata");
}

#[test]
fn test_decode_subscribe_packets() {
    let p = Packet::Subscribe {
        packet_id: 0x1234,
        topic_filters: vec![("test", QoS::AtLeastOnce), ("filter", QoS::ExactlyOnce)],
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
        topic_filters: vec!["test", "filter"],
    };

    assert_eq!(decode_unsubscribe_header(b"\x12\x34\x00\x04test\x00\x06filter"),
               Done(&b""[..], p.clone()));
    assert_eq!(decode_packet(b"\xa2\x10\x12\x34\x00\x04test\x00\x06filter"),
               Done(&b""[..], p));

    assert_eq!(decode_packet(b"\xb0\x02\x43\x21"),
               Done(&b""[..], Packet::UnsubscribeAck { packet_id: 0x4321 }));
}

#[test]
fn test_encode_subscribe_packets() {
    assert_packet!(Packet::Subscribe {
                       packet_id: 0x1234,
                       topic_filters: vec![("test", QoS::AtLeastOnce),
                                           ("filter", QoS::ExactlyOnce)],
                   },
                   b"\x82\x12\x12\x34\x00\x04test\x01\x00\x06filter\x02");

    assert_packet!(Packet::SubscribeAck {
                       packet_id: 0x1234,
                       status: vec![SubscribeReturnCode::Success(QoS::AtLeastOnce),
                                    SubscribeReturnCode::Failure,
                                    SubscribeReturnCode::Success(QoS::ExactlyOnce)],
                   },
                   b"\x90\x05\x12\x34\x01\x80\x02");

    assert_packet!(Packet::Unsubscribe {
                       packet_id: 0x1234,
                       topic_filters: vec!["test", "filter"],
                   },
                   b"\xa2\x10\x12\x34\x00\x04test\x00\x06filter");

    assert_packet!(Packet::UnsubscribeAck { packet_id: 0x4321 },
                   b"\xb0\x02\x43\x21");
}

#[test]
fn test_decode_ping_packets() {
    assert_eq!(decode_packet(b"\xc0\x00"),
               Done(&b""[..], Packet::PingRequest));
    assert_eq!(decode_packet(b"\xd0\x00"),
               Done(&b""[..], Packet::PingResponse));
}

#[test]
fn test_encode_ping_packets() {
    assert_packet!(Packet::PingRequest, b"\xc0\x00");
    assert_packet!(Packet::PingResponse, b"\xd0\x00");
}
