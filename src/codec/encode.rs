use bytes::{BufMut, Bytes, BytesMut};
use string::String;

use super::*;
use packet::*;
use proto::*;

pub const MAX_VARIABLE_LENGTH: usize = 268435455; // 0xFF,0xFF,0xFF,0x7F

#[inline]
fn write_fixed_header(packet: &Packet, dst: &mut BytesMut) {
    let content_size = get_encoded_size(packet);

    debug!(
        "write FixedHeader {{ type={}, flags={}, remaining_length={} }} ",
        packet.packet_type(),
        packet.packet_flags(),
        content_size
    );

    dst.put_u8((packet.packet_type() << 4) | packet.packet_flags());
    write_variable_length(content_size, dst);
}

fn write_content(packet: &Packet, dst: &mut BytesMut) {
    match *packet {
        Packet::Connect { ref connect } => {
            match **connect {
                Connect {
                    protocol,
                    clean_session,
                    keep_alive,
                    ref last_will,
                    ref client_id,
                    ref username,
                    ref password,
                } => {
                    write_fixed_length_bytes(&Bytes::from_static(protocol.name().as_bytes()), dst);
                    // write_utf8_str(&protocol.name().to_owned(), dst);

                    let mut flags = ConnectFlags::empty();

                    if username.is_some() {
                        flags |= ConnectFlags::USERNAME;
                    }
                    if password.is_some() {
                        flags |= ConnectFlags::PASSWORD;
                    }

                    if let Some(LastWill { qos, retain, .. }) = *last_will {
                        flags |= ConnectFlags::WILL;

                        if retain {
                            flags |= ConnectFlags::WILL_RETAIN;
                        }

                        let b: u8 = qos.into();

                        flags |= ConnectFlags::from_bits_truncate(b << WILL_QOS_SHIFT);
                    }

                    if clean_session {
                        flags |= ConnectFlags::CLEAN_SESSION;
                    }

                    dst.put_slice(&[protocol.level(), flags.bits()]);

                    dst.put_u16_be(keep_alive);

                    write_utf8_str(client_id, dst);

                    if let Some(LastWill {
                        ref topic,
                        ref message,
                        ..
                    }) = *last_will
                    {
                        write_utf8_str(topic, dst);
                        write_fixed_length_bytes(message, dst);
                    }

                    if let Some(ref s) = *username {
                        write_utf8_str(s, dst);
                    }

                    if let Some(ref s) = *password {
                        write_fixed_length_bytes(s, dst);
                    }
                }
            }
        }

        Packet::ConnectAck {
            session_present,
            return_code,
        } => {
            dst.put_slice(&[
                if session_present { 0x01 } else { 0x00 },
                return_code.into(),
            ]);
        }

        Packet::Publish {
            qos,
            ref topic,
            packet_id,
            ref payload,
            ..
        } => {
            write_utf8_str(topic, dst);

            if qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce {
                dst.put_u16_be(packet_id.unwrap());
            }

            dst.put(payload);
        }

        Packet::PublishAck { packet_id }
        | Packet::PublishReceived { packet_id }
        | Packet::PublishRelease { packet_id }
        | Packet::PublishComplete { packet_id }
        | Packet::UnsubscribeAck { packet_id } => {
            dst.put_u16_be(packet_id);
        }

        Packet::Subscribe {
            packet_id,
            ref topic_filters,
        } => {
            dst.put_u16_be(packet_id);

            for &(ref filter, qos) in topic_filters {
                write_utf8_str(filter, dst);
                dst.put_slice(&[qos.into()]);
            }
        }

        Packet::SubscribeAck {
            packet_id,
            ref status,
        } => {
            dst.put_u16_be(packet_id);

            let buf: Vec<u8> = status
                .iter()
                .map(|s| {
                    if let SubscribeReturnCode::Success(qos) = *s {
                        qos.into()
                    } else {
                        0x80
                    }
                }).collect();

            dst.put_slice(&buf);
        }

        Packet::Unsubscribe {
            packet_id,
            ref topic_filters,
        } => {
            dst.put_u16_be(packet_id);

            for filter in topic_filters {
                write_utf8_str(filter, dst);
            }
        }

        Packet::PingRequest | Packet::PingResponse | Packet::Disconnect => {}
    }
}

#[inline]
fn write_utf8_str(s: &String<Bytes>, dst: &mut BytesMut) {
    dst.put_u16_be(s.len() as u16);
    dst.put(s.get_ref());
}

#[inline]
fn write_fixed_length_bytes(s: &Bytes, dst: &mut BytesMut) {
    let r = s.as_ref();
    dst.put_u16_be(r.len() as u16);
    dst.put(s);
}

#[inline]
fn write_variable_length(size: usize, dst: &mut BytesMut) {
    // todo: verify at higher level
    // if size > MAX_VARIABLE_LENGTH {
    //     Err(Error::new(ErrorKind::Other, "out of range"))
    if size <= 127 {
        dst.put_u8(size as u8);
    } else if size <= 16383 {
        // 127 + 127 << 7
        dst.put_slice(&[((size % 128) | 0x80) as u8, (size >> 7) as u8]);
    } else if size <= 2097151 {
        // 127 + 127 << 7 + 127 << 14
        dst.put_slice(&[
            ((size % 128) | 0x80) as u8,
            (((size >> 7) % 128) | 0x80) as u8,
            (size >> 14) as u8,
        ]);
    } else {
        dst.put_slice(&[
            ((size % 128) | 0x80) as u8,
            (((size >> 7) % 128) | 0x80) as u8,
            (((size >> 14) % 128) | 0x80) as u8,
            (size >> 21) as u8,
        ]);
    }
}

pub fn write_packet(packet: &Packet, dst: &mut BytesMut) {
    write_fixed_header(packet, dst);
    write_content(packet, dst);
}

pub fn get_encoded_size(packet: &Packet) -> usize {
    match *packet {
        Packet::Connect { ref connect } => {
            match **connect {
                Connect {ref last_will, ref client_id, ref username, ref password, ..} =>
                {
                    // Protocol Name + Protocol Level + Connect Flags + Keep Alive
                    let mut n = 2 + 4 + 1 + 1 + 2;

                    // Client Id
                    n += 2 + client_id.len();

                    // Will Topic + Will Message
                    if let Some(LastWill { ref topic, ref message, .. }) = *last_will {
                        n += 2 + topic.len() + 2 + message.len();
                    }

                    if let Some(ref s) = *username {
                        n += 2 + s.len();
                    }

                    if let Some(ref s) = *password {
                        n += 2 + s.len();
                    }

                    n
                }
            }
        }


        Packet::Publish { ref topic, packet_id, ref payload, .. } => {
            // Topic + Packet Id + Payload
            2 + topic.len() + packet_id.map_or(0, |_| 2) + payload.len()
        }

        Packet::ConnectAck { .. } | // Flags + Return Code
        Packet::PublishAck { .. } | // Packet Id
        Packet::PublishReceived { .. } | // Packet Id
        Packet::PublishRelease { .. } | // Packet Id
        Packet::PublishComplete { .. } | // Packet Id
        Packet::UnsubscribeAck { .. } => 2, // Packet Id

        Packet::Subscribe { ref topic_filters, .. } => {
            2 + topic_filters.iter().fold(0, |acc, &(ref filter, _)| acc + 2 + filter.len() + 1)
        }

        Packet::SubscribeAck { ref status, .. } => 2 + status.len(),

        Packet::Unsubscribe { ref topic_filters, .. } => {
            2 + topic_filters.iter().fold(0, |acc, filter| acc + 2 + filter.len())
        }

        Packet::PingRequest | Packet::PingResponse | Packet::Disconnect => 0,
    }
}

#[cfg(test)]
mod tests {
    //extern crate env_logger;

    use super::*;
    use bytes::Bytes;
    use decode::*;
    use packet::*;
    use proto::*;

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
    fn test_encode_fixed_header() {
        let mut v = Vec::new();
        let p = Packet::PingRequest;

        assert_eq!(calc_remaining_length(&p), 0);
        assert_eq!(v.write_fixed_header(&p).unwrap(), 2);
        assert_eq!(v, b"\xc0\x00");

        v.clear();

        let p = Packet::Publish {
            dup: true,
            retain: true,
            qos: QoS::ExactlyOnce,
            topic: "topic".to_owned(),
            packet_id: Some(0x4321),
            payload: (0..255).collect::<Vec<u8>>().into(),
        };

        assert_eq!(calc_remaining_length(&p), 264);
        assert_eq!(v.write_fixed_header(&p).unwrap(), 3);
        assert_eq!(v, b"\x3d\x88\x02");
    }

    macro_rules! assert_packet {
        ($p:expr, $data:expr) => {
            let mut v = Vec::new();
            assert_eq!(v.write_packet(&$p).unwrap(), $data.len());
            assert_eq!(v, $data);
            assert_eq!(read_packet($data).unwrap(), (&b""[..], $p));
        };
    }

    #[test]
    fn test_encode_connect_packets() {
        assert_packet!(
            Packet::Connect {
                protocol: Protocol::MQTT(4),
                clean_session: false,
                keep_alive: 60,
                client_id: "12345".to_owned(),
                last_will: None,
                username: Some("user".to_owned()),
                password: Some(Bytes::from(&b"pass"[..])),
            },
            &b"\x10\x1D\x00\x04MQTT\x04\xC0\x00\x3C\x00\
\x0512345\x00\x04user\x00\x04pass"[..]
        );

        assert_packet!(
            Packet::Connect {
                protocol: Protocol::MQTT(4),
                clean_session: false,
                keep_alive: 60,
                client_id: "12345".to_owned(),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: "topic".to_owned(),
                    message: Bytes::from(&b"message"[..]),
                }),
                username: None,
                password: None,
            },
            &b"\x10\x21\x00\x04MQTT\x04\x14\x00\x3C\x00\
\x0512345\x00\x05topic\x00\x07message"[..]
        );

        assert_packet!(Packet::Disconnect, b"\xe0\x00");
    }

    #[test]
    fn test_encode_publish_packets() {
        assert_packet!(
            Packet::Publish {
                dup: true,
                retain: true,
                qos: QoS::ExactlyOnce,
                topic: "topic".to_owned(),
                packet_id: Some(0x4321),
                payload: PayloadPromise::from(&b"data"[..]),
            },
            b"\x3d\x0D\x00\x05topic\x43\x21data"
        );

        assert_packet!(
            Packet::Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                topic: "topic".to_owned(),
                packet_id: None,
                payload: PayloadPromise::from(&b"data"[..]),
            },
            b"\x30\x0b\x00\x05topicdata"
        );
    }

    #[test]
    fn test_encode_subscribe_packets() {
        assert_packet!(
            Packet::Subscribe {
                packet_id: 0x1234,
                topic_filters: vec![
                    ("test".to_owned(), QoS::AtLeastOnce),
                    ("filter".to_owned(), QoS::ExactlyOnce)
                ],
            },
            b"\x82\x12\x12\x34\x00\x04test\x01\x00\x06filter\x02"
        );

        assert_packet!(
            Packet::SubscribeAck {
                packet_id: 0x1234,
                status: vec![
                    SubscribeReturnCode::Success(QoS::AtLeastOnce),
                    SubscribeReturnCode::Failure,
                    SubscribeReturnCode::Success(QoS::ExactlyOnce)
                ],
            },
            b"\x90\x05\x12\x34\x01\x80\x02"
        );

        assert_packet!(
            Packet::Unsubscribe {
                packet_id: 0x1234,
                topic_filters: vec!["test".to_owned(), "filter".to_owned()],
            },
            b"\xa2\x10\x12\x34\x00\x04test\x00\x06filter"
        );

        assert_packet!(
            Packet::UnsubscribeAck { packet_id: 0x4321 },
            b"\xb0\x02\x43\x21"
        );
    }

    #[test]
    fn test_encode_ping_packets() {
        assert_packet!(Packet::PingRequest, b"\xc0\x00");
        assert_packet!(Packet::PingResponse, b"\xd0\x00");
    }
}
