use std::io::{self, Result, Error, ErrorKind};

use byteorder::{BigEndian, WriteBytesExt};

use proto::*;
use packet::*;

pub const MAX_VARIABLE_LENGTH: usize = 268435455; // 0xFF,0xFF,0xFF,0x7F

pub trait WritePacketHelper: io::Write {
    #[inline]
    fn write_fixed_header(&mut self, packet: &Packet) -> Result<usize> {
        let content_size = self.calc_content_size(packet);

        debug!("write FixedHeader {{ type={}, flags={}, remaining_length={} }} ",
               packet.packet_type(),
               packet.packet_flags(),
               content_size);

        Ok(self.write(&[(packet.packet_type() << 4) | packet.packet_flags()])? +
           self.write_variable_length(content_size)?)
    }

    fn calc_content_size(&mut self, packet: &Packet) -> usize {
        match *packet {
            Packet::Connect { ref last_will, ref client_id, ref username, ref password, .. } => {
                // Protocol Name + Protocol Level + Connect Flags + Keep Alive
                let mut n = 2 + 4 + 1 + 1 + 2;

                // Client Id
                n += 2 + client_id.len();

                // Will Topic + Will Message
                if let &Some(LastWill { ref topic, ref message, .. }) = last_will {
                    n += 2 + topic.len() + 2 + message.len();
                }

                if let &Some(ref s) = username {
                    n += 2 + s.len();
                }

                if let &Some(ref s) = password {
                    n += 2 + s.len();
                }

                n
            }

            Packet::ConnectAck { .. } => 2, // Flags + Return Code

            Packet::Publish { ref topic, packet_id, ref payload, .. } => {
                // Topic + Packet Id + Payload
                2 + topic.len() + packet_id.map_or(0, |_| 2) + payload.len()
            }

            Packet::PublishAck { .. } |
            Packet::PublishReceived { .. } |
            Packet::PublishRelease { .. } |
            Packet::PublishComplete { .. } |
            Packet::UnsubscribeAck { .. } => 2, // Packet Id

            Packet::Subscribe { ref topic_filters, .. } => {
                2 + topic_filters.iter().fold(0, |acc, &(ref filter, _)| acc + 2 + filter.len() + 1)
            }

            Packet::SubscribeAck { ref status, .. } => 2 + status.len(),

            Packet::Unsubscribe { ref topic_filters, .. } => {
                2 + topic_filters.iter().fold(0, |acc, ref filter| acc + 2 + filter.len())
            }

            Packet::PingRequest | Packet::PingResponse | Packet::Disconnect => 0,
        }
    }

    fn write_content(&mut self, packet: &Packet) -> Result<usize> {
        let mut n = 0;

        match *packet {
            Packet::Connect { protocol,
                              clean_session,
                              keep_alive,
                              ref last_will,
                              ref client_id,
                              ref username,
                              ref password } => {
                n += self.write_utf8_str(&protocol.name().to_owned())?;

                let mut flags = ConnectFlags::empty();

                if username.is_some() {
                    flags |= USERNAME;
                }
                if password.is_some() {
                    flags |= PASSWORD;
                }

                if let &Some(LastWill { qos, retain, .. }) = last_will {
                    flags |= WILL;

                    if retain {
                        flags |= WILL_RETAIN;
                    }

                    let b: u8 = qos.into();

                    flags |= ConnectFlags::from_bits_truncate(b << WILL_QOS_SHIFT);
                }

                if clean_session {
                    flags |= CLEAN_SESSION;
                }

                n += self.write(&[protocol.level(), flags.bits()])?;

                self.write_u16::<BigEndian>(keep_alive)?;
                n += 2;

                n += self.write_utf8_str(client_id)?;

                if let &Some(LastWill { ref topic, ref message, .. }) = last_will {
                    n += self.write_utf8_str(&topic)?;
                    n += self.write_fixed_length_bytes(message)?;
                }

                if let &Some(ref s) = username {
                    n += self.write_utf8_str(&s)?;
                }

                if let &Some(ref s) = password {
                    n += self.write_fixed_length_bytes(s)?;
                }
            }

            Packet::ConnectAck { session_present, return_code } => {
                n += self.write(&[if session_present { 0x01 } else { 0x00 }, return_code.into()])?;
            }

            Packet::Publish { qos, ref topic, packet_id, ref payload, .. } => {
                n += self.write_utf8_str(&topic)?;

                if qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce {
                    self.write_u16::<BigEndian>(packet_id.unwrap())?;

                    n += 2;
                }

                n += self.write(&payload)?;
            }

            Packet::PublishAck { packet_id } |
            Packet::PublishReceived { packet_id } |
            Packet::PublishRelease { packet_id } |
            Packet::PublishComplete { packet_id } |
            Packet::UnsubscribeAck { packet_id } => {
                self.write_u16::<BigEndian>(packet_id)?;

                n += 2;
            }

            Packet::Subscribe { packet_id, ref topic_filters } => {
                self.write_u16::<BigEndian>(packet_id)?;

                n += 2;

                for &(ref filter, qos) in topic_filters {
                    n += self.write_utf8_str(&filter)? + self.write(&[qos.into()])?;
                }
            }

            Packet::SubscribeAck { packet_id, ref status } => {
                self.write_u16::<BigEndian>(packet_id)?;

                n += 2;

                let buf: Vec<u8> = status.iter()
                    .map(|s| if let SubscribeReturnCode::Success(qos) = *s {
                        qos.into()
                    } else {
                        0x80
                    })
                    .collect();

                n += self.write(&buf)?;
            }

            Packet::Unsubscribe { packet_id, ref topic_filters } => {
                self.write_u16::<BigEndian>(packet_id)?;

                n += 2;

                for ref filter in topic_filters {
                    n += self.write_utf8_str(&filter)?;
                }
            }

            Packet::PingRequest | Packet::PingResponse | Packet::Disconnect => {}
        }

        Ok(n)
    }

    #[inline]
    fn write_utf8_str(&mut self, s: &String) -> Result<usize> {
        self.write_u16::<BigEndian>(s.len() as u16)?;

        Ok(2 + self.write(s.as_bytes())?)
    }

    #[inline]
    fn write_fixed_length_bytes<T: AsRef<[u8]>>(&mut self, s: T) -> Result<usize> {
        let r = s.as_ref();
        self.write_u16::<BigEndian>(r.len() as u16)?;

        Ok(2 + self.write(r)?)
    }

    #[inline]
    fn write_variable_length(&mut self, size: usize) -> Result<usize> {
        if size > MAX_VARIABLE_LENGTH {
            Err(Error::new(ErrorKind::Other, "out of range"))
        } else if size < 128 {
            self.write(&[size as u8])
        } else {
            let mut v = Vec::new();
            let mut s = size;

            while s > 0 {
                let mut b = (s % 128) as u8;

                s >>= 7;

                if s > 0 {
                    b |= 0x80;
                }

                v.push(b);
            }

            debug!("write variable length {} in {} bytes", size, v.len());

            self.write(&v)
        }
    }
}

/// Extends `Write` with methods for writing packet.
///
/// ```
/// use mqtt::{WritePacketExt, Packet};
///
/// let mut v = Vec::new();
/// let p = Packet::PingResponse;
///
/// assert_eq!(v.write_packet(&p).unwrap(), 2);
/// assert_eq!(v, b"\xd0\x00");
/// ```
pub trait WritePacketExt: io::Write {
    #[inline]
    /// Writes packet to the underlying writer.
    fn write_packet(&mut self, packet: &Packet) -> Result<usize> {
        Ok(self.write_fixed_header(packet)? + self.write_content(packet)?)
    }
}

impl<W: io::Write + ?Sized> WritePacketHelper for W {}
impl<W: io::Write + ?Sized> WritePacketExt for W {}

#[cfg(test)]
mod tests {
    //extern crate env_logger;

    use proto::*;
    use packet::*;
    use decode::*;
    use super::*;

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

        assert_eq!(v.calc_content_size(&p), 0);
        assert_eq!(v.write_fixed_header(&p).unwrap(), 2);
        assert_eq!(v, b"\xc0\x00");

        v.clear();

        let p = Packet::Publish {
            dup: true,
            retain: true,
            qos: QoS::ExactlyOnce,
            topic: "topic".to_owned(),
            packet_id: Some(0x4321),
            payload: Bytes::from((0..255).collect::<Vec<u8>>()),
        };

        assert_eq!(v.calc_content_size(&p), 264);
        assert_eq!(v.write_fixed_header(&p).unwrap(), 3);
        assert_eq!(v, b"\x3d\x88\x02");
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
                           protocol: Protocol::MQTT(4),
                           clean_session: false,
                           keep_alive: 60,
                           client_id: "12345".to_owned(),
                           last_will: None,
                           username: Some("user".to_owned()),
                           password: Some(Bytes::from(&b"pass"[..])),
                       },
                       &b"\x10\x1D\x00\x04MQTT\x04\xC0\x00\x3C\x00\
\x0512345\x00\x04user\x00\x04pass"[..]);

        assert_packet!(Packet::Connect {
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
\x0512345\x00\x05topic\x00\x07message"[..]);

        assert_packet!(Packet::Disconnect, b"\xe0\x00");
    }

    #[test]
    fn test_encode_publish_packets() {
        assert_packet!(Packet::Publish {
                           dup: true,
                           retain: true,
                           qos: QoS::ExactlyOnce,
                           topic: "topic".to_owned(),
                           packet_id: Some(0x4321),
                           payload: Bytes::from(&b"data"[..]),
                       },
                       b"\x3d\x0D\x00\x05topic\x43\x21data");

        assert_packet!(Packet::Publish {
                           dup: false,
                           retain: false,
                           qos: QoS::AtMostOnce,
                           topic: "topic".to_owned(),
                           packet_id: None,
                           payload: Bytes::from(&b"data"[..]),
                       },
                       b"\x30\x0b\x00\x05topicdata");
    }

    #[test]
    fn test_encode_subscribe_packets() {
        assert_packet!(Packet::Subscribe {
                           packet_id: 0x1234,
                           topic_filters: vec![("test".to_owned(), QoS::AtLeastOnce),
                                               ("filter".to_owned(), QoS::ExactlyOnce)],
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
                           topic_filters: vec!["test".to_owned(), "filter".to_owned()],
                       },
                       b"\xa2\x10\x12\x34\x00\x04test\x00\x06filter");

        assert_packet!(Packet::UnsubscribeAck { packet_id: 0x4321 },
                       b"\xb0\x02\x43\x21");
    }

    #[test]
    fn test_encode_ping_packets() {
        assert_packet!(Packet::PingRequest, b"\xc0\x00");
        assert_packet!(Packet::PingResponse, b"\xd0\x00");
    }
}
