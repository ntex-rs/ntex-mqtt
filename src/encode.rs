use std::io::{self, Result, Error, ErrorKind};

use byteorder::{BigEndian, WriteBytesExt};

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
            Packet::Connect { ref will, client_id, username, password, .. } => {
                // Protocol Name + Protocol Level + Connect Flags + Keep Alive
                let mut n = 2 + 4 + 1 + 1 + 2;

                // Client Id
                n += 2 + client_id.len();

                // Will Topic + Will Message
                if let &Some(ConnectionWill { topic, message, .. }) = will {
                    n += 2 + topic.len() + 2 + message.len();
                }

                if let Some(s) = username {
                    n += 2 + s.len();
                }

                if let Some(s) = password {
                    n += 2 + s.len();
                }

                n
            }

            Packet::ConnectAck { .. } => 2, // Flags + Return Code

            Packet::Publish { topic, packet_id, payload, .. } => {
                // Topic + Packet Id + Payload
                2 + topic.len() + packet_id.map_or(0, |_| 2) + payload.len()
            }

            Packet::PublishAck { .. } |
            Packet::PublishReceived { .. } |
            Packet::PublishRelease { .. } |
            Packet::PublishComplete { .. } |
            Packet::UnsubscribeAck { .. } => 2, // Packet Id

            Packet::Subscribe { ref topic_filters, .. } => {
                2 + topic_filters.iter().fold(0, |acc, &(filter, _)| acc + 2 + filter.len() + 1)
            }

            Packet::SubscribeAck { ref status, .. } => 2 + status.len(),

            Packet::Unsubscribe { ref topic_filters, .. } => {
                2 + topic_filters.iter().fold(0, |acc, &filter| acc + 2 + filter.len())
            }

            Packet::PingRequest | Packet::PingResponse | Packet::Disconnect => 0,
        }
    }

    fn write_content(&mut self, packet: &Packet) -> Result<usize> {
        let mut n = 0;

        match *packet {
            Packet::Connect { clean_session,
                              keep_alive,
                              ref will,
                              client_id,
                              username,
                              password } => {
                n += self.write_utf8_str("MQTT")?;

                let mut flags = ConnectFlags::empty();

                if username.is_some() {
                    flags |= USERNAME;
                }
                if password.is_some() {
                    flags |= PASSWORD;
                }

                if let &Some(ConnectionWill { qos, retain, .. }) = will {
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

                n += self.write(&[4, flags.bits()])?;

                self.write_u16::<BigEndian>(keep_alive)?;
                n += 2;

                n += self.write_fixed_length_bytes(client_id)?;

                if let &Some(ConnectionWill { topic, message, .. }) = will {
                    n += self.write_utf8_str(topic)?;
                    n += self.write_utf8_str(message)?;
                }

                if let Some(s) = username {
                    n += self.write_utf8_str(s)?;
                }

                if let Some(s) = password {
                    n += self.write_fixed_length_bytes(s)?;
                }
            }

            Packet::ConnectAck { session_present, return_code } => {
                n += self.write(&[if session_present { 0x01 } else { 0x00 }, return_code.into()])?;
            }

            Packet::Publish { qos, topic, packet_id, payload, .. } => {
                n += self.write_utf8_str(topic)?;

                if qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce {
                    self.write_u16::<BigEndian>(packet_id.unwrap())?;

                    n += 2;
                }

                n += self.write(payload)?;
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

                for &(filter, qos) in topic_filters {
                    n += self.write_utf8_str(filter)? + self.write(&[qos.into()])?;
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

                for filter in topic_filters {
                    n += self.write_utf8_str(filter)?;
                }
            }

            Packet::PingRequest | Packet::PingResponse | Packet::Disconnect => {}
        }

        Ok(n)
    }

    #[inline]
    fn write_utf8_str(&mut self, s: &str) -> Result<usize> {
        self.write_u16::<BigEndian>(s.len() as u16)?;

        Ok(2 + self.write(s.as_bytes())?)
    }

    #[inline]
    fn write_fixed_length_bytes(&mut self, s: &[u8]) -> Result<usize> {
        self.write_u16::<BigEndian>(s.len() as u16)?;

        Ok(2 + self.write(s)?)
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
