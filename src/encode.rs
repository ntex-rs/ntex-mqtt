use std::io::{self, Result, Cursor};

use byteorder::{BigEndian, WriteBytesExt};

use packet::*;

pub trait WritePacketExt: io::Write + io::Seek {
    #[inline]
    fn write_packet(&mut self, packet: &Packet) -> Result<usize> {
        let mut c = Cursor::new(Vec::new());

        match *packet {
            Packet::Connect { clean_session,
                              keep_alive,
                              ref will,
                              client_id,
                              username,
                              password } => {
                self.write_utf8_str("MQTT")?;

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

                self.write(&[4, flags.bits()])?;
                c.write_u16::<BigEndian>(keep_alive)?;
                self.write_fixed_length_bytes(client_id)?;

                if let &Some(ConnectionWill { topic, message, .. }) = will {
                    self.write_utf8_str(topic)?;
                    self.write_utf8_str(message)?;
                }

                if let Some(s) = username {
                    self.write_utf8_str(s)?;
                }

                if let Some(s) = password {
                    self.write_fixed_length_bytes(s)?;
                }
            }

            Packet::ConnectAck { session_present, return_code } => {
                self.write(&[if session_present { 0x01 } else { 0x00 }, return_code.into()])?;
            }

            Packet::Publish { qos, topic, packet_id, payload, .. } => {
                self.write_utf8_str(topic)?;

                if qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce {
                    c.write_u16::<BigEndian>(packet_id.unwrap())?;
                }

                self.write(payload)?;
            }

            Packet::PublishAck { packet_id } |
            Packet::PublishReceived { packet_id } |
            Packet::PublishRelease { packet_id } |
            Packet::PublishComplete { packet_id } |
            Packet::UnsubscribeAck { packet_id } => {
                c.write_u16::<BigEndian>(packet_id)?;
            }

            Packet::Subscribe { packet_id, ref topic_filters } => {
                c.write_u16::<BigEndian>(packet_id)?;

                for &(filter, qos) in topic_filters {
                    self.write_utf8_str(filter)? + self.write(&[qos.into()])?;
                }
            }

            Packet::SubscribeAck { packet_id, ref status } => {
                c.write_u16::<BigEndian>(packet_id)?;

                let buf: Vec<u8> = status.iter()
                    .map(|s| if let SubscribeStatus::Success(qos) = *s {
                        qos.into()
                    } else {
                        0x80
                    })
                    .collect();

                self.write(&buf)?;
            }

            Packet::Unsubscribe { packet_id, ref topic_filters } => {
                c.write_u16::<BigEndian>(packet_id)?;

                for filter in topic_filters {
                    self.write_utf8_str(filter)?;
                }
            }

            _ => {}
        }

        let buf = c.into_inner();

        Ok(self.write(&[(packet.packet_type() << 4) + packet.packet_flags()])? +
           self.write_variable_length(buf.len())? + self.write(&buf)?)
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
        let mut n = 0;
        let mut s = size;

        while s > 0 {
            let mut b = s % 128;
            s >>= 7;

            if s > 0 {
                b |= 0x80;
            }

            n += self.write(&[b as u8])?
        }

        Ok(n)
    }
}

impl<W: io::Write + io::Seek + ?Sized> WritePacketExt for W {}
