use std::mem;
use std::io::prelude::*;

use bytes::{Buf, BufMut, ByteBuf, SliceBuf};
use nom::IError;

use error::*;
use decode::*;
use encode::*;

#[macro_export]
macro_rules! const_enum {
    ($name:ty : $repr:ty) => {
        impl From<$repr> for $name {
            fn from(u: $repr) -> Self {
                unsafe { mem::transmute(u) }
            }
        }

        impl Into<$repr> for $name {
            fn into(self) -> $repr {
                unsafe { mem::transmute(self) }
            }
        }
    }
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
/// Quality of Service levels
pub enum QoS {
    /// At most once delivery
    ///
    /// The message is delivered according to the capabilities of the underlying network.
    /// No response is sent by the receiver and no retry is performed by the sender.
    /// The message arrives at the receiver either once or not at all.
    AtMostOnce = 0,
    /// At least once delivery
    ///
    /// This quality of service ensures that the message arrives at the receiver at least once.
    /// A QoS 1 PUBLISH Packet has a Packet Identifier in its variable header
    /// and is acknowledged by a PUBACK Packet.
    AtLeastOnce = 1,
    /// Exactly once delivery
    ///
    /// This is the highest quality of service,
    /// for use when neither loss nor duplication of messages are acceptable.
    /// There is an increased overhead associated with this quality of service.
    ExactlyOnce = 2,
}

const_enum!(QoS: u8);

pub enum State {
    Receiving(ByteBuf),
    Sending(SliceBuf<Vec<u8>>, ByteBuf),
    Closed,
}

impl State {
    pub fn receiving() -> State {
        State::Receiving(ByteBuf::with_capacity(8 * 1024))
    }

    #[inline]
    fn read_buf(&self) -> &[u8] {
        match *self {
            State::Receiving(ref buf) => buf.bytes(),
            _ => panic!("connection not in reading state"),
        }
    }

    #[inline]
    fn mut_read_buf(&mut self) -> &mut [u8] {
        match *self {
            State::Receiving(ref mut buf) => unsafe { buf.bytes_mut() },
            _ => panic!("connection not in reading state"),
        }
    }

    #[inline]
    fn write_buf(&self) -> &[u8] {
        match *self {
            State::Sending(ref buf, _) => buf.bytes(),
            _ => panic!("connection not in writing state"),
        }
    }

    #[inline]
    fn mut_write_buf(&mut self) -> &mut [u8] {
        match *self {
            State::Sending(ref mut buf, _) => unsafe { buf.bytes_mut() },
            _ => panic!("connection not in writing state"),
        }
    }

    #[inline]
    fn unwrap_read_buf(self) -> ByteBuf {
        match self {
            State::Receiving(buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    #[inline]
    fn unwrap_write_buf(self) -> (SliceBuf<Vec<u8>>, ByteBuf) {
        match self {
            State::Sending(buf, remaining) => (buf, remaining),
            _ => panic!("connection not in writing state"),
        }
    }

    #[inline]
    pub fn async_read<R: Read>(&mut self, r: &mut R) -> Result<()> {
        match r.read(self.mut_read_buf()) {
            Ok(0) => {
                debug!("read 0 bytes from client, buffered {} bytes",
                       self.read_buf().len());

                match self.read_buf().len() {
                    n if n > 0 => self.try_transition_to_writing(0),
                    _ => {
                        *self = State::Closed;
                        Ok(())
                    }
                }
            }
            Ok(n) => {
                debug!("read {} bytes from client", n);

                self.try_transition_to_writing(n)
            }
            Err(err) => {
                warn!("read failed, {}", err);

                bail!(err)
            }
        }
    }

    #[inline]
    pub fn async_write<W: Write>(&mut self, w: &mut W) -> Result<()> {
        match w.write(self.mut_write_buf()) {
            Ok(0) => {
                debug!("wrote 0 bytes to client, try again later");

                Ok(())
            }
            Ok(n) => {
                debug!("wrote {} of {} bytes to client", n, self.write_buf().len());

                self.try_transition_to_reading(n);

                Ok(())
            }
            Err(err) => {
                warn!("write failed, {}", err);

                bail!(err)
            }
        }
    }

    fn try_transition_to_writing(&mut self, n: usize) -> Result<()> {
        let mut buf = mem::replace(self, State::Closed).unwrap_read_buf();

        unsafe {
            buf.advance_mut(n);
        }

        match read_packet(buf.bytes()) {
            Ok((remaining, packet)) => {
                debug!("decoded request packet {:?}", packet);

                let mut data = Vec::with_capacity(1024);

                data.write_packet(&packet)?;

                debug!("encoded response packet {:?} in {} bytes",
                       packet,
                       data.len());

                *self = State::Sending(SliceBuf::new(data), ByteBuf::from_slice(remaining));

                Ok(())
            }
            Err(IError::Incomplete(_)) => {
                debug!("packet incomplete, read again");

                Ok(())
            }
            Err(err) => {
                warn!("fail to parse packet, {:?}", err);

                bail!(ErrorKind::InvalidPacket)
            }
        }
    }

    fn try_transition_to_reading(&mut self, n: usize) {
        let (mut buf, remaining) = mem::replace(self, State::Closed).unwrap_write_buf();

        if buf.remaining() > n {
            buf.advance(n);

            *self = State::Sending(buf, remaining);
        } else {
            let mut buf = ByteBuf::with_capacity(8 * 1024);

            buf.copy_from_slice(remaining.bytes());

            *self = State::Receiving(buf);
        }
    }
}
