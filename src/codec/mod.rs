use std::io::Cursor;
use tokio_io::codec::{Decoder, Encoder};
use bytes::{BytesMut, BufMut};

use super::Packet;
use error::*;

mod encode;
mod decode;

use self::decode::*;
use self::encode::*;

bitflags! {
    pub flags ConnectFlags: u8 {
        const USERNAME      = 0b10000000,
        const PASSWORD      = 0b01000000,
        const WILL_RETAIN   = 0b00100000,
        const WILL_QOS      = 0b00011000,
        const WILL          = 0b00000100,
        const CLEAN_SESSION = 0b00000010,
    }
}

pub const WILL_QOS_SHIFT: u8 = 3;

bitflags! {
    pub flags ConnectAckFlags: u8 {
        const SESSION_PRESENT = 0b00000001,
    }
}

pub struct Codec {
    state: DecodeState
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    FrameHeader,
    Frame(FixedHeader),
}

impl Codec {
    pub fn new() -> Codec {
        Codec { state: DecodeState::FrameHeader }
    }
}

impl Decoder for Codec {
    type Item = Packet;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        loop {
            match self.state {
                DecodeState::FrameHeader => {
                    if src.len() < 2 {
                        return Ok(None);
                    }
                    let fixed = src.as_ref()[0];
                    match decode_variable_length(&src.as_ref()[1..])? {
                        Some((remaining_length, consumed)) => {
                            src.split_to(consumed + 1);
                            self.state = DecodeState::Frame(FixedHeader {
                                packet_type: fixed >> 4,
                                packet_flags: fixed & 0xF,
                                remaining_length: remaining_length,
                            });
                            // todo: validate remaining_length against max frame size config
                            if src.len() < remaining_length {
                                // todo: subtract?
                                src.reserve(remaining_length); // extend receiving buffer to fit the whole frame -- todo: too eager?
                                return Ok(None);
                            }
                        },
                        None => {
                            return Ok(None);
                        }
                    }
                },
                DecodeState::Frame(fixed) => {
                    if src.len() < fixed.remaining_length {
                        return Ok(None);
                    }
                    let packet_buf = src.split_to(fixed.remaining_length);
                    let mut packet_cur = Cursor::new(packet_buf.freeze());
                    let packet = read_packet(&mut packet_cur, fixed)?;
                    self.state = DecodeState::FrameHeader;
                    src.reserve(2);
                    return Ok(Some(packet));
                }
            }
        }
    }
}

impl Encoder for Codec {
    type Item = Packet;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        let content_size = get_encoded_size(&item);
        dst.reserve(content_size + 5);
        write_packet(&item, dst);
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) struct FixedHeader {
    /// MQTT Control Packet type
    pub packet_type: u8,
    /// Flags specific to each MQTT Control Packet type
    pub packet_flags: u8,
    /// the number of bytes remaining within the current packet,
    /// including data in the variable header and the payload.
    pub remaining_length: usize,
}
