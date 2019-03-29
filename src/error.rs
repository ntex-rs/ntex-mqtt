use std::{io, str};

#[derive(Debug)]
pub enum ParseError {
    InvalidProtocol,
    InvalidLength,
    UnsupportedProtocolLevel,
    ConnectReservedFlagSet,
    ConnAckReservedFlagSet,
    InvalidClientId,
    UnsupportedPacketType,
    PacketIdRequired,
    IoError(io::Error),
    Utf8Error(str::Utf8Error),
}

impl From<io::Error> for ParseError {
    fn from(err: io::Error) -> Self {
        ParseError::IoError(err)
    }
}

impl From<str::Utf8Error> for ParseError {
    fn from(err: str::Utf8Error) -> Self {
        ParseError::Utf8Error(err)
    }
}

pub enum MqttTopicError {
    InvalidTopic,
}
