use std::{io, str};

#[derive(Debug)]
pub enum DecodeError {
    InvalidProtocol,
    InvalidLength,
    UnsupportedProtocolLevel,
    ConnectReservedFlagSet,
    ConnAckReservedFlagSet,
    InvalidClientId,
    UnsupportedPacketType,
    IoError(io::Error),
    Utf8Error(str::Utf8Error),
}

impl From<io::Error> for DecodeError {
    fn from(err: io::Error) -> Self {
        DecodeError::IoError(err)
    }
}

impl From<str::Utf8Error> for DecodeError {
    fn from(err: str::Utf8Error) -> Self {
        DecodeError::Utf8Error(err)
    }
}

#[derive(Debug)]
pub enum MqttError {
    OutOfMemory,
    InvalidState,
    InvalidPacket,
    InvalidTopic,
    SpawnError,
    Decode(DecodeError),
}

impl From<DecodeError> for MqttError {
    fn from(err: DecodeError) -> Self {
        MqttError::Decode(err)
    }
}
