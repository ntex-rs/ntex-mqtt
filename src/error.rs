use std::convert::From;

#[derive(Debug)]
pub enum DecodeError {
    InvalidProtocol,
    InvalidLength,
    UnsupportedProtocolLevel,
    ConnectReservedFlagSet,
    ConnAckReservedFlagSet,
    InvalidClientId,
    UnsupportedPacketType
}

error_chain!{
    foreign_links {
        Fmt(::std::fmt::Error);
        Io(::std::io::Error);
        Canceled(::futures::Canceled);
        Utf8(::std::str::Utf8Error);
    }

    errors {
        DecodeError(e: DecodeError) {
            description("error occured while decoding")
            display("error occured while decoding: '{:?}'", e)
        }
        OutOfMemory
        InvalidState
        InvalidPacket
        InvalidTopic
        SpawnError
    }
}

impl From<DecodeError> for Error {
    fn from(v: DecodeError) -> Error {
        ErrorKind::DecodeError(v).into()
    }
}