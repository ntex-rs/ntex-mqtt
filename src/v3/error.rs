use derive_more::{Display, From};
use either::Either;
use std::io;

pub use crate::{error::*, v3::codec};

/// Errors which can occur when attempting to handle mqtt client connection.
#[derive(Debug, Display, From)]
pub enum ClientError {
    /// Connect negotiation failed
    #[display(fmt = "Connect ack failed")]
    Ack { session_present: bool, return_code: codec::ConnectAckReason },
    /// Protocol error
    #[display(fmt = "Protocol error: {:?}", _0)]
    Protocol(ProtocolError),
    /// Handshake timeout
    #[display(fmt = "Handshake timeout")]
    HandshakeTimeout,
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
    /// Connect error
    #[display(fmt = "Connect error: {}", _0)]
    Connect(ntex::connect::ConnectError),
}

impl std::error::Error for ClientError {}

impl From<Either<EncodeError, io::Error>> for ClientError {
    fn from(err: Either<EncodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(ProtocolError::Encode(err)),
            Either::Right(err) => ClientError::Protocol(ProtocolError::Io(err)),
        }
    }
}
