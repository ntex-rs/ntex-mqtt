use std::fmt;

use ntex::channel::mpsc;
use ntex_codec::Framed;

use super::{codec, sink::MqttSink};
use crate::handshake::HandshakeResult;

/// Connect message
pub struct Connect<Io> {
    connect: codec::Connect,
    sink: MqttSink,
    io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
}

impl<Io> Connect<Io> {
    pub(crate) fn new(
        connect: codec::Connect,
        io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
        sink: MqttSink,
    ) -> Self {
        Self { connect, io, sink }
    }

    pub fn packet(&self) -> &codec::Connect {
        &self.connect
    }

    pub fn packet_mut(&mut self) -> &mut codec::Connect {
        &mut self.connect
    }

    #[inline]
    pub fn io(&mut self) -> &mut Framed<Io, codec::Codec> {
        self.io.io()
    }

    /// Returns mqtt server sink
    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }

    /// Ack connect message and set state
    pub fn ack<St>(self, st: St) -> ConnectAck<Io, St> {
        let mut packet = codec::ConnectAck::default();
        packet.reason_code = codec::ConnectAckReason::Success;

        ConnectAck { io: self.io, sink: self.sink, session: Some(st), packet }
    }

    /// Create connect ack object with error
    pub fn failed<St>(self, reason: codec::ConnectAckReason) -> ConnectAck<Io, St> {
        let mut packet = codec::ConnectAck::default();
        packet.reason_code = reason;

        ConnectAck { io: self.io, sink: self.sink, session: None, packet }
    }
}

impl<T> fmt::Debug for Connect<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.connect.fmt(f)
    }
}

/// Ack connect message
pub struct ConnectAck<Io, St> {
    pub(crate) io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
    pub(crate) session: Option<St>,
    pub(crate) sink: MqttSink,
    pub(crate) packet: codec::ConnectAck,
}

impl<Io, St> ConnectAck<Io, St> {
    /// Set idle keep-alive for the connection in seconds.
    /// This method sets `server_keepalive_sec` property for `ConnectAck`
    /// response packet.
    ///
    /// By default idle keep-alive is set to 30 seconds. Panics if timeout is `0`.
    pub fn keep_alive(mut self, timeout: u16) -> Self {
        if timeout == 0 {
            panic!("Timeout must be greater than 0")
        }
        self.io.set_keepalive_timeout(timeout as usize);
        self
    }

    /// Allows modifications to conneck acknowledgement packet
    #[inline]
    pub fn with(mut self, f: impl FnOnce(&mut codec::ConnectAck)) -> Self {
        f(&mut self.packet);
        self
    }
}
