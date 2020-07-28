use std::fmt;
use std::time::Duration;

use ntex::channel::mpsc;
use ntex::codec::Framed;

use super::codec as mqtt;
use super::sink::MqttSink;
use crate::handshake::HandshakeResult;

/// Connect message
pub struct Connect<Io> {
    connect: mqtt::Connect,
    sink: MqttSink,
    keep_alive: Duration,
    inflight: usize,
    io: HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
}

impl<Io> Connect<Io> {
    pub(crate) fn new(
        connect: mqtt::Connect,
        io: HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
        sink: MqttSink,
        inflight: usize,
    ) -> Self {
        Self {
            keep_alive: Duration::from_secs(connect.keep_alive as u64),
            connect,
            io,
            sink,
            inflight,
        }
    }

    pub fn packet(&self) -> &mqtt::Connect {
        &self.connect
    }

    pub fn packet_mut(&mut self) -> &mut mqtt::Connect {
        &mut self.connect
    }

    #[inline]
    pub fn io(&mut self) -> &mut Framed<Io, mqtt::Codec> {
        self.io.io()
    }

    /// Returns mqtt server sink
    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }

    /// Ack connect message and set state
    pub fn ack<St>(self, st: St, session_present: bool) -> ConnectAck<Io, St> {
        ConnectAck::new(
            self.io,
            self.sink,
            st,
            session_present,
            self.keep_alive,
            self.inflight,
        )
    }

    /// Create connect ack object with `identifier rejected` return code
    pub fn identifier_rejected<St>(self) -> ConnectAck<Io, St> {
        ConnectAck {
            io: self.io,
            sink: self.sink,
            session: None,
            session_present: false,
            return_code: mqtt::ConnectAckReason::IdentifierRejected,
            keep_alive: Duration::from_secs(5),
            inflight: 15,
        }
    }

    /// Create connect ack object with `bad user name or password` return code
    pub fn bad_username_or_pwd<St>(self) -> ConnectAck<Io, St> {
        ConnectAck {
            io: self.io,
            sink: self.sink,
            session: None,
            session_present: false,
            return_code: mqtt::ConnectAckReason::BadUserNameOrPassword,
            keep_alive: Duration::from_secs(5),
            inflight: 15,
        }
    }

    /// Create connect ack object with `not authorized` return code
    pub fn not_authorized<St>(self) -> ConnectAck<Io, St> {
        ConnectAck {
            io: self.io,
            sink: self.sink,
            session: None,
            session_present: false,
            return_code: mqtt::ConnectAckReason::NotAuthorized,
            keep_alive: Duration::from_secs(5),
            inflight: 15,
        }
    }
}

impl<T> fmt::Debug for Connect<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.connect.fmt(f)
    }
}

/// Ack connect message
pub struct ConnectAck<Io, St> {
    pub(crate) io: HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
    pub(crate) session: Option<St>,
    pub(crate) session_present: bool,
    pub(crate) return_code: mqtt::ConnectAckReason,
    pub(crate) keep_alive: Duration,
    pub(crate) inflight: usize,
    pub(crate) sink: MqttSink,
}

impl<Io, St> ConnectAck<Io, St> {
    /// Create connect ack, `session_present` indicates that previous session is presents
    pub(crate) fn new(
        io: HandshakeResult<Io, (), mqtt::Codec, mpsc::Receiver<mqtt::Packet>>,
        sink: MqttSink,
        session: St,
        session_present: bool,
        keep_alive: Duration,
        inflight: usize,
    ) -> Self {
        Self {
            io,
            sink,
            session_present,
            keep_alive,
            inflight,
            session: Some(session),
            return_code: mqtt::ConnectAckReason::ConnectionAccepted,
        }
    }

    /// Set idle time-out for the connection in milliseconds
    ///
    /// By default idle time-out is set to 300000 milliseconds
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.keep_alive = timeout;
        self
    }

    /// Set in-flight count. Total number of `in-flight` packets
    ///
    /// By default in-flight count is set to 15
    pub fn in_flight(mut self, in_flight: usize) -> Self {
        self.inflight = in_flight;
        self
    }
}
