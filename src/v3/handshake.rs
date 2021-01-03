use std::fmt;

use crate::io::IoState;

use super::codec as mqtt;
use super::sink::MqttSink;

/// Connect message
pub struct Handshake<Io> {
    io: Io,
    pkt: mqtt::Connect,
    sink: MqttSink,
    state: IoState<mqtt::Codec>,
}

impl<Io> Handshake<Io> {
    pub(crate) fn new(
        pkt: mqtt::Connect,
        io: Io,
        sink: MqttSink,
        state: IoState<mqtt::Codec>,
    ) -> Self {
        Self { pkt, io, sink, state }
    }

    pub fn packet(&self) -> &mqtt::Connect {
        &self.pkt
    }

    pub fn packet_mut(&mut self) -> &mut mqtt::Connect {
        &mut self.pkt
    }

    #[inline]
    pub fn io(&mut self) -> &mut Io {
        &mut self.io
    }

    /// Returns mqtt server sink
    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }

    /// Ack handshake message and set state
    pub fn ack<St>(self, st: St, session_present: bool) -> HandshakeAck<Io, St> {
        HandshakeAck {
            session_present,
            io: self.io,
            sink: self.sink,
            state: self.state,
            session: Some(st),
            keepalive: 30,
            return_code: mqtt::ConnectAckReason::ConnectionAccepted,
        }
    }

    /// Create connect ack object with `identifier rejected` return code
    pub fn identifier_rejected<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            sink: self.sink,
            state: self.state,
            session: None,
            session_present: false,
            keepalive: 30,
            return_code: mqtt::ConnectAckReason::IdentifierRejected,
        }
    }

    /// Create connect ack object with `bad user name or password` return code
    pub fn bad_username_or_pwd<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            sink: self.sink,
            state: self.state,
            session: None,
            session_present: false,
            keepalive: 30,
            return_code: mqtt::ConnectAckReason::BadUserNameOrPassword,
        }
    }

    /// Create connect ack object with `not authorized` return code
    pub fn not_authorized<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            sink: self.sink,
            state: self.state,
            session: None,
            session_present: false,
            keepalive: 30,
            return_code: mqtt::ConnectAckReason::NotAuthorized,
        }
    }

    /// Create connect ack object with `service unavailable` return code
    pub fn service_unavailable<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            sink: self.sink,
            state: self.state,
            session: None,
            session_present: false,
            keepalive: 30,
            return_code: mqtt::ConnectAckReason::ServiceUnavailable,
        }
    }
}

impl<T> fmt::Debug for Handshake<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pkt.fmt(f)
    }
}

/// Ack connect message
pub struct HandshakeAck<Io, St> {
    pub(crate) io: Io,
    pub(crate) session: Option<St>,
    pub(crate) session_present: bool,
    pub(crate) return_code: mqtt::ConnectAckReason,
    pub(crate) sink: MqttSink,
    pub(crate) state: IoState<mqtt::Codec>,
    pub(crate) keepalive: u16,
}

impl<Io, St> HandshakeAck<Io, St> {
    /// Set idle time-out for the connection in seconds
    ///
    /// By default idle time-out is set to 30 seconds.
    pub fn idle_timeout(mut self, timeout: u16) -> Self {
        self.keepalive = timeout;
        self
    }
}
