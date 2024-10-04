use std::{fmt, rc::Rc};

use ntex_io::IoBoxed;
use ntex_util::time::Seconds;

use super::{codec as mqtt, shared::MqttShared, sink::MqttSink};

const DEFAULT_KEEPALIVE: Seconds = Seconds(30);

/// Connect message
pub struct Handshake {
    io: IoBoxed,
    pkt: Box<mqtt::Connect>,
    pkt_size: u32,
    shared: Rc<MqttShared>,
}

impl Handshake {
    pub(crate) fn new(
        pkt: Box<mqtt::Connect>,
        pkt_size: u32,
        io: IoBoxed,
        shared: Rc<MqttShared>,
    ) -> Self {
        Self { io, pkt, pkt_size, shared }
    }

    #[inline]
    pub fn packet(&self) -> &mqtt::Connect {
        &self.pkt
    }

    #[inline]
    pub fn packet_mut(&mut self) -> &mut mqtt::Connect {
        &mut self.pkt
    }

    #[inline]
    pub fn packet_size(&self) -> u32 {
        self.pkt_size
    }

    #[inline]
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    /// Returns mqtt server sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    /// Ack handshake message and set state
    pub fn ack<St>(self, st: St, session_present: bool) -> HandshakeAck<St> {
        let Handshake { io, shared, pkt, .. } = self;
        // [MQTT-3.1.2-24].
        let keepalive = if pkt.keep_alive != 0 {
            Seconds((pkt.keep_alive >> 1).checked_add(pkt.keep_alive).unwrap_or(u16::MAX))
        } else {
            DEFAULT_KEEPALIVE
        };
        HandshakeAck {
            io,
            shared,
            keepalive,
            session_present,
            session: Some(st),
            max_send: None,
            return_code: mqtt::ConnectAckReason::ConnectionAccepted,
        }
    }

    /// Create connect ack object with `identifier rejected` return code
    pub fn identifier_rejected<St>(self) -> HandshakeAck<St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            keepalive: DEFAULT_KEEPALIVE,
            max_send: None,
            return_code: mqtt::ConnectAckReason::IdentifierRejected,
        }
    }

    /// Create connect ack object with `bad user name or password` return code
    pub fn bad_username_or_pwd<St>(self) -> HandshakeAck<St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            max_send: None,
            keepalive: DEFAULT_KEEPALIVE,
            return_code: mqtt::ConnectAckReason::BadUserNameOrPassword,
        }
    }

    /// Create connect ack object with `not authorized` return code
    pub fn not_authorized<St>(self) -> HandshakeAck<St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            max_send: None,
            keepalive: DEFAULT_KEEPALIVE,
            return_code: mqtt::ConnectAckReason::NotAuthorized,
        }
    }

    /// Create connect ack object with `service unavailable` return code
    pub fn service_unavailable<St>(self) -> HandshakeAck<St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            max_send: None,
            keepalive: DEFAULT_KEEPALIVE,
            return_code: mqtt::ConnectAckReason::ServiceUnavailable,
        }
    }
}

impl fmt::Debug for Handshake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pkt.fmt(f)
    }
}

/// Ack connect message
pub struct HandshakeAck<St> {
    pub(crate) io: IoBoxed,
    pub(crate) session: Option<St>,
    pub(crate) session_present: bool,
    pub(crate) return_code: mqtt::ConnectAckReason,
    pub(crate) shared: Rc<MqttShared>,
    pub(crate) keepalive: Seconds,
    pub(crate) max_send: Option<u16>,
}

impl<St> HandshakeAck<St> {
    /// Set idle time-out for the connection in seconds
    ///
    /// By default idle time-out is set to 30 seconds.
    pub fn idle_timeout(mut self, timeout: Seconds) -> Self {
        self.keepalive = timeout;
        self
    }

    /// Number of outgoing concurrent messages.
    ///
    /// By default outgoing is set to 16 messages
    pub fn max_send(mut self, val: u16) -> Self {
        self.max_send = Some(val);
        self
    }
}
