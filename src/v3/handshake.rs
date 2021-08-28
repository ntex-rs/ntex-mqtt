use std::{fmt, rc::Rc};

use ntex::time::Seconds;

use super::codec as mqtt;
use super::shared::MqttShared;
use super::sink::MqttSink;

/// Connect message
pub struct Handshake<Io> {
    io: Io,
    pkt: Box<mqtt::Connect>,
    shared: Rc<MqttShared>,
}

impl<Io> Handshake<Io> {
    pub(crate) fn new(pkt: Box<mqtt::Connect>, io: Io, shared: Rc<MqttShared>) -> Self {
        Self { io, pkt, shared }
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
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    /// Ack handshake message and set state
    pub fn ack<St>(self, st: St, session_present: bool) -> HandshakeAck<Io, St> {
        HandshakeAck {
            session_present,
            io: self.io,
            shared: self.shared,
            session: Some(st),
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            keepalive: Seconds(30),
            return_code: mqtt::ConnectAckReason::ConnectionAccepted,
        }
    }

    /// Create connect ack object with `identifier rejected` return code
    pub fn identifier_rejected<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            keepalive: Seconds(30),
            return_code: mqtt::ConnectAckReason::IdentifierRejected,
        }
    }

    /// Create connect ack object with `bad user name or password` return code
    pub fn bad_username_or_pwd<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            keepalive: Seconds(30),
            return_code: mqtt::ConnectAckReason::BadUserNameOrPassword,
        }
    }

    /// Create connect ack object with `not authorized` return code
    pub fn not_authorized<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            keepalive: Seconds(30),
            return_code: mqtt::ConnectAckReason::NotAuthorized,
        }
    }

    /// Create connect ack object with `service unavailable` return code
    pub fn service_unavailable<St>(self) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            keepalive: Seconds(30),
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
    pub(crate) shared: Rc<MqttShared>,
    pub(crate) keepalive: Seconds,
    pub(crate) lw: u16,
    pub(crate) read_hw: u16,
    pub(crate) write_hw: u16,
}

impl<Io, St> HandshakeAck<Io, St> {
    /// Set idle time-out for the connection in seconds
    ///
    /// By default idle time-out is set to 30 seconds.
    pub fn idle_timeout(mut self, timeout: Seconds) -> Self {
        self.keepalive = timeout;
        self
    }

    #[inline]
    /// Set read/write buffer sizes
    ///
    /// By default max buffer size is 4kb for both read and write buffer,
    /// Min size is 256 bytes.
    pub fn buffer_params(
        mut self,
        max_read_buf: u16,
        max_write_buf: u16,
        min_buf_size: u16,
    ) -> Self {
        self.read_hw = max_read_buf;
        self.write_hw = max_write_buf;
        self.lw = min_buf_size;
        self
    }

    #[doc(hidden)]
    #[deprecated(since = "0.6.3")]
    /// Set buffer low watermark size
    ///
    /// Low watermark is the same for read and write buffers.
    /// By default lw value is 256 bytes.
    pub fn low_watermark(mut self, lw: u16) -> Self {
        self.lw = lw;
        self
    }

    #[doc(hidden)]
    #[deprecated(since = "0.6.3")]
    /// Set read buffer high water mark size
    ///
    /// By default read hw is 4kb
    pub fn read_high_watermark(mut self, hw: u16) -> Self {
        self.read_hw = hw;
        self
    }

    #[doc(hidden)]
    #[deprecated(since = "0.6.3")]
    /// Set write buffer high watermark size
    ///
    /// By default write hw is 4kb
    pub fn write_high_watermark(mut self, hw: u16) -> Self {
        self.write_hw = hw;
        self
    }
}
