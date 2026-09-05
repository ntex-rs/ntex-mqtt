use std::{fmt, num::NonZeroU32, rc::Rc};

use ntex_io::IoBoxed;
use ntex_util::time::Seconds;

use super::{Session, codec as mqtt, shared::MqttShared, sink::MqttSink};

const DEFAULT_KEEPALIVE: Seconds = Seconds(30);

/// Connect message
pub struct Connect<St = ()> {
    io: IoBoxed,
    st: St,
    pkt: Box<mqtt::Connect>,
    pkt_size: u32,
    shared: Rc<MqttShared>,
}

impl<St> Connect<St> {
    pub(crate) fn new(
        pkt: Box<mqtt::Connect>,
        pkt_size: u32,
        io: IoBoxed,
        st: St,
        shared: Rc<MqttShared>,
    ) -> Self {
        Self {
            io,
            st,
            pkt,
            pkt_size,
            shared,
        }
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

    #[inline]
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Returns mqtt server sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    /// Ack handshake message and set state
    pub fn ack<AppSt>(self, st: AppSt, session_present: bool) -> ConnectAck<AppSt> {
        self.ack_and_session(st, session_present).0
    }

    /// Ack handshake message and set state
    pub fn ack_and_session<AppSt>(
        self,
        st: AppSt,
        session_present: bool,
    ) -> (ConnectAck<AppSt>, Session<AppSt>) {
        let Connect {
            io, shared, pkt, ..
        } = self;
        // [MQTT-3.1.2-24].
        let keepalive = if pkt.keep_alive != 0 {
            Seconds((pkt.keep_alive >> 1).saturating_add(pkt.keep_alive))
        } else {
            DEFAULT_KEEPALIVE
        };
        let session = Session::new(st, MqttSink::new(shared.clone()), io.shared());

        (
            ConnectAck {
                io,
                shared,
                keepalive,
                session_present,
                session: Some(session.clone()),
                max_send: None,
                max_packet_size: None,
                return_code: mqtt::ConnectAckReason::ConnectionAccepted,
            },
            session,
        )
    }

    /// Create connect ack object with `identifier rejected` return code
    pub fn identifier_rejected<AppSt>(self) -> ConnectAck<AppSt> {
        self.failed(mqtt::ConnectAckReason::IdentifierRejected)
    }

    /// Create connect ack object with `bad user name or password` return code
    pub fn bad_username_or_pwd<AppSt>(self) -> ConnectAck<AppSt> {
        self.failed(mqtt::ConnectAckReason::BadUserNameOrPassword)
    }

    /// Create connect ack object with `not authorized` return code
    pub fn not_authorized<AppSt>(self) -> ConnectAck<AppSt> {
        self.failed(mqtt::ConnectAckReason::NotAuthorized)
    }

    /// Create connect ack object with `service unavailable` return code
    pub fn service_unavailable<AppSt>(self) -> ConnectAck<AppSt> {
        self.failed(mqtt::ConnectAckReason::ServiceUnavailable)
    }

    #[inline]
    /// Create handshake ack object with error
    pub fn failed<AppSt>(self, return_code: mqtt::ConnectAckReason) -> ConnectAck<AppSt> {
        ConnectAck {
            return_code,
            io: self.io,
            shared: self.shared,
            session: None,
            session_present: false,
            max_send: None,
            max_packet_size: None,
            keepalive: DEFAULT_KEEPALIVE,
        }
    }
}

impl<St> fmt::Debug for Connect<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pkt.fmt(f)
    }
}

/// Ack connect message
pub struct ConnectAck<St> {
    pub(crate) io: IoBoxed,
    pub(crate) session: Option<Session<St>>,
    pub(crate) session_present: bool,
    pub(crate) return_code: mqtt::ConnectAckReason,
    pub(crate) shared: Rc<MqttShared>,
    pub(crate) keepalive: Seconds,
    pub(crate) max_send: Option<u16>,
    pub(crate) max_packet_size: Option<NonZeroU32>,
}

impl<St> fmt::Debug for ConnectAck<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectAck")
            .field("session_present", &self.session_present)
            .field("return_code", &self.return_code)
            .field("keepalive", &self.keepalive)
            .field("max_send", &self.max_send)
            .field("max_packet_size", &self.max_packet_size)
            .finish()
    }
}

impl<St> ConnectAck<St> {
    #[must_use]
    /// Set idle time-out for the connection in seconds.
    ///
    /// By default idle time-out is set to 30 seconds.
    pub fn idle_timeout(mut self, timeout: Seconds) -> Self {
        self.keepalive = timeout;
        self
    }

    #[must_use]
    /// Number of outgoing concurrent messages.
    ///
    /// By default outgoing is set to 16 messages
    pub fn max_send(mut self, val: Option<u16>) -> Self {
        if val == Some(0) {
            self.max_send = None;
        } else {
            self.max_send = val;
        }
        self
    }

    #[must_use]
    /// Maximum supported size for incoming packets.
    pub fn max_packet_size(mut self, val: NonZeroU32) -> Self {
        self.max_packet_size = Some(val);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use ntex_io::{Io, IoBoxed, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;
    use crate::v3::shared::MqttShared;

    #[ntex::test]
    async fn test_debug() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("test"));
        let codec = mqtt::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));
        let connect = Box::new(mqtt::Connect::default());
        let h = Connect::new(connect, 0, IoBoxed::from(io), (), shared);

        // Handshake delegates to the Connect packet
        let dbg = format!("{h:?}");
        assert!(!dbg.is_empty());

        // HandshakeAck
        let ack = h.ack(42u32, false);
        let dbg = format!("{ack:?}");
        assert!(dbg.contains("ConnectAck"));
        assert!(dbg.contains("session_present"));
    }
}
