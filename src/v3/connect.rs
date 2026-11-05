use std::{fmt, num::NonZeroU32, rc::Rc};

use ntex_io::IoBoxed;
use ntex_util::time::Seconds;

use super::{Session, codec as mqtt, shared::MqttShared, sink::MqttSink};

/// Idle timeout used when the client disables keep-alive or the handshake is rejected.
const DEFAULT_KEEPALIVE: Seconds = Seconds(30);

/// Keep-alive timeout for a keep-alive interval, 1.5 times of it rounded up
///
/// Rounding down would close the connection before one and a half times
/// the keep-alive interval for odd values (MQTT 3.1.1, 3.1.2.10).
fn keep_alive_timeout(keep_alive: u16) -> Seconds {
    Seconds(keep_alive.saturating_add(keep_alive.div_ceil(2)))
}

/// Incoming CONNECT handshake owning the transport and pre-handshake state.
pub struct Connect<St = ()> {
    /// Connection transport, transferred to the handshake acknowledgment.
    io: IoBoxed,
    /// Pre-handshake state supplied by the enclosing service.
    st: St,
    /// Owned client CONNECT packet used for authentication and timeout defaults.
    pkt: Box<mqtt::Connect>,
    /// CONNECT remaining length in bytes, excluding its fixed header.
    pkt_size: u32,
    /// Connection state shared with sink handles created during the handshake.
    shared: Rc<MqttShared>,
}

impl<St> Connect<St> {
    /// Wrap a decoded CONNECT packet and take ownership of its transport and initial state.
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
    /// Returns reference to the CONNECT packet
    pub fn packet(&self) -> &mqtt::Connect {
        &self.pkt
    }

    #[inline]
    /// Returns mutable reference to the CONNECT packet
    pub fn packet_mut(&mut self) -> &mut mqtt::Connect {
        &mut self.pkt
    }

    #[inline]
    /// Return the CONNECT remaining length in bytes, excluding the fixed header.
    pub fn packet_size(&self) -> u32 {
        self.pkt_size
    }

    #[inline]
    /// Return the transport owned by this handshake.
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    #[inline]
    /// Returns reference to the connection state
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Create a sink handle sharing this connection’s transport and acknowledgment state.
    ///
    /// Outgoing acknowledgment capacity is configured when the handshake is accepted.
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    /// Accept the handshake and create a session containing `st`.
    ///
    /// `session_present` reports whether an existing session is being resumed; the
    /// application is responsible for maintaining that session state. The server sends
    /// CONNACK after the handshake service returns the acknowledgment.
    pub fn ack<AppSt>(self, st: AppSt, session_present: bool) -> ConnectAck<AppSt> {
        self.ack_and_session(st, session_present).0
    }

    /// Accept the handshake and return both its acknowledgment and a handle to the new session.
    ///
    /// The returned session shares `st` and the connection sink with the acknowledgment.
    /// As with `ack`, the application supplies the session-resumption flag; this does
    /// not restore subscriptions or stored messages automatically.
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
            keep_alive_timeout(pkt.keep_alive)
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
    /// Reject the handshake with the supplied CONNACK return code.
    ///
    /// No session is created; the server sends the return code and closes the transport.
    /// Use a refusal code rather than `ConnectAckReason::ConnectionAccepted`.
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

/// Handshake result carrying the transport, optional session, and per-connection limits.
pub struct ConnectAck<St> {
    /// Transport returned by the handshake service.
    pub(crate) io: IoBoxed,
    /// Accepted session; `None` causes the server to send a refusal and close.
    pub(crate) session: Option<Session<St>>,
    /// Whether the accepted connection resumes an existing session.
    pub(crate) session_present: bool,
    /// CONNACK refusal code used when `session` is absent.
    pub(crate) return_code: mqtt::ConnectAckReason,
    /// Shared codec and sink state retained across the handshake.
    pub(crate) shared: Rc<MqttShared>,
    /// Idle timeout in seconds, passed to the connection dispatcher.
    pub(crate) keepalive: Seconds,
    /// Optional per-connection limit for outgoing messages awaiting acknowledgment.
    pub(crate) max_send: Option<u16>,
    /// Optional per-connection remaining-length limit in bytes for the codec.
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
    /// By default the timeout is 1.5 times the client's keep-alive, rounded up and
    /// saturated at `u16::MAX` seconds, or 30 seconds if the client's keep-alive is zero.
    ///
    /// See [`MqttServiceConfig`](crate::MqttServiceConfig#read-timeouts) for how
    /// keep-alive interacts with the frame read rate.
    pub fn idle_timeout(mut self, timeout: Seconds) -> Self {
        self.keepalive = timeout;
        self
    }

    #[must_use]
    /// Set the limit on outgoing packets awaiting acknowledgment.
    ///
    /// If value is `None` or `Some(0)`, the `MqttServiceConfig` value is used
    /// (16 messages by default).
    pub fn max_send(mut self, val: Option<u16>) -> Self {
        if val == Some(0) {
            self.max_send = None;
        } else {
            self.max_send = val;
        }
        self
    }

    #[must_use]
    /// Override the codec’s remaining-length limit in bytes after CONNECT is accepted.
    ///
    /// The fixed header is excluded. The same limit also applies to outgoing PUBLISH
    /// packets, but not other outgoing control packets.
    pub fn max_packet_size(mut self, val: NonZeroU32) -> Self {
        self.max_packet_size = Some(val);
        self
    }
}

/// Unit tests for handshake state and keep-alive timeouts.
#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use ntex_io::{Io, IoBoxed, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;
    use crate::v3::shared::MqttShared;

    /// STATEMENT: Keep-alive grace periods round up and saturate at the supported seconds limit.
    ///
    /// HOW:
    /// - Calculate timeouts for odd and even intervals and `u16::MAX`.
    /// - Compare the results with rounded-up 1.5-times intervals and the saturated maximum.
    #[test]
    fn test_keep_alive_timeout() {
        assert_eq!(keep_alive_timeout(1), Seconds(2));
        assert_eq!(keep_alive_timeout(2), Seconds(3));
        assert_eq!(keep_alive_timeout(3), Seconds(5));
        assert_eq!(keep_alive_timeout(10), Seconds(15));
        assert_eq!(keep_alive_timeout(u16::MAX), Seconds(u16::MAX));
    }

    /// STATEMENT: Accepted handshakes derive idle timeouts from client keep-alive or the fallback.
    ///
    /// HOW:
    /// - Create connections advertising zero and three seconds of keep-alive and acknowledge them.
    /// - Assert the acknowledgments contain the default timeout and five seconds, respectively.
    #[ntex::test]
    async fn test_ack_keep_alive() {
        for (keep_alive, timeout) in [(0, DEFAULT_KEEPALIVE), (3, Seconds(5))] {
            let io = Io::new(IoTest::create().0, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                mqtt::Codec::default(),
                false,
                Rc::default(),
            ));
            let connect = Box::new(mqtt::Connect {
                keep_alive,
                ..Default::default()
            });
            let ack = Connect::new(connect, 0, IoBoxed::from(io), (), shared).ack((), false);
            assert_eq!(ack.keepalive, timeout);
        }
    }

    /// STATEMENT: Handshake debug formatting exposes packet and acknowledgment metadata.
    ///
    /// HOW:
    /// - Format a newly created `Connect` and verify the output is nonempty.
    /// - Acknowledge it with application state and verify its debug output names `ConnectAck` and `session_present`.
    #[ntex::test]
    async fn test_debug() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("test"));
        let codec = mqtt::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));
        let connect = Box::new(mqtt::Connect::default());
        let h = Connect::new(connect, 0, IoBoxed::from(io), (), shared);

        // Handshake delegates to the Connect packet
        let dbg = format!("{h:?}");
        assert_ne!(dbg, "");

        // HandshakeAck
        let ack = h.ack(42u32, false);
        let dbg = format!("{ack:?}");
        assert!(dbg.contains("ConnectAck"));
        assert!(dbg.contains("session_present"));
    }
}
