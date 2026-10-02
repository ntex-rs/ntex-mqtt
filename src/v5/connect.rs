use ntex_io::IoBoxed;
use std::{fmt, num::NonZeroU16, rc::Rc};

use super::{Session, codec, shared::MqttShared, sink::MqttSink};

/// Connect message
pub struct Connect<St = ()> {
    io: IoBoxed,
    st: St,
    pkt: Box<codec::Connect>,
    size: u32,
    pub(super) shared: Rc<MqttShared>,
}

impl<St> Connect<St> {
    pub(crate) fn new(
        pkt: Box<codec::Connect>,
        size: u32,
        io: IoBoxed,
        st: St,
        shared: Rc<MqttShared>,
    ) -> Self {
        Self {
            io,
            st,
            pkt,
            size,
            shared,
        }
    }

    #[inline]
    /// Returns reference to connect packet
    pub fn packet(&self) -> &codec::Connect {
        &self.pkt
    }

    #[inline]
    /// Returns mutable reference to connect packet
    pub fn packet_mut(&mut self) -> &mut codec::Connect {
        &mut self.pkt
    }

    #[inline]
    /// Returns size of the packet
    pub fn packet_size(&self) -> u32 {
        self.size
    }

    #[inline]
    /// Returns reference to the io object of the connection
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    #[inline]
    /// Returns reference to the handshake state
    pub fn st(&self) -> &St {
        &self.st
    }

    #[inline]
    /// Returns mqtt server sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Ack Connect message and set state
    pub fn ack<AppSt>(self, st: AppSt) -> ConnectAck<AppSt> {
        self.ack_and_session(st).0
    }

    #[inline]
    /// Ack Connect message and set state
    pub fn ack_and_session<AppSt>(self, st: AppSt) -> (ConnectAck<AppSt>, Session<AppSt>) {
        let max_pkt_size = self.shared.codec.max_inbound_size();
        let receive_max = self.shared.receive_max();
        let packet = codec::ConnectAck {
            reason_code: codec::ConnectAckReason::Success,
            max_qos: self.shared.max_qos(),
            topic_alias_max: self.shared.topic_alias_max(),
            receive_max: NonZeroU16::new(receive_max).unwrap_or(crate::v5::RECEIVE_MAX_DEFAULT),
            max_packet_size: if max_pkt_size == 0 {
                None
            } else {
                Some(max_pkt_size)
            },
            ..codec::ConnectAck::default()
        };

        let io = self.io;
        let shared = self.shared;
        let session = Session::new(st, MqttSink::new(shared.clone()), io.shared());

        (
            ConnectAck {
                io,
                shared,
                keepalive: None,
                packet,
                session: Some(session.clone()),
                max_send: None,
            },
            session,
        )
    }

    #[inline]
    /// Create Connect ack object with error
    pub fn failed<AppSt>(self, reason_code: codec::ConnectAckReason) -> ConnectAck<AppSt> {
        ConnectAck {
            io: self.io,
            shared: self.shared,
            session: None,
            keepalive: None,
            max_send: None,
            packet: codec::ConnectAck {
                reason_code,
                ..codec::ConnectAck::default()
            },
        }
    }

    #[inline]
    /// Create Connect ack object with provided `ConnectAck` packet
    pub fn fail_with<AppSt>(self, ack: codec::ConnectAck) -> ConnectAck<AppSt> {
        ConnectAck {
            io: self.io,
            shared: self.shared,
            session: None,
            packet: ack,
            max_send: None,
            keepalive: None,
        }
    }
}

/// Server keep-alive if the client's keep-alive is 0
const DEFAULT_KEEPALIVE: u16 = 30;

/// Keep-alive timeout for a keep-alive interval, 1.2 times of it rounded up
///
/// It is below the 1.5 times limit of [MQTT-3.1.2-22] (MQTT 5.0, 3.1.2.10).
fn keep_alive_timeout(keep_alive: u16) -> u16 {
    keep_alive.saturating_add(keep_alive.div_ceil(5))
}

/// Set `server_keepalive_sec` and return the keep-alive timeout the server enforces
///
/// The server must use the client's keep-alive unless it sends Server Keep Alive
/// [MQTT-3.2.2-22], so the server keep-alive is advertised if it is lower than
/// the client's keep-alive or the client's keep-alive is 0.
///
/// The server enforces 1.2 times of the advertised keep-alive [MQTT-3.1.2-22],
/// otherwise the timeout set by the application as is, or 1.2 times of
/// the client's keep-alive.
pub(crate) fn server_keep_alive(
    client_keep_alive: u16,
    timeout: Option<u16>,
    server_keepalive_sec: &mut Option<u16>,
) -> u16 {
    if server_keepalive_sec.is_none() {
        match timeout {
            Some(t) if client_keep_alive == 0 || client_keep_alive > t => {
                *server_keepalive_sec = Some(t);
            }
            None if client_keep_alive == 0 => *server_keepalive_sec = Some(DEFAULT_KEEPALIVE),
            _ => (),
        }
    }
    match (*server_keepalive_sec, timeout) {
        (Some(keep_alive), _) => keep_alive_timeout(keep_alive),
        (None, Some(timeout)) => timeout,
        (None, None) => keep_alive_timeout(client_keep_alive),
    }
}

impl fmt::Debug for Connect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pkt.fmt(f)
    }
}

/// Connect ack message
pub struct ConnectAck<St> {
    pub(crate) io: IoBoxed,
    pub(crate) session: Option<Session<St>>,
    pub(crate) shared: Rc<MqttShared>,
    pub(crate) packet: codec::ConnectAck,
    pub(crate) keepalive: Option<u16>,
    pub(crate) max_send: Option<u16>,
}

impl<St> fmt::Debug for ConnectAck<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectAck")
            .field("packet", &self.packet)
            .field("keepalive", &self.keepalive)
            .field("max_send", &self.max_send)
            .finish()
    }
}

impl<St> ConnectAck<St> {
    #[inline]
    #[must_use]
    /// Set idle keep-alive for the connection in seconds.
    /// This method sets `server_keepalive_sec` property for `ConnectAck`
    /// response packet.
    ///
    /// `server_keepalive_sec` is set to the timeout only if it is not set explicitly
    /// and the timeout is lower than the client's keep-alive, or the client's
    /// keep-alive is 0. If `server_keepalive_sec` is set, the server closes
    /// the connection after 1.2 times of it, otherwise after exactly this timeout.
    ///
    /// By default the server sets `server_keepalive_sec` to 30 seconds if the client's
    /// keep-alive is 0, and closes the connection after 1.2 times of
    /// `server_keepalive_sec` if it is set, or of the client's keep-alive.
    ///
    /// See [`MqttServiceConfig`](crate::MqttServiceConfig#read-timeouts) for how
    /// keep-alive interacts with the frame read rate.
    ///
    /// # Panics
    ///
    /// Panics if timeout is `0`.
    pub fn keep_alive(mut self, timeout: u16) -> Self {
        assert!(timeout != 0, "Timeout must be greater than 0");
        self.keepalive = Some(timeout);
        self
    }

    #[must_use]
    /// Number of outgoing concurrent messages.
    ///
    /// If value is `None` or `Some(0)`, the `MqttServiceConfig` value is used
    /// (16 messages by default). The value is also capped by the client's
    /// receive maximum.
    pub fn max_send(mut self, val: Option<u16>) -> Self {
        if val == Some(0) {
            self.max_send = None;
        } else {
            self.max_send = val;
        }
        self
    }

    #[inline]
    #[must_use]
    /// Access to `ConnectAck` packet
    pub fn with(mut self, f: impl FnOnce(&mut codec::ConnectAck)) -> Self {
        f(&mut self.packet);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use ntex_io::{Io, IoBoxed, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;
    use crate::v5::shared::MqttShared;

    #[ntex::test]
    async fn test_debug() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("test"));
        let codec_v5 = codec::Codec::new();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec_v5, Rc::default()));
        let connect = Box::new(codec::Connect::default());
        let h = Connect::new(connect, 0, IoBoxed::from(io), (), shared);

        // Connect delegates to the Connect packet
        let dbg = format!("{h:?}");
        assert_ne!(dbg, "");

        // ConnectAck
        let ack = h.ack(42u32);
        let dbg = format!("{ack:?}");
        assert!(dbg.contains("ConnectAck"));
        assert!(dbg.contains("keepalive"));
    }

    #[test]
    fn test_keep_alive_timeout() {
        assert_eq!(keep_alive_timeout(0), 0);
        assert_eq!(keep_alive_timeout(1), 2);
        assert_eq!(keep_alive_timeout(2), 3);
        assert_eq!(keep_alive_timeout(5), 6);
        assert_eq!(keep_alive_timeout(6), 8);
        assert_eq!(keep_alive_timeout(10), 12);
        assert_eq!(keep_alive_timeout(60), 72);
        assert_eq!(keep_alive_timeout(u16::MAX), u16::MAX);
    }

    #[test]
    fn test_server_keep_alive() {
        // default, client keep-alive 0, the server keep-alive is advertised
        let mut ka = None;
        assert_eq!(server_keep_alive(0, None, &mut ka), 36);
        assert_eq!(ka, Some(30));

        // default, 1.2 times of the client's keep-alive
        let mut ka = None;
        assert_eq!(server_keep_alive(10, None, &mut ka), 12);
        assert_eq!(ka, None);
        let mut ka = None;
        assert_eq!(server_keep_alive(3, None, &mut ka), 4);
        assert_eq!(ka, None);

        // default, explicit server keep-alive
        let mut ka = Some(60);
        assert_eq!(server_keep_alive(10, None, &mut ka), 72);
        assert_eq!(ka, Some(60));
        let mut ka = Some(4);
        assert_eq!(server_keep_alive(0, None, &mut ka), 5);
        assert_eq!(ka, Some(4));
        let mut ka = Some(0);
        assert_eq!(server_keep_alive(10, None, &mut ka), 0);
        assert_eq!(ka, Some(0));

        // advertised application timeout, 1.2 times of it
        let mut ka = None;
        assert_eq!(server_keep_alive(0, Some(5), &mut ka), 6);
        assert_eq!(ka, Some(5));
        let mut ka = None;
        assert_eq!(server_keep_alive(10, Some(4), &mut ka), 5);
        assert_eq!(ka, Some(4));

        // application timeout is not advertised, it is enforced as is
        let mut ka = None;
        assert_eq!(server_keep_alive(10, Some(10), &mut ka), 10);
        assert_eq!(ka, None);
        let mut ka = None;
        assert_eq!(server_keep_alive(10, Some(20), &mut ka), 20);
        assert_eq!(ka, None);

        // explicit server keep-alive overrides application timeout
        let mut ka = Some(60);
        assert_eq!(server_keep_alive(10, Some(20), &mut ka), 72);
        assert_eq!(ka, Some(60));
    }
}
