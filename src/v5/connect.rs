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
    pub fn packet(&self) -> &codec::Connect {
        &self.pkt
    }

    #[inline]
    pub fn packet_mut(&mut self) -> &mut codec::Connect {
        &mut self.pkt
    }

    #[inline]
    pub fn packet_size(&self) -> u32 {
        self.size
    }

    #[inline]
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    #[inline]
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
        let pkt = self.pkt;
        let shared = self.shared;

        // [MQTT-3.1.2-22]
        let keepalive = if pkt.keep_alive != 0 {
            (pkt.keep_alive >> 1).saturating_add(pkt.keep_alive)
        } else {
            30
        };
        let session = Session::new(st, MqttSink::new(shared.clone()), io.shared());

        (
            ConnectAck {
                io,
                shared,
                keepalive,
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
            keepalive: 30,
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
            keepalive: 30,
        }
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
    pub(crate) keepalive: u16,
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
    /// By default idle keep-alive is set to 30 seconds.
    ///
    /// # Panics
    ///
    /// Panics if timeout is `0`.
    pub fn keep_alive(mut self, timeout: u16) -> Self {
        assert!(timeout != 0, "Timeout must be greater than 0");
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
        assert!(!dbg.is_empty());

        // ConnectAck
        let ack = h.ack(42u32);
        let dbg = format!("{ack:?}");
        assert!(dbg.contains("ConnectAck"));
        assert!(dbg.contains("keepalive"));
    }
}
