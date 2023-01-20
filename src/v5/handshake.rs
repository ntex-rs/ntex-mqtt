use ntex::io::IoBoxed;
use std::{fmt, num::NonZeroU16, rc::Rc};

use super::{codec, shared::MqttShared, sink::MqttSink};

/// Handshake message
pub struct Handshake {
    io: IoBoxed,
    pkt: Box<codec::Connect>,
    pub(super) shared: Rc<MqttShared>,
}

impl Handshake {
    pub(crate) fn new(pkt: Box<codec::Connect>, io: IoBoxed, shared: Rc<MqttShared>) -> Self {
        Self { io, pkt, shared }
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
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    #[inline]
    /// Returns mqtt server sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Ack handshake message and set state
    pub fn ack<St>(self, st: St) -> HandshakeAck<St> {
        let max_pkt_size = self.shared.codec.max_inbound_size();
        let receive_max = self.shared.receive_max();
        let packet = codec::ConnectAck {
            reason_code: codec::ConnectAckReason::Success,
            max_qos: self.shared.max_qos(),
            topic_alias_max: self.shared.topic_alias_max(),
            receive_max: NonZeroU16::new(receive_max).unwrap_or(crate::v5::RECEIVE_MAX_DEFAULT),
            max_packet_size: if max_pkt_size == 0 { None } else { Some(max_pkt_size) },
            ..codec::ConnectAck::default()
        };

        let Handshake { io, shared, pkt, .. } = self;
        // [MQTT-3.1.2-22]
        let keepalive = if pkt.keep_alive != 0 {
            (pkt.keep_alive >> 1).checked_add(pkt.keep_alive).unwrap_or(u16::MAX)
        } else {
            30
        };
        HandshakeAck { io, shared, keepalive, packet, session: Some(st) }
    }

    #[inline]
    /// Create handshake ack object with error
    pub fn failed<St>(self, reason_code: codec::ConnectAckReason) -> HandshakeAck<St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            keepalive: 30,
            packet: codec::ConnectAck { reason_code, ..codec::ConnectAck::default() },
        }
    }

    #[inline]
    /// Create handshake ack object with provided ConnectAck packet
    pub fn fail_with<St>(self, ack: codec::ConnectAck) -> HandshakeAck<St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            packet: ack,
            keepalive: 30,
        }
    }
}

impl fmt::Debug for Handshake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pkt.fmt(f)
    }
}

/// Handshake ack message
pub struct HandshakeAck<St> {
    pub(crate) io: IoBoxed,
    pub(crate) session: Option<St>,
    pub(crate) shared: Rc<MqttShared>,
    pub(crate) packet: codec::ConnectAck,
    pub(crate) keepalive: u16,
}

impl<St> HandshakeAck<St> {
    #[inline]
    /// Set idle keep-alive for the connection in seconds.
    /// This method sets `server_keepalive_sec` property for `ConnectAck`
    /// response packet.
    ///
    /// By default idle keep-alive is set to 30 seconds. Panics if timeout is `0`.
    pub fn keep_alive(mut self, timeout: u16) -> Self {
        if timeout == 0 {
            panic!("Timeout must be greater than 0")
        }
        self.keepalive = timeout;
        self
    }

    /// Access to ConnectAck packet
    #[inline]
    pub fn with(mut self, f: impl FnOnce(&mut codec::ConnectAck)) -> Self {
        f(&mut self.packet);
        self
    }
}
