use std::{fmt, num::NonZeroU16, rc::Rc};

use super::{codec, shared::MqttShared, sink::MqttSink};

/// Handshake message
pub struct Handshake<Io> {
    io: Io,
    pkt: codec::Connect,
    shared: Rc<MqttShared>,
    max_size: u32,
    max_receive: u16,
    max_topic_alias: u16,
}

impl<Io> Handshake<Io> {
    pub(crate) fn new(
        pkt: codec::Connect,
        io: Io,
        shared: Rc<MqttShared>,
        max_size: u32,
        max_receive: u16,
        max_topic_alias: u16,
    ) -> Self {
        Self { pkt, io, shared, max_size, max_receive, max_topic_alias }
    }

    pub fn packet(&self) -> &codec::Connect {
        &self.pkt
    }

    pub fn packet_mut(&mut self) -> &mut codec::Connect {
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
    pub fn ack<St>(self, st: St) -> HandshakeAck<Io, St> {
        let mut packet = codec::ConnectAck {
            reason_code: codec::ConnectAckReason::Success,
            topic_alias_max: self.max_topic_alias,
            ..codec::ConnectAck::default()
        };
        if self.max_size != 0 {
            packet.max_packet_size = Some(self.max_size);
        }
        if self.max_receive != 0 {
            packet.receive_max = Some(NonZeroU16::new(self.max_receive).unwrap());
        }

        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: Some(st),
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            keepalive: 30,
            packet,
        }
    }

    /// Create handshake ack object with error
    pub fn failed<St>(self, reason_code: codec::ConnectAckReason) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            keepalive: 30,
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            packet: codec::ConnectAck { reason_code, ..codec::ConnectAck::default() },
        }
    }

    /// Create handshake ack object with provided ConnectAck packet
    pub fn fail_with<St>(self, ack: codec::ConnectAck) -> HandshakeAck<Io, St> {
        HandshakeAck {
            io: self.io,
            shared: self.shared,
            session: None,
            packet: ack,
            lw: 256,
            read_hw: 4 * 1024,
            write_hw: 4 * 1024,
            keepalive: 30,
        }
    }
}

impl<T> fmt::Debug for Handshake<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pkt.fmt(f)
    }
}

/// Handshake ack message
pub struct HandshakeAck<Io, St> {
    pub(crate) io: Io,
    pub(crate) session: Option<St>,
    pub(crate) shared: Rc<MqttShared>,
    pub(crate) packet: codec::ConnectAck,
    pub(crate) keepalive: u16,
    pub(crate) lw: u16,
    pub(crate) read_hw: u16,
    pub(crate) write_hw: u16,
}

impl<Io, St> HandshakeAck<Io, St> {
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

    #[inline]
    /// Set buffer low watermark size
    ///
    /// Low watermark is the same for read and write buffers.
    /// By default lw value is 256 bytes.
    pub fn low_watermark(mut self, lw: u16) -> Self {
        self.lw = lw;
        self
    }

    #[inline]
    /// Set read buffer high water mark size
    ///
    /// By default read hw is 4kb
    pub fn read_high_watermark(mut self, hw: u16) -> Self {
        self.read_hw = hw;
        self
    }

    #[inline]
    /// Set write buffer high watermark size
    ///
    /// By default write hw is 4kb
    pub fn write_high_watermark(mut self, hw: u16) -> Self {
        self.write_hw = hw;
        self
    }

    /// Access to ConnectAck packet
    #[inline]
    pub fn with(mut self, f: impl FnOnce(&mut codec::ConnectAck)) -> Self {
        f(&mut self.packet);
        self
    }
}
