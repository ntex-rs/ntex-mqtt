use std::fmt;
use std::ops::Deref;
use std::time::Duration;

use ntex::channel::mpsc;
use ntex::codec::Framed;

use super::{codec, sink::MqttSink};
use crate::handshake::HandshakeResult;

/// Connect message
pub struct Connect<Io> {
    connect: codec::Connect,
    sink: MqttSink,
    keep_alive: Duration,
    inflight: usize,
    max_topic_alias: u16,
    io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
}

impl<Io> Connect<Io> {
    pub(crate) fn new(
        connect: codec::Connect,
        io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
        sink: MqttSink,
        max_topic_alias: u16,
        inflight: usize,
    ) -> Self {
        Self {
            keep_alive: Duration::from_secs(connect.keep_alive as u64),
            connect,
            io,
            sink,
            inflight,
            max_topic_alias,
        }
    }

    pub fn packet(&self) -> &codec::Connect {
        &self.connect
    }

    pub fn packet_mut(&mut self) -> &mut codec::Connect {
        &mut self.connect
    }

    #[inline]
    pub fn io(&mut self) -> &mut Framed<Io, codec::Codec> {
        self.io.io()
    }

    /// Returns mqtt server sink
    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }

    /// Ack connect message and set state
    pub fn ack<St>(self, st: St) -> ConnectAck<Io, St> {
        let mut packet = codec::ConnectAck::default();
        packet.reason_code = codec::ConnectAckReason::Success;
        packet.topic_alias_max = self.max_topic_alias;

        ConnectAck {
            io: self.io,
            sink: self.sink,
            keep_alive: self.keep_alive,
            inflight: self.inflight,
            session: Some(st),
            packet,
        }
    }

    /// Create connect ack object with error
    pub fn failed<St>(self) -> ConnectAck<Io, St> {
        let mut packet = codec::ConnectAck::default();
        packet.reason_code = codec::ConnectAckReason::UnspecifiedError;
        packet.topic_alias_max = self.max_topic_alias;

        ConnectAck {
            io: self.io,
            sink: self.sink,
            session: None,
            keep_alive: self.keep_alive,
            inflight: self.inflight,
            packet,
        }
    }
}

impl<Io> Deref for Connect<Io> {
    type Target = codec::Connect;

    fn deref(&self) -> &Self::Target {
        &self.connect
    }
}

impl<T> fmt::Debug for Connect<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.connect.fmt(f)
    }
}

/// Ack connect message
pub struct ConnectAck<Io, St> {
    pub(crate) io: HandshakeResult<Io, (), codec::Codec, mpsc::Receiver<codec::Packet>>,
    pub(crate) session: Option<St>,
    pub(crate) keep_alive: Duration,
    pub(crate) inflight: usize,
    pub(crate) sink: MqttSink,
    pub(crate) packet: codec::ConnectAck,
}

impl<Io, St> ConnectAck<Io, St> {
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

    /// Set if server has saved session.
    pub fn session_present(mut self, val: bool) -> Self {
        self.packet.session_present = val;
        self
    }

    /// Update user properties for connect ack packet.
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.packet.user_properties);
        self
    }
}
