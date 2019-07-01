use std::fmt;
use std::ops::Deref;
use std::time::Duration;

use actix_ioframe as ioframe;
use either::Either;
use mqtt_codec as mqtt;

use crate::sink::MqttSink;

/// Connect message
pub struct Connect<Io> {
    connect: mqtt::Connect,
    sink: MqttSink,
    io: ioframe::ConnectResult<Io, (), mqtt::Codec>,
}

impl<Io> Connect<Io> {
    pub(crate) fn new(
        connect: mqtt::Connect,
        io: ioframe::ConnectResult<Io, (), mqtt::Codec>,
        sink: MqttSink,
    ) -> Self {
        Self { connect, io, sink }
    }

    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        self.io.get_ref()
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        self.io.get_mut()
    }

    /// Returns mqtt server sink
    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }

    /// Ack connect message and set state
    pub fn ack<St>(self, st: St, session_present: bool) -> ConnectAck<Io, St> {
        ConnectAck::new(self.io, st, session_present)
    }

    /// Create connect ack object with `identifier rejected` return code
    pub fn identifier_rejected<St>(self) -> ConnectAck<Io, St> {
        ConnectAck {
            io: self.io,
            session: None,
            session_present: false,
            return_code: mqtt::ConnectCode::IdentifierRejected,
            timeout: Duration::from_secs(5),
            in_flight: 15,
        }
    }

    /// Create connect ack object with `bad user name or password` return code
    pub fn bad_username_or_pwd<St>(self) -> ConnectAck<Io, St> {
        ConnectAck {
            io: self.io,
            session: None,
            session_present: false,
            return_code: mqtt::ConnectCode::BadUserNameOrPassword,
            timeout: Duration::from_secs(5),
            in_flight: 15,
        }
    }

    /// Create connect ack object with `not authorized` return code
    pub fn not_authorized<St>(self) -> ConnectAck<Io, St> {
        ConnectAck {
            io: self.io,
            session: None,
            session_present: false,
            return_code: mqtt::ConnectCode::NotAuthorized,
            timeout: Duration::from_secs(5),
            in_flight: 15,
        }
    }
}

impl<Io> Deref for Connect<Io> {
    type Target = mqtt::Connect;

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
    io: ioframe::ConnectResult<Io, (), mqtt::Codec>,
    session: Option<St>,
    session_present: bool,
    return_code: mqtt::ConnectCode,
    timeout: Duration,
    in_flight: usize,
}

impl<Io, St> ConnectAck<Io, St> {
    /// Create connect ack, `session_present` indicates that previous session is presents
    pub(crate) fn new(
        io: ioframe::ConnectResult<Io, (), mqtt::Codec>,
        session: St,
        session_present: bool,
    ) -> Self {
        Self {
            io,
            session: Some(session),
            session_present,
            return_code: mqtt::ConnectCode::ConnectionAccepted,
            timeout: Duration::from_secs(5),
            in_flight: 15,
        }
    }

    /// Set idle time-out for the connection in milliseconds
    ///
    /// By default idle time-out is set to 300000 milliseconds
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set in-flight count. Total number of `in-flight` packets
    ///
    /// By default in-flight count is set to 15
    pub fn in_flight(mut self, in_flight: usize) -> Self {
        self.in_flight = in_flight;
        self
    }

    pub(crate) fn into_inner(
        self,
    ) -> Either<
        (ioframe::ConnectResult<Io, (), mqtt::Codec>, St, bool),
        (
            ioframe::ConnectResult<Io, (), mqtt::Codec>,
            mqtt::ConnectCode,
        ),
    > {
        if let Some(session) = self.session {
            Either::Left((self.io, session, self.session_present))
        } else {
            Either::Right((self.io, self.return_code))
        }
    }
}
