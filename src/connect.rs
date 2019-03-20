use std::fmt;
use std::ops::Deref;
use std::time::Duration;

use either::Either;
use mqtt_codec as mqtt;

use crate::sink::MqttSink;

pub struct Connect<T = ()> {
    connect: mqtt::Connect,
    sink: MqttSink,
    param: T,
}

impl<T> Connect<T> {
    pub(crate) fn new(connect: mqtt::Connect, param: T, sink: MqttSink) -> Self {
        Self {
            connect,
            param,
            sink,
        }
    }

    pub fn get_param(&self) -> &T {
        &self.param
    }

    pub fn get_param_mut(&mut self) -> &mut T {
        &mut self.param
    }

    pub fn sink(&self) -> &MqttSink {
        &self.sink
    }
}

impl<T> Deref for Connect<T> {
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

pub struct ConnectAck<S> {
    session: Option<S>,
    session_present: bool,
    return_code: mqtt::ConnectCode,
    timeout: Duration,
    in_flight: usize,
}

impl<S> ConnectAck<S> {
    /// Create connect ack, `session_present` indicates that previous session is presents
    pub fn new(session: S, session_present: bool) -> Self {
        Self {
            session: Some(session),
            session_present,
            return_code: mqtt::ConnectCode::ConnectionAccepted,
            timeout: Duration::from_secs(5),
            in_flight: 15,
        }
    }

    /// Create connect ack object with `identifier rejected` return code
    pub fn identifier_rejected() -> Self {
        Self {
            session: None,
            session_present: false,
            return_code: mqtt::ConnectCode::IdentifierRejected,
            timeout: Duration::from_secs(5),
            in_flight: 15,
        }
    }

    /// Create connect ack object with `bad user name or password` return code
    pub fn bad_username_or_pwd() -> Self {
        Self {
            session: None,
            session_present: false,
            return_code: mqtt::ConnectCode::BadUserNameOrPassword,
            timeout: Duration::from_secs(5),
            in_flight: 15,
        }
    }

    /// Create connect ack object with `not authorized` return code
    pub fn not_authorized() -> Self {
        Self {
            session: None,
            session_present: false,
            return_code: mqtt::ConnectCode::NotAuthorized,
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

    pub(crate) fn into_inner(self) -> Either<(S, bool), mqtt::ConnectCode> {
        if let Some(session) = self.session {
            Either::Left((session, self.session_present))
        } else {
            Either::Right(self.return_code)
        }
    }
}
