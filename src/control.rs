//! Connection lifecycle and write-backpressure messages for control services.
use std::{io, marker::PhantomData};

use ntex_error::Failure;
use ntex_service::{Ctx, Service, ServiceFactory};

use crate::error;

/// Connection lifecycle and transport backpressure notifications.
#[derive(Debug)]
pub enum Control<E> {
    /// Write back-pressure is enabled/disabled
    WrBackpressure(WrBackpressure),
    /// The dispatcher is stopping and reports the terminal reason.
    ///
    /// The control service will receive this message only once.
    /// After receiving this message dispatcher stops.
    Stop(Reason<E>),
}

/// Dispatcher stop reasons
#[derive(Debug)]
pub enum Reason<E> {
    /// Unhandled application error from a connection service.
    Error(Error<E>),
    /// Protocol level error
    Protocol(ProtocolError),
    /// Peer is gone
    PeerGone(PeerGone),
}

impl<E> Control<E> {
    /// Create a notification for the current write-backpressure state.
    pub(super) fn wr(state: bool) -> Self {
        Control::WrBackpressure(WrBackpressure(state))
    }

    /// Create a terminal notification carrying an application error.
    pub(super) fn err(err: E) -> Self {
        Control::Stop(Reason::Error(Error::new(err)))
    }

    /// Create a terminal peer-disconnection notification.
    pub(super) fn peer_gone(err: Option<io::Error>) -> Self {
        Control::Stop(Reason::PeerGone(PeerGone(err)))
    }

    /// Create a terminal protocol-error notification.
    pub(super) fn proto(err: error::MqttProtocolError) -> Self {
        Control::Stop(Reason::Protocol(ProtocolError::new(err)))
    }
}

/// Write back-pressure control message
#[derive(Debug, Copy, Clone)]
pub struct WrBackpressure(bool);

impl WrBackpressure {
    #[inline]
    /// Is write back-pressure enabled
    pub fn enabled(&self) -> bool {
        self.0
    }
}

/// Service level error
#[derive(Debug, Clone)]
pub struct Error<E> {
    /// Owned application error reported by the connection service.
    err: E,
}

impl<E> Error<E> {
    /// Create new service level error
    pub fn new(err: E) -> Self {
        Self { err }
    }

    #[inline]
    /// Borrow the application service error.
    pub fn get_ref(&self) -> &E {
        &self.err
    }

    #[inline]
    /// Return inner error
    pub fn into(self) -> E {
        self.err
    }
}

/// Protocol level error
#[derive(Debug, Clone)]
pub struct ProtocolError {
    /// Protocol error that caused the dispatcher to stop.
    err: error::MqttProtocolError,
}

impl ProtocolError {
    /// Create new protocol level error
    pub fn new(err: error::MqttProtocolError) -> Self {
        Self { err }
    }

    #[inline]
    /// Returns reference to a protocol error
    pub fn get_ref(&self) -> &error::MqttProtocolError {
        &self.err
    }

    #[inline]
    /// Return inner error
    pub fn into(self) -> error::MqttProtocolError {
        self.err
    }
}

#[derive(Debug)]
/// Peer disconnection, optionally carrying a transport error.
pub struct PeerGone(pub(crate) Option<io::Error>);

impl PeerGone {
    #[inline]
    /// Borrow the transport error, or return `None` for an orderly disconnect.
    pub fn err(&self) -> Option<&io::Error> {
        self.0.as_ref()
    }

    #[inline]
    /// Consume the message and return its optional transport error.
    pub fn into(self) -> Option<io::Error> {
        self.0
    }
}

/// Default control service that logs unconfigured requests and returns no packet.
#[derive(Debug)]
pub struct DefaultControlService<E, R>(PhantomData<(E, R)>);

impl<E, R> Default for DefaultControlService<E, R> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<St, E, Req, R> ServiceFactory<St, Req> for DefaultControlService<E, R> {
    type Res = Option<R>;
    type Error = E;

    type Service = DefaultControlService<E, R>;
    type InitError = Failure;

    async fn create(&self, _: &St) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }
}

impl<St, E, Req, R> Service<St, Req> for DefaultControlService<E, R> {
    type Res = Option<R>;
    type Error = E;

    async fn call(&self, _: Req, _: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        log::warn!("MQTT5 Control service is not configured");
        Ok(None)
    }
}

/// Unit tests for control message formatting and error ownership.
#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: Control wrapper debug output identifies the represented message type.
    ///
    /// HOW:
    /// - Format `WrBackpressure`, `Error`, and `PeerGone` values and check their type names.
    #[test]
    fn test_debug() {
        // WrBackpressure, Error, PeerGone
        assert!(format!("{:?}", WrBackpressure(false)).contains("WrBackpressure"));
        assert!(format!("{:?}", Error { err: () }).contains("Error"));
        assert!(format!("{:?}", PeerGone(None)).contains("PeerGone"));
    }

    /// STATEMENT: Control error accessors borrow or transfer the original error values.
    ///
    /// HOW:
    /// - Borrow and consume application and protocol errors, checking their values.
    /// - Check both present and absent transport errors through `PeerGone`.
    #[test]
    fn test_accessors() {
        let err = Error::new(1);
        assert_eq!(*err.get_ref(), 1);
        assert_eq!(err.into(), 1);

        let err = ProtocolError::new(error::MqttProtocolError::KeepAliveTimeout);
        assert_eq!(err.get_ref(), &error::MqttProtocolError::KeepAliveTimeout);
        assert_eq!(err.into(), error::MqttProtocolError::KeepAliveTimeout);

        let gone = PeerGone(Some(io::Error::other("gone")));
        assert_eq!(gone.err().unwrap().to_string(), "gone");
        assert_eq!(gone.into().unwrap().to_string(), "gone");
        assert!(PeerGone(None).into().is_none());
    }
}
