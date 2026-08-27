//! Control message for connection management service
use std::{convert::Infallible, io, marker::PhantomData};

use ntex_service::{Ctx, Service, ServiceFactory};

use crate::error;

/// Connection control messages
#[derive(Debug)]
pub enum Control<E> {
    /// Write back-pressure is enabled/disabled
    WrBackpressure(WrBackpressure),
    /// Dispatcher is preparing for shutdown.
    ///
    /// The control service will receive this message only once.
    /// After receiving this message dispatcher stops.
    Stop(Reason<E>),
}

/// Dispatcher stop reasons
#[derive(Debug)]
pub enum Reason<E> {
    /// Unhandled application level error from handshake, publish and control services
    Error(Error<E>),
    /// Protocol level error
    Protocol(ProtocolError),
    /// Peer is gone
    PeerGone(PeerGone),
}

impl<E> Control<E> {
    pub(super) fn wr(state: bool) -> Self {
        Control::WrBackpressure(WrBackpressure(state))
    }

    pub(super) fn err(err: E) -> Self {
        Control::Stop(Reason::Error(Error::new(err)))
    }

    pub(super) fn peer_gone(err: Option<io::Error>) -> Self {
        Control::Stop(Reason::PeerGone(PeerGone(err)))
    }

    pub(super) fn proto(err: error::ProtocolError) -> Self {
        Control::Stop(Reason::Protocol(ProtocolError::new(err)))
    }
}

/// Write back-pressure `CtlFrame` message
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
    err: E,
}

impl<E> Error<E> {
    pub fn new(err: E) -> Self {
        Self { err }
    }

    #[inline]
    /// Returns reference to mqtt error
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
    err: error::ProtocolError,
}

impl ProtocolError {
    pub fn new(err: error::ProtocolError) -> Self {
        Self { err }
    }

    #[inline]
    /// Returns reference to a protocol error
    pub fn get_ref(&self) -> &error::ProtocolError {
        &self.err
    }

    #[inline]
    /// Return inner error
    pub fn into(self) -> error::ProtocolError {
        self.err
    }
}

#[derive(Debug)]
/// Peer gone control message
pub struct PeerGone(pub(crate) Option<io::Error>);

impl PeerGone {
    #[inline]
    /// Returns error reference
    pub fn err(&self) -> Option<&io::Error> {
        self.0.as_ref()
    }

    #[inline]
    /// Take error
    pub fn into(self) -> Option<io::Error> {
        self.0
    }
}

/// Default control service
#[derive(Debug)]
pub struct DefaultControlService<E, R>(PhantomData<(E, R)>);

impl<E, R> Default for DefaultControlService<E, R> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<St, E, Req, R, Cfg> ServiceFactory<St, Req, Cfg> for DefaultControlService<E, R> {
    type Res = Option<R>;
    type Error = E;

    type Service = DefaultControlService<E, R>;
    type InitError = Infallible;

    async fn create(&self, _: &Cfg) -> Result<Self::Service, Self::InitError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug() {
        // WrBackpressure, Error, PeerGone
        assert!(format!("{:?}", WrBackpressure(false)).contains("WrBackpressure"));
        assert!(format!("{:?}", Error { err: () }).contains("Error"));
        assert!(format!("{:?}", PeerGone(None)).contains("PeerGone"));
    }
}
