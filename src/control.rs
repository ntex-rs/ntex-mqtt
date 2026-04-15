//! Control message for connection management service
use std::{fmt, io, marker::PhantomData};

use ntex_service::{Service, ServiceCtx, ServiceFactory};

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
    pub fn take(&mut self) -> Option<io::Error> {
        self.0.take()
    }
}

/// Default control service
#[derive(Debug)]
pub struct DefaultControlService<S, E, R, Err = ()>(PhantomData<(S, E, R, Err)>);

impl<S, E: fmt::Debug, R, Err> Default for DefaultControlService<S, E, R, Err> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S, E: fmt::Debug, R, Err> ServiceFactory<Control<E>, S>
    for DefaultControlService<S, E, R, Err>
{
    type Response = Option<R>;
    type Error = E;
    type InitError = Err;
    type Service = DefaultControlService<S, E, R, ()>;

    async fn create(&self, _: S) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }
}

impl<S, E: fmt::Debug, R> Service<Control<E>> for DefaultControlService<S, E, R, ()> {
    type Response = Option<R>;
    type Error = E;

    async fn call(
        &self,
        pkt: Control<E>,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::warn!("MQTT5 Control service is not configured, pkt: {pkt:?}");
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
