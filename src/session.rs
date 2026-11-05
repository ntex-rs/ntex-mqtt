use std::{fmt, ops::Deref, rc::Rc};

use ntex_service::cfg::{Cfg, Configuration, SharedCfg};

/// Reference-counted connection state and sink shared by application services.
///
/// Clones share the same application state, sink, and configuration. Dereferencing
/// a session borrows its application state; cloning does not clone that state.
pub struct Session<T, St>(Rc<SessionInner<T, St>>);

/// Connection resources retained until the last session clone is dropped.
struct SessionInner<T, St> {
    /// Application state returned by the handshake.
    st: St,
    /// Connection sink used to send packets to this peer.
    sink: T,
    /// Shared configuration registry associated with the connection.
    shared: SharedCfg,
}

impl<T, St> Clone for Session<T, St> {
    #[inline]
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<T, St> Session<T, St> {
    /// Own the handshake state, connection sink, and shared configuration.
    pub(crate) fn new(st: St, sink: T, shared: SharedCfg) -> Self {
        Session(Rc::new(SessionInner { st, sink, shared }))
    }

    #[inline]
    /// Returns reference to the connection sink
    pub fn sink(&self) -> &T {
        &self.0.sink
    }

    #[inline]
    /// Return the configuration of type `U` for this connection.
    ///
    /// Missing entries are supplied by `U::default()`.
    ///
    /// # Panics
    ///
    /// Panics if the shared configuration is still being built.
    pub fn cfg<U: Configuration>(&self) -> Cfg<U> {
        self.0.shared.get()
    }
}

impl<T, St> Deref for Session<T, St> {
    type Target = St;

    #[inline]
    fn deref(&self) -> &St {
        &self.0.st
    }
}

impl<T, St> fmt::Debug for Session<T, St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session").finish()
    }
}

/// Unit tests for session state access and formatting.
#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: A session exposes its original application state and sink with stable debug formatting.
    ///
    /// HOW:
    /// - Create a session with numeric state, a string sink, and default shared configuration.
    /// - Assert sink access, dereferenced state, and the exact debug representation.
    #[test]
    fn test_session() {
        let s = Session::new(42u32, "sink", SharedCfg::default());
        assert_eq!(s.sink(), &"sink");
        assert_eq!(*s, 42u32);
        assert_eq!(format!("{s:?}"), "Session");
    }
}
