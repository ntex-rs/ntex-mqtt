use std::{fmt, ops::Deref, rc::Rc};

use ntex_service::cfg::{Cfg, Configuration, SharedCfg};

/// Mqtt connection session
pub struct Session<T, St>(Rc<SessionInner<T, St>>);

struct SessionInner<T, St> {
    st: St,
    sink: T,
    shared: SharedCfg,
}

impl<T, St> Clone for Session<T, St> {
    #[inline]
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<T, St> Session<T, St> {
    pub(crate) fn new(st: St, sink: T, shared: SharedCfg) -> Self {
        Session(Rc::new(SessionInner { st, sink, shared }))
    }

    #[inline]
    pub fn sink(&self) -> &T {
        &self.0.sink
    }

    #[inline]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session() {
        let s = Session::new(42u32, "sink");
        assert_eq!(s.sink(), &"sink");
        assert_eq!(*s, 42u32);
        assert_eq!(format!("{s:?}"), "Session");
    }
}
