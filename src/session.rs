use std::rc::Rc;
use std::time::Duration;

/// Mqtt connection session
pub struct Session<T, St>(Rc<SessionInner<T, St>>);

struct SessionInner<T, St> {
    st: St,
    sink: T,
    timeout: Duration,
    in_flight: usize,
}

impl<T, St> Clone for Session<T, St> {
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<T, St> Session<T, St> {
    pub(crate) fn new(st: St, sink: T, timeout: Duration, in_flight: usize) -> Self {
        Session(Rc::new(SessionInner { st, sink, timeout, in_flight }))
    }

    pub fn sink(&self) -> &T {
        &self.0.sink
    }

    pub fn state(&self) -> &St {
        &self.0.st
    }

    pub(super) fn params(&self) -> (Duration, usize) {
        let inner = self.0.as_ref();
        (inner.timeout, inner.in_flight)
    }
}
