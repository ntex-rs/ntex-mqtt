use std::rc::Rc;

/// Mqtt connection session
pub struct Session<T, St>(Rc<SessionInner<T, St>>);

struct SessionInner<T, St> {
    st: St,
    sink: T,
    in_flight: usize,
}

impl<T, St> Clone for Session<T, St> {
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<T, St> Session<T, St> {
    pub(crate) fn new(st: St, sink: T, in_flight: usize) -> Self {
        Session(Rc::new(SessionInner { st, sink, in_flight }))
    }

    pub fn sink(&self) -> &T {
        &self.0.sink
    }

    pub fn state(&self) -> &St {
        &self.0.st
    }

    pub(super) fn max_inflight(&self) -> usize {
        self.0.as_ref().in_flight
    }
}
