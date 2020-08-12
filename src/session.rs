use std::rc::Rc;

/// Mqtt connection session
pub struct Session<T, St>(Rc<SessionInner<T, St>>);

struct SessionInner<T, St> {
    st: St,
    sink: T,
}

impl<T, St> Clone for Session<T, St> {
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<T, St> Session<T, St> {
    pub(crate) fn new(st: St, sink: T) -> Self {
        Session(Rc::new(SessionInner { st, sink }))
    }

    pub fn sink(&self) -> &T {
        &self.0.sink
    }

    pub fn state(&self) -> &St {
        &self.0.st
    }
}
