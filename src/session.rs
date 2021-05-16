use std::ops::Deref;
use std::rc::Rc;

/// Mqtt connection session
pub struct Session<T, St>(Rc<SessionInner<T, St>>);

struct SessionInner<T, St> {
    st: St,
    sink: T,
    max_receive: u16,
    max_topic_alias: u16,
}

impl<T, St> Clone for Session<T, St> {
    #[inline]
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<T, St> Session<T, St> {
    pub(crate) fn new(st: St, sink: T) -> Self {
        Session(Rc::new(SessionInner { st, sink, max_receive: 0, max_topic_alias: 0 }))
    }

    pub(crate) fn new_v5(st: St, sink: T, max_receive: u16, max_topic_alias: u16) -> Self {
        Session(Rc::new(SessionInner { st, sink, max_receive, max_topic_alias }))
    }

    #[inline]
    pub fn sink(&self) -> &T {
        &self.0.sink
    }

    #[inline]
    pub fn state(&self) -> &St {
        &self.0.st
    }

    pub(crate) fn params(&self) -> (u16, u16) {
        (self.0.max_receive, self.0.max_topic_alias)
    }
}

impl<T, St> Deref for Session<T, St> {
    type Target = St;

    #[inline]
    fn deref(&self) -> &St {
        &self.0.st
    }
}
