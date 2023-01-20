use std::ops::Deref;
use std::rc::Rc;

/// Mqtt connection session
pub struct Session<T, St>(Rc<SessionInner<T, St>>);

struct SessionInner<T, St> {
    st: St,
    sink: T,
}

impl<T, St> Clone for Session<T, St> {
    #[inline]
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<T, St> Session<T, St> {
    pub(crate) fn new(st: St, sink: T) -> Self {
        Session(Rc::new(SessionInner { st, sink }))
    }

    #[inline]
    pub fn sink(&self) -> &T {
        &self.0.sink
    }

    #[inline]
    pub fn state(&self) -> &St {
        &self.0.st
    }
}

impl<T, St> Deref for Session<T, St> {
    type Target = St;

    #[inline]
    fn deref(&self) -> &St {
        &self.0.st
    }
}
