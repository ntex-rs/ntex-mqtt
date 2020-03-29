use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;
use std::time::Duration;

use crate::sink::MqttSink;

/// Mqtt connection session
pub struct Session<St>(Rc<RefCell<SessionInner<St>>>);

struct SessionInner<St> {
    st: St,
    sink: MqttSink,
    timeout: Duration,
    in_flight: usize,
}

impl<St> Clone for Session<St> {
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

impl<St> Session<St> {
    pub(crate) fn new(st: St, sink: MqttSink, timeout: Duration, in_flight: usize) -> Self {
        Session(Rc::new(RefCell::new(SessionInner {
            st,
            sink,
            timeout,
            in_flight,
        })))
    }

    pub fn sink(&self) -> Ref<'_, MqttSink> {
        Ref::map(self.0.borrow(), |inner| &inner.sink)
    }

    pub fn state(&self) -> RefMut<'_, St> {
        RefMut::map(self.0.borrow_mut(), |inner| &mut inner.st)
    }

    pub(super) fn params(&self) -> (Duration, usize) {
        let inner = self.0.borrow();
        (inner.timeout, inner.in_flight)
    }
}
