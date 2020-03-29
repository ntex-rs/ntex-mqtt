use std::rc::Rc;
use std::time::Duration;

use crate::sink::MqttSink;

/// Mqtt connection session
pub struct Session<St>(Rc<SessionInner<St>>);

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
        Session(Rc::new(SessionInner {
            st,
            sink,
            timeout,
            in_flight,
        }))
    }

    pub fn sink(&self) -> &MqttSink {
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
