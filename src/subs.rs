use bytes::Bytes;
use mqtt_codec as mqtt;
use string::String;

use crate::cell::Cell;

pub struct Subscribe<S> {
    topics: Vec<(String<Bytes>, mqtt::QoS)>,
    session: Cell<S>,
}

impl<S> Subscribe<S> {
    pub(crate) fn new(session: Cell<S>, topics: Vec<(String<Bytes>, mqtt::QoS)>) -> Self {
        Self { topics, session }
    }

    #[inline]
    pub fn session(&self) -> &S {
        &*self.session
    }

    #[inline]
    pub fn session_mut(&mut self) -> &mut S {
        self.session.get_mut()
    }
}
