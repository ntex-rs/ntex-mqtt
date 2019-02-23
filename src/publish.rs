use bytes::Bytes;
use mqtt_codec as mqtt;

use crate::cell::Cell;

pub struct Publish<S> {
    publish: mqtt::Publish,
    session: Cell<S>,
}

impl<S> Publish<S> {
    pub(crate) fn new(session: Cell<S>, publish: mqtt::Publish) -> Self {
        Self { publish, session }
    }

    /// only present in PUBLISH Packets where the QoS level is 1 or 2.
    pub fn packet_id(&self) -> Option<u16> {
        self.publish.packet_id
    }

    #[inline]
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(&self) -> bool {
        self.publish.dup
    }

    #[inline]
    pub fn retain(&self) -> bool {
        self.publish.retain
    }

    #[inline]
    /// the level of assurance for delivery of an Application Message.
    pub fn qos(&self) -> mqtt::QoS {
        self.publish.qos
    }

    #[inline]
    /// the information channel to which payload data is published.
    pub fn topic(&self) -> &str {
        &self.publish.topic
    }

    #[inline]
    /// the Application Message that is being published.
    pub fn payload(&self) -> &Bytes {
        &self.publish.payload
    }

    #[inline]
    pub fn session(&self) -> &S {
        &*self.session
    }

    #[inline]
    pub fn session_mut(&mut self) -> &mut S {
        self.session.get_mut()
    }

    pub fn take_payload(self) -> Bytes {
        self.publish.payload
    }
}
