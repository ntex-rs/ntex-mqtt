use actix_router::Path;
use bytes::Bytes;
use mqtt_codec as mqtt;
use string::TryFrom;

use crate::cell::Cell;

pub struct Publish<S> {
    publish: mqtt::Publish,
    session: Cell<S>,
    topic: Path<string::String<Bytes>>,
    query: Option<string::String<Bytes>>,
}

impl<S> Publish<S> {
    pub(crate) fn new(session: Cell<S>, publish: mqtt::Publish) -> Self {
        let (topic, query) = if let Some(pos) = publish.topic.find('?') {
            (
                string::String::try_from(publish.topic.get_ref().slice(0, pos)).unwrap(),
                Some(
                    string::String::try_from(
                        publish.topic.get_ref().slice(pos + 1, publish.topic.len()),
                    )
                    .unwrap(),
                ),
            )
        } else {
            (publish.topic.clone(), None)
        };
        let topic = Path::new(topic);
        Self {
            publish,
            session,
            topic,
            query,
        }
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

    pub fn path(&self) -> &Path<string::String<Bytes>> {
        &self.topic
    }

    pub fn path_mut(&mut self) -> &mut Path<string::String<Bytes>> {
        &mut self.topic
    }

    pub fn query(&self) -> &str {
        self.query.as_ref().map(|s| s.as_ref()).unwrap_or("")
    }

    pub fn packet(&self) -> &mqtt::Publish {
        &self.publish
    }
}

impl<S> std::fmt::Debug for Publish<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.publish.fmt(f)
    }
}
