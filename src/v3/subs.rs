use std::marker::PhantomData;

use crate::codec3 as mqtt;
use bytestring::ByteString;

/// Subscribe message
pub struct Subscribe {
    topics: Vec<(ByteString, mqtt::QoS)>,
    codes: Vec<mqtt::SubscribeReturnCode>,
}

/// Result of a subscribe message
pub struct SubscribeResult {
    pub(crate) codes: Vec<mqtt::SubscribeReturnCode>,
}

impl Subscribe {
    pub(crate) fn new(topics: Vec<(ByteString, mqtt::QoS)>) -> Self {
        let mut codes = Vec::with_capacity(topics.len());
        (0..topics.len()).for_each(|_| codes.push(mqtt::SubscribeReturnCode::Failure));

        Self { topics, codes }
    }

    #[inline]
    /// returns iterator over subscription topics
    pub fn iter_mut(&mut self) -> SubscribeIter {
        SubscribeIter {
            subs: self as *const _ as *mut _,
            entry: 0,
            lt: PhantomData,
        }
    }

    #[inline]
    /// convert subscription to a result
    pub fn into_result(self) -> SubscribeResult {
        SubscribeResult { codes: self.codes }
    }
}

impl<'a> IntoIterator for &'a mut Subscribe {
    type Item = Subscription<'a>;
    type IntoIter = SubscribeIter<'a>;

    fn into_iter(self) -> SubscribeIter<'a> {
        self.iter_mut()
    }
}

/// Iterator over subscription topics
pub struct SubscribeIter<'a> {
    subs: *mut Subscribe,
    entry: usize,
    lt: PhantomData<&'a mut Subscribe>,
}

impl<'a> SubscribeIter<'a> {
    fn next_unsafe(&mut self) -> Option<Subscription<'a>> {
        let subs = unsafe { &mut *self.subs };

        if self.entry < subs.topics.len() {
            let s = Subscription {
                topic: &subs.topics[self.entry].0,
                qos: subs.topics[self.entry].1,
                code: &mut subs.codes[self.entry],
            };
            self.entry += 1;
            Some(s)
        } else {
            None
        }
    }
}

impl<'a> Iterator for SubscribeIter<'a> {
    type Item = Subscription<'a>;

    #[inline]
    fn next(&mut self) -> Option<Subscription<'a>> {
        self.next_unsafe()
    }
}

/// Subscription topic
pub struct Subscription<'a> {
    topic: &'a ByteString,
    qos: mqtt::QoS,
    code: &'a mut mqtt::SubscribeReturnCode,
}

impl<'a> Subscription<'a> {
    #[inline]
    /// subscription topic
    pub fn topic(&self) -> &'a ByteString {
        &self.topic
    }

    #[inline]
    /// the level of assurance for delivery of an Application Message.
    pub fn qos(&self) -> mqtt::QoS {
        self.qos
    }

    #[inline]
    /// fail to subscribe to the topic
    pub fn fail(&mut self) {
        *self.code = mqtt::SubscribeReturnCode::Failure
    }

    #[inline]
    /// subscribe to a topic with specific qos
    pub fn subscribe(&mut self, qos: mqtt::QoS) {
        *self.code = mqtt::SubscribeReturnCode::Success(qos)
    }
}

/// Unsubscribe message
pub struct Unsubscribe {
    topics: Vec<ByteString>,
}

impl Unsubscribe {
    pub(crate) fn new(topics: Vec<ByteString>) -> Self {
        Self { topics }
    }

    /// returns iterator over unsubscribe topics
    pub fn iter(&self) -> impl Iterator<Item = &ByteString> {
        self.topics.iter()
    }
}
