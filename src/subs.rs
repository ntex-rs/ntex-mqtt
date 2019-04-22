use std::marker::PhantomData;

use bytes::Bytes;
use mqtt_codec as mqtt;
use string::String;

use crate::cell::Cell;

pub struct Subscribe<S> {
    topics: Vec<(String<Bytes>, mqtt::QoS)>,
    codes: Vec<mqtt::SubscribeReturnCode>,
    session: Cell<S>,
}

pub struct SubscribeResult {
    pub(crate) codes: Vec<mqtt::SubscribeReturnCode>,
}

impl<S> Subscribe<S> {
    pub(crate) fn new(session: Cell<S>, topics: Vec<(String<Bytes>, mqtt::QoS)>) -> Self {
        let mut codes = Vec::with_capacity(topics.len());
        (0..topics.len()).for_each(|_| codes.push(mqtt::SubscribeReturnCode::Failure));

        Self {
            topics,
            session,
            codes,
        }
    }

    #[inline]
    pub fn session(&self) -> &S {
        &*self.session
    }

    #[inline]
    pub fn session_mut(&mut self) -> &mut S {
        self.session.get_mut()
    }

    pub fn iter_mut(&mut self) -> SubscribeIter<S> {
        SubscribeIter {
            subs: self as *const _ as *mut _,
            entry: 0,
            lt: PhantomData,
        }
    }

    pub fn finish(self) -> SubscribeResult {
        SubscribeResult { codes: self.codes }
    }
}

impl<'a, S> IntoIterator for &'a mut Subscribe<S> {
    type Item = Subscription<'a, S>;
    type IntoIter = SubscribeIter<'a, S>;

    fn into_iter(self) -> SubscribeIter<'a, S> {
        self.iter_mut()
    }
}

pub struct SubscribeIter<'a, S> {
    subs: *mut Subscribe<S>,
    entry: usize,
    lt: PhantomData<&'a mut Subscribe<S>>,
}

impl<'a, S> SubscribeIter<'a, S> {
    fn next_unsafe(&mut self) -> Option<Subscription<'a, S>> {
        let subs = unsafe { &mut *self.subs };

        if self.entry < subs.topics.len() {
            let s = Subscription {
                topic: &subs.topics[self.entry].0,
                qos: subs.topics[self.entry].1,
                cell: &mut subs.session,
                code: &mut subs.codes[self.entry],
            };
            Some(s)
        } else {
            None
        }
    }
}

impl<'a, S> Iterator for SubscribeIter<'a, S> {
    type Item = Subscription<'a, S>;

    #[inline]
    fn next(&mut self) -> Option<Subscription<'a, S>> {
        self.next_unsafe()
    }
}

pub struct Subscription<'a, S> {
    topic: &'a String<Bytes>,
    cell: &'a mut Cell<S>,
    qos: mqtt::QoS,
    code: &'a mut mqtt::SubscribeReturnCode,
}

impl<'a, S> Subscription<'a, S> {
    #[inline]
    pub fn session(&self) -> &S {
        &*self.cell
    }

    #[inline]
    pub fn session_mut(&mut self) -> &mut S {
        self.cell.get_mut()
    }

    pub fn topic(&self) -> &'a String<Bytes> {
        &self.topic
    }

    pub fn qos(&self) -> mqtt::QoS {
        self.qos
    }

    pub fn fail(&mut self) {
        *self.code = mqtt::SubscribeReturnCode::Failure
    }

    pub fn subscribe(&mut self, qos: mqtt::QoS) {
        *self.code = mqtt::SubscribeReturnCode::Success(qos)
    }
}

pub struct Unsubscribe<S> {
    topics: Vec<String<Bytes>>,
    session: Cell<S>,
}

impl<S> Unsubscribe<S> {
    pub(crate) fn new(session: Cell<S>, topics: Vec<String<Bytes>>) -> Self {
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

    pub fn iter(&self) -> impl Iterator<Item = &String<Bytes>> {
        self.topics.iter()
    }
}
