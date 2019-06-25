use crate::cell::Cell;
use std::ops::{Deref, DerefMut};

/// Connection session
///
/// Connection session is an arbitrary data attached to the each incoming message.
/// Connect service return session as a response for connect message.
#[derive(Debug)]
pub struct Session<T>(Cell<T>);

impl<T> Session<T> {
    pub(crate) fn new(st: Cell<T>) -> Self {
        Session(st)
    }
}

impl<T> Deref for Session<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.get_ref()
    }
}

impl<T> DerefMut for Session<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.get_mut()
    }
}
