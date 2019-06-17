use crate::cell::Cell;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct State<T>(Cell<T>);

impl<T> State<T> {
    pub(crate) fn new(st: Cell<T>) -> Self {
        State(st)
    }
}

impl<T> Deref for State<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.get_ref()
    }
}

impl<T> DerefMut for State<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.get_mut()
    }
}
