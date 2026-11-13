//! FIFO queue of acknowledged sends waiting for send credit.
//!
//! Sends are encoded in queue order: only the queue head can be granted
//! credit, and it keeps its entry until the packet is encoded. Only the head
//! waits for write capacity, so the wake order of transport write waiters
//! does not reorder packets.
use std::task::{Context, Poll, Waker};
use std::{cell::RefCell, collections::VecDeque, future::Future, pin::Pin, rc::Rc};

use crate::error::SendPacketError;

/// Connection state that owns a [`SendQueue`].
pub(crate) trait SendQueueOwner {
    /// Returns the connection send queue.
    fn send_queue(&self) -> &RefCell<SendQueue>;

    /// Grants send credit to the queue head if credit is available.
    fn grant_send(&self);
}

/// Sends waiting for credit, in the order of arrival.
#[derive(Debug, Default)]
pub(crate) struct SendQueue {
    /// Queued sends in ascending ticket order, the head may be granted.
    waiters: VecDeque<Waiter>,
    /// Next monotonically increasing ticket.
    next_ticket: u64,
}

/// Queued send.
#[derive(Debug)]
struct Waiter {
    /// Stable identifier used to find this entry after other waiters leave.
    ticket: u64,
    /// Whether the entry holds send credit.
    granted: bool,
    /// Most recent task waiting for a grant or connection closure.
    waker: Option<Waker>,
}

impl SendQueue {
    /// Returns the number of queued sends, including the granted head.
    pub(crate) fn len(&self) -> usize {
        self.waiters.len()
    }

    /// Returns whether no send is queued.
    pub(crate) fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }

    /// Grants credit to the queue head if `used` leaves free credit within `cap`.
    pub(crate) fn grant(&mut self, used: usize, cap: usize) {
        if let Some(head) = self.waiters.front_mut()
            && !head.granted
            && used < cap
        {
            head.granted = true;
            if let Some(waker) = head.waker.take() {
                waker.wake();
            }
        }
    }

    /// Wakes and removes all queued sends, their waits fail.
    pub(crate) fn close(&mut self) {
        for waiter in self.waiters.drain(..) {
            if let Some(waker) = waiter.waker {
                waker.wake();
            }
        }
    }

    /// Queues a new send.
    fn push(&mut self) -> u64 {
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.waiters.push_back(Waiter {
            ticket,
            granted: false,
            waker: None,
        });
        ticket
    }

    /// Finds a queued entry, removed entries are not found.
    fn find(&self, ticket: u64) -> Option<usize> {
        self.waiters
            .binary_search_by_key(&ticket, |w| w.ticket)
            .ok()
    }

    /// Returns `true` once granted, `false` if the entry is removed by [`close`](Self::close).
    fn poll(&mut self, ticket: u64, cx: &mut Context<'_>) -> Poll<bool> {
        let Some(idx) = self.find(ticket) else {
            return Poll::Ready(false);
        };
        let waiter = &mut self.waiters[idx];
        if waiter.granted {
            Poll::Ready(true)
        } else {
            match waiter.waker {
                Some(ref mut waker) => waker.clone_from(cx.waker()),
                None => waiter.waker = Some(cx.waker().clone()),
            }
            Poll::Pending
        }
    }

    /// Removes an entry and releases its credit.
    fn remove(&mut self, ticket: u64) {
        if let Some(idx) = self.find(ticket) {
            self.waiters.remove(idx);
        }
    }
}

/// Queues a send, the returned future resolves once it is the granted queue head.
///
/// The caller is responsible for checking connection activity first.
pub(crate) fn send_permit<S: SendQueueOwner>(shared: &Rc<S>) -> WaitSendPermit<S> {
    let ticket = shared.send_queue().borrow_mut().push();
    shared.grant_send();
    WaitSendPermit {
        shared: shared.clone(),
        ticket: Some(ticket),
    }
}

/// Future that fails immediately with [`SendPacketError::Disconnected`].
pub(crate) fn send_permit_disconnected<S: SendQueueOwner>(shared: &Rc<S>) -> WaitSendPermit<S> {
    WaitSendPermit {
        shared: shared.clone(),
        ticket: None,
    }
}

/// Send credit held by the queue head.
///
/// Later sends wait until it is dropped. Register the packet in flight
/// before dropping it to transfer accounting from the queue to the in-flight packet.
pub(crate) struct SendPermit<S: SendQueueOwner> {
    shared: Rc<S>,
    ticket: u64,
}

impl<S: SendQueueOwner> Drop for SendPermit<S> {
    fn drop(&mut self) {
        self.shared.send_queue().borrow_mut().remove(self.ticket);
        self.shared.grant_send();
    }
}

/// Future yielding the [`SendPermit`] of a queued send.
///
/// Fails if the connection is closed while waiting. Dropping it removes the
/// send from the queue and passes the turn to the next one.
pub(crate) struct WaitSendPermit<S: SendQueueOwner> {
    shared: Rc<S>,
    /// Queued entry, `None` once resolved or for a closed connection.
    ticket: Option<u64>,
}

impl<S: SendQueueOwner> Future for WaitSendPermit<S> {
    type Output = Result<SendPermit<S>, SendPacketError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(ticket) = this.ticket else {
            return Poll::Ready(Err(SendPacketError::Disconnected));
        };
        let granted = this.shared.send_queue().borrow_mut().poll(ticket, cx);
        match granted {
            Poll::Pending => Poll::Pending,
            Poll::Ready(granted) => {
                this.ticket = None;
                if granted {
                    Poll::Ready(Ok(SendPermit {
                        shared: this.shared.clone(),
                        ticket,
                    }))
                } else {
                    Poll::Ready(Err(SendPacketError::Disconnected))
                }
            }
        }
    }
}

impl<S: SendQueueOwner> Drop for WaitSendPermit<S> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.shared.send_queue().borrow_mut().remove(ticket);
            self.shared.grant_send();
        }
    }
}
