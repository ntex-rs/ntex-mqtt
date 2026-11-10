use std::fmt;

use ntex_util::services::ordered;

use crate::types::QoS;

/// Arrival position of an incoming `QoS 1` or `QoS 2` publish.
///
/// The codec issues tickets in decode order, one sequence per connection.
/// The oldest outstanding ticket holds the turn. Passing or dropping it hands
/// the turn to the next ticket; a ticket dropped before its turn is skipped.
/// Publishes that are not delivered to the application, such as ignored
/// re-deliveries, drop their tickets and never hold up later ones.
///
/// Holding a ticket blocks the turn of every later ticket of the connection,
/// pass or drop it once its ordered side effect is done.
pub use ntex_util::services::ordered::Ticket;

/// Ticket issuer of a codec, a clone starts an independent sequence.
#[derive(Default)]
pub(crate) struct Tickets(ordered::Tickets);

impl Tickets {
    /// Issues the next ticket for a `QoS 1` or `QoS 2` publish, `None` for `QoS 0`.
    pub(crate) fn issue(&self, qos: QoS) -> Option<Ticket> {
        if qos == QoS::AtMostOnce {
            None
        } else {
            Some(self.0.issue())
        }
    }
}

impl Clone for Tickets {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl fmt::Debug for Tickets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: Only acknowledged publications consume arrival tickets, and cloned issuers are independent.
    ///
    /// HOW:
    /// - Issue tickets for each `QoS` and verify consecutive sequences and the oldest ticket's turn.
    /// - Clone the issuer and assert its first ticket starts at zero and differs from the original.
    #[test]
    fn issue_by_qos() {
        let tickets = Tickets::default();
        assert!(tickets.issue(QoS::AtMostOnce).is_none());
        let t0 = tickets.issue(QoS::AtLeastOnce).unwrap();
        let t1 = tickets.issue(QoS::ExactlyOnce).unwrap();
        assert_eq!((t0.seq(), t1.seq()), (0, 1));
        assert!(t0.is_turn());
        assert!(!t1.is_turn());

        // a clone starts an independent sequence
        let cloned = tickets.clone();
        let c0 = cloned.issue(QoS::AtLeastOnce).unwrap();
        assert_eq!(c0.seq(), 0);
        assert!(c0.is_turn());
        assert_ne!(c0, t0);
    }

    /// STATEMENT: Dropping an out-of-turn ticket skips its position without releasing later tickets early.
    ///
    /// HOW:
    /// - Issue three mixed-QoS tickets around a `QoS 0` request and poll the last ticket's turn.
    /// - Drop the middle ticket and verify the last still waits; drop the first and verify it becomes ready.
    /// - Verify diagnostics identify the ticket issuer and the next issued ticket owns the next sequence.
    #[ntex::test]
    async fn dropped_ticket_preserves_order() {
        use ntex_util::future::lazy;
        use std::future::Future;

        let tickets = Tickets::default();
        let first = tickets.issue(QoS::AtLeastOnce).unwrap();
        let skipped = tickets.issue(QoS::ExactlyOnce).unwrap();
        assert!(tickets.issue(QoS::AtMostOnce).is_none());
        let last = tickets.issue(QoS::AtLeastOnce).unwrap();
        assert_eq!(last.seq(), 2);
        assert!(format!("{tickets:?}").contains("Tickets"));
        let mut turn = Box::pin(last.turn());
        assert!(lazy(|cx| turn.as_mut().poll(cx)).await.is_pending());
        drop(skipped);
        assert!(lazy(|cx| turn.as_mut().poll(cx)).await.is_pending());
        drop(first);
        assert!(lazy(|cx| turn.as_mut().poll(cx)).await.is_ready());
        assert!(last.is_turn());
        drop(turn);
        drop(last);
        let next = tickets.issue(QoS::ExactlyOnce).unwrap();
        assert_eq!(next.seq(), 3);
        assert!(next.is_turn());
    }
}
