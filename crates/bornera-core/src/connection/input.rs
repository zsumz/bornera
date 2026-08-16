//! Data-only inputs accepted by one connection epoch.

use calandria::Moment;

use crate::{CloseReason, ConnectionEpoch, OperationId};

/// An observation or command applied to the deterministic epoch machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionInput {
    /// The protocol session owner completed establishment.
    OpenAdmission {
        /// Exact epoch whose protocol session was established.
        epoch: ConnectionEpoch,
    },
    /// Explicitly cancel local operation ownership.
    Cancel {
        /// Epoch named by the caller.
        epoch: ConnectionEpoch,
        /// Operation to cancel.
        operation: OperationId,
    },
    /// A scheduled absolute deadline was observed.
    DeadlineElapsed {
        /// Epoch named by the timer event.
        epoch: ConnectionEpoch,
        /// Operation whose deadline was scheduled.
        operation: OperationId,
        /// Current fixed-width monotonic moment.
        now: Moment,
    },
    /// The adapter rejected a complete inbound frame as malformed.
    ReplyMalformed {
        /// Epoch that produced the malformed frame.
        epoch: ConnectionEpoch,
    },
    /// Close admission, then finish already accepted work.
    BeginDrain {
        /// Exact epoch whose admission should close before draining.
        epoch: ConnectionEpoch,
    },
    /// Force closure of this epoch.
    CloseRequested {
        /// Epoch named by the requester or capability.
        epoch: ConnectionEpoch,
        /// Mechanical closure reason.
        reason: CloseReason,
    },
    /// Confirm that the physical capability is closed.
    EpochClosed {
        /// Epoch whose capability closed.
        epoch: ConnectionEpoch,
    },
}

/// A complete opaque frame classified by an adapter as a reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundReply<F> {
    pub(crate) epoch: ConnectionEpoch,
    pub(crate) key: crate::MatchKey,
    pub(crate) frame: F,
}

impl<F> InboundReply<F> {
    /// Creates a reply input after protocol-specific classification.
    pub const fn new(epoch: ConnectionEpoch, key: crate::MatchKey, frame: F) -> Self {
        Self { epoch, key, frame }
    }

    /// Returns the epoch that produced the frame.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the protocol-visible match key extracted by the adapter.
    pub const fn key(&self) -> crate::MatchKey {
        self.key
    }

    /// Recovers the opaque frame before it is applied to a machine.
    pub fn into_frame(self) -> F {
        self.frame
    }
}
