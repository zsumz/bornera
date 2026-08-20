//! Immutable observations of one production connection owner.

use bornera_core::ConnectionSnapshot;
use calandria::{MailboxSnapshot, RetainedBytes};

use crate::OwnerFailure;

/// Current physical state of the private plaintext capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportState {
    /// Nonblocking connect completion is pending.
    Connecting,
    /// The TCP capability is established.
    Open,
    /// The exact connection epoch owns no live TCP capability.
    Closed,
}

/// Immutable, data-only production-owner snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineSnapshot {
    /// Deterministic connection-policy state.
    pub connection: ConnectionSnapshot,
    /// Latched fatal-owner category, if normal mutation has stopped.
    pub owner_failure: Option<OwnerFailure>,
    /// Current private transport lifecycle.
    pub transport: TransportState,
    /// Complete frames retained by the write owner.
    pub queued_write_frames: usize,
    /// Complete-frame bytes retained by the write owner.
    pub buffered_write_bytes: RetainedBytes,
    /// Bytes retained inside the protocol decoder.
    pub buffered_read_bytes: RetainedBytes,
    /// Terminal outcomes waiting for the protocol duty.
    pub pending_outcomes: usize,
    /// Connection lifecycle edges waiting for the protocol duty.
    pub pending_events: usize,
    /// Last successfully published lifecycle sequence.
    pub event_sequence: u64,
    /// Absolute operation deadlines currently retained.
    pub pending_deadlines: usize,
    /// Bounded command mailbox pressure and admission counters.
    pub commands: MailboxSnapshot,
    /// Backend events discarded after registration retirement.
    pub stale_backend_events: u64,
    /// Resource events discarded by generation fencing.
    pub stale_resource_events: u64,
    /// Epoch-fenced commands discarded before policy application.
    pub stale_commands: u64,
}
