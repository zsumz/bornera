//! Immutable observations of selector-free slots and their shared owner.

use std::io;

use bornera_core::ConnectionSnapshot;
use calandria::{MailboxSnapshot, ResourceTableSnapshot, RetainedBytes};
use calandria_mio::MioPollerSnapshot;

use crate::OwnerFailure;

/// Current physical state of one registered transport capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransportState {
    /// Nonblocking connect completion is pending.
    Connecting,
    /// The TCP capability is established.
    Open,
    /// Core policy requested physical teardown.
    Closing,
    /// The exact connection epoch owns no live TCP capability.
    Closed,
}

/// Transport operation that produced the last mechanical I/O diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransportFailurePhase {
    /// Asynchronous nonblocking connection completion.
    Connect,
    /// Post-connect TCP socket option application.
    SocketPolicy,
    /// Established-stream read.
    Read,
    /// Established-stream write.
    Write,
    /// Selector registration, reregistration, or deregistration.
    Readiness,
}

/// Bounded operating-system diagnostic without retaining an allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TransportDiagnostic {
    /// Transport operation that failed.
    pub phase: TransportFailurePhase,
    /// Portable I/O error category.
    pub kind: io::ErrorKind,
    /// Platform error code when the operating system supplied one.
    pub raw_os_error: Option<i32>,
}

impl TransportDiagnostic {
    pub(crate) fn from_io(phase: TransportFailurePhase, error: &io::Error) -> Self {
        Self {
            phase,
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
        }
    }
}

/// Immutable, data-only state for one selector-free connection slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConnectionSlotSnapshot {
    /// Deterministic connection-policy state.
    pub connection: ConnectionSnapshot,
    /// Latched fatal slot category, if normal mutation has stopped.
    pub owner_failure: Option<OwnerFailure>,
    /// Current private transport lifecycle.
    pub transport: TransportState,
    /// Most recently retained mechanical transport failure.
    pub transport_diagnostic: Option<TransportDiagnostic>,
    /// Complete frames retained by the write owner.
    pub queued_write_frames: usize,
    /// Complete-frame memory retained by the write owner.
    pub buffered_write_retained_bytes: RetainedBytes,
    /// Bytes retained inside the protocol decoder.
    pub buffered_read_bytes: RetainedBytes,
    /// Terminal outcomes waiting for the protocol owner.
    pub pending_outcomes: usize,
    /// Connection lifecycle edges waiting for the protocol owner.
    pub pending_events: usize,
    /// Last successfully published lifecycle sequence.
    pub event_sequence: u64,
    /// Absolute operation deadlines currently retained.
    pub pending_deadlines: usize,
}

/// Immutable state for one shared selector and bounded connection set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConnectionSetSnapshot {
    /// Latched set-wide selector failure, if normal driving has stopped.
    pub owner_failure: Option<OwnerFailure>,
    /// Generation-fenced connection-slot ownership.
    pub connections: ResourceTableSnapshot,
    /// Mio registration and backend-token state.
    pub poller: MioPollerSnapshot,
    /// Connections queued for fair progression.
    pub ready_connections: usize,
    /// Bounded set-level command mailbox pressure.
    pub commands: MailboxSnapshot,
    /// Backend events discarded after registration retirement.
    pub stale_backend_events: u64,
    /// Resource events discarded by token or epoch fencing.
    pub stale_resource_events: u64,
    /// Commands discarded by token or epoch fencing.
    pub stale_commands: u64,
}
