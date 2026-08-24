//! Separately bounded connection-lifecycle publications.

use bornera_core::{CloseReason, ConnectionEpoch};
use calandria::{Retained, RetainedBytes};

/// One mechanical lifecycle edge for a fixed connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionEvent {
    /// The private transport capability completed establishment.
    TransportOpened {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact established socket lifetime.
        epoch: ConnectionEpoch,
    },
    /// The protocol session owner opened regular operation admission.
    AdmissionOpened {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact session-bearing socket lifetime.
        epoch: ConnectionEpoch,
    },
    /// Core policy began terminal closure.
    Closing {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact closing socket lifetime.
        epoch: ConnectionEpoch,
        /// Mechanical reason retained by the fixed epoch.
        reason: CloseReason,
    },
    /// The private transport capability completed closure.
    Closed {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact closed socket lifetime.
        epoch: ConnectionEpoch,
        /// Mechanical reason retained by the fixed epoch.
        reason: CloseReason,
    },
}

impl ConnectionEvent {
    /// Returns the monotonic sequence assigned at publication.
    pub const fn sequence(self) -> u64 {
        match self {
            Self::TransportOpened { sequence, .. }
            | Self::AdmissionOpened { sequence, .. }
            | Self::Closing { sequence, .. }
            | Self::Closed { sequence, .. } => sequence,
        }
    }

    /// Returns the exact socket lifetime that produced the edge.
    pub const fn epoch(self) -> ConnectionEpoch {
        match self {
            Self::TransportOpened { epoch, .. }
            | Self::AdmissionOpened { epoch, .. }
            | Self::Closing { epoch, .. }
            | Self::Closed { epoch, .. } => epoch,
        }
    }
}

impl Retained for ConnectionEvent {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}
