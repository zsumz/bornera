//! Separately bounded connection-lifecycle publications.

use bornera_core::{CloseReason, ConnectionEpoch};
use calandria::{Retained, RetainedBytes};

/// One mechanical lifecycle edge for a fixed connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionEvent {
    /// The transport became ready to exchange application bytes.
    ///
    /// Protocol negotiation and regular operation admission remain caller-owned.
    TransportOpened {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact established transport lifetime.
        epoch: ConnectionEpoch,
    },
    /// The protocol session owner opened regular operation admission.
    AdmissionOpened {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact session-bearing transport lifetime.
        epoch: ConnectionEpoch,
    },
    /// Core policy began terminal closure.
    Closing {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact closing transport lifetime.
        epoch: ConnectionEpoch,
        /// Mechanical reason retained by the fixed epoch.
        reason: CloseReason,
    },
    /// The private transport capability completed closure.
    Closed {
        /// Monotonic event sequence within this engine.
        sequence: u64,
        /// Exact closed transport lifetime.
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
