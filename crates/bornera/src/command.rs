//! Fixed-size commands fenced to one exact connection-set generation.

use bornera_core::{OperationId, RetainedBytes};
use calandria::Retained;

use crate::ConnectionToken;

/// One mechanical command queued for an exact live connection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionCommand {
    /// Opens regular admission after protocol-owned session establishment.
    OpenAdmission {
        /// Exact set generation authorized by the sender.
        connection: ConnectionToken,
    },
    /// Explicitly cancels local observation of one accepted operation.
    Cancel {
        /// Exact set generation authorized by the sender.
        connection: ConnectionToken,
        /// Operation to cancel.
        operation: OperationId,
    },
    /// Closes admission before draining accepted work.
    BeginDrain {
        /// Exact set generation authorized by the sender.
        connection: ConnectionToken,
    },
    /// Forces requested closure of one exact connection lifetime.
    Close {
        /// Exact set generation authorized by the sender.
        connection: ConnectionToken,
    },
}

impl ConnectionCommand {
    pub(crate) const fn connection(self) -> ConnectionToken {
        match self {
            Self::OpenAdmission { connection }
            | Self::Cancel { connection, .. }
            | Self::BeginDrain { connection }
            | Self::Close { connection } => connection,
        }
    }
}

impl Retained for ConnectionCommand {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}
