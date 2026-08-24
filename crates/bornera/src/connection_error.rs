//! Generation-fenced set access and connection retirement failures.

use core::fmt;

use bornera_core::{OperationPermit, ReserveError};

use crate::{EngineCommitError, EngineError, OwnerFailure};

/// Generation fencing or fatal-owner failure during direct set access.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConnectionAccessError {
    /// The supplied token no longer names its original connection generation.
    StaleConnection,
    /// The exact live connection or its shared readiness owner failed fatally.
    Owner(EngineError),
}

impl fmt::Display for ConnectionAccessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleConnection => formatter.write_str("connection generation is stale"),
            Self::Owner(source) => source.fmt(formatter),
        }
    }
}

impl core::error::Error for ConnectionAccessError {}

/// Generation fencing or core-policy rejection during set-level reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionReserveError {
    /// The supplied connection token no longer names its original generation.
    StaleConnection,
    /// The exact live slot rejected admission.
    Rejected(ReserveError),
}

impl fmt::Display for ConnectionReserveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleConnection => formatter.write_str("connection generation is stale"),
            Self::Rejected(source) => source.fmt(formatter),
        }
    }
}

impl core::error::Error for ConnectionReserveError {}

/// Ownership-preserving set-level frame commit failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConnectionCommitError<F> {
    /// The token was stale, so neither permit nor frame transferred.
    StaleConnection {
        /// Still-affine reservation permit.
        permit: OperationPermit,
        /// Exact unadmitted frame.
        frame: F,
    },
    /// The exact live slot rejected or fatally failed the commit.
    Connection(EngineCommitError<F>),
}

impl<F: fmt::Debug> fmt::Display for ConnectionCommitError<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleConnection { .. } => formatter.write_str("connection generation is stale"),
            Self::Connection(source) => source.fmt(formatter),
        }
    }
}

impl<F: fmt::Debug> core::error::Error for ConnectionCommitError<F> {}

/// Why a closed connection generation cannot yet be retired from its set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionRetireError {
    /// The token or complete connection identity is stale.
    StaleConnection,
    /// The physical capability has not completed teardown.
    TransportLive,
    /// Fatal slot state must be transferred through recovery instead.
    OwnerFailed(OwnerFailure),
    /// Outcomes or lifecycle events remain owned by the slot.
    PublicationsPending,
}

impl fmt::Display for ConnectionRetireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StaleConnection => "connection generation is stale",
            Self::TransportLive => "connection transport is still live",
            Self::OwnerFailed(_) => "failed connection must be recovered",
            Self::PublicationsPending => "connection still owns undrained publications",
        })
    }
}

impl core::error::Error for ConnectionRetireError {}

/// Why one connection generation could not transfer recovery ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionRecoveryError {
    /// The token or complete connection identity is stale.
    StaleConnection,
    /// Normal mutation remains safe, so fatal recovery is not permitted.
    OwnerRunning,
}

impl fmt::Display for ConnectionRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StaleConnection => "connection generation is stale",
            Self::OwnerRunning => "connection owner is still running",
        })
    }
}

impl core::error::Error for ConnectionRecoveryError {}
