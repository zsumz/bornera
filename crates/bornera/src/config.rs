//! Fixed identities, address candidate, and hard production-owner limits.

use core::num::NonZeroUsize;
use std::net::SocketAddr;

use bornera_core::{ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, LaneId};
use calandria::{
    EventBatchLimits, LaneLimits, MailboxLimits, ResourceOwnerId, RetainedBytes, TimerLimits,
    TimerOwnerId,
};
use calandria_mio::MioPollerLimits;

/// One already-resolved plaintext connection attempt and its identity domains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    /// Logical physical endpoint identity.
    pub endpoint: EndpointId,
    /// Opaque traffic lane identity.
    pub lane: LaneId,
    /// Physical connection-slot identity.
    pub connection: ConnectionId,
    /// Exact socket lifetime.
    pub epoch: ConnectionEpoch,
    /// One DNS-selected address candidate.
    pub address: SocketAddr,
    /// Calandria resource-table ownership domain.
    pub resource_owner: ResourceOwnerId,
    /// Calandria timer-queue ownership domain.
    pub timer_owner: TimerOwnerId,
}

/// Hard bounds for one production connection owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineLimits {
    connection: ConnectionLimits,
    decoder: DecoderLimits,
    turn: TurnLimits,
    publication: PublicationLimits,
    outcome_bytes: RetainedBytes,
    operation_capacity: NonZeroUsize,
}

impl EngineLimits {
    /// Creates coherent bounds for policy, decoding, outcomes, polling, and I/O.
    pub fn new(
        connection: ConnectionLimits,
        decoder: DecoderLimits,
        turn: TurnLimits,
        publication: PublicationLimits,
    ) -> Result<Self, EngineLimitsError> {
        let operations = u64::try_from(connection.max_operations())
            .map_err(|_| EngineLimitsError::OutcomeRetainedOverflow)?;
        let operation_capacity = NonZeroUsize::new(connection.max_operations())
            .ok_or(EngineLimitsError::ZeroOperationCapacity)?;
        let outcome_bytes = decoder
            .reply_bytes
            .get()
            .checked_mul(operations)
            .map(RetainedBytes::new)
            .ok_or(EngineLimitsError::OutcomeRetainedOverflow)?;
        Ok(Self {
            connection,
            decoder,
            turn,
            publication,
            outcome_bytes,
            operation_capacity,
        })
    }

    /// Returns deterministic per-epoch connection-policy bounds.
    pub const fn connection(self) -> ConnectionLimits {
        self.connection
    }

    /// Returns the protocol decoder's aggregate retained-byte bound.
    pub const fn decoder_bytes(self) -> RetainedBytes {
        self.decoder.buffered_bytes
    }

    /// Returns the maximum retained bytes for one published reply frame.
    pub const fn reply_bytes(self) -> RetainedBytes {
        self.decoder.reply_bytes
    }

    /// Returns the maximum nonblocking operations performed in one turn.
    pub const fn io_operations(self) -> NonZeroUsize {
        self.turn.io_operations
    }

    /// Returns the maximum commands drained during one bounded owner turn.
    pub const fn command_operations(self) -> NonZeroUsize {
        self.turn.command_operations
    }

    /// Returns the maximum bytes attempted by one read or write operation.
    pub const fn io_chunk_bytes(self) -> NonZeroUsize {
        self.turn.io_chunk_bytes
    }

    pub(crate) fn poller(self) -> MioPollerLimits {
        MioPollerLimits::new(self.turn.poll_events, nonzero_one())
    }

    pub(crate) fn timers(self) -> TimerLimits {
        TimerLimits::new(self.operation_capacity(), RetainedBytes::ZERO)
    }

    pub(crate) fn outcomes(self) -> EventBatchLimits {
        EventBatchLimits::new(self.operation_capacity(), self.outcome_bytes)
    }

    pub(crate) fn lifecycle(self) -> EventBatchLimits {
        EventBatchLimits::new(self.publication.lifecycle_events, RetainedBytes::ZERO)
    }

    pub(crate) fn recovery_lifecycle(self) -> EventBatchLimits {
        const FIXED_EPOCH_EDGES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(3);
        EventBatchLimits::new(
            self.publication.lifecycle_events.max(FIXED_EPOCH_EDGES),
            RetainedBytes::ZERO,
        )
    }

    pub(crate) fn commands(self) -> MailboxLimits {
        let lane = LaneLimits::new(self.turn.command_operations, RetainedBytes::ZERO);
        MailboxLimits::new(lane, lane)
    }

    pub(crate) fn operation_capacity(self) -> NonZeroUsize {
        self.operation_capacity
    }
}

/// Protocol-decoder and reply-publication retained-byte bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecoderLimits {
    buffered_bytes: RetainedBytes,
    reply_bytes: RetainedBytes,
}

impl DecoderLimits {
    /// Creates explicit aggregate decoder and per-reply bounds.
    pub const fn new(buffered_bytes: RetainedBytes, reply_bytes: RetainedBytes) -> Self {
        Self {
            buffered_bytes,
            reply_bytes,
        }
    }
}

/// Per-turn readiness, command, I/O, and chunk bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnLimits {
    poll_events: NonZeroUsize,
    command_operations: NonZeroUsize,
    io_operations: NonZeroUsize,
    io_chunk_bytes: NonZeroUsize,
}

impl TurnLimits {
    /// Creates explicit work and byte bounds for one owner turn.
    pub const fn new(
        poll_events: NonZeroUsize,
        command_operations: NonZeroUsize,
        io_operations: NonZeroUsize,
        io_chunk_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            poll_events,
            command_operations,
            io_operations,
            io_chunk_bytes,
        }
    }
}

/// Separately bounded lifecycle-publication capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationLimits {
    lifecycle_events: NonZeroUsize,
}

impl PublicationLimits {
    /// Creates the lifecycle event count bound for one fixed epoch.
    pub const fn new(lifecycle_events: NonZeroUsize) -> Self {
        Self { lifecycle_events }
    }
}

/// Invalid combination of production-owner hard limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineLimitsError {
    /// The supplied core limits unexpectedly permit no operation.
    ZeroOperationCapacity,
    /// Reserving one maximum reply per operation overflowed fixed-width bytes.
    OutcomeRetainedOverflow,
}

impl core::fmt::Display for EngineLimitsError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::ZeroOperationCapacity => "operation capacity must be nonzero",
            Self::OutcomeRetainedOverflow => {
                "maximum retained outcome bytes overflow the accounting domain"
            }
        })
    }
}

impl core::error::Error for EngineLimitsError {}

const fn nonzero_one() -> NonZeroUsize {
    NonZeroUsize::MIN
}
