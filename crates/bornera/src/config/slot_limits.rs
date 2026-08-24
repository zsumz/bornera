//! Coherent decoder, I/O, publication, and core bounds for one slot.

use core::num::NonZeroUsize;

use bornera_core::{ConnectionLimits, RetainedBytes};
use calandria::{EventBatchLimits, TimerLimits};

/// Hard bounds for one selector-free connection slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSlotLimits {
    connection: ConnectionLimits,
    decoder: DecoderLimits,
    io: IoLimits,
    publication: PublicationLimits,
    outcome_bytes: RetainedBytes,
    operation_capacity: NonZeroUsize,
}

impl ConnectionSlotLimits {
    /// Creates coherent policy, decoding, I/O, and publication bounds.
    pub fn new(
        connection: ConnectionLimits,
        decoder: DecoderLimits,
        io: IoLimits,
        publication: PublicationLimits,
    ) -> Result<Self, ConnectionSlotLimitsError> {
        let operations = u64::try_from(connection.max_operations())
            .map_err(|_| ConnectionSlotLimitsError::OutcomeRetainedOverflow)?;
        let operation_capacity = NonZeroUsize::new(connection.max_operations())
            .ok_or(ConnectionSlotLimitsError::ZeroOperationCapacity)?;
        let outcome_bytes = decoder
            .reply_bytes
            .get()
            .checked_mul(operations)
            .map(RetainedBytes::new)
            .ok_or(ConnectionSlotLimitsError::OutcomeRetainedOverflow)?;
        Ok(Self {
            connection,
            decoder,
            io,
            publication,
            outcome_bytes,
            operation_capacity,
        })
    }

    pub(crate) const fn connection(self) -> ConnectionLimits {
        self.connection
    }

    pub(crate) const fn decoder_bytes(self) -> RetainedBytes {
        self.decoder.buffered_bytes
    }

    /// Returns the aggregate retained-byte bound for protocol decoder state.
    pub const fn decoder_retained_bytes(self) -> RetainedBytes {
        self.decoder.buffered_bytes
    }

    pub(crate) const fn reply_bytes(self) -> RetainedBytes {
        self.decoder.reply_bytes
    }

    /// Returns the retained-byte bound for one complete decoded reply.
    pub const fn reply_retained_bytes(self) -> RetainedBytes {
        self.decoder.reply_bytes
    }

    pub(crate) const fn io_operations(self) -> NonZeroUsize {
        self.io.operations
    }

    pub(crate) const fn io_chunk_bytes(self) -> NonZeroUsize {
        self.io.chunk_bytes
    }

    pub(crate) const fn timers(self) -> TimerLimits {
        TimerLimits::new(self.operation_capacity, RetainedBytes::ZERO)
    }

    pub(crate) const fn outcomes(self) -> EventBatchLimits {
        EventBatchLimits::new(self.operation_capacity, self.outcome_bytes)
    }

    pub(crate) const fn lifecycle(self) -> EventBatchLimits {
        EventBatchLimits::new(self.publication.lifecycle_events, RetainedBytes::ZERO)
    }

    pub(crate) fn recovery_lifecycle(self) -> EventBatchLimits {
        const FIXED_EPOCH_EDGES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(3);
        EventBatchLimits::new(
            self.publication.lifecycle_events.max(FIXED_EPOCH_EDGES),
            RetainedBytes::ZERO,
        )
    }

    pub(crate) const fn operation_capacity(self) -> NonZeroUsize {
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

/// Per-slot nonblocking I/O work and chunk bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoLimits {
    operations: NonZeroUsize,
    chunk_bytes: NonZeroUsize,
}

impl IoLimits {
    /// Creates explicit I/O operation and byte bounds for one slot quantum.
    pub const fn new(operations: NonZeroUsize, chunk_bytes: NonZeroUsize) -> Self {
        Self {
            operations,
            chunk_bytes,
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

/// Invalid combination of selector-free connection-slot limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionSlotLimitsError {
    /// The supplied core limits unexpectedly permit no operation.
    ZeroOperationCapacity,
    /// Reserving one maximum reply per operation overflowed fixed-width bytes.
    OutcomeRetainedOverflow,
}

impl core::fmt::Display for ConnectionSlotLimitsError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::ZeroOperationCapacity => "operation capacity must be nonzero",
            Self::OutcomeRetainedOverflow => {
                "maximum retained outcome bytes overflow the accounting domain"
            }
        })
    }
}

impl core::error::Error for ConnectionSlotLimitsError {}
