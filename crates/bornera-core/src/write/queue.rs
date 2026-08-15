//! Count- and byte-bounded FIFO storage with exact write advancement.

use std::{collections::VecDeque, fmt, num::NonZeroUsize};

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, Delivery, EffectId, OperationId};

use super::{
    DiscardedWrite, DiscardedWrites, WriteBoundary, WriteFrame, WriteProgress, WriteProgressError,
    WriteQueueLimits, WriteSlice,
};

/// Single-epoch owner of complete outbound frames in wire order.
pub struct WriteQueue<F> {
    pub(super) epoch: ConnectionEpoch,
    pub(super) limits: WriteQueueLimits,
    pub(super) frames: VecDeque<QueuedWrite<F>>,
    pub(super) retained_bytes: RetainedBytes,
}

impl<F: WriteFrame> WriteQueue<F> {
    /// Creates an empty writer for one exact connection epoch.
    pub(crate) fn new(epoch: ConnectionEpoch, limits: WriteQueueLimits) -> Self {
        Self {
            epoch,
            limits,
            frames: VecDeque::new(),
            retained_bytes: RetainedBytes::ZERO,
        }
    }

    /// Borrows at most `max_bytes` from only the FIFO front.
    pub fn front(&self, max_bytes: NonZeroUsize) -> Option<WriteSlice<'_>> {
        let front = self.frames.front()?;
        let remaining = &front.frame.bytes()[front.written..];
        let length = remaining.len().min(max_bytes.get());
        Some(WriteSlice {
            epoch: self.epoch,
            operation: front.operation,
            effect: front.effect,
            bytes: &remaining[..length],
        })
    }

    /// Applies exact positive or zero progress to the named FIFO-front effect.
    pub(crate) fn advance(
        &mut self,
        epoch: ConnectionEpoch,
        effect: EffectId,
        written: usize,
    ) -> Result<WriteProgress<F>, WriteProgressError> {
        self.validate_progress_epoch(epoch)?;
        let retained_before = self.retained_bytes;
        let Some(front) = self.frames.front_mut() else {
            return Err(WriteProgressError::NoPendingWrite);
        };
        if front.effect != effect {
            return Err(WriteProgressError::OutOfOrderEffect {
                expected: front.effect,
                received: effect,
            });
        }
        let remaining = front.frame.bytes().len() - front.written;
        if written > remaining {
            return Err(WriteProgressError::ExceedsRemaining { written, remaining });
        }
        let retained_after = if written == remaining {
            Some(retained_before.checked_sub(front.retained_bytes).ok_or(
                WriteProgressError::RetainedAccountingUnderflow {
                    retained: retained_before,
                    released: front.retained_bytes,
                },
            )?)
        } else {
            None
        };
        let boundary = if written > 0 && !front.started {
            front.started = true;
            WriteBoundary::Crossed
        } else {
            WriteBoundary::Unchanged
        };
        front.written = front
            .written
            .checked_add(written)
            .ok_or(WriteProgressError::ExceedsRemaining { written, remaining })?;
        let delivery = if front.started {
            Delivery::PossiblySent
        } else {
            Delivery::NotSent
        };
        if front.written < front.frame.bytes().len() {
            return Ok(WriteProgress::Pending {
                operation: front.operation,
                effect,
                remaining: front.frame.bytes().len() - front.written,
                boundary,
                delivery,
            });
        }
        let Some(completed) = self.frames.pop_front() else {
            return Err(WriteProgressError::NoPendingWrite);
        };
        let Some(retained_after) = retained_after else {
            return Err(WriteProgressError::RetainedAccountingUnderflow {
                retained: self.retained_bytes,
                released: completed.retained_bytes,
            });
        };
        self.retained_bytes = retained_after;
        Ok(WriteProgress::Complete {
            operation: completed.operation,
            effect,
            frame: completed.frame,
            boundary,
            delivery,
        })
    }

    /// Removes one exact effect, including a partially progressed front.
    pub(crate) fn discard(
        &mut self,
        epoch: ConnectionEpoch,
        effect: EffectId,
    ) -> Result<Option<DiscardedWrite<F>>, WriteProgressError> {
        self.validate_progress_epoch(epoch)?;
        let Some(index) = self.frames.iter().position(|frame| frame.effect == effect) else {
            return Ok(None);
        };
        let released = self.frames[index].retained_bytes;
        let retained_bytes = self.retained_bytes.checked_sub(released).ok_or(
            WriteProgressError::RetainedAccountingUnderflow {
                retained: self.retained_bytes,
                released,
            },
        )?;
        let Some(discarded) = self.frames.remove(index) else {
            return Ok(None);
        };
        self.retained_bytes = retained_bytes;
        Ok(Some(discarded.into_discarded()))
    }

    /// Removes every retained frame in original wire order.
    pub(crate) fn discard_all(&mut self) -> DiscardedWrites<F> {
        let retained_bytes = self.retained_bytes;
        let writes = self
            .frames
            .drain(..)
            .map(QueuedWrite::into_discarded)
            .collect();
        self.retained_bytes = RetainedBytes::ZERO;
        DiscardedWrites {
            writes,
            retained_bytes,
        }
    }

    /// Returns complete frames still retained by the writer.
    pub fn queued_frames(&self) -> usize {
        self.frames.len()
    }

    /// Returns the write identity retained for an accepted operation.
    pub fn effect_for(&self, operation: OperationId) -> Option<EffectId> {
        self.frames
            .iter()
            .find(|frame| frame.operation == operation)
            .map(|frame| frame.effect)
    }

    /// Returns variable memory retained by all queued frames.
    pub const fn retained_bytes(&self) -> RetainedBytes {
        self.retained_bytes
    }

    fn validate_progress_epoch(&self, epoch: ConnectionEpoch) -> Result<(), WriteProgressError> {
        if epoch == self.epoch {
            Ok(())
        } else {
            Err(WriteProgressError::StaleEpoch {
                expected: self.epoch,
                received: epoch,
            })
        }
    }
}

impl<F> fmt::Debug for WriteQueue<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteQueue")
            .field("epoch", &self.epoch)
            .field("limits", &self.limits)
            .field("queued_frames", &self.frames.len())
            .field("retained_bytes", &self.retained_bytes)
            .finish_non_exhaustive()
    }
}

pub(super) struct QueuedWrite<F> {
    pub(super) operation: OperationId,
    pub(super) effect: EffectId,
    pub(super) frame: F,
    pub(super) retained_bytes: RetainedBytes,
    pub(super) written: usize,
    pub(super) started: bool,
}

impl<F> QueuedWrite<F> {
    fn into_discarded(self) -> DiscardedWrite<F> {
        DiscardedWrite {
            operation: self.operation,
            effect: self.effect,
            frame: self.frame,
            written: self.written,
            delivery: if self.started {
                Delivery::PossiblySent
            } else {
                Delivery::NotSent
            },
        }
    }
}
