//! Count- and retained-byte-bounded FIFO storage with exact write advancement.

use std::{collections::VecDeque, fmt, num::NonZeroUsize};

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, Delivery, EffectId, OperationId};

use super::{
    DiscardedWrite, DiscardedWrites, FrameContractViolation, WriteBoundary, WriteFrame,
    WriteProgress, WriteProgressError, WriteQueueLimits, WriteSlice, queued::QueuedWrite,
};

/// Single-epoch owner of complete outbound frames in wire order.
pub(crate) struct WriteQueue<F> {
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
            frames: VecDeque::with_capacity(limits.max_frames()),
            retained_bytes: RetainedBytes::ZERO,
        }
    }

    /// Borrows at most `max_bytes` from only the FIFO front.
    pub(crate) fn front(
        &self,
        max_bytes: NonZeroUsize,
    ) -> Result<Option<WriteSlice<'_>>, FrameContractViolation> {
        let Some(front) = self.frames.front() else {
            return Ok(None);
        };
        let bytes = front.frame.bytes();
        let measured = front.measure.wire_bytes();
        if bytes.len() != measured {
            return Err(FrameContractViolation::WireLengthChanged {
                measured,
                observed: bytes.len(),
            });
        }
        let remaining = bytes.get(front.written..measured).ok_or(
            FrameContractViolation::WriteRangeUnavailable {
                measured,
                written: front.written,
                observed: bytes.len(),
            },
        )?;
        let length = remaining.len().min(max_bytes.get());
        Ok(Some(WriteSlice {
            epoch: self.epoch,
            operation: front.operation,
            effect: front.effect,
            bytes: remaining.get(..length).ok_or(
                FrameContractViolation::WriteRangeUnavailable {
                    measured,
                    written: front.written,
                    observed: bytes.len(),
                },
            )?,
        }))
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
        let Some(remaining) = front.measure.wire_bytes().checked_sub(front.written) else {
            return Err(WriteProgressError::ProgressAccountingOverflow {
                measured: front.measure.wire_bytes(),
                written: front.written,
            });
        };
        if written > remaining {
            front.started |= written > 0;
            return Err(WriteProgressError::ExceedsRemaining { written, remaining });
        }
        let retained_after = if written == remaining {
            Some(
                retained_before
                    .checked_sub(front.measure.retained_bytes())
                    .ok_or(WriteProgressError::RetainedAccountingUnderflow {
                        retained: retained_before,
                        released: front.measure.retained_bytes(),
                    })?,
            )
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
        if front.written < front.measure.wire_bytes() {
            return Ok(WriteProgress::Pending {
                operation: front.operation,
                boundary,
            });
        }
        let Some(completed) = self.frames.pop_front() else {
            return Err(WriteProgressError::NoPendingWrite);
        };
        let Some(retained_after) = retained_after else {
            return Err(WriteProgressError::RetainedAccountingUnderflow {
                retained: self.retained_bytes,
                released: completed.measure.retained_bytes(),
            });
        };
        self.retained_bytes = retained_after;
        Ok(WriteProgress::Complete {
            operation: completed.operation,
            effect,
            frame: completed.frame,
            measure: completed.measure,
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
        let released = self.frames[index].measure.retained_bytes();
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
    pub(crate) fn queued_frames(&self) -> usize {
        self.frames.len()
    }

    /// Returns the write identity retained for an accepted operation.
    pub(crate) fn effect_for(&self, operation: OperationId) -> Option<EffectId> {
        let index = self.index_of_operation(operation)?;
        self.frames.get(index).map(|frame| frame.effect)
    }

    /// Returns variable memory retained by all queued frames.
    pub(crate) const fn retained_bytes(&self) -> RetainedBytes {
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

    pub(super) fn index_of_operation(&self, operation: OperationId) -> Option<usize> {
        self.frames
            .iter()
            .position(|frame| frame.operation == operation)
    }

    pub(super) fn index_of_effect(&self, effect: EffectId) -> Option<usize> {
        self.frames.iter().position(|frame| frame.effect == effect)
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
