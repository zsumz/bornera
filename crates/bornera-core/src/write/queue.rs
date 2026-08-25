//! Count- and retained-byte-bounded FIFO storage with exact write advancement.

use std::{collections::VecDeque, num::NonZeroUsize};

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, Delivery, EffectId, OperationId};

use super::{
    DiscardedWrite, DiscardedWrites, FrameContractViolation, WriteBoundary, WriteFrame,
    WriteProgress, WriteProgressError, WriteQueueLimits, WriteSlice, accounting::RetainedBudget,
    queued::QueuedWrite,
};

/// Single-epoch owner of complete outbound frames in wire order.
pub(crate) struct WriteQueue<F> {
    pub(super) epoch: ConnectionEpoch,
    pub(super) limits: WriteQueueLimits,
    pub(super) frames: VecDeque<QueuedWrite<F>>,
    pub(super) retained_budget: RetainedBudget,
}

impl<F: WriteFrame> WriteQueue<F> {
    /// Creates an empty writer for one exact connection epoch.
    pub(crate) fn new(epoch: ConnectionEpoch, limits: WriteQueueLimits) -> Self {
        Self {
            epoch,
            limits,
            frames: VecDeque::with_capacity(limits.max_frames()),
            retained_budget: RetainedBudget::new(limits.max_retained_bytes()),
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
        let retained_before = self.retained_bytes();
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
        let completed_release = if written == remaining {
            let released = front.measure.retained_bytes();
            if released > self.retained_budget.used() {
                return Err(WriteProgressError::RetainedAccountingUnderflow {
                    retained: retained_before,
                    released,
                });
            }
            Some(released)
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
        let Some(released) = completed_release else {
            return Err(WriteProgressError::RetainedAccountingUnderflow {
                retained: retained_before,
                released: front.measure.retained_bytes(),
            });
        };
        self.release_retained(released)?;
        let Some(completed) = self.frames.pop_front() else {
            return Err(WriteProgressError::NoPendingWrite);
        };
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
        if released > self.retained_budget.used() {
            return Err(WriteProgressError::RetainedAccountingUnderflow {
                retained: self.retained_bytes(),
                released,
            });
        }
        self.release_retained(released)?;
        let Some(discarded) = self.frames.remove(index) else {
            return Ok(None);
        };
        Ok(Some(discarded.into_discarded()))
    }

    /// Removes every retained frame in original wire order.
    pub(crate) fn discard_all(&mut self) -> DiscardedWrites<F> {
        let writes = self
            .frames
            .drain(..)
            .map(QueuedWrite::into_discarded)
            .collect();
        let retained_bytes = self.retained_budget.clear();
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
        self.retained_budget.used()
    }

    fn release_retained(&mut self, released: RetainedBytes) -> Result<(), WriteProgressError> {
        let retained = self.retained_bytes();
        if self.retained_budget.release(released) {
            Ok(())
        } else {
            Err(WriteProgressError::RetainedAccountingUnderflow { retained, released })
        }
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
