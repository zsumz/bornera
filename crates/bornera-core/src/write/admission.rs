//! Atomic complete-frame admission into the bounded writer.

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, EffectId, OperationId};

use super::{
    FrameMeasure, WriteAdmissionError, WriteAdmissionFailure, WriteFrame, WriteIdentityKind,
    WriteQueue, queued::QueuedWrite,
};

impl<F: WriteFrame> WriteQueue<F> {
    /// Accepts one complete frame or returns that exact frame as `NotSent`.
    pub(crate) fn admit(
        &mut self,
        epoch: ConnectionEpoch,
        operation: OperationId,
        effect: EffectId,
        measure: FrameMeasure,
        frame: F,
    ) -> Result<(), WriteAdmissionError<F>> {
        let incoming = measure.retained_bytes();
        if let Some(failure) = self.admission_failure(epoch, operation, effect, incoming) {
            return Err(WriteAdmissionError::new(failure, frame));
        }
        if !self.retained_budget.try_reserve(incoming) {
            return Err(WriteAdmissionError::new(
                WriteAdmissionFailure::RetainedByteCapacity {
                    retained: self.retained_bytes(),
                    incoming,
                    limit: self.limits.max_retained_bytes(),
                },
                frame,
            ));
        }
        self.frames.push_back(QueuedWrite {
            operation,
            effect,
            frame,
            measure,
            written: 0,
            started: false,
        });
        Ok(())
    }

    fn admission_failure(
        &self,
        epoch: ConnectionEpoch,
        operation: OperationId,
        effect: EffectId,
        incoming: RetainedBytes,
    ) -> Option<WriteAdmissionFailure> {
        if epoch != self.epoch {
            return Some(WriteAdmissionFailure::StaleEpoch {
                expected: self.epoch,
                received: epoch,
            });
        }
        if self.index_of_operation(operation).is_some() {
            return Some(WriteAdmissionFailure::IdentityInUse(
                WriteIdentityKind::Operation,
            ));
        }
        if self.index_of_effect(effect).is_some() {
            return Some(WriteAdmissionFailure::IdentityInUse(
                WriteIdentityKind::Effect,
            ));
        }
        if self.frames.len() == self.limits.max_frames() {
            return Some(WriteAdmissionFailure::FrameCapacityReached {
                limit: self.limits.max_frames(),
            });
        }
        if !self.retained_budget.can_reserve(incoming) {
            return Some(WriteAdmissionFailure::RetainedByteCapacity {
                retained: self.retained_bytes(),
                incoming,
                limit: self.limits.max_retained_bytes(),
            });
        }
        None
    }
}
