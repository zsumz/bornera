//! Atomic complete-frame admission into the bounded writer.

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, EffectId, OperationId};

use super::{
    WriteAccepted, WriteAdmissionError, WriteAdmissionFailure, WriteFrame, WriteIdentityKind,
    WriteQueue, queue::QueuedWrite,
};

impl<F: WriteFrame> WriteQueue<F> {
    /// Accepts one complete frame or returns that exact frame as `NotSent`.
    pub(crate) fn admit(
        &mut self,
        epoch: ConnectionEpoch,
        operation: OperationId,
        effect: EffectId,
        frame: F,
    ) -> Result<WriteAccepted, WriteAdmissionError<F>> {
        let incoming = frame.retained_bytes();
        if let Some(failure) = self.admission_failure(epoch, operation, effect, incoming) {
            return Err(WriteAdmissionError::new(failure, frame));
        }
        let Some(retained_bytes) = self.retained_bytes.checked_add(incoming) else {
            return Err(WriteAdmissionError::new(
                WriteAdmissionFailure::RetainedByteCapacity {
                    retained: self.retained_bytes,
                    incoming,
                    limit: self.limits.max_retained_bytes(),
                },
                frame,
            ));
        };
        let accepted = WriteAccepted {
            epoch,
            operation,
            effect,
            frame_bytes: frame.bytes().len(),
            retained_bytes: incoming,
        };
        self.frames.push_back(QueuedWrite {
            operation,
            effect,
            frame,
            retained_bytes: incoming,
            written: 0,
            started: false,
        });
        self.retained_bytes = retained_bytes;
        Ok(accepted)
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
        if self.frames.iter().any(|frame| frame.operation == operation) {
            return Some(WriteAdmissionFailure::IdentityInUse(
                WriteIdentityKind::Operation,
            ));
        }
        if self.frames.iter().any(|frame| frame.effect == effect) {
            return Some(WriteAdmissionFailure::IdentityInUse(
                WriteIdentityKind::Effect,
            ));
        }
        if self.frames.len() == self.limits.max_frames() {
            return Some(WriteAdmissionFailure::FrameCapacityReached {
                limit: self.limits.max_frames(),
            });
        }
        let accepted = self.retained_bytes.checked_add(incoming);
        if accepted.is_none_or(|bytes| bytes > self.limits.max_retained_bytes()) {
            return Some(WriteAdmissionFailure::RetainedByteCapacity {
                retained: self.retained_bytes,
                incoming,
                limit: self.limits.max_retained_bytes(),
            });
        }
        None
    }
}
