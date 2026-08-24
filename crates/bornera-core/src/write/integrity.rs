//! Internal writer ownership and retained-accounting audits.

use calandria::RetainedBytes;

use crate::{EffectId, OperationId, WriteFrame};

use super::WriteQueue;

impl<F: WriteFrame> WriteQueue<F> {
    pub(crate) fn identities(&self) -> impl Iterator<Item = (OperationId, EffectId)> + '_ {
        self.frames
            .iter()
            .map(|frame| (frame.operation, frame.effect))
    }

    pub(crate) fn accounting_is_valid(&self) -> bool {
        let retained = self
            .frames
            .iter()
            .try_fold(RetainedBytes::ZERO, |retained, frame| {
                retained.checked_add(frame.measure.retained_bytes())
            });
        retained == Some(self.retained_bytes)
    }
}
