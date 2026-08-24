//! One measured complete frame retained in exact wire order.

use crate::{Delivery, DiscardedWrite, EffectId, FrameMeasure, OperationId};

pub(super) struct QueuedWrite<F> {
    pub(super) operation: OperationId,
    pub(super) effect: EffectId,
    pub(super) frame: F,
    pub(super) measure: FrameMeasure,
    pub(super) written: usize,
    pub(super) started: bool,
}

impl<F> QueuedWrite<F> {
    pub(super) fn into_discarded(self) -> DiscardedWrite<F> {
        DiscardedWrite {
            operation: self.operation,
            effect: self.effect,
            frame: self.frame,
            measure: self.measure,
            written: self.written,
            delivery: if self.started {
                Delivery::PossiblySent
            } else {
                Delivery::NotSent
            },
        }
    }
}
