//! Recovery of one policy record and its exact retained frame.

use crate::{Delivery, DiscardedWrite, EffectId, OperationId, OperationPhase, RecoveredOperation};

use super::journal::JournalOperation;

pub(super) fn recover_journal_operation<F>(
    record: JournalOperation,
    writes: &mut Vec<DiscardedWrite<F>>,
    operations: &mut Vec<RecoveredOperation<F>>,
    ownership_diverged: &mut bool,
) {
    if record.phase == OperationPhase::Terminal {
        return;
    }
    let write = take_write(writes, record.operation, record.effect);
    if record.write_held != write.is_some() {
        *ownership_diverged = true;
    }
    let (delivery, frame) = write.map_or((record.delivery, None), |write| {
        (weaken(record.delivery, write.delivery), Some(write.frame))
    });
    operations.push(RecoveredOperation {
        operation: record.operation,
        delivery,
        frame,
    });
}

pub(super) fn take_write<F>(
    writes: &mut Vec<DiscardedWrite<F>>,
    operation: OperationId,
    effect: EffectId,
) -> Option<DiscardedWrite<F>> {
    let index = writes
        .iter()
        .position(|write| write.operation == operation && write.effect == effect)?;
    Some(writes.remove(index))
}

pub(super) const fn weaken(policy: Delivery, writer: Delivery) -> Delivery {
    if matches!(policy, Delivery::PossiblySent) || matches!(writer, Delivery::PossiblySent) {
        Delivery::PossiblySent
    } else {
        Delivery::NotSent
    }
}
