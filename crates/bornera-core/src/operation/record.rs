//! Internal bounded operation state for one epoch.

use calandria::Deadline;

use crate::{CompletionMode, Delivery, EffectId, OperationId, OperationPhase};

use crate::admission::Reservation;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OperationRecord {
    pub(crate) id: OperationId,
    pub(crate) effect: EffectId,
    pub(crate) deadline: Deadline,
    pub(crate) reservation: Reservation,
    pub(crate) phase: OperationPhase,
    pub(crate) completion: CompletionMode,
    pub(crate) delivery: Delivery,
    pub(crate) write_held: bool,
}
