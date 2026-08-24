//! External action interpretation for production-slot simulation.

use bornera::{EngineCommitError, OutboundFrame};
use bornera_core::{Moment, OperationPermit};

use crate::{
    Accepted, OperationIndex, ReplayOwner, SlotAction, SlotActionFailure, SlotActionResult,
    encode_reply,
};

impl ReplayOwner {
    pub(crate) fn apply_action(&mut self, now: Moment, action: SlotAction) -> SlotActionResult {
        let Some(slot) = self.slot.as_mut() else {
            return SlotActionResult::Rejected(SlotActionFailure::SlotRecovered);
        };
        match action {
            SlotAction::ConnectReady => {
                self.transport.observe_connect_ready();
                SlotActionResult::ConnectReady
            }
            SlotAction::OpenAdmission => slot
                .open_admission()
                .map_or_else(|error| rejected_owner(&error), SlotActionResult::Admission),
            SlotAction::Submit { options, frame } => {
                let permit = match slot.reserve(now, options) {
                    Ok(permit) => permit,
                    Err(error) => {
                        return SlotActionResult::Rejected(SlotActionFailure::Reserve(error));
                    }
                };
                submit(slot, &mut self.accepted, permit, &frame)
            }
            SlotAction::WriteReady { bytes } => {
                self.transport.allow_write(bytes);
                SlotActionResult::WriteReady(bytes)
            }
            SlotAction::Reply { operation, payload } => {
                let Some(accepted) = self.accepted.get(operation.get()).copied() else {
                    return SlotActionResult::Rejected(SlotActionFailure::UnknownOperation(
                        operation,
                    ));
                };
                match encode_reply(accepted.key, &payload) {
                    Ok(encoded) => {
                        self.transport.inject_read(encoded);
                        SlotActionResult::ReplyInjected(operation)
                    }
                    Err(_) => SlotActionResult::Rejected(SlotActionFailure::ReplyEncoding),
                }
            }
            SlotAction::Cancel { operation } => {
                let Some(accepted) = self.accepted.get(operation.get()).copied() else {
                    return SlotActionResult::Rejected(SlotActionFailure::UnknownOperation(
                        operation,
                    ));
                };
                slot.cancel(accepted.operation)
                    .map_or_else(|error| rejected_owner(&error), SlotActionResult::Cancelled)
            }
            SlotAction::BeginDrain => slot
                .begin_drain()
                .map_or_else(|error| rejected_owner(&error), SlotActionResult::Drain),
            SlotAction::Close { reason } => slot
                .finalize(reason)
                .map_or_else(|error| rejected_owner(&error), SlotActionResult::Close),
            SlotAction::PeerClosed => {
                self.transport.observe_peer_closed();
                SlotActionResult::PeerClosed
            }
            SlotAction::SettleTransport => {
                let should_settle = {
                    let snapshot = slot.snapshot();
                    snapshot.owner_failure.is_some()
                        || snapshot.transport == bornera::TransportState::Closing
                };
                let settled = if should_settle {
                    self.transport.close();
                    slot.settle_transport_closed()
                } else {
                    false
                };
                SlotActionResult::TransportSettled(settled)
            }
            SlotAction::Drive => SlotActionResult::Driven,
            SlotAction::Recover { reason } => {
                let Some(slot) = self.slot.take() else {
                    return SlotActionResult::Rejected(SlotActionFailure::SlotRecovered);
                };
                self.transport.close();
                SlotActionResult::Recovered(slot.recover(reason))
            }
        }
    }
}

fn submit(
    slot: &mut crate::SimSlot,
    accepted: &mut Vec<Accepted>,
    permit: OperationPermit,
    frame: &crate::SimFrame,
) -> SlotActionResult {
    let operation = permit.operation_id();
    let key = permit.match_key();
    let Ok(outbound) = OutboundFrame::copy_from_slice(frame.as_bytes()) else {
        drop(permit);
        return SlotActionResult::Rejected(SlotActionFailure::FrameEncoding);
    };
    match slot.commit(permit, outbound) {
        Ok(committed) => {
            let index = OperationIndex::new(accepted.len());
            accepted.push(Accepted { operation, key });
            SlotActionResult::Submitted {
                index,
                operation: committed,
                match_key: key,
            }
        }
        Err(EngineCommitError::Rejected(error)) => {
            SlotActionResult::Rejected(SlotActionFailure::Commit(error.failure()))
        }
        Err(EngineCommitError::AcceptedOwnerFailure {
            operation: committed,
            source,
        }) => {
            let index = OperationIndex::new(accepted.len());
            accepted.push(Accepted {
                operation: committed,
                key,
            });
            SlotActionResult::SubmittedOwnerFailed {
                index,
                operation: committed,
                match_key: key,
                reason: bornera::OwnerFailure::from(&source),
            }
        }
        Err(EngineCommitError::OwnerFailed { reason, .. }) => {
            SlotActionResult::Rejected(SlotActionFailure::Owner(reason))
        }
        Err(_) => SlotActionResult::Rejected(SlotActionFailure::Owner(
            bornera::OwnerFailure::OwnerInvariant,
        )),
    }
}

fn rejected_owner(error: &bornera::EngineError) -> SlotActionResult {
    SlotActionResult::Rejected(SlotActionFailure::Owner(bornera::OwnerFailure::from(error)))
}
