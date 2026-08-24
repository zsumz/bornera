//! Pure replay interpreter over the deterministic connection aggregate.

use core::num::NonZeroUsize;

use bornera_core::{
    ConnectionCore, ConnectionEpoch, ConnectionInput, InboundReply, MatchKey, OperationId,
};

use crate::{
    EpochTarget, OperationIndex, ReplayReport, SimFrame, SimulationConfig, StepObservation,
    StepResult, Trace, TraceAction, report,
};

/// Stateless deterministic trace replay factory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Simulator {
    config: SimulationConfig,
}

impl Simulator {
    /// Creates a replay factory for one exact fixed-epoch configuration.
    pub const fn new(config: SimulationConfig) -> Self {
        Self { config }
    }

    /// Replays every action from fresh state and returns exact observations.
    pub fn replay(&self, trace: &Trace) -> ReplayReport {
        let mut core = ConnectionCore::new(
            self.config.endpoint(),
            self.config.lane(),
            self.config.connection(),
            self.config.epoch(),
            self.config.limits(),
        );
        let mut accepted = Vec::with_capacity(trace.actions().len());
        let mut observations = Vec::with_capacity(trace.actions().len());
        for (action, command) in trace.actions().iter().enumerate() {
            let result = apply(&mut core, &mut accepted, command);
            observations.push(StepObservation {
                action,
                result,
                snapshot: core.snapshot(),
            });
        }
        ReplayReport::new(observations, core.snapshot())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Accepted {
    operation: OperationId,
    key: MatchKey,
}

fn apply(
    core: &mut ConnectionCore<SimFrame>,
    accepted: &mut Vec<Accepted>,
    action: &TraceAction,
) -> StepResult {
    match action {
        TraceAction::Submit {
            now,
            options,
            frame,
        } => submit(core, accepted, *now, *options, frame.clone()),
        TraceAction::AdvanceWrite { bytes } => advance_write(core, *bytes),
        TraceAction::Reply {
            operation,
            frame,
            epoch,
        } => accepted_operation(accepted, *operation).map_or(
            StepResult::UnknownOperation(*operation),
            |accepted| {
                report::reply_result(core.apply_reply(InboundReply::new(
                    select_epoch(core.epoch(), *epoch),
                    accepted.key,
                    frame.clone(),
                )))
            },
        ),
        TraceAction::Cancel { operation, epoch } => {
            with_operation(core, accepted, *operation, *epoch, |epoch, operation| {
                ConnectionInput::Cancel { epoch, operation }
            })
        }
        TraceAction::Deadline {
            operation,
            now,
            epoch,
        } => with_operation(core, accepted, *operation, *epoch, |epoch, operation| {
            ConnectionInput::DeadlineElapsed {
                epoch,
                operation,
                now: *now,
            }
        }),
        TraceAction::OpenAdmission { epoch } => {
            report::unit_result(core.apply(ConnectionInput::OpenAdmission {
                epoch: select_epoch(core.epoch(), *epoch),
            }))
        }
        TraceAction::BeginDrain { epoch } => {
            report::unit_result(core.apply(ConnectionInput::BeginDrain {
                epoch: select_epoch(core.epoch(), *epoch),
            }))
        }
        TraceAction::Close { epoch, reason } => {
            report::unit_result(core.apply(ConnectionInput::CloseRequested {
                epoch: select_epoch(core.epoch(), *epoch),
                reason: *reason,
            }))
        }
        TraceAction::EpochClosed { epoch } => {
            report::unit_result(core.apply(ConnectionInput::EpochClosed {
                epoch: select_epoch(core.epoch(), *epoch),
            }))
        }
        TraceAction::Recover => StepResult::Recovered(core.recover()),
    }
}

fn submit(
    core: &mut ConnectionCore<SimFrame>,
    accepted: &mut Vec<Accepted>,
    now: bornera_core::Moment,
    options: bornera_core::OperationOptions,
    frame: SimFrame,
) -> StepResult {
    let permit = match core.reserve(now, options) {
        Ok(permit) => permit,
        Err(error) => return StepResult::ReserveRejected(error),
    };
    let key = permit.match_key();
    match core.commit(permit, frame) {
        Ok((operation, transition)) => {
            let index = OperationIndex::new(accepted.len());
            accepted.push(Accepted { operation, key });
            StepResult::Submitted {
                index,
                operation,
                match_key: key,
                transition,
            }
        }
        Err(error) => {
            let failure = error.failure();
            let (permit, _frame) = error.into_parts();
            drop(permit);
            StepResult::CommitRejected(failure)
        }
    }
}

fn advance_write(core: &mut ConnectionCore<SimFrame>, bytes: usize) -> StepResult {
    let front = match core.front_write(NonZeroUsize::MAX) {
        Ok(Some(front)) => (front.operation, front.effect),
        Ok(None) => return StepResult::NoPendingWrite,
        Err(error) => return StepResult::CoreFailed(error),
    };
    match core.advance_write(core.epoch(), front.1, bytes) {
        Ok(transition) => StepResult::WriteTransition {
            operation: front.0,
            bytes,
            transition,
        },
        Err(error) => StepResult::CoreFailed(error),
    }
}

fn with_operation(
    core: &mut ConnectionCore<SimFrame>,
    accepted: &[Accepted],
    index: OperationIndex,
    target: EpochTarget,
    input: impl FnOnce(ConnectionEpoch, OperationId) -> ConnectionInput,
) -> StepResult {
    accepted_operation(accepted, index).map_or(StepResult::UnknownOperation(index), |accepted| {
        report::unit_result(core.apply(input(
            select_epoch(core.epoch(), target),
            accepted.operation,
        )))
    })
}

fn accepted_operation(accepted: &[Accepted], index: OperationIndex) -> Option<Accepted> {
    accepted.get(index.get()).copied()
}

const fn select_epoch(epoch: ConnectionEpoch, target: EpochTarget) -> ConnectionEpoch {
    match target {
        EpochTarget::Current => epoch,
        EpochTarget::Stale => ConnectionEpoch::new(epoch.get().wrapping_add(1)),
    }
}
