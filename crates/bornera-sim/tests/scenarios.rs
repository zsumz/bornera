//! Targeted epoch-fencing, draining, and recovery simulation scenarios.

mod common;

use std::error::Error;

use bornera_core::{
    CloseReason, CompletionMode, ConnectionPhase, Delivery, InputDisposition, Moment,
};
use bornera_sim::{EpochTarget, OperationIndex, SimFrame, Simulator, StepResult, TraceAction};

use common::{simulator_config, submit, trace};

#[test]
fn stale_epoch_actions_are_observed_without_mutating_the_owner() -> Result<(), Box<dyn Error>> {
    let mut trace = trace(16, 64)?;
    submit(&mut trace, CompletionMode::ReplyExpected, &[1])?;
    trace.try_push(TraceAction::Cancel {
        operation: OperationIndex::new(0),
        epoch: EpochTarget::Stale,
    })?;
    trace.try_push(TraceAction::Deadline {
        operation: OperationIndex::new(0),
        now: Moment::from_nanos(1_000),
        epoch: EpochTarget::Stale,
    })?;
    trace.try_push(TraceAction::Reply {
        operation: OperationIndex::new(0),
        frame: SimFrame::copy_from_slice(&[2])?,
        epoch: EpochTarget::Stale,
    })?;
    trace.try_push(TraceAction::OpenAdmission {
        epoch: EpochTarget::Stale,
    })?;
    trace.try_push(TraceAction::Close {
        epoch: EpochTarget::Stale,
        reason: CloseReason::Requested,
    })?;
    trace.try_push(TraceAction::EpochClosed {
        epoch: EpochTarget::Stale,
    })?;

    let report = Simulator::new(simulator_config()?).replay(&trace);
    let baseline = report.observations()[0].snapshot;
    for observation in &report.observations()[1..] {
        assert_eq!(observation.snapshot, baseline);
        assert_eq!(
            disposition(&observation.result),
            Some(InputDisposition::IgnoredStaleEpoch)
        );
    }
    Ok(())
}

#[test]
fn write_complete_operation_finishes_a_drain_without_a_reply() -> Result<(), Box<dyn Error>> {
    let mut trace = trace(8, 16)?;
    submit(&mut trace, CompletionMode::WriteComplete, &[1, 2])?;
    trace.try_push(TraceAction::BeginDrain {
        epoch: EpochTarget::Current,
    })?;
    trace.try_push(TraceAction::AdvanceWrite { bytes: 2 })?;
    trace.try_push(TraceAction::EpochClosed {
        epoch: EpochTarget::Current,
    })?;

    let report = Simulator::new(simulator_config()?).replay(&trace);
    assert_eq!(report.final_snapshot().phase, ConnectionPhase::Closed);
    assert_eq!(report.final_snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn recovery_returns_exact_nonterminal_wire_ownership() -> Result<(), Box<dyn Error>> {
    let mut trace = trace(12, 32)?;
    submit(&mut trace, CompletionMode::ReplyExpected, &[1, 2, 3])?;
    submit(&mut trace, CompletionMode::WriteComplete, &[4])?;
    trace.try_push(TraceAction::AdvanceWrite { bytes: 1 })?;
    trace.try_push(TraceAction::Cancel {
        operation: OperationIndex::new(1),
        epoch: EpochTarget::Current,
    })?;
    trace.try_push(TraceAction::Recover)?;

    let report = Simulator::new(simulator_config()?).replay(&trace);
    let StepResult::Recovered(recovery) = &report.observations()[4].result else {
        return Err(std::io::Error::other("expected recovery observation").into());
    };
    assert!(!recovery.ownership_diverged);
    assert!(recovery.unmatched_writes.is_empty());
    assert_eq!(recovery.operations.len(), 1);
    assert_eq!(recovery.operations[0].delivery, Delivery::PossiblySent);
    assert_eq!(
        recovery.operations[0]
            .frame
            .as_ref()
            .map(SimFrame::as_bytes),
        Some(&[1, 2, 3][..])
    );
    assert_eq!(report.final_snapshot().owned_operations, 0);
    Ok(())
}

fn disposition(result: &StepResult) -> Option<InputDisposition> {
    match result {
        StepResult::UnitTransition(transition)
        | StepResult::Submitted { transition, .. }
        | StepResult::WriteTransition { transition, .. } => Some(transition.disposition()),
        StepResult::ReplyTransition(transition) => Some(transition.disposition()),
        _ => None,
    }
}
