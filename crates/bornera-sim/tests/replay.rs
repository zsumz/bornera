//! Exact replay and bounded trace-admission tests.

mod common;

use std::{error::Error, num::NonZeroUsize};

use bornera_core::{CompletionMode, ConnectionPhase, RetainedBytes};
use bornera_sim::{
    EpochTarget, OperationIndex, SimFrame, Simulator, Trace, TraceAction, TraceAdmissionFailure,
    TraceLimits,
};

use common::{simulator_config, submit, trace};

#[test]
fn replay_is_exact_from_fresh_state() -> Result<(), Box<dyn Error>> {
    let mut trace = trace(16, 64)?;
    submit(&mut trace, CompletionMode::ReplyExpected, &[1, 2])?;
    submit(&mut trace, CompletionMode::WriteComplete, &[3])?;
    trace.try_push(TraceAction::AdvanceWrite { bytes: 2 })?;
    trace.try_push(TraceAction::Reply {
        operation: OperationIndex::new(0),
        frame: SimFrame::copy_from_slice(&[9, 8])?,
        epoch: EpochTarget::Current,
    })?;
    trace.try_push(TraceAction::AdvanceWrite { bytes: 1 })?;
    trace.try_push(TraceAction::BeginDrain {
        epoch: EpochTarget::Current,
    })?;
    trace.try_push(TraceAction::EpochClosed {
        epoch: EpochTarget::Current,
    })?;

    let simulator = Simulator::new(simulator_config()?);
    let first = simulator.replay(&trace);
    let second = simulator.replay(&trace);

    assert_eq!(first, second);
    assert_eq!(first.observations().len(), trace.actions().len());
    assert_eq!(first.final_snapshot().phase, ConnectionPhase::Closed);
    assert_eq!(first.final_snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn trace_rejection_preserves_the_exact_action() -> Result<(), Box<dyn Error>> {
    let mut count_bounded = Trace::new(TraceLimits::new(NonZeroUsize::MIN, RetainedBytes::new(8)));
    count_bounded.try_push(TraceAction::AdvanceWrite { bytes: 1 })?;
    let rejected = TraceAction::AdvanceWrite { bytes: 2 };
    let error = count_bounded
        .try_push(rejected.clone())
        .err()
        .ok_or_else(|| std::io::Error::other("count-bounded action was accepted"))?;
    assert_eq!(error.failure(), TraceAdmissionFailure::ActionCapacity);
    assert_eq!(error.into_action(), rejected);

    let mut byte_bounded = Trace::new(TraceLimits::new(NonZeroUsize::MIN, RetainedBytes::new(1)));
    let rejected = TraceAction::Reply {
        operation: OperationIndex::new(0),
        frame: SimFrame::copy_from_slice(&[1, 2])?,
        epoch: EpochTarget::Current,
    };
    let error = byte_bounded
        .try_push(rejected.clone())
        .err()
        .ok_or_else(|| std::io::Error::other("byte-bounded action was accepted"))?;
    assert_eq!(error.failure(), TraceAdmissionFailure::ByteCapacity);
    assert_eq!(error.into_action(), rejected);
    assert_eq!(byte_bounded.retained_bytes(), RetainedBytes::ZERO);
    Ok(())
}
