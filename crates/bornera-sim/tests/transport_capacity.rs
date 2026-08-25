//! Fixed transport allocation and input-capacity simulation evidence.

use std::error::Error;

use bornera_core::{CompletionMode, RetainedBytes};
use bornera_sim::{
    OperationIndex, SimFrame, SlotAction, SlotActionFailure, SlotActionResult, SlotObservationKind,
    SlotReplayError, SlotSimulator,
};

#[path = "common/slot.rs"]
mod support;
use support::{config_with_transport, push, submit, trace};

#[test]
fn simulated_transport_refuses_input_beyond_preallocated_pressure() -> Result<(), Box<dyn Error>> {
    let simulator = SlotSimulator::new(config_with_transport(16)?);
    let mut trace = trace(8, 64)?;
    push(&mut trace, 0, SlotAction::ConnectReady)?;
    push(&mut trace, 1, SlotAction::OpenAdmission)?;
    submit(&mut trace, 2, CompletionMode::ReplyExpected, 50, &[1])?;
    push(
        &mut trace,
        3,
        SlotAction::Reply {
            operation: OperationIndex::new(0),
            payload: SimFrame::copy_from_slice(&[9])?,
        },
    )?;

    let report = simulator.replay(&trace)?;
    assert!(report.observations().iter().any(|observation| matches!(
        observation.kind,
        SlotObservationKind::Action {
            result: SlotActionResult::Rejected(SlotActionFailure::SimulatedInputCapacity),
            ..
        }
    )));
    assert!(report.final_snapshot().is_some_and(|snapshot| {
        snapshot
            .transport_pressure
            .is_some_and(|pressure| pressure.total() <= RetainedBytes::new(16))
    }));
    Ok(())
}

#[test]
fn transport_preallocation_failure_has_a_distinct_replay_error() -> Result<(), Box<dyn Error>> {
    let simulator = SlotSimulator::new(config_with_transport(u64::MAX)?);
    let empty = trace(1, 1)?;
    assert!(matches!(
        simulator.replay(&empty),
        Err(SlotReplayError::TransportConstruction)
    ));
    Ok(())
}
