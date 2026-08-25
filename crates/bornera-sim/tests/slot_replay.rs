//! Production-slot replay over Calandria virtual time and simulated transport.

use std::error::Error;

use bornera::{ConnectionEvent, OwnerFailure, TcpSocketPolicy, TransportState};
use bornera_core::{
    CloseReason, CompletionMode, Delivery, Moment, OperationFailure, OperationOutcome,
};
use bornera_sim::{
    OperationIndex, SimFrame, SlotAction, SlotActionResult, SlotObservationKind, SlotSimulator,
};

#[path = "common/slot.rs"]
mod support;
use support::{config_with_transport, push, submit, trace};

#[test]
fn exact_replay_exercises_production_decode_deadline_and_publication_owners()
-> Result<(), Box<dyn Error>> {
    let simulator = SlotSimulator::new(config_with_transport(512)?);
    let mut trace = trace(16, 128)?;
    push(&mut trace, 0, SlotAction::ConnectReady)?;
    push(&mut trace, 1, SlotAction::OpenAdmission)?;
    submit(
        &mut trace,
        2,
        CompletionMode::ReplyExpected,
        50,
        &[1, 2, 3, 4],
    )?;
    push(&mut trace, 3, SlotAction::WriteReady { bytes: 4 })?;
    push(
        &mut trace,
        4,
        SlotAction::Reply {
            operation: OperationIndex::new(0),
            payload: SimFrame::copy_from_slice(&[9, 8])?,
        },
    )?;
    submit(&mut trace, 5, CompletionMode::WriteComplete, 50, &[5, 6])?;
    push(&mut trace, 6, SlotAction::WriteReady { bytes: 2 })?;
    push(&mut trace, 7, SlotAction::BeginDrain)?;
    push(&mut trace, 8, SlotAction::SettleTransport)?;

    let first = simulator.replay(&trace)?;
    let second = simulator.replay(&trace)?;
    assert_eq!(first, second);
    assert_eq!(first.outbound_bytes(), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(first.applied_policy(), Some(TcpSocketPolicy::DEFAULT));
    assert_eq!(
        first.final_snapshot().map(|snapshot| snapshot.transport),
        Some(TransportState::Closed)
    );

    let outcomes: Vec<_> = first
        .observations()
        .iter()
        .flat_map(|observation| observation.outcomes.iter())
        .map(bornera::EngineOutcome::outcome)
        .collect();
    assert_eq!(outcomes.len(), 2);
    assert!(matches!(
        outcomes.first(),
        Some(OperationOutcome::Reply(reply)) if reply.payload() == [9, 8]
    ));
    assert!(matches!(
        outcomes.get(1),
        Some(OperationOutcome::WriteComplete { .. })
    ));
    let events: Vec<_> = first
        .observations()
        .iter()
        .flat_map(|observation| observation.events.iter())
        .collect();
    assert!(matches!(
        events.first(),
        Some(ConnectionEvent::TransportOpened { .. })
    ));
    assert!(matches!(
        events.last(),
        Some(ConnectionEvent::Closed { .. })
    ));
    Ok(())
}
#[test]
fn virtual_deadline_precedes_a_later_cancellation() -> Result<(), Box<dyn Error>> {
    let simulator = SlotSimulator::new(config_with_transport(512)?);
    let mut trace = trace(8, 64)?;
    push(&mut trace, 0, SlotAction::ConnectReady)?;
    push(&mut trace, 1, SlotAction::OpenAdmission)?;
    submit(&mut trace, 2, CompletionMode::ReplyExpected, 5, &[1, 2])?;
    push(&mut trace, 3, SlotAction::WriteReady { bytes: 2 })?;
    push(
        &mut trace,
        10,
        SlotAction::Cancel {
            operation: OperationIndex::new(0),
        },
    )?;

    let report = simulator.replay(&trace)?;
    let deadline = report
        .observations()
        .iter()
        .position(|observation| matches!(observation.kind, SlotObservationKind::Deadline))
        .ok_or_else(|| std::io::Error::other("virtual deadline was not scheduled"))?;
    let cancellation = report
        .observations()
        .iter()
        .position(|observation| {
            matches!(
                observation.kind,
                SlotObservationKind::Action {
                    result: SlotActionResult::Cancelled(_),
                    ..
                }
            )
        })
        .ok_or_else(|| std::io::Error::other("later cancellation was not replayed"))?;
    assert!(deadline < cancellation);
    assert_eq!(report.observations()[deadline].at, Moment::from_nanos(5));
    assert!(
        report.observations()[deadline]
            .outcomes
            .iter()
            .any(|outcome| matches!(
                outcome.outcome(),
                OperationOutcome::Failed {
                    failure: OperationFailure::ConnectionClosed(
                        CloseReason::DeadlineAfterPossibleSend
                    ),
                    delivery: Delivery::PossiblySent,
                }
            ))
    );
    Ok(())
}
#[test]
fn consuming_slot_recovery_is_exactly_replayable() -> Result<(), Box<dyn Error>> {
    let simulator = SlotSimulator::new(config_with_transport(512)?);
    let mut trace = trace(8, 64)?;
    push(&mut trace, 0, SlotAction::ConnectReady)?;
    push(&mut trace, 1, SlotAction::OpenAdmission)?;
    submit(
        &mut trace,
        2,
        CompletionMode::ReplyExpected,
        50,
        &[4, 3, 2, 1],
    )?;
    push(
        &mut trace,
        3,
        SlotAction::Recover {
            reason: OwnerFailure::OwnerInvariant,
        },
    )?;

    let report = simulator.replay(&trace)?;
    assert_eq!(report.final_snapshot(), None);
    let recovered = report.observations().iter().find_map(|observation| {
        let SlotObservationKind::Action {
            result: SlotActionResult::Recovered,
            ..
        } = &observation.kind
        else {
            return None;
        };
        observation.recovery.as_ref()
    });
    let recovered = recovered.ok_or_else(|| std::io::Error::other("recovery was not observed"))?;
    assert_eq!(recovered.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(recovered.operations.len(), 1);
    assert_eq!(
        recovered.operations[0]
            .frame
            .as_ref()
            .map(bornera::OutboundFrame::as_bytes),
        Some(&[4, 3, 2, 1][..])
    );
    Ok(())
}
