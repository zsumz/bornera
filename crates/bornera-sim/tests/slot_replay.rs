//! Production-slot replay over Calandria virtual time and simulated transport.

use std::{error::Error, num::NonZeroUsize};

use bornera::{
    ConnectionEvent, ConnectionIdentity, ConnectionSlotConfig, ConnectionSlotLimits, DecoderLimits,
    IoLimits, OwnerFailure, PublicationLimits, TcpSocketPolicy, TransportState,
};
use bornera_core::{
    CloseReason, CompletionMode, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline,
    Delivery, EndpointId, LaneId, MatchKeySpace, Moment, OperationFailure, OperationOptions,
    OperationOutcome, RetainedBytes,
};
use bornera_sim::{
    OperationIndex, SimFrame, SlotAction, SlotActionResult, SlotObservationKind,
    SlotSimulationConfig, SlotSimulator, SlotTrace, SlotTraceLimits,
};
use calandria::TimerOwnerId;
use calandria_sim::TimelineId;

#[test]
fn exact_replay_exercises_production_decode_deadline_and_publication_owners()
-> Result<(), Box<dyn Error>> {
    let simulator = SlotSimulator::new(config()?);
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
    let simulator = SlotSimulator::new(config()?);
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
    let simulator = SlotSimulator::new(config()?);
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
            result: SlotActionResult::Recovered(report),
            ..
        } = &observation.kind
        else {
            return None;
        };
        Some(report)
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

fn config() -> Result<SlotSimulationConfig, Box<dyn Error>> {
    let core = ConnectionLimits::new(
        8,
        RetainedBytes::new(256),
        8,
        RetainedBytes::new(256),
        MatchKeySpace::new(10, 17)?,
    )?;
    let limits = ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(128), RetainedBytes::new(64)),
        IoLimits::new(nz(8)?, nz(128)?),
        PublicationLimits::new(nz(16)?),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    let slot = ConnectionSlotConfig::new(
        identity,
        Deadline::at(Moment::from_nanos(100)),
        TimerOwnerId::new(5),
    );
    Ok(SlotSimulationConfig::new(slot, limits, TimelineId::new(6)))
}

fn trace(actions: usize, bytes: u64) -> Result<SlotTrace, Box<dyn Error>> {
    Ok(SlotTrace::new(SlotTraceLimits::new(
        nz(actions)?,
        RetainedBytes::new(bytes),
    )))
}

fn submit(
    trace: &mut SlotTrace,
    at: u64,
    mode: CompletionMode,
    deadline: u64,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(deadline)))
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(u64::try_from(bytes.len())?))
        .completion_mode(mode);
    push(
        trace,
        at,
        SlotAction::Submit {
            options,
            frame: SimFrame::copy_from_slice(bytes)?,
        },
    )
}

fn push(trace: &mut SlotTrace, at: u64, action: SlotAction) -> Result<(), Box<dyn Error>> {
    trace.try_push(Moment::from_nanos(at), action)?;
    Ok(())
}

fn nz(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
