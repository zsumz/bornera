//! Focused evidence for cancellation, stale isolation, draining, and closure.

use std::error::Error;

use bornera_core::{
    AdmissionGate, CancelOutcome, CloseReason, CommitErrorKind, ConnectionCore, ConnectionEffect,
    ConnectionEpoch, ConnectionId, ConnectionInput, ConnectionLimits, ConnectionPhase, Deadline,
    Delivery, EffectId, EndpointId, FrameCommitFailure, InputDisposition, LaneId, MatchKeySpace,
    Moment, OperationId, OperationOptions, OperationOutcome, ReserveError, RetainedBytes,
    WriteProgressError,
};

mod support;

use support::TestFrame;

fn machine() -> Result<ConnectionCore<TestFrame>, Box<dyn Error>> {
    let limits = ConnectionLimits::new(
        4,
        RetainedBytes::new(40),
        4,
        RetainedBytes::new(40),
        MatchKeySpace::new(0, 3)?,
    )?;
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        limits,
    ))
}

fn frame(length: usize) -> TestFrame {
    TestFrame(std::iter::repeat_n(0, length).collect())
}

fn front_identity(
    machine: &ConnectionCore<TestFrame>,
) -> Result<(OperationId, EffectId), std::io::Error> {
    machine
        .front_write(std::num::NonZeroUsize::MAX)
        .map(|front| (front.operation, front.effect))
        .ok_or_else(|| std::io::Error::other("aggregate retained no write front"))
}

fn commit_one(
    machine: &mut ConnectionCore<TestFrame>,
    deadline: u64,
) -> Result<(OperationId, EffectId), Box<dyn Error>> {
    let permit = machine.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(deadline)))
            .session()
            .retained_bytes(RetainedBytes::new(2))
            .write_bytes(RetainedBytes::new(3)),
    )?;
    let (operation, _) = machine.commit(permit, frame(3))?;
    let (_, effect) = front_identity(machine)?;
    Ok((operation, effect))
}

#[test]
fn stale_epoch_and_effect_inputs_cannot_mutate_current_state() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, effect) = commit_one(&mut machine, 10)?;
    let before = machine.snapshot();

    let stale_epoch = machine.advance_write(ConnectionEpoch::new(3), effect, 1);
    assert!(matches!(
        stale_epoch,
        Err(bornera_core::ConnectionCoreError::Write(
            WriteProgressError::StaleEpoch { .. }
        ))
    ));
    assert_eq!(machine.snapshot(), before);

    let stale_effect = machine.advance_write(machine.epoch(), EffectId::new(effect.get() + 1), 1);
    assert!(matches!(
        stale_effect,
        Err(bornera_core::ConnectionCoreError::Write(
            WriteProgressError::OutOfOrderEffect { .. }
        ))
    ));
    assert_eq!(machine.snapshot(), before);
    assert_eq!(operation, front_identity(&machine)?.0);
    Ok(())
}

#[test]
fn not_sent_cancellation_releases_capacity_and_publishes_once() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, _) = commit_one(&mut machine, 10)?;
    let cancelled = machine.apply(ConnectionInput::Cancel {
        epoch: machine.epoch(),
        operation,
    })?;
    assert_eq!(
        cancelled.cancel_outcome(),
        Some(CancelOutcome::CancelledNotSent)
    );
    assert_eq!(cancelled.effects().len(), 2);
    assert!(cancelled.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            outcome: OperationOutcome::Cancelled {
                delivery: Delivery::NotSent
            },
            ..
        }
    )));
    assert_eq!(machine.snapshot().owned_operations, 0);

    let repeated = machine.apply(ConnectionInput::Cancel {
        epoch: machine.epoch(),
        operation,
    })?;
    assert_eq!(
        repeated.cancel_outcome(),
        Some(CancelOutcome::AlreadyTerminal)
    );
    assert_eq!(repeated.effects().len(), 0);
    Ok(())
}

#[test]
fn possibly_sent_cancellation_stops_observation_without_claiming_remote_cancel()
-> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, effect) = commit_one(&mut machine, 10)?;
    let started = machine.advance_write(machine.epoch(), effect, 1)?;
    assert_eq!(started.disposition(), InputDisposition::Applied);

    let cancelled = machine.apply(ConnectionInput::Cancel {
        epoch: machine.epoch(),
        operation,
    })?;
    assert_eq!(
        cancelled.cancel_outcome(),
        Some(CancelOutcome::ObservationCancelled {
            delivery: Delivery::PossiblySent
        })
    );
    assert_eq!(cancelled.effects().len(), 1);
    assert_eq!(machine.snapshot().owned_operations, 1);
    assert_eq!(machine.snapshot().active_operations, 0);
    assert_eq!(machine.snapshot().terminal_slots, 1);
    assert_eq!(machine.snapshot().active_match_keys, 1);

    let completed = machine.advance_write(machine.epoch(), effect, 2)?;
    assert_eq!(completed.disposition(), InputDisposition::Applied);
    assert_eq!(machine.snapshot().buffered_write_bytes, RetainedBytes::ZERO);

    let repeated = machine.apply(ConnectionInput::Cancel {
        epoch: machine.epoch(),
        operation,
    })?;
    assert_eq!(
        repeated.cancel_outcome(),
        Some(CancelOutcome::AlreadyTerminal)
    );
    assert_eq!(repeated.effects().len(), 0);
    Ok(())
}

#[test]
fn drain_closes_admission_before_finishing_accepted_work() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, _) = commit_one(&mut machine, 10)?;
    let drain = machine.apply(ConnectionInput::BeginDrain {
        epoch: machine.epoch(),
    })?;
    assert_eq!(drain.disposition(), InputDisposition::Applied);
    assert_eq!(drain.effects().len(), 0);
    assert_eq!(machine.snapshot().gate, AdmissionGate::Draining);
    assert_eq!(
        machine
            .reserve(
                Moment::ORIGIN,
                OperationOptions::until(Deadline::at(Moment::from_nanos(10)))
                    .session()
                    .write_bytes(RetainedBytes::new(1)),
            )
            .err(),
        Some(ReserveError::AdmissionClosed)
    );

    let cancelled = machine.apply(ConnectionInput::Cancel {
        epoch: machine.epoch(),
        operation,
    })?;
    assert!(matches!(
        cancelled.effects().last(),
        Some(ConnectionEffect::CloseEpoch {
            reason: CloseReason::Drained,
            ..
        })
    ));
    assert_eq!(machine.snapshot().gate, AdmissionGate::Closed);
    assert_eq!(machine.snapshot().phase, ConnectionPhase::Closing);
    Ok(())
}

#[test]
fn closure_emits_one_terminal_outcome_per_unfinished_operation() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (first, first_effect) = commit_one(&mut machine, 10)?;
    let (second, _) = commit_one(&mut machine, 11)?;
    machine.advance_write(machine.epoch(), first_effect, 1)?;

    let closed = machine.apply(ConnectionInput::CloseRequested {
        epoch: machine.epoch(),
        reason: CloseReason::TransportLost,
    })?;
    let publications: Vec<_> = closed
        .effects()
        .iter()
        .filter_map(|effect| match effect {
            ConnectionEffect::PublishOutcome {
                operation, outcome, ..
            } => Some((*operation, outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(publications.len(), 2);
    assert!(publications.iter().any(|(operation, outcome)| {
        *operation == first
            && matches!(
                outcome,
                OperationOutcome::Failed {
                    delivery: Delivery::PossiblySent,
                    ..
                }
            )
    }));
    assert!(publications.iter().any(|(operation, outcome)| {
        *operation == second
            && matches!(
                outcome,
                OperationOutcome::Failed {
                    delivery: Delivery::NotSent,
                    ..
                }
            )
    }));
    assert_eq!(machine.snapshot().owned_operations, 0);

    let repeated = machine.apply(ConnectionInput::CloseRequested {
        epoch: machine.epoch(),
        reason: CloseReason::TransportLost,
    })?;
    assert_eq!(repeated.effects().len(), 0);
    Ok(())
}

#[test]
fn an_outstanding_permit_cannot_commit_after_drain_begins() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let permit = machine.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(10)))
            .session()
            .write_bytes(RetainedBytes::new(3)),
    )?;
    let drain = machine.apply(ConnectionInput::BeginDrain {
        epoch: machine.epoch(),
    })?;
    assert!(matches!(
        drain.effects(),
        [ConnectionEffect::CloseEpoch {
            reason: CloseReason::Drained,
            ..
        }]
    ));
    let error = machine.commit(permit, frame(3));
    let Err(error) = error else {
        return Err(std::io::Error::other("permit committed after drain").into());
    };
    assert_eq!(
        error.failure(),
        FrameCommitFailure::Policy(CommitErrorKind::AdmissionClosed)
    );
    drop(error);
    assert_eq!(machine.snapshot().owned_operations, 0);
    Ok(())
}
