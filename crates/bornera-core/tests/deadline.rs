//! Focused evidence for absolute deadlines and monotonic delivery certainty.

use std::error::Error;

use bornera_core::{
    CloseReason, ConnectionCore, ConnectionEffect, ConnectionEpoch, ConnectionId, ConnectionInput,
    ConnectionLimits, ConnectionPhase, Deadline, Delivery, EffectId, EndpointId, InputDisposition,
    LaneId, MatchKeySpace, Moment, OperationFailure, OperationId, OperationOptions,
    OperationOutcome, RetainedBytes,
};

mod support;

use support::TestFrame;

fn machine() -> Result<ConnectionCore<TestFrame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        ConnectionLimits::new(
            3,
            RetainedBytes::new(30),
            3,
            RetainedBytes::new(30),
            MatchKeySpace::new(0, 2)?,
        )?,
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
fn early_timer_delivery_reschedules_the_same_absolute_deadline() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, _) = commit_one(&mut machine, 10)?;
    let early = machine.apply(ConnectionInput::DeadlineElapsed {
        epoch: machine.epoch(),
        operation,
        now: Moment::from_nanos(9),
    })?;
    assert_eq!(early.disposition(), InputDisposition::Applied);
    assert_eq!(
        early.effects(),
        [ConnectionEffect::ScheduleDeadline {
            epoch: machine.epoch(),
            operation,
            deadline: Deadline::at(Moment::from_nanos(10)),
        }]
    );
    Ok(())
}

#[test]
fn maximum_absolute_deadline_is_preserved_without_arithmetic() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, _) = commit_one(&mut machine, u64::MAX)?;
    let early = machine.apply(ConnectionInput::DeadlineElapsed {
        epoch: machine.epoch(),
        operation,
        now: Moment::from_nanos(u64::MAX - 1),
    })?;
    assert_eq!(
        early.effects(),
        [ConnectionEffect::ScheduleDeadline {
            epoch: machine.epoch(),
            operation,
            deadline: Deadline::at(Moment::from_nanos(u64::MAX)),
        }]
    );
    Ok(())
}

#[test]
fn not_sent_deadline_fails_locally_and_releases_capacity() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, _) = commit_one(&mut machine, 10)?;
    let elapsed = machine.apply(ConnectionInput::DeadlineElapsed {
        epoch: machine.epoch(),
        operation,
        now: Moment::from_nanos(10),
    })?;
    assert!(elapsed.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            outcome: OperationOutcome::Failed {
                failure: OperationFailure::DeadlineElapsed,
                delivery: Delivery::NotSent,
            },
            ..
        }
    )));
    assert_eq!(machine.snapshot().owned_operations, 0);
    assert_eq!(machine.snapshot().active_match_keys, 0);
    Ok(())
}

#[test]
fn possibly_sent_deadline_closes_epoch_and_never_strengthens_delivery() -> Result<(), Box<dyn Error>>
{
    let mut machine = machine()?;
    let (first, first_effect) = commit_one(&mut machine, 10)?;
    let (second, _) = commit_one(&mut machine, 20)?;
    machine.advance_write(machine.epoch(), first_effect, 1)?;

    let elapsed = machine.apply(ConnectionInput::DeadlineElapsed {
        epoch: machine.epoch(),
        operation: first,
        now: Moment::from_nanos(10),
    })?;
    assert!(matches!(
        elapsed.effects().first(),
        Some(ConnectionEffect::CloseEpoch {
            reason: CloseReason::DeadlineAfterPossibleSend,
            ..
        })
    ));
    assert!(elapsed.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            operation,
            outcome: OperationOutcome::Failed {
                delivery: Delivery::PossiblySent,
                ..
            },
            ..
        } if *operation == first
    )));
    assert!(elapsed.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            operation,
            outcome: OperationOutcome::Failed {
                delivery: Delivery::NotSent,
                ..
            },
            ..
        } if *operation == second
    )));
    assert_eq!(machine.snapshot().phase, ConnectionPhase::Closing);
    assert_eq!(machine.snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn cancelled_possibly_sent_operation_is_not_published_twice_at_deadline()
-> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    let (operation, effect) = commit_one(&mut machine, 10)?;
    machine.advance_write(machine.epoch(), effect, 1)?;
    machine.apply(ConnectionInput::Cancel {
        epoch: machine.epoch(),
        operation,
    })?;

    let elapsed = machine.apply(ConnectionInput::DeadlineElapsed {
        epoch: machine.epoch(),
        operation,
        now: Moment::from_nanos(10),
    })?;
    let publications = elapsed
        .effects()
        .iter()
        .filter(|effect| matches!(effect, ConnectionEffect::PublishOutcome { .. }))
        .count();
    assert_eq!(publications, 0);
    assert!(
        elapsed
            .effects()
            .iter()
            .any(|effect| matches!(effect, ConnectionEffect::CloseEpoch { .. }))
    );
    Ok(())
}

#[test]
fn physical_close_confirmation_is_epoch_scoped() -> Result<(), Box<dyn Error>> {
    let mut machine = machine()?;
    machine.apply(ConnectionInput::CloseRequested {
        epoch: machine.epoch(),
        reason: CloseReason::Requested,
    })?;
    let stale = machine.apply(ConnectionInput::EpochClosed {
        epoch: ConnectionEpoch::new(3),
    })?;
    assert_eq!(stale.disposition(), InputDisposition::IgnoredStaleEpoch);
    assert_eq!(machine.snapshot().phase, ConnectionPhase::Closing);

    let current = machine.apply(ConnectionInput::EpochClosed {
        epoch: machine.epoch(),
    })?;
    assert_eq!(current.disposition(), InputDisposition::Applied);
    assert_eq!(machine.snapshot().phase, ConnectionPhase::Closed);
    Ok(())
}
