//! Focused evidence for atomic admission, rollback, and commit ownership.

use std::error::Error;

use bornera_core::{
    AdmissionGate, CommitErrorKind, ConnectionCore, ConnectionEffect, ConnectionEpoch,
    ConnectionId, ConnectionInput, ConnectionLimits, Deadline, Delivery, EndpointId,
    FrameCommitFailure, LaneId, MatchKey, MatchKeySpace, Moment, OperationOptions, ReserveError,
    RetainedBytes,
};

mod support;

use support::TestFrame;

fn frame(length: usize) -> TestFrame {
    TestFrame(std::iter::repeat_n(0, length).collect())
}

fn limits(
    operations: usize,
    retained: u64,
    writes: usize,
    write_bytes: u64,
    first_key: u32,
    last_key: u32,
) -> Result<ConnectionLimits, Box<dyn Error>> {
    Ok(ConnectionLimits::new(
        operations,
        RetainedBytes::new(retained),
        writes,
        RetainedBytes::new(write_bytes),
        MatchKeySpace::new(first_key, last_key)?,
    )?)
}

fn machine(limits: ConnectionLimits, epoch: u64) -> ConnectionCore<TestFrame> {
    ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(epoch),
        limits,
    )
}

fn options(retained: u64, write: u64) -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(10)))
        .retained_bytes(RetainedBytes::new(retained))
        .write_bytes(RetainedBytes::new(write))
}

#[test]
fn session_gate_rejects_regular_work_until_opened() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(limits(2, 20, 2, 20, 0, 1)?, 4);
    let error = machine.reserve(Moment::ORIGIN, options(1, 1)).err();
    assert_eq!(error, Some(ReserveError::AdmissionClosed));

    let session = machine.reserve(Moment::ORIGIN, options(1, 1).session())?;
    drop(session);
    assert_eq!(machine.snapshot().gate, AdmissionGate::SessionOnly);
    assert_eq!(
        machine
            .apply(ConnectionInput::OpenAdmission {
                epoch: machine.epoch(),
            })?
            .effects()
            .len(),
        0
    );
    let regular = machine.reserve(Moment::ORIGIN, options(1, 1))?;
    drop(regular);
    Ok(())
}

#[test]
fn dropping_a_permit_atomically_restores_all_capacity() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(limits(1, 7, 1, 9, 17, 17)?, 4);
    let permit = machine.reserve(Moment::ORIGIN, options(7, 9).session())?;
    assert_eq!(permit.match_key(), MatchKey::new(17));
    let owned = machine.snapshot();
    assert_eq!(owned.owned_operations, 1);
    assert_eq!(owned.reserved_permits, 1);
    assert_eq!(owned.retained_bytes, RetainedBytes::new(7));
    assert_eq!(owned.buffered_write_bytes, RetainedBytes::new(9));

    drop(permit);
    let released = machine.snapshot();
    assert_eq!(released.owned_operations, 0);
    assert_eq!(released.reserved_permits, 0);
    assert_eq!(released.active_match_keys, 0);
    assert_eq!(released.retained_bytes, RetainedBytes::ZERO);
    assert_eq!(released.buffered_write_bytes, RetainedBytes::ZERO);

    let replacement = machine.reserve(Moment::ORIGIN, options(7, 9).session())?;
    assert_eq!(replacement.match_key(), MatchKey::new(17));
    Ok(())
}

#[test]
fn each_admission_limit_fails_before_publication() -> Result<(), Box<dyn Error>> {
    let mut count = machine(limits(1, 20, 2, 20, 0, 1)?, 1);
    let count_permit = count.reserve(Moment::ORIGIN, options(1, 1).session())?;
    assert_eq!(
        count.reserve(Moment::ORIGIN, options(1, 1).session()).err(),
        Some(ReserveError::OperationCapacity)
    );
    drop(count_permit);

    let mut retained = machine(limits(2, 3, 2, 20, 0, 1)?, 2);
    assert_eq!(
        retained
            .reserve(Moment::ORIGIN, options(4, 1).session())
            .err(),
        Some(ReserveError::RetainedByteCapacity)
    );

    let mut write = machine(limits(2, 20, 1, 3, 0, 1)?, 3);
    assert_eq!(
        write.reserve(Moment::ORIGIN, options(1, 4).session()).err(),
        Some(ReserveError::WriteCapacity)
    );

    let mut keys = machine(limits(2, 20, 2, 20, 9, 9)?, 4);
    let key = keys.reserve(Moment::ORIGIN, options(1, 1).session())?;
    assert_eq!(
        keys.reserve(Moment::ORIGIN, options(1, 1).session()).err(),
        Some(ReserveError::MatchKeyExhausted)
    );
    drop(key);
    Ok(())
}

#[test]
fn commit_shrinks_write_reservation_to_exact_frame_bytes() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(limits(1, 20, 1, 20, 5, 5)?, 4);
    let permit = machine.reserve(Moment::ORIGIN, options(2, 10).session())?;
    let (operation, transition) = machine.commit(permit, frame(6))?;
    assert_eq!(machine.snapshot().reserved_permits, 0);
    assert_eq!(
        machine.snapshot().buffered_write_bytes,
        RetainedBytes::new(6)
    );
    assert_eq!(transition.effects().len(), 1);
    assert!(matches!(
        transition.effects(),
        [ConnectionEffect::ScheduleDeadline { operation: owned, .. }] if *owned == operation
    ));
    Ok(())
}

#[test]
fn failed_commit_retains_permit_until_caller_drops_it() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(limits(1, 20, 1, 5, 0, 0)?, 4);
    let permit = machine.reserve(Moment::ORIGIN, options(1, 5).session())?;
    let error = machine.commit(permit, frame(6));
    let Err(error) = error else {
        return Err(std::io::Error::other("oversized frame unexpectedly committed").into());
    };
    assert_eq!(
        error.failure(),
        FrameCommitFailure::Policy(CommitErrorKind::FrameTooLarge)
    );
    assert_eq!(machine.snapshot().reserved_permits, 1);
    let (permit, returned) = error.into_parts();
    assert_eq!(returned, frame(6));
    drop(permit);
    assert_eq!(machine.snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn foreign_machine_cannot_consume_another_epochs_permit() -> Result<(), Box<dyn Error>> {
    let mut owner = machine(limits(1, 20, 1, 5, 0, 0)?, 4);
    let mut foreign = machine(limits(1, 20, 1, 5, 0, 0)?, 5);
    let permit = owner.reserve(Moment::ORIGIN, options(1, 5).session())?;
    let error = foreign.commit(permit, frame(5));
    let Err(error) = error else {
        return Err(std::io::Error::other("foreign permit unexpectedly committed").into());
    };
    assert_eq!(
        error.failure(),
        FrameCommitFailure::Policy(CommitErrorKind::ForeignPermit)
    );
    let (permit, returned) = error.into_parts();
    assert_eq!(returned, frame(5));
    let (operation, _) = owner.commit(permit, returned)?;
    assert_eq!(operation.get(), 0);
    Ok(())
}

#[test]
fn elapsed_deadline_is_rejected_without_capacity_change() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(limits(1, 20, 1, 20, 0, 0)?, 4);
    let error = machine
        .reserve(Moment::from_nanos(10), options(1, 1).session())
        .err();
    assert_eq!(error, Some(ReserveError::DeadlineElapsed));
    assert_eq!(machine.snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn atomic_commit_transfers_the_frame_without_an_enqueue_effect() -> Result<(), Box<dyn Error>> {
    let limits = limits(1, 20, 1, 20, 0, 0)?;
    let mut machine = machine(limits, 4);
    let permit = machine.reserve(Moment::ORIGIN, options(1, 10).session())?;
    let (operation, transition) = machine.commit(permit, TestFrame(Vec::from([1, 2, 3])))?;

    assert_eq!(operation.get(), 0);
    assert_eq!(machine.queued_write_frames(), 1);
    assert_eq!(
        machine.snapshot().buffered_write_bytes,
        RetainedBytes::new(3)
    );
    assert!(matches!(
        transition.effects(),
        [ConnectionEffect::ScheduleDeadline { operation: owned, .. }] if *owned == operation
    ));
    Ok(())
}

#[test]
fn atomic_commit_rejection_returns_the_permit_and_exact_frame() -> Result<(), Box<dyn Error>> {
    let limits = limits(1, 20, 1, 2, 0, 0)?;
    let mut machine = machine(limits, 4);
    let permit = machine.reserve(Moment::ORIGIN, options(1, 2).session())?;
    let frame = TestFrame(Vec::from([1, 2, 3]));
    let error = machine
        .commit(permit, frame.clone())
        .err()
        .ok_or_else(|| std::io::Error::other("oversized frame unexpectedly committed"))?;

    assert!(matches!(
        error.failure(),
        FrameCommitFailure::Policy(CommitErrorKind::FrameTooLarge)
    ));
    assert_eq!(error.delivery(), Delivery::NotSent);
    let (permit, returned) = error.into_parts();
    assert_eq!(returned, frame);
    assert_eq!(machine.snapshot().reserved_permits, 1);
    drop(permit);
    assert_eq!(machine.snapshot().owned_operations, 0);
    assert_eq!(machine.queued_write_frames(), 0);
    Ok(())
}
