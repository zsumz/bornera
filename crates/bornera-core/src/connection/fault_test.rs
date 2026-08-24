//! Internal fault injection for aggregate recovery guarantees.

use calandria::{Deadline, Moment, RetainedBytes};
use std::error::Error;

use crate::{
    ConnectionCore, ConnectionCoreError, ConnectionCoreInvariant, ConnectionEpoch, ConnectionId,
    ConnectionInput, ConnectionLimits, Delivery, EffectId, EndpointId, LaneId, MatchKeySpace,
    OperationId, OperationOptions, WriteFrame,
};

#[derive(Debug, Eq, PartialEq)]
struct Frame(Vec<u8>);

impl WriteFrame for Frame {
    fn bytes(&self) -> &[u8] {
        &self.0
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::new(u64::try_from(self.0.len()).unwrap_or(u64::MAX))
    }
}

fn core() -> Result<ConnectionCore<Frame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        ConnectionLimits::new(
            3,
            RetainedBytes::new(32),
            3,
            RetainedBytes::new(32),
            MatchKeySpace::new(0, 2)?,
        )?,
    ))
}

fn commit(
    core: &mut ConnectionCore<Frame>,
    bytes: Vec<u8>,
) -> Result<(OperationId, EffectId), Box<dyn Error>> {
    let retained = RetainedBytes::new(u64::try_from(bytes.len())?);
    let permit = core.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(20)))
            .session()
            .retained_bytes(retained)
            .write_retained_bytes(retained),
    )?;
    let (operation, _) = core.commit(permit, Frame(bytes))?;
    let effect = core
        .write_effect(operation)
        .ok_or_else(|| std::io::Error::other("fixture operation lost its write"))?;
    Ok((operation, effect))
}

#[test]
fn reconciliation_journal_preserves_removed_operations_and_frames() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let (completed, completed_effect) = commit(&mut core, Vec::from([1]))?;
    let (journaled, _) = commit(&mut core, Vec::from([2]))?;
    let (missing, missing_effect) = commit(&mut core, Vec::from([3]))?;
    let _transition = core.advance_write(core.epoch(), completed_effect, 1)?;
    core.begin_recovery_journal()?;
    let transition = core.machine.apply(ConnectionInput::CloseRequested {
        epoch: core.epoch(),
        reason: crate::CloseReason::Requested,
    });
    let removed = core
        .writes
        .discard(core.epoch(), missing_effect)?
        .ok_or_else(|| std::io::Error::other("third write did not exist"))?;

    let Err(error) = core.reconcile(transition) else {
        return Err(std::io::Error::other("reconciliation divergence was accepted").into());
    };
    assert_eq!(
        error,
        ConnectionCoreError::Invariant(ConnectionCoreInvariant::MissingWrite {
            operation: missing,
            effect: missing_effect,
        })
    );

    let recovery = core.recover();
    assert_eq!(
        recovery
            .operations
            .iter()
            .map(|operation| operation.operation)
            .collect::<Vec<_>>(),
        Vec::from([completed, journaled, missing])
    );
    assert_eq!(recovery.operations[0].delivery, Delivery::PossiblySent);
    assert_eq!(recovery.operations[1].delivery, Delivery::NotSent);
    assert_eq!(
        recovery.operations[1]
            .frame
            .as_ref()
            .map(|frame| frame.0.as_slice()),
        Some(&[2][..])
    );
    assert!(recovery.operations[0].frame.is_none());
    assert!(recovery.operations[2].frame.is_none());
    assert!(recovery.unmatched_writes.is_empty());
    assert_eq!(removed.operation, missing);
    assert_eq!(removed.frame, Frame(Vec::from([3])));
    assert!(recovery.ownership_diverged);
    Ok(())
}

#[test]
fn journal_order_survives_an_interior_destructive_failure() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let (first, _) = commit(&mut core, Vec::from([1]))?;
    let (middle, middle_effect) = commit(&mut core, Vec::from([2]))?;
    let (last, _) = commit(&mut core, Vec::from([3]))?;
    core.begin_operation_recovery_journal(middle)?;
    let transition = core.machine.apply(ConnectionInput::Cancel {
        epoch: core.epoch(),
        operation: middle,
    });
    let removed = core
        .writes
        .discard(core.epoch(), middle_effect)?
        .ok_or_else(|| std::io::Error::other("middle write did not exist"))?;

    let Err(error) = core.reconcile(transition) else {
        return Err(std::io::Error::other("interior ownership loss was accepted").into());
    };
    assert_eq!(
        error,
        ConnectionCoreError::Invariant(ConnectionCoreInvariant::MissingWrite {
            operation: middle,
            effect: middle_effect,
        })
    );

    let recovery = core.recover();
    assert_eq!(
        recovery
            .operations
            .iter()
            .map(|operation| operation.operation)
            .collect::<Vec<_>>(),
        Vec::from([first, middle, last])
    );
    assert_eq!(recovery.operations[0].frame, Some(Frame(Vec::from([1]))));
    assert!(recovery.operations[1].frame.is_none());
    assert_eq!(recovery.operations[2].frame, Some(Frame(Vec::from([3]))));
    assert_eq!(removed.operation, middle);
    assert_eq!(removed.frame, Frame(Vec::from([2])));
    assert!(recovery.ownership_diverged);
    Ok(())
}

#[test]
fn recovery_returns_unmatched_writer_frames_instead_of_dropping_them() -> Result<(), Box<dyn Error>>
{
    let mut core = core()?;
    let frame = Frame(Vec::from([7, 8]));
    let measure = crate::FrameMeasure::capture(&frame);
    core.writes.admit(
        core.epoch(),
        OperationId::new(99),
        EffectId::new(100),
        measure,
        frame,
    )?;

    let Err(error) = core.apply(ConnectionInput::OpenAdmission {
        epoch: core.epoch(),
    }) else {
        return Err(std::io::Error::other("writer-only ownership was accepted").into());
    };
    assert!(matches!(
        error,
        ConnectionCoreError::Invariant(ConnectionCoreInvariant::UnexpectedWrite { .. })
    ));

    let recovery = core.recover();
    assert!(recovery.operations.is_empty());
    assert_eq!(recovery.unmatched_writes.len(), 1);
    assert_eq!(recovery.unmatched_writes[0].frame, Frame(Vec::from([7, 8])));
    assert!(recovery.ownership_diverged);
    Ok(())
}

#[test]
fn write_progress_without_policy_is_recovery_total() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let operation = OperationId::new(99);
    let effect = EffectId::new(100);
    let frame = Frame(Vec::from([7, 8]));
    let measure = crate::FrameMeasure::capture(&frame);
    core.writes
        .admit(core.epoch(), operation, effect, measure, frame)?;

    let Err(error) = core.advance_write(core.epoch(), effect, 1) else {
        return Err(std::io::Error::other("writer-only progress was accepted").into());
    };
    assert_eq!(
        error,
        ConnectionCoreError::Invariant(ConnectionCoreInvariant::WritePolicyMismatch {
            operation,
            effect,
            disposition: crate::InputDisposition::IgnoredUnknownOperation,
        })
    );

    let recovery = core.recover();
    assert!(recovery.operations.is_empty());
    assert_eq!(recovery.unmatched_writes.len(), 1);
    assert_eq!(recovery.unmatched_writes[0].operation, operation);
    assert_eq!(recovery.unmatched_writes[0].written, 1);
    assert_eq!(
        recovery.unmatched_writes[0].delivery,
        Delivery::PossiblySent
    );
    assert_eq!(recovery.unmatched_writes[0].frame, Frame(Vec::from([7, 8])));
    assert!(recovery.ownership_diverged);
    Ok(())
}

#[test]
fn completed_write_without_policy_retains_the_exact_frame_for_recovery()
-> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let operation = OperationId::new(99);
    let effect = EffectId::new(100);
    let frame = Frame(Vec::from([7, 8]));
    let measure = crate::FrameMeasure::capture(&frame);
    core.writes
        .admit(core.epoch(), operation, effect, measure, frame)?;

    let Err(error) = core.advance_write(core.epoch(), effect, 2) else {
        return Err(std::io::Error::other("writer-only completion was accepted").into());
    };
    assert_eq!(
        error,
        ConnectionCoreError::Invariant(ConnectionCoreInvariant::UnexpectedWrite {
            operation,
            effect,
        })
    );

    let recovery = core.recover();
    assert!(recovery.operations.is_empty());
    assert_eq!(recovery.unmatched_writes.len(), 1);
    assert_eq!(recovery.unmatched_writes[0].operation, operation);
    assert_eq!(recovery.unmatched_writes[0].written, 2);
    assert_eq!(
        recovery.unmatched_writes[0].delivery,
        Delivery::PossiblySent
    );
    assert_eq!(recovery.unmatched_writes[0].frame, Frame(Vec::from([7, 8])));
    assert!(recovery.ownership_diverged);
    Ok(())
}
