//! Bounded transport-local progression and scheduler contract regressions.

use std::{error::Error, io, num::NonZeroUsize};

use bornera::{EngineError, EngineInvariant, OwnerFailure, TransportProgress};
use bornera_core::{CompletionMode, Deadline, Moment, OperationOptions, RetainedBytes};

use super::support::{ProgressTransport, nonzero, slot};

#[test]
fn deadline_budget_retains_immediate_transport_work() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::new(TransportProgress::operation());
    let opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert!(opened.saturated());
    slot.open_admission()?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(1)))
        .completion_mode(CompletionMode::ReplyExpected)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, bornera::OutboundFrame::copy_from_slice(&[1])?)?;

    let deadline = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(deadline.work(), 1);
    assert!(deadline.saturated());

    let control = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(control.work(), 1);
    assert!(!control.saturated());
    Ok(())
}

#[test]
fn advertised_transport_work_must_report_progress() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::new(TransportProgress::IDLE);
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;

    let error = slot
        .drive_quantum(Moment::ORIGIN, Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("idle transport work was accepted"))?;
    assert!(matches!(
        error,
        EngineError::Invariant(EngineInvariant::TransportNoProgress)
    ));
    assert_eq!(
        slot.snapshot().owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    Ok(())
}

#[test]
fn transport_progress_cannot_exceed_hard_bounds() -> Result<(), Box<dyn Error>> {
    for reported in [
        TransportProgress::new(nonzero(2)?, 0, 0),
        TransportProgress::new(NonZeroUsize::MIN, 9, 0),
    ] {
        let mut slot = slot(4)?;
        let mut transport = ProgressTransport::new(reported);
        let error = slot
            .drive_quantum(Moment::ORIGIN, Some(&mut transport))
            .err()
            .ok_or_else(|| io::Error::other("over-budget transport progress was accepted"))?;
        assert!(matches!(
            error,
            EngineError::Invariant(EngineInvariant::TransportProgressContract {
                reported: observed,
                ..
            }) if observed == reported
        ));
    }
    Ok(())
}

#[test]
fn preopened_transport_is_rejected_before_policy_bypass() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::preopened();
    let error = slot
        .drive_quantum(Moment::ORIGIN, Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("preopened transport bypassed establishment policy"))?;
    assert!(matches!(
        error,
        EngineError::Invariant(EngineInvariant::TransportOpenedBeforeEstablishment)
    ));
    Ok(())
}

#[test]
fn transport_work_remains_after_write_complete_releases_the_frame() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::buffered_write();
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(50)))
        .completion_mode(CompletionMode::WriteComplete)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, bornera::OutboundFrame::copy_from_slice(&[7])?)?;

    let accepted = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(slot.snapshot().queued_write_frames, 0);
    assert_eq!(slot.snapshot().pending_outcomes, 1);
    assert!(accepted.saturated());

    let drained = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(drained.work(), 1);
    assert!(!drained.saturated());
    Ok(())
}
