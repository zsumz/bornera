//! Transport-owned memory binding and runtime enforcement regressions.

use std::{error::Error, io};

use bornera::{
    EngineError, EngineInvariant, OwnerFailure, TransportFailureKind, TransportPressure,
    TransportProgress,
};
use bornera_core::{CompletionMode, Deadline, Moment, OperationOptions, RetainedBytes};

use super::support::ProgressTransport;

#[test]
fn pressure_is_checked_before_work_and_retained_for_recovery() -> Result<(), Box<dyn Error>> {
    let pressure = TransportPressure::new(
        RetainedBytes::new(3),
        RetainedBytes::new(2),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut slot = super::support::slot_with_transport_limit(1, RetainedBytes::new(8))?;
    let mut transport = ProgressTransport::with_pressure(pressure);

    let error = slot
        .drive_quantum(Moment::ORIGIN, Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("excess initial pressure was accepted"))?;
    assert!(matches!(
        error,
        EngineError::Invariant(EngineInvariant::TransportRetainedCapacity {
            reported,
            ..
        }) if reported == pressure
    ));
    let report = slot.recover(OwnerFailure::OwnerInvariant);
    assert_eq!(report.transport_pressure, Some(pressure));
    assert_eq!(report.transport_retained_limit, Some(RetainedBytes::new(4)));
    assert_eq!(
        report.transport_retained_ceiling,
        Some(RetainedBytes::new(8))
    );
    assert_eq!(
        report.transport_diagnostic.map(|value| value.failure),
        Some(TransportFailureKind::Capacity)
    );
    Ok(())
}

#[test]
fn transport_cannot_change_its_bound_ceiling() -> Result<(), Box<dyn Error>> {
    let mut slot = super::support::slot_with_transport_limit(1, RetainedBytes::new(8))?;
    let mut transport = ProgressTransport::new(TransportProgress::operation())
        .declared_limit(RetainedBytes::new(4))
        .limit_after_establishment(RetainedBytes::new(3));

    assert!(matches!(
        slot.drive_quantum(Moment::ORIGIN, Some(&mut transport)),
        Err(EngineError::Invariant(
            EngineInvariant::TransportLimitContract { limit, reported }
        )) if limit == RetainedBytes::new(4) && reported == RetainedBytes::new(3)
    ));
    let snapshot = slot.snapshot();
    assert_eq!(
        snapshot.transport_retained_limit,
        Some(RetainedBytes::new(4))
    );
    assert_eq!(snapshot.transport_retained_ceiling, RetainedBytes::new(8));
    Ok(())
}

#[test]
fn selector_free_transport_limit_is_bound_before_work() -> Result<(), Box<dyn Error>> {
    let mut slot = super::support::slot_with_transport_limit(1, RetainedBytes::new(4))?;
    let mut transport = ProgressTransport::new(TransportProgress::operation())
        .declared_limit(RetainedBytes::new(5));

    assert!(matches!(
        slot.drive_quantum(Moment::ORIGIN, Some(&mut transport)),
        Err(EngineError::Invariant(
            EngineInvariant::TransportLimitContract { limit, reported }
        )) if limit == RetainedBytes::new(4) && reported == RetainedBytes::new(5)
    ));
    assert_eq!(
        slot.snapshot().owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    Ok(())
}

#[test]
fn untouched_selector_free_slot_reports_unobserved_transport_memory() -> Result<(), Box<dyn Error>>
{
    let slot = super::support::slot_with_transport_limit(1, RetainedBytes::new(8))?;
    let report = slot.recover(OwnerFailure::OwnerInvariant);
    assert_eq!(report.transport_pressure, None);
    assert_eq!(report.transport_retained_limit, None);
    assert_eq!(
        report.transport_retained_ceiling,
        Some(RetainedBytes::new(8))
    );
    Ok(())
}

#[test]
fn pressure_equal_to_the_limit_is_admissible() -> Result<(), Box<dyn Error>> {
    let pressure = TransportPressure::new(
        RetainedBytes::new(2),
        RetainedBytes::new(2),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut slot = super::support::slot_with_transport_limit(1, pressure.total())?;
    let mut transport = ProgressTransport::with_pressure(pressure);
    let progress = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(progress.work(), 1);
    assert_eq!(slot.snapshot().transport_pressure, Some(pressure));
    Ok(())
}

#[test]
fn pressure_is_rechecked_after_application_write_ownership_moves() -> Result<(), Box<dyn Error>> {
    let pressure = TransportPressure::new(
        RetainedBytes::ZERO,
        RetainedBytes::new(5),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut slot = super::support::slot_with_transport_limit(1, RetainedBytes::new(4))?;
    let mut transport = ProgressTransport::pressure_after_write(pressure);
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(50)))
        .completion_mode(CompletionMode::WriteComplete)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, bornera::OutboundFrame::copy_from_slice(&[9])?)?;

    assert!(matches!(
        slot.drive_quantum(Moment::ORIGIN, Some(&mut transport)),
        Err(EngineError::Invariant(
            EngineInvariant::TransportRetainedCapacity { reported, .. }
        )) if reported == pressure
    ));
    assert_eq!(slot.snapshot().queued_write_frames, 0);
    assert_eq!(slot.snapshot().pending_outcomes, 1);
    let report = slot.recover(OwnerFailure::OwnerInvariant);
    assert_eq!(report.transport_pressure, Some(pressure));
    assert_eq!(report.outcomes.len(), 1);
    Ok(())
}

#[test]
fn pressure_is_rechecked_after_transport_local_progress() -> Result<(), Box<dyn Error>> {
    let pressure = TransportPressure::new(
        RetainedBytes::new(5),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut slot = super::support::slot_with_transport_limit(1, RetainedBytes::new(4))?;
    let mut transport =
        ProgressTransport::new(TransportProgress::operation()).pressure_after_transport(pressure);
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;

    assert!(matches!(
        slot.drive_quantum(Moment::ORIGIN, Some(&mut transport)),
        Err(EngineError::Invariant(
            EngineInvariant::TransportRetainedCapacity { reported, .. }
        )) if reported == pressure
    ));
    assert_eq!(slot.snapshot().transport_pressure, Some(pressure));
    Ok(())
}

#[test]
fn pressure_sum_overflow_is_rejected_at_construction() {
    assert!(
        TransportPressure::new(
            RetainedBytes::new(u64::MAX),
            RetainedBytes::new(1),
            RetainedBytes::ZERO,
            RetainedBytes::ZERO,
        )
        .is_err()
    );
}
