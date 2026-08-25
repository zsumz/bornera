//! Selector-free bounded graceful-shutdown contracts.

use std::{error::Error, io};

use bornera::{
    ConnectionEvent, EngineError, EngineInvariant, OutboundFrame, OwnerFailure,
    TransportFailureKind, TransportFailurePhase, TransportPressure, TransportState,
};
use bornera_core::{
    CloseReason, CompletionMode, Deadline, InputDisposition, Moment, OperationOptions,
    RetainedBytes,
};

#[path = "common/shutdown_protocol.rs"]
mod protocol;
#[path = "common/shutdown_transport.rs"]
mod support;
use protocol::{Classifier, Decoder, slot};
use support::ShutdownTransport;

#[test]
fn shutdown_begins_exactly_once_even_when_completion_was_already_visible()
-> Result<(), Box<dyn Error>> {
    let mut slot = slot(2, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::complete_before_begin();
    open(&mut slot, &mut transport)?;
    assert_eq!(slot.begin_drain(deadline(20))?, InputDisposition::Applied);
    assert!(!slot.transport_release_ready());

    let progress = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(progress.work(), 1);
    assert_eq!(transport.begin_calls(), 1);
    assert!(slot.transport_release_ready());
    let repeated = slot.drive_quantum(Moment::from_nanos(2), Some(&mut transport))?;
    assert_eq!(repeated.work(), 0);
    assert_eq!(transport.begin_calls(), 1);
    assert!(slot.settle_transport_closed());
    assert_eq!(slot.snapshot().transport, TransportState::Closed);
    let events: Vec<_> = slot.drain_events().collect();
    assert!(matches!(
        events.as_slice(),
        [
            ConnectionEvent::TransportOpened { sequence: 1, .. },
            ConnectionEvent::Closing {
                sequence: 2,
                reason: CloseReason::Drained,
                ..
            },
            ConnectionEvent::Closed {
                sequence: 3,
                reason: CloseReason::Drained,
                ..
            }
        ]
    ));
    Ok(())
}

#[test]
fn transport_flush_progresses_across_bounded_quanta() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::steps(2);
    open(&mut slot, &mut transport)?;
    slot.begin_drain(deadline(20))?;
    assert!(!slot.settle_transport_closed());

    let begin = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(begin.work(), 1);
    assert!(begin.saturated());
    assert_eq!(transport.begin_calls(), 1);
    let first = slot.drive_quantum(Moment::from_nanos(2), Some(&mut transport))?;
    assert_eq!(first.work(), 1);
    assert!(first.saturated());
    assert_eq!(transport.drive_calls(), 1);
    let second = slot.drive_quantum(Moment::from_nanos(3), Some(&mut transport))?;
    assert_eq!(second.work(), 1);
    assert!(!second.saturated());
    assert_eq!(transport.drive_calls(), 2);
    assert!(slot.transport_release_ready());
    Ok(())
}

#[test]
fn shutdown_waits_without_spinning_until_transport_work_is_runnable() -> Result<(), Box<dyn Error>>
{
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::waiting();
    open(&mut slot, &mut transport)?;
    slot.begin_drain(deadline(20))?;
    let begin = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(begin.work(), 1);
    assert!(!begin.saturated());

    let waiting = slot.drive_quantum(Moment::from_nanos(2), Some(&mut transport))?;
    assert_eq!(waiting.work(), 0);
    assert!(!waiting.saturated());
    assert_eq!(transport.drive_calls(), 0);
    transport.allow_transport_work();
    let completed = slot.drive_quantum(Moment::from_nanos(3), Some(&mut transport))?;
    assert_eq!(completed.work(), 1);
    assert!(slot.transport_release_ready());
    Ok(())
}

#[test]
fn elapsed_absolute_deadline_forces_release_before_shutdown_begins() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::steps(1);
    open(&mut slot, &mut transport)?;
    slot.begin_drain(deadline(5))?;

    let timed_out = slot.drive_quantum(Moment::from_nanos(5), Some(&mut transport))?;
    assert_eq!(timed_out.work(), 1);
    assert_eq!(transport.begin_calls(), 0);
    assert!(slot.transport_release_ready());
    let snapshot = slot.snapshot();
    assert_eq!(snapshot.connection.close_reason, Some(CloseReason::Drained));
    assert!(matches!(
        snapshot.transport_diagnostic,
        Some(diagnostic)
            if diagnostic.phase == TransportFailurePhase::Shutdown
                && diagnostic.failure == TransportFailureKind::TimedOut
    ));
    Ok(())
}

#[test]
fn forced_finalize_preempts_pending_grace_without_rewriting_core_reason()
-> Result<(), Box<dyn Error>> {
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::steps(1);
    open(&mut slot, &mut transport)?;
    slot.begin_drain(deadline(20))?;
    assert_eq!(
        slot.finalize(CloseReason::Requested)?,
        InputDisposition::Applied
    );
    assert_eq!(transport.begin_calls(), 0);
    assert!(slot.transport_release_ready());
    assert_eq!(
        slot.snapshot().connection.close_reason,
        Some(CloseReason::Drained)
    );
    assert!(slot.settle_transport_closed());
    Ok(())
}

#[test]
fn shutdown_does_not_begin_before_the_final_operation_terminates() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::complete_before_begin().writable();
    open(&mut slot, &mut transport)?;
    slot.open_admission()?;
    let options = OperationOptions::until(deadline(50))
        .completion_mode(CompletionMode::WriteComplete)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, OutboundFrame::copy_from_slice(&[9])?)?;
    slot.begin_drain(deadline(20))?;
    assert_eq!(slot.snapshot().transport, TransportState::Open);
    assert_eq!(transport.begin_calls(), 0);

    let write = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(write.work(), 1);
    assert_eq!(transport.begin_calls(), 0);
    assert_eq!(slot.snapshot().transport, TransportState::Closing);
    let shutdown = slot.drive_quantum(Moment::from_nanos(2), Some(&mut transport))?;
    assert_eq!(shutdown.work(), 1);
    assert_eq!(transport.begin_calls(), 1);
    assert!(slot.transport_release_ready());
    Ok(())
}

#[test]
fn shutdown_error_retains_diagnostic_and_forces_release() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::begin_failure();
    open(&mut slot, &mut transport)?;
    slot.begin_drain(deadline(20))?;
    let progress = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(progress.work(), 1);
    assert!(slot.transport_release_ready());
    assert!(matches!(
        slot.snapshot().transport_diagnostic,
        Some(diagnostic)
            if diagnostic.phase == TransportFailurePhase::Shutdown
                && diagnostic.failure == TransportFailureKind::Protocol
    ));
    Ok(())
}

#[test]
fn shutdown_progress_contract_failure_latches_and_remains_settle_ready()
-> Result<(), Box<dyn Error>> {
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::idle_progress();
    open(&mut slot, &mut transport)?;
    slot.begin_drain(deadline(20))?;
    let error = slot
        .drive_quantum(Moment::from_nanos(1), Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("idle shutdown progress was accepted"))?;
    assert!(matches!(
        error,
        EngineError::Invariant(EngineInvariant::TransportNoProgress)
    ));
    assert_eq!(
        slot.snapshot().owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    assert!(slot.transport_release_ready());
    Ok(())
}

#[test]
fn shutdown_begin_pressure_overflow_fails_closed() -> Result<(), Box<dyn Error>> {
    let pressure = TransportPressure::new(
        RetainedBytes::new(5),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut slot = slot(1, RetainedBytes::new(4))?;
    let mut transport = ShutdownTransport::pressure_after_begin(pressure, RetainedBytes::new(4));
    open(&mut slot, &mut transport)?;
    slot.begin_drain(deadline(20))?;
    assert!(matches!(
        slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport)),
        Err(EngineError::Invariant(
            EngineInvariant::TransportRetainedCapacity { reported, .. }
        )) if reported == pressure
    ));
    assert_eq!(
        slot.snapshot().owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    assert!(slot.transport_release_ready());
    Ok(())
}

#[test]
fn owner_abort_skips_shutdown_and_clears_the_drain_deadline() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1, RetainedBytes::new(64))?;
    let mut transport = ShutdownTransport::steps(0).idle_control_work();
    open(&mut slot, &mut transport)?;
    slot.open_admission()?;
    let options = OperationOptions::until(deadline(50))
        .completion_mode(CompletionMode::ReplyExpected)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, OutboundFrame::copy_from_slice(&[9])?)?;
    slot.begin_drain(deadline(20))?;

    assert!(matches!(
        slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport)),
        Err(EngineError::Invariant(EngineInvariant::TransportNoProgress))
    ));
    assert_eq!(transport.begin_calls(), 0);
    let snapshot = slot.snapshot();
    assert_eq!(snapshot.shutdown_deadline, None);
    assert_eq!(snapshot.owner_failure, Some(OwnerFailure::OwnerInvariant));
    assert!(slot.transport_release_ready());
    Ok(())
}

fn open(
    slot: &mut bornera::ConnectionSlot<Decoder, Classifier>,
    transport: &mut ShutdownTransport,
) -> Result<(), EngineError> {
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(transport))?;
    Ok(())
}

const fn deadline(nanos: u64) -> Deadline {
    Deadline::at(Moment::from_nanos(nanos))
}
