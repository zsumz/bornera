//! Direct selector-free slot contracts for alternate transport hosts.

use std::{error::Error, io};

use bornera::{
    ConnectionEvent, EngineError, EngineInvariant, OutboundFrame, OwnerFailure,
    TransportFailurePhase, TransportState,
};
use bornera_core::{
    CloseReason, CompletionMode, Deadline, Moment, OperationOptions, RetainedBytes,
};

#[path = "common/slot.rs"]
mod slot_support;
use slot_support::{Decoder, TestTransport, slot, slot_with_decoder, slot_with_io_operations};

#[test]
fn deadline_budget_reports_a_ready_write_until_the_next_quantum_completes_it()
-> Result<(), Box<dyn Error>> {
    let mut slot = slot_with_io_operations(Decoder::retaining(0), 1)?;
    let mut transport = TestTransport::writable();
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    commit_byte(&mut slot, 1, CompletionMode::ReplyExpected, 1)?;
    commit_byte(&mut slot, 50, CompletionMode::WriteComplete, 2)?;

    let deadline = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(deadline.work(), 1);
    assert!(deadline.saturated());
    assert_eq!(slot.snapshot().queued_write_frames, 1);

    let write = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(write.work(), 1);
    assert!(!write.saturated());
    assert_eq!(transport.written_bytes(), 1);
    assert_eq!(slot.snapshot().queued_write_frames, 0);
    assert_eq!(slot.snapshot().pending_outcomes, 2);
    Ok(())
}

#[test]
fn deadline_budget_reports_an_empty_write_without_socket_writability() -> Result<(), Box<dyn Error>>
{
    let mut slot = slot_with_io_operations(Decoder::retaining(0), 1)?;
    let mut transport = TestTransport::benign();
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    commit_byte(&mut slot, 1, CompletionMode::ReplyExpected, 1)?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(50)))
        .completion_mode(CompletionMode::WriteComplete);
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, OutboundFrame::copy_from_slice(&[])?)?;

    let deadline = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert!(deadline.saturated());
    let empty = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(empty.work(), 1);
    assert!(!empty.saturated());
    assert_eq!(slot.snapshot().queued_write_frames, 0);
    assert_eq!(slot.snapshot().pending_outcomes, 2);
    Ok(())
}

#[test]
fn empty_write_does_not_spin_before_transport_establishment() -> Result<(), Box<dyn Error>> {
    let mut slot = slot_with_io_operations(Decoder::retaining(0), 1)?;
    let mut transport = TestTransport::connect_pending();
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(50)))
        .session()
        .completion_mode(CompletionMode::WriteComplete);
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, OutboundFrame::copy_from_slice(&[])?)?;

    let progress = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(progress.work(), 0);
    assert!(!progress.saturated());
    assert_eq!(slot.snapshot().queued_write_frames, 1);
    Ok(())
}

#[test]
fn deadline_budget_reports_ready_read_and_decoder_work_until_both_are_driven()
-> Result<(), Box<dyn Error>> {
    let mut slot = slot_with_io_operations(Decoder::retaining(0), 1)?;
    let mut transport = TestTransport::readable(&[3, 4, 5]);
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    commit_byte(&mut slot, 1, CompletionMode::ReplyExpected, 1)?;

    let deadline = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(deadline.work(), 1);
    assert!(deadline.saturated());

    let read = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(read.work(), 1);
    assert!(read.saturated());
    assert_eq!(slot.snapshot().buffered_read_bytes, RetainedBytes::new(3));

    let decode = slot.drive_quantum(Moment::from_nanos(1), Option::<&mut TestTransport>::None)?;
    assert_eq!(decode.work(), 1);
    assert!(!decode.saturated());
    Ok(())
}

#[test]
fn safe_transport_read_overreport_fails_closed_against_the_supplied_slice()
-> Result<(), Box<dyn Error>> {
    let mut slot = slot_with_decoder(Decoder::retaining(4))?;
    let mut transport = TestTransport::malicious_read(2);

    let error = slot
        .drive_quantum(Moment::ORIGIN, Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("malicious read count was accepted"))?;
    assert!(matches!(
        error,
        EngineError::Invariant(EngineInvariant::TransportReadContract {
            capacity: 4,
            reported: 6,
        })
    ));
    assert_eq!(
        slot.snapshot().owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    assert!(matches!(
        slot.drive_quantum(Moment::ORIGIN, Some(&mut transport)),
        Err(EngineError::OwnerFailed(OwnerFailure::OwnerInvariant))
    ));
    Ok(())
}

#[test]
fn direct_slot_recovery_consumes_the_owner_and_returns_the_queued_frame()
-> Result<(), Box<dyn Error>> {
    let mut slot = slot()?;
    let mut transport = TestTransport::benign();
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(50)))
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(3));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, OutboundFrame::copy_from_slice(&[7, 8, 9])?)?;

    let report = slot.recover(OwnerFailure::OwnerInvariant);
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(
        report.operations[0]
            .frame
            .as_ref()
            .map(OutboundFrame::as_bytes),
        Some(&[7, 8, 9][..])
    );
    Ok(())
}

#[test]
fn already_open_establishes_and_applies_policy_exactly_once() -> Result<(), Box<dyn Error>> {
    let mut slot = slot()?;
    let mut transport = TestTransport::sticky_already_open();

    let _progress = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(slot.snapshot().transport, TransportState::Open);
    assert_eq!(transport.policy_applications(), 1);
    let events: Vec<_> = slot.drain_events().collect();
    assert!(matches!(
        events.as_slice(),
        [ConnectionEvent::TransportOpened { .. }]
    ));

    let _progress = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(transport.policy_applications(), 1);
    assert_eq!(slot.drain_events().count(), 0);
    Ok(())
}

#[test]
fn connect_failure_closes_with_connect_diagnostic() -> Result<(), Box<dyn Error>> {
    let mut slot = slot()?;
    let mut transport = TestTransport::connect_failed();

    let _progress = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_failed_connect(
        &mut slot,
        TransportFailurePhase::Connect,
        io::ErrorKind::ConnectionRefused,
    );
    assert_eq!(transport.policy_applications(), 0);
    Ok(())
}

#[test]
fn socket_policy_failure_is_distinct_from_connect_failure() -> Result<(), Box<dyn Error>> {
    let mut slot = slot()?;
    let mut transport = TestTransport::policy_failed();

    let _progress = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_failed_connect(
        &mut slot,
        TransportFailurePhase::SocketPolicy,
        io::ErrorKind::PermissionDenied,
    );
    assert_eq!(transport.policy_applications(), 1);
    Ok(())
}

#[test]
fn safe_write_overreport_fails_closed_without_overstating_not_sent() -> Result<(), Box<dyn Error>> {
    let mut slot = slot()?;
    let mut transport = TestTransport::malicious_write(1);
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(50)))
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(3));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, OutboundFrame::copy_from_slice(&[7, 8, 9])?)?;

    let error = slot
        .drive_quantum(Moment::ORIGIN, Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("malicious write count was accepted"))?;
    assert!(matches!(
        error,
        EngineError::Core(bornera_core::ConnectionCoreError::Invariant(
            bornera_core::ConnectionCoreInvariant::WriteProgressContract {
                written: 4,
                remaining: 3,
            }
        ))
    ));
    assert_eq!(slot.snapshot().owner_failure, Some(OwnerFailure::Core));

    let recovery = slot.recover(OwnerFailure::Core);
    assert_eq!(recovery.operations.len(), 1);
    assert_eq!(
        recovery.operations[0].delivery,
        bornera_core::Delivery::PossiblySent
    );
    assert_eq!(
        recovery.operations[0]
            .frame
            .as_ref()
            .map(OutboundFrame::as_bytes),
        Some(&[7, 8, 9][..])
    );
    Ok(())
}

fn assert_failed_connect(
    slot: &mut bornera::ConnectionSlot<slot_support::Decoder, slot_support::Classifier>,
    phase: TransportFailurePhase,
    kind: io::ErrorKind,
) {
    let snapshot = slot.snapshot();
    assert_eq!(snapshot.transport, TransportState::Closing);
    assert_eq!(
        snapshot.connection.close_reason,
        Some(CloseReason::ConnectFailed)
    );
    assert_eq!(
        snapshot.transport_diagnostic.map(|value| value.phase),
        Some(phase)
    );
    assert_eq!(
        snapshot.transport_diagnostic.map(|value| value.kind),
        Some(kind)
    );
    let events: Vec<_> = slot.drain_events().collect();
    assert!(matches!(
        events.as_slice(),
        [ConnectionEvent::Closing {
            reason: CloseReason::ConnectFailed,
            ..
        }]
    ));
}

fn commit_byte(
    slot: &mut bornera::ConnectionSlot<slot_support::Decoder, slot_support::Classifier>,
    deadline: u64,
    completion: CompletionMode,
    byte: u8,
) -> Result<(), Box<dyn Error>> {
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(deadline)))
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1))
        .completion_mode(completion);
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, OutboundFrame::copy_from_slice(&[byte])?)?;
    Ok(())
}
