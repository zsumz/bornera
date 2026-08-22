//! Public recovery attempts preserve a healthy fixed-epoch owner.

mod support;

use std::{error::Error, net::TcpListener};

use bornera::{OutboundFrame, OwnerFailure, TransportState};
use bornera_core::{Deadline, Moment, OperationOptions, RetainedBytes};

use support::{engine, request};

#[test]
fn healthy_recovery_attempt_returns_the_engine_with_accepted_work() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut engine = engine(listener.local_addr()?)?;
    let permit = engine.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(20)))
            .session()
            .retained_bytes(RetainedBytes::new(8))
            .write_bytes(RetainedBytes::new(8)),
    )?;
    let bytes = request(permit.match_key(), 17);
    let operation = engine.commit(permit, OutboundFrame::copy_from_slice(&bytes)?)?;

    let engine = match engine.try_recover() {
        Ok(_) => return Err(std::io::Error::other("healthy owner was recovered").into()),
        Err(running) => running.into_engine(),
    };
    let snapshot = engine.snapshot();
    assert!(snapshot.owner_failure.is_none());
    assert!(snapshot.commands.receiver_alive());
    assert!(!matches!(snapshot.transport, TransportState::Closed));
    assert_eq!(snapshot.connection.owned_operations, 1);
    assert_eq!(snapshot.queued_write_frames, 1);

    let report = engine.abandon(OwnerFailure::OwnerInvariant);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(report.operations[0].operation, operation);
    assert_eq!(
        report.operations[0]
            .frame
            .as_ref()
            .map(OutboundFrame::as_bytes),
        Some(bytes.as_slice())
    );
    Ok(())
}
