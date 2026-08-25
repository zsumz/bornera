//! Capacity-first registered-transport construction contracts.

use std::{error::Error, io};

use bornera::{
    ConnectError, ConnectionAccessError, ConnectionEvent, OutboundFrame, OwnerFailure,
    TransportFailureKind, TransportFailurePhase, TransportLimits, TransportPressure,
};
use bornera_core::{
    CompletionMode, ConnectionEpoch, Deadline, Delivery, InputDisposition, Moment,
    OperationOptions, RetainedBytes,
};

#[path = "common/registration_protocol.rs"]
mod protocol;
#[path = "common/registration_probe.rs"]
mod registration_probe;
#[path = "common/registration_transport.rs"]
mod support;
use protocol::{Classifier, Decoder};
use registration_probe::RegistrationProbe;
use support::{PressureConnector, PressureScript, connection_config, connection_set, slot_limits};

#[test]
fn incompatible_initial_adapter_limit_rejects_before_registration() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let mut set = connection_set(89)?;
    let error = set
        .connect_with(
            connection_config(89),
            slot_limits(RetainedBytes::new(4))?,
            Decoder,
            Classifier,
            PressureConnector::new(
                probe.clone(),
                TransportPressure::ZERO,
                PressureScript {
                    declared_limit: Some(TransportLimits::new(RetainedBytes::new(5))),
                    ..PressureScript::NONE
                },
            ),
        )
        .err()
        .ok_or_else(|| io::Error::other("incompatible adapter limit was registered"))?;
    assert!(matches!(
        error,
        ConnectError::TransportLimit { limit, reported }
            if limit == RetainedBytes::new(4) && reported == RetainedBytes::new(5)
    ));
    assert_eq!(probe.registrations(), 0);
    assert_eq!(probe.deregistrations(), 0);
    assert_eq!(set.snapshot().connections.active(), 0);
    Ok(())
}

#[test]
fn registration_cannot_change_the_bound_adapter_limit() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let mut set = connection_set(90)?;
    let error = set
        .connect_with(
            connection_config(90),
            slot_limits(RetainedBytes::new(4))?,
            Decoder,
            Classifier,
            PressureConnector::new(
                probe.clone(),
                TransportPressure::ZERO,
                PressureScript {
                    limit_after_registration: Some(TransportLimits::new(RetainedBytes::new(3))),
                    ..PressureScript::NONE
                },
            ),
        )
        .err()
        .ok_or_else(|| io::Error::other("registration changed the adapter limit"))?;
    assert!(matches!(
        error,
        ConnectError::TransportLimit { limit, reported }
            if limit == RetainedBytes::new(4) && reported == RetainedBytes::new(3)
    ));
    assert_eq!(probe.registrations(), 1);
    assert_eq!(probe.deregistrations(), 1);
    assert_eq!(set.snapshot().connections.active(), 0);
    assert_eq!(set.snapshot().poller.registrations(), 0);
    Ok(())
}

#[test]
fn excess_initial_pressure_rejects_before_selector_registration() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let pressure = TransportPressure::new(
        RetainedBytes::new(3),
        RetainedBytes::new(2),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut set = connection_set(91)?;
    let error = set
        .connect_with(
            connection_config(91),
            slot_limits(RetainedBytes::new(4))?,
            Decoder,
            Classifier,
            PressureConnector::new(probe.clone(), pressure, PressureScript::NONE),
        )
        .err()
        .ok_or_else(|| io::Error::other("excess-pressure transport was registered"))?;
    assert!(matches!(
        error,
        ConnectError::TransportCapacity {
            limit,
            reported,
        } if limit == RetainedBytes::new(4) && reported == pressure
    ));
    assert_eq!(probe.registrations(), 0);
    assert_eq!(probe.deregistrations(), 0);
    assert_eq!(set.snapshot().connections.active(), 0);
    assert_eq!(set.snapshot().connections.vacant(), 1);
    Ok(())
}

#[test]
fn registration_pressure_growth_is_deregistered_and_rejected() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let pressure = TransportPressure::new(
        RetainedBytes::ZERO,
        RetainedBytes::new(5),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut set = connection_set(92)?;
    let error = set
        .connect_with(
            connection_config(92),
            slot_limits(RetainedBytes::new(4))?,
            Decoder,
            Classifier,
            PressureConnector::new(
                probe.clone(),
                TransportPressure::ZERO,
                PressureScript {
                    after_registration: Some(pressure),
                    ..PressureScript::NONE
                },
            ),
        )
        .err()
        .ok_or_else(|| io::Error::other("registration pressure growth was accepted"))?;
    assert!(matches!(
        error,
        ConnectError::TransportCapacity { reported, .. } if reported == pressure
    ));
    assert_eq!(probe.registrations(), 1);
    assert_eq!(probe.reregistrations(), 0);
    assert_eq!(probe.deregistrations(), 1);
    assert_eq!(set.snapshot().connections.active(), 0);
    Ok(())
}

#[test]
fn within_limit_registration_pressure_is_cached() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let pressure = TransportPressure::new(
        RetainedBytes::new(1),
        RetainedBytes::new(2),
        RetainedBytes::ZERO,
        RetainedBytes::new(1),
    )?;
    let mut set = connection_set(93)?;
    let connection = set.connect_with(
        connection_config(93),
        slot_limits(RetainedBytes::new(4))?,
        Decoder,
        Classifier,
        PressureConnector::new(
            probe.clone(),
            TransportPressure::ZERO,
            PressureScript {
                after_registration: Some(pressure),
                ..PressureScript::NONE
            },
        ),
    )?;

    assert_eq!(
        set.connection_snapshot(connection)?.transport_pressure,
        Some(pressure)
    );
    assert_eq!(
        probe.connector_limits(),
        Some(TransportLimits::new(RetainedBytes::new(4)))
    );
    assert_eq!(probe.registrations(), 1);
    assert_eq!(probe.reregistrations(), 0);
    assert_eq!(probe.deregistrations(), 0);
    drop(set);
    assert_eq!(probe.deregistrations(), 1);
    Ok(())
}

#[test]
fn runtime_reregistration_pressure_overflow_is_exactly_recoverable() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let pressure = TransportPressure::new(
        RetainedBytes::new(2),
        RetainedBytes::new(3),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?;
    let mut set = connection_set(94)?;
    let connection = set.connect_with(
        connection_config(94),
        slot_limits(RetainedBytes::new(4))?,
        Decoder,
        Classifier,
        PressureConnector::new(
            probe.clone(),
            TransportPressure::ZERO,
            PressureScript {
                after_reregistration: Some(pressure),
                ..PressureScript::NONE
            },
        ),
    )?;

    let _open = set.turn_component(Moment::ORIGIN)?;
    assert!(set.is_transport_open(connection)?);
    assert_eq!(probe.reregistrations(), 0);
    assert_eq!(set.open_admission(connection)?, InputDisposition::Applied);
    let expected_frame = OutboundFrame::copy_from_slice(&[1, 2, 3])?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(100)))
        .completion_mode(CompletionMode::ReplyExpected)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(3));
    let permit = set.reserve(connection, Moment::ORIGIN, options)?;
    let operation = set.commit(connection, permit, expected_frame.clone())?;
    assert_eq!(probe.reregistrations(), 0);

    let _failed = set.turn_component(Moment::ORIGIN)?;
    let snapshot = set.connection_snapshot(connection)?;
    assert_eq!(snapshot.owner_failure, Some(OwnerFailure::OwnerInvariant));
    assert_eq!(snapshot.transport_pressure, Some(pressure));
    assert_eq!(set.snapshot().owner_failure, None);
    assert_eq!(probe.registrations(), 1);
    assert_eq!(probe.reregistrations(), 1);

    let report = set.try_recover(connection)?;
    assert_eq!(report.epoch, ConnectionEpoch::new(1));
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(report.operations[0].operation, operation);
    assert_eq!(report.operations[0].delivery, Delivery::NotSent);
    assert_eq!(report.operations[0].frame.as_ref(), Some(&expected_frame));
    assert!(report.unmatched_writes.is_empty());
    assert!(report.outcomes.is_empty());
    assert_eq!(
        report.events,
        [
            ConnectionEvent::TransportOpened {
                sequence: 1,
                epoch: ConnectionEpoch::new(1),
            },
            ConnectionEvent::AdmissionOpened {
                sequence: 2,
                epoch: ConnectionEpoch::new(1),
            },
        ]
    );
    assert!(matches!(
        report.transport_diagnostic,
        Some(diagnostic)
            if diagnostic.phase == TransportFailurePhase::Pressure
                && diagnostic.failure == TransportFailureKind::Capacity
    ));
    assert_eq!(report.transport_pressure, Some(pressure));
    assert_eq!(report.transport_retained_limit, Some(RetainedBytes::new(4)));
    assert!(report.ownership_diverged);
    assert_eq!(probe.deregistrations(), 1);
    assert_eq!(set.snapshot().connections.active(), 0);
    assert_eq!(set.snapshot().poller.registrations(), 0);
    assert!(matches!(
        set.connection_snapshot(connection),
        Err(ConnectionAccessError::StaleConnection)
    ));
    Ok(())
}
