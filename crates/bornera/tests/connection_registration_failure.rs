//! Pressure observations remain exact across failing selector mutations.

use std::error::Error;

use bornera::{
    ConnectionAccessError, ConnectionSetConfig, EngineError, EngineInvariant, OutboundFrame,
    OwnerFailure, StandaloneConnection, StandaloneConnectionConfig, TransportFailureKind,
    TransportLimits, TransportPressure,
};
use bornera_core::{
    CloseReason, CompletionMode, Deadline, Delivery, Moment, OperationId, OperationOptions,
    RetainedBytes,
};
use calandria::ResourceOwnerId;

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
fn failing_reregistration_retains_post_failure_pressure() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let pressure = output_pressure(5)?;
    let mut set = connection_set(95)?;
    let connection = set.connect_with(
        connection_config(95),
        slot_limits(RetainedBytes::new(4))?,
        Decoder,
        Classifier,
        PressureConnector::new(
            probe.clone(),
            TransportPressure::ZERO,
            PressureScript {
                after_reregistration: Some(pressure),
                fail_reregistration: true,
                ..PressureScript::NONE
            },
        ),
    )?;
    let (operation, frame) = open_and_submit(&mut set, connection)?;

    assert!(matches!(
        set.turn_component(Moment::ORIGIN),
        Err(EngineError::Mio(_))
    ));
    assert_eq!(set.snapshot().owner_failure, Some(OwnerFailure::Readiness));
    assert_eq!(
        set.connection_snapshot(connection)?.transport_pressure,
        Some(pressure)
    );
    assert_eq!(
        probe.connector_limits(),
        Some(TransportLimits::new(RetainedBytes::new(4)))
    );
    assert_eq!(probe.registrations(), 1);
    let report = set.try_recover(connection)?;
    assert_eq!(report.reason, OwnerFailure::Readiness);
    assert_recovered(report.operations.as_slice(), operation, &frame);
    assert_eq!(report.transport_pressure, Some(pressure));
    assert_eq!(probe.reregistrations(), 1);
    assert_eq!(probe.deregistrations(), 1);
    Ok(())
}

#[test]
fn failed_settlement_and_recovery_deregistration_are_resampled() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let pressure = output_pressure(6)?;
    let mut set = connection_set(96)?;
    let connection = set.connect_with(
        connection_config(96),
        slot_limits(RetainedBytes::new(4))?,
        Decoder,
        Classifier,
        PressureConnector::new(
            probe.clone(),
            TransportPressure::ZERO,
            PressureScript {
                after_deregistration: Some(pressure),
                limit_after_deregistration: Some(TransportLimits::new(RetainedBytes::new(3))),
                fail_deregistration: true,
                ..PressureScript::NONE
            },
        ),
    )?;

    assert!(matches!(
        set.finalize(connection, CloseReason::Requested),
        Err(ConnectionAccessError::Owner(EngineError::Mio(_)))
    ));
    assert_eq!(
        set.connection_snapshot(connection)?.transport_pressure,
        Some(pressure)
    );
    let report = set.try_recover(connection)?;
    assert_eq!(report.transport_pressure, Some(pressure));
    assert_eq!(report.transport_retained_limit, Some(RetainedBytes::new(4)));
    assert!(report.ownership_diverged);
    assert_eq!(probe.deregistrations(), 2);
    Ok(())
}

#[test]
fn successful_deregistration_cannot_hide_pressure_overflow() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let pressure = output_pressure(5)?;
    let mut set = connection_set(98)?;
    let connection = set.connect_with(
        connection_config(98),
        slot_limits(RetainedBytes::new(4))?,
        Decoder,
        Classifier,
        PressureConnector::new(
            probe.clone(),
            TransportPressure::ZERO,
            PressureScript {
                after_deregistration: Some(pressure),
                ..PressureScript::NONE
            },
        ),
    )?;

    assert!(matches!(
        set.finalize(connection, CloseReason::Requested),
        Err(ConnectionAccessError::Owner(EngineError::Invariant(_)))
    ));
    let report = set.try_recover(connection)?;
    assert_eq!(report.transport_pressure, Some(pressure));
    assert!(report.ownership_diverged);
    assert_eq!(probe.deregistrations(), 1);
    Ok(())
}

#[test]
fn successful_deregistration_cannot_change_the_bound_limit() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let mut set = connection_set(99)?;
    let connection = set.connect_with(
        connection_config(99),
        slot_limits(RetainedBytes::new(4))?,
        Decoder,
        Classifier,
        PressureConnector::new(
            probe.clone(),
            TransportPressure::ZERO,
            PressureScript {
                limit_after_deregistration: Some(TransportLimits::new(RetainedBytes::new(3))),
                ..PressureScript::NONE
            },
        ),
    )?;

    assert!(matches!(
        set.finalize(connection, CloseReason::Requested),
        Err(ConnectionAccessError::Owner(EngineError::Invariant(
            EngineInvariant::TransportLimitContract { limit, reported }
        ))) if limit == RetainedBytes::new(4) && reported == RetainedBytes::new(3)
    ));
    let report = set.try_recover(connection)?;
    assert_eq!(report.transport_retained_limit, Some(RetainedBytes::new(4)));
    assert_eq!(
        report.transport_diagnostic.map(|value| value.failure),
        Some(TransportFailureKind::Contract)
    );
    assert!(report.ownership_diverged);
    assert_eq!(probe.deregistrations(), 1);
    Ok(())
}

#[test]
fn standalone_failed_recovery_deregisters_once_before_reporting() -> Result<(), Box<dyn Error>> {
    let probe = RegistrationProbe::new();
    let overflow = output_pressure(5)?;
    let recovered = output_pressure(6)?;
    let config = StandaloneConnectionConfig::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(97)),
        connection_config(97),
    );
    let limits = slot_limits(RetainedBytes::new(4))?;
    let connector = PressureConnector::new(
        probe.clone(),
        TransportPressure::ZERO,
        PressureScript {
            after_reregistration: Some(overflow),
            after_deregistration: Some(recovered),
            fail_deregistration: true,
            ..PressureScript::NONE
        },
    );
    let mut owner =
        StandaloneConnection::connect_with(config, limits, Decoder, Classifier, connector)?;
    let token = owner.token();
    let _open = owner.turn_component(Moment::ORIGIN)?;
    owner.open_admission()?;
    let permit = owner.reserve(Moment::ORIGIN, options())?;
    let operation = owner.commit(permit, OutboundFrame::copy_from_slice(&[1, 2, 3])?)?;

    assert!(matches!(
        owner.turn_component(Moment::ORIGIN),
        Err(EngineError::OwnerFailed(OwnerFailure::OwnerInvariant))
    ));
    assert_eq!(owner.token(), token);
    let report = owner
        .try_recover()
        .map_err(|_| std::io::Error::other("failed standalone owner rejected recovery"))?;
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(report.operations[0].operation, operation);
    assert_eq!(report.transport_pressure, Some(recovered));
    assert!(report.ownership_diverged);
    assert_eq!(probe.deregistrations(), 1);
    Ok(())
}

fn open_and_submit(
    set: &mut support::RegistrationSet<Decoder, Classifier>,
    connection: bornera::ConnectionToken,
) -> Result<(OperationId, OutboundFrame), Box<dyn Error>> {
    let _open = set.turn_component(Moment::ORIGIN)?;
    set.open_admission(connection)?;
    let frame = OutboundFrame::copy_from_slice(&[1, 2, 3])?;
    let permit = set.reserve(connection, Moment::ORIGIN, options())?;
    let operation = set.commit(connection, permit, frame.clone())?;
    Ok((operation, frame))
}

fn options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(100)))
        .completion_mode(CompletionMode::ReplyExpected)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(3))
}

fn output_pressure(bytes: u64) -> Result<TransportPressure, Box<dyn Error>> {
    Ok(TransportPressure::new(
        RetainedBytes::ZERO,
        RetainedBytes::new(bytes),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?)
}

fn assert_recovered(
    operations: &[bornera_core::RecoveredOperation<OutboundFrame>],
    operation: OperationId,
    frame: &OutboundFrame,
) {
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].operation, operation);
    assert_eq!(operations[0].delivery, Delivery::NotSent);
    assert_eq!(operations[0].frame.as_ref(), Some(frame));
}
