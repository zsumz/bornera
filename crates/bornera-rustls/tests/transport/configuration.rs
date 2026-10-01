//! Fail-closed TLS identity, logical-buffer, and retained-charge configuration.

use std::{error::Error, net::SocketAddr, num::NonZeroUsize};

use bornera::{
    ConnectionEvent, TransportFailureKind, TransportFailurePhase, TransportLimits,
    TransportPressure, TransportState,
};
use bornera_core::RetainedBytes;
use bornera_rustls::{
    RustlsConfigError, RustlsConnectError, RustlsDiagnostic, RustlsTransport,
    RustlsTransportConfig, RustlsTransportLimits, RustlsTransportLimitsError,
};

use super::{
    config, drive_until_closed, drive_until_open, drive_until_outcome, far_deadline, owner,
    protocol, tls,
};

#[test]
fn tls12_remains_available_for_kafka_transports() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::with_protocol_versions(&[&rustls::version::TLS12])?;
    let limits = config::transport_limits()?;
    let (clean_sender, clean_receiver) = std::sync::mpsc::channel();
    let server = tls::echo_server(tls.server, clean_sender)?;
    let connector = config::connector(tls.client, limits)?;
    let mut owner = owner::connect(server.address(), connector, limits, 2, 64)?;
    drive_until_open(&mut owner)?;
    owner.open_admission()?;
    let options = bornera_core::OperationOptions::until(far_deadline())
        .retained_bytes(RetainedBytes::new(8))
        .write_retained_bytes(RetainedBytes::new(8));
    let permit = owner.reserve(bornera_core::Moment::ORIGIN, options)?;
    let frame = protocol::request(permit.match_key(), 12);
    let operation = owner.commit(permit, bornera::OutboundFrame::copy_from_slice(&frame)?)?;
    let outcome = drive_until_outcome(&mut owner)?;
    assert_eq!(outcome.operation(), operation);
    assert_eq!(
        outcome.into_outcome(),
        bornera_core::OperationOutcome::Reply(protocol::Frame::from_bytes(frame))
    );
    owner.begin_drain(far_deadline())?;
    drive_until_closed(&mut owner)?;
    clean_receiver.recv_timeout(std::time::Duration::from_secs(2))?;
    server.join()?;
    Ok(())
}

#[test]
fn tls13_cross_epoch_handshake_is_rejected_before_open() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    let server = tls::cross_epoch_server(tls.server)?;
    let connector = config::connector(tls.client, limits)?;
    let mut owner = owner::connect(server.address(), connector, limits, 2, 4_096)?;
    drive_until_closed(&mut owner)?;
    server.join()?;
    let snapshot = owner.snapshot()?;
    assert_eq!(snapshot.transport, TransportState::Closed);
    let diagnostic = snapshot
        .transport_diagnostic
        .ok_or_else(|| std::io::Error::other("hostile TLS peer produced no diagnostic"))?;
    assert_eq!(diagnostic.phase, TransportFailurePhase::Establishment);
    assert_eq!(diagnostic.failure, TransportFailureKind::Protocol);
    assert_eq!(diagnostic.code, Some(RustlsDiagnostic::Protocol.code()));
    assert!(!owner.drain_events()?.any(|event| matches!(
        event,
        ConnectionEvent::TransportOpened { .. } | ConnectionEvent::AdmissionOpened { .. }
    )));
    Ok(())
}

#[test]
fn configuration_rejects_invalid_identity_and_capacity() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    assert_eq!(
        RustlsTransportConfig::for_server_name(tls.client.clone(), "not a name", limits).err(),
        Some(RustlsConfigError::InvalidServerName)
    );
    let configured = RustlsTransportConfig::for_server_name(tls.client, "localhost", limits)?;
    let error = RustlsTransport::connect(
        SocketAddr::from(([127, 0, 0, 1], 1)),
        &configured,
        TransportLimits::new(RetainedBytes::ZERO),
    )
    .err()
    .ok_or_else(|| std::io::Error::other("undersized slot acquired a TLS socket"))?;
    assert!(matches!(
        error,
        RustlsConnectError::Capacity { required, supplied }
            if required == limits.transport_limits().retained_bytes()
                && supplied == RetainedBytes::ZERO
    ));
    assert_eq!(
        limits.transport_limits().retained_bytes(),
        RetainedBytes::new(525_312)
    );
    Ok(())
}

#[test]
fn opaque_retained_categories_require_explicit_charges() -> Result<(), Box<dyn Error>> {
    let one = NonZeroUsize::MIN;
    let missing_inbound = TransportPressure::new(
        RetainedBytes::ZERO,
        RetainedBytes::new(1),
        RetainedBytes::new(2),
        RetainedBytes::new(1),
    )?;
    let missing_protocol = TransportPressure::new(
        RetainedBytes::new(1),
        RetainedBytes::new(1),
        RetainedBytes::new(2),
        RetainedBytes::ZERO,
    )?;
    assert_eq!(
        RustlsTransportLimits::new(one, one, one, missing_inbound).err(),
        Some(RustlsTransportLimitsError::MissingInboundCharge)
    );
    assert_eq!(
        RustlsTransportLimits::new(one, one, one, missing_protocol).err(),
        Some(RustlsTransportLimitsError::MissingProtocolCharge)
    );
    Ok(())
}

#[test]
fn initial_tls_egress_bound_is_checked_before_socket_acquisition() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits_with(1_024, 1, 131_072)?;
    let configured = RustlsTransportConfig::for_server_name(tls.client, "localhost", limits)?;
    let error = RustlsTransport::connect(
        SocketAddr::from(([127, 0, 0, 1], 1)),
        &configured,
        limits.transport_limits(),
    )
    .err()
    .ok_or_else(|| std::io::Error::other("unbounded ClientHello acquired a socket"))?;
    assert!(matches!(
        error,
        RustlsConnectError::Transport(diagnostic)
            if diagnostic.phase == TransportFailurePhase::Establishment
                && diagnostic.failure == TransportFailureKind::Capacity
                && diagnostic.code == Some(RustlsDiagnostic::Capacity.code())
    ));
    Ok(())
}
