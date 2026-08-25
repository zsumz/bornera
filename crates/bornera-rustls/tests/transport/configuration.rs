//! Fail-closed TLS identity, logical-buffer, and retained-charge configuration.

use std::{error::Error, net::SocketAddr, num::NonZeroUsize};

use bornera::{TransportFailureKind, TransportFailurePhase, TransportLimits, TransportPressure};
use bornera_core::RetainedBytes;
use bornera_rustls::{
    RustlsConfigError, RustlsConnectError, RustlsDiagnostic, RustlsTransport,
    RustlsTransportConfig, RustlsTransportLimits, RustlsTransportLimitsError,
};

use super::{config, tls};

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
