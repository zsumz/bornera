//! Conservative fixed rustls charges used by loopback qualification.

use std::{error::Error, num::NonZeroUsize, sync::Arc};

use bornera::TransportPressure;
use bornera_rustls::{RustlsConnector, RustlsTransportConfig, RustlsTransportLimits};
use calandria::RetainedBytes;
use rustls::ClientConfig;

pub(crate) fn transport_limits() -> Result<RustlsTransportLimits, Box<dyn Error>> {
    transport_limits_with(1_024, 131_072, 131_072)
}

pub(crate) fn transport_limits_with(
    writer: usize,
    tls_egress: usize,
    plaintext: usize,
) -> Result<RustlsTransportLimits, Box<dyn Error>> {
    let plaintext_charge = writer
        .checked_add(plaintext)
        .ok_or_else(|| std::io::Error::other("TLS plaintext charge overflowed"))?;
    Ok(RustlsTransportLimits::new(
        nonzero(writer)?,
        nonzero(tls_egress)?,
        nonzero(plaintext)?,
        TransportPressure::new(
            RetainedBytes::new(131_072),
            retained(tls_egress)?,
            retained(plaintext_charge)?,
            RetainedBytes::new(131_072),
        )?,
    )?)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("TLS bound must be nonzero").into())
}

pub(crate) fn connector(
    client: Arc<ClientConfig>,
    limits: RustlsTransportLimits,
) -> Result<RustlsConnector, Box<dyn Error>> {
    connector_for_server_name(client, limits, "localhost")
}

pub(crate) fn connector_for_server_name(
    client: Arc<ClientConfig>,
    limits: RustlsTransportLimits,
    server_name: &str,
) -> Result<RustlsConnector, Box<dyn Error>> {
    let config = RustlsTransportConfig::for_server_name(client, server_name, limits)?;
    Ok(RustlsConnector::new(config))
}

fn retained(value: usize) -> Result<RetainedBytes, Box<dyn Error>> {
    Ok(RetainedBytes::new(u64::try_from(value)?))
}
