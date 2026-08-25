//! Immutable rustls connection configuration and accounted memory bounds.

use std::{io, num::NonZeroUsize, sync::Arc};

use bornera::{TransportLimits, TransportPressure};
use calandria::RetainedBytes;
use rustls::{ClientConfig, pki_types::ServerName};

/// Stable per-connection charges for rustls-owned variable memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RustlsTransportLimits {
    application_write_buffer_bytes: NonZeroUsize,
    max_tls_egress_bytes: NonZeroUsize,
    max_plaintext_bytes: NonZeroUsize,
    pressure: TransportPressure,
}

impl RustlsTransportLimits {
    /// Creates mechanically enforced logical-buffer bounds and one stable memory charge.
    ///
    /// The application-write bound is applied through rustls's buffer limit. The
    /// egress and readable-plaintext bounds are checked after every rustls state
    /// transition. `pressure` is the caller-audited conservative charge for rustls's
    /// private allocation capacities and per-connection cryptographic state; rustls
    /// does not expose those capacities for mechanical measurement. The outbound and
    /// plaintext charges must cover their logical maxima, while inbound and protocol
    /// charges must cover the exact rustls version, provider, and client configuration.
    pub fn new(
        application_write_buffer_bytes: NonZeroUsize,
        max_tls_egress_bytes: NonZeroUsize,
        max_plaintext_bytes: NonZeroUsize,
        pressure: TransportPressure,
    ) -> Result<Self, RustlsTransportLimitsError> {
        if pressure.inbound() == RetainedBytes::ZERO {
            return Err(RustlsTransportLimitsError::MissingInboundCharge);
        }
        if pressure.protocol() == RetainedBytes::ZERO {
            return Err(RustlsTransportLimitsError::MissingProtocolCharge);
        }
        let application_write = u64::try_from(application_write_buffer_bytes.get())
            .map(RetainedBytes::new)
            .map_err(|_| RustlsTransportLimitsError::AddressSpace)?;
        let readable_plaintext = u64::try_from(max_plaintext_bytes.get())
            .map(RetainedBytes::new)
            .map_err(|_| RustlsTransportLimitsError::AddressSpace)?;
        let Some(minimum_plaintext) = application_write.checked_add(readable_plaintext) else {
            return Err(RustlsTransportLimitsError::AddressSpace);
        };
        if pressure.plaintext() < minimum_plaintext {
            return Err(RustlsTransportLimitsError::PlaintextCharge {
                minimum: minimum_plaintext,
                supplied: pressure.plaintext(),
            });
        }
        let max_tls_egress = u64::try_from(max_tls_egress_bytes.get())
            .map(RetainedBytes::new)
            .map_err(|_| RustlsTransportLimitsError::AddressSpace)?;
        if pressure.outbound() < max_tls_egress {
            return Err(RustlsTransportLimitsError::EgressCharge {
                minimum: max_tls_egress,
                supplied: pressure.outbound(),
            });
        }
        Ok(Self {
            application_write_buffer_bytes,
            max_tls_egress_bytes,
            max_plaintext_bytes,
            pressure,
        })
    }

    /// Returns the hard rustls application-write buffer limit.
    pub const fn application_write_buffer_bytes(self) -> NonZeroUsize {
        self.application_write_buffer_bytes
    }

    /// Returns the maximum observable encrypted output retained by rustls.
    pub const fn max_tls_egress_bytes(self) -> NonZeroUsize {
        self.max_tls_egress_bytes
    }

    /// Returns the maximum observable application plaintext retained by rustls.
    pub const fn max_plaintext_bytes(self) -> NonZeroUsize {
        self.max_plaintext_bytes
    }

    /// Returns the stable caller-audited rustls retained-memory charge.
    pub const fn pressure(self) -> TransportPressure {
        self.pressure
    }

    /// Returns the exact aggregate charge advertised to Bornera.
    pub const fn transport_limits(self) -> TransportLimits {
        TransportLimits::new(self.pressure.total())
    }
}

/// Invalid rustls transport-memory accounting configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RustlsTransportLimitsError {
    /// The platform pointer width cannot fit the fixed retained-byte domain.
    AddressSpace,
    /// No conservative charge was supplied for rustls's private encoded-input storage.
    MissingInboundCharge,
    /// No conservative charge was supplied for rustls and provider protocol state.
    MissingProtocolCharge,
    /// The configured outbound charge cannot cover the observable TLS-egress bound.
    EgressCharge {
        /// Minimum charge required by the configured logical bound.
        minimum: RetainedBytes,
        /// Outbound charge supplied by the caller.
        supplied: RetainedBytes,
    },
    /// The configured plaintext charge cannot cover both plaintext buffer bounds.
    PlaintextCharge {
        /// Minimum charge required by the configured logical bounds.
        minimum: RetainedBytes,
        /// Plaintext charge supplied by the caller.
        supplied: RetainedBytes,
    },
}

/// Immutable configuration for one logical TLS server identity.
#[derive(Clone, Debug)]
pub struct RustlsTransportConfig {
    client: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    limits: RustlsTransportLimits,
}

impl RustlsTransportConfig {
    /// Creates configuration from an already validated owned server name.
    pub fn new(
        client: Arc<ClientConfig>,
        server_name: ServerName<'static>,
        limits: RustlsTransportLimits,
    ) -> Self {
        Self {
            client,
            server_name,
            limits,
        }
    }

    /// Validates and owns one DNS name or IP address for certificate verification and SNI.
    pub fn for_server_name(
        client: Arc<ClientConfig>,
        server_name: &str,
        limits: RustlsTransportLimits,
    ) -> Result<Self, RustlsConfigError> {
        let server_name = ServerName::try_from(server_name.to_owned())
            .map_err(|_| RustlsConfigError::InvalidServerName)?;
        Ok(Self::new(client, server_name, limits))
    }

    /// Returns the shared rustls client configuration.
    pub fn client_config(&self) -> &Arc<ClientConfig> {
        &self.client
    }

    /// Returns the owned logical server identity.
    pub const fn server_name(&self) -> &ServerName<'static> {
        &self.server_name
    }

    /// Returns the exact per-connection transport limits.
    pub const fn limits(&self) -> RustlsTransportLimits {
        self.limits
    }
}

/// Invalid logical TLS configuration established before socket acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RustlsConfigError {
    /// The logical server identity is not a valid DNS name or IP address.
    InvalidServerName,
}

/// Failure before one rustls transport becomes registered or observable.
#[derive(Debug)]
#[non_exhaustive]
pub enum RustlsConnectError {
    /// The configured adapter charge exceeds the Bornera slot ceiling.
    Capacity {
        /// Charge required by this rustls adapter configuration.
        required: RetainedBytes,
        /// Ceiling supplied by the exact Bornera connection slot.
        supplied: RetainedBytes,
    },
    /// Rustls's initial logical buffers exceeded their configured bound.
    Transport(bornera::TransportDiagnostic),
    /// Rustls rejected construction of the client connection.
    Tls(rustls::Error),
    /// The operating system rejected creation of the nonblocking TCP stream.
    Io(io::Error),
}
