//! Formatting and error sources for construction-time failures.

use core::fmt;

use crate::{
    RustlsConfigError, RustlsConnectError, RustlsServerSessionError, RustlsTransportLimitsError,
};

impl fmt::Display for RustlsTransportLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AddressSpace => "rustls logical buffer exceeds retained-byte address space",
            Self::MissingInboundCharge => "rustls encoded-input charge must be nonzero",
            Self::MissingProtocolCharge => "rustls protocol-state charge must be nonzero",
            Self::EgressCharge { .. } => {
                "rustls outbound charge is smaller than its observable TLS-egress bound"
            }
            Self::PlaintextCharge { .. } => {
                "rustls plaintext charge is smaller than its logical buffer bounds"
            }
        })
    }
}

impl core::error::Error for RustlsTransportLimitsError {}

impl fmt::Display for RustlsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidServerName => formatter.write_str("invalid TLS server name"),
        }
    }
}

impl core::error::Error for RustlsConfigError {}

impl fmt::Display for RustlsConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { .. } => {
                formatter.write_str("rustls transport exceeds the supplied slot capacity")
            }
            Self::Transport(_) => {
                formatter.write_str("initial rustls transport state exceeds its configured bound")
            }
            Self::Tls(source) => source.fmt(formatter),
            Self::Io(source) => source.fmt(formatter),
        }
    }
}

impl core::error::Error for RustlsConnectError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Tls(source) => Some(source),
            Self::Io(source) => Some(source),
            Self::Capacity { .. } | Self::Transport(_) => None,
        }
    }
}

impl fmt::Display for RustlsServerSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { .. } => {
                formatter.write_str("rustls server session exceeds the supplied capacity")
            }
            Self::Transport(_) => {
                formatter.write_str("initial rustls server state exceeds its configured bound")
            }
            Self::Tls(source) => source.fmt(formatter),
        }
    }
}

impl core::error::Error for RustlsServerSessionError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Tls(source) => Some(source),
            Self::Capacity { .. } | Self::Transport(_) => None,
        }
    }
}
