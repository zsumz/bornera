//! Stable provider codes for bounded rustls failure projections.

use std::io;

use bornera::{TransportDiagnostic, TransportError, TransportFailureKind, TransportFailurePhase};

/// Stable rustls-specific code retained in a Bornera transport diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[repr(u32)]
pub enum RustlsDiagnostic {
    /// TLS framing, cryptography, alerts, or negotiation failed.
    Protocol = 1,
    /// Peer certificate validation failed.
    Certificate = 2,
    /// The verifier rejected the logical server-name type.
    ServerName = 3,
    /// Raw transport EOF arrived without authenticated TLS closure.
    Truncated = 4,
    /// Rustls logical input or output exceeded its configured bound.
    Capacity = 5,
    /// The caller requested an operation outside the session lifecycle.
    CallerState = 6,
}

impl RustlsDiagnostic {
    /// Returns the stable provider code stored by Bornera.
    pub const fn code(self) -> u32 {
        self as u32
    }

    /// Interprets a stored provider code produced by this crate.
    pub const fn from_code(code: u32) -> Option<Self> {
        match code {
            1 => Some(Self::Protocol),
            2 => Some(Self::Certificate),
            3 => Some(Self::ServerName),
            4 => Some(Self::Truncated),
            5 => Some(Self::Capacity),
            6 => Some(Self::CallerState),
            _ => None,
        }
    }
}

pub(crate) fn protocol_error(
    phase: TransportFailurePhase,
    error: &rustls::Error,
) -> TransportError {
    let (failure, code) = match error {
        rustls::Error::InvalidCertificate(
            rustls::CertificateError::NotValidForName
            | rustls::CertificateError::NotValidForNameContext { .. },
        )
        | rustls::Error::UnsupportedNameType => (
            TransportFailureKind::ServerName,
            RustlsDiagnostic::ServerName,
        ),
        rustls::Error::InvalidCertificate(_)
        | rustls::Error::InvalidCertRevocationList(_)
        | rustls::Error::NoCertificatesPresented => (
            TransportFailureKind::Certificate,
            RustlsDiagnostic::Certificate,
        ),
        _ => (TransportFailureKind::Protocol, RustlsDiagnostic::Protocol),
    };
    TransportError::new(TransportDiagnostic::new(
        phase,
        failure,
        io::ErrorKind::InvalidData,
        Some(code.code()),
    ))
}

pub(crate) fn truncated_error(phase: TransportFailurePhase) -> TransportError {
    TransportError::new(TransportDiagnostic::new(
        phase,
        TransportFailureKind::Truncated,
        io::ErrorKind::UnexpectedEof,
        Some(RustlsDiagnostic::Truncated.code()),
    ))
}

pub(crate) fn caller_state_error(
    phase: TransportFailurePhase,
    kind: io::ErrorKind,
) -> TransportError {
    TransportError::new(TransportDiagnostic::new(
        phase,
        TransportFailureKind::Contract,
        kind,
        Some(RustlsDiagnostic::CallerState.code()),
    ))
}
