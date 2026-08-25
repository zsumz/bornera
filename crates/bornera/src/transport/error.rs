//! Bounded typed failure returned by transport-local progression.

use core::fmt;
use std::io;

use crate::{TransportDiagnostic, TransportFailurePhase};

/// Transport-local progression failure with a bounded retained diagnostic.
#[derive(Debug)]
pub struct TransportError {
    diagnostic: TransportDiagnostic,
    source: Option<io::Error>,
}

impl TransportError {
    /// Retains one provider-neutral diagnostic without an allocated source string.
    pub const fn new(diagnostic: TransportDiagnostic) -> Self {
        Self {
            diagnostic,
            source: None,
        }
    }

    /// Retains one operating-system error and its bounded diagnostic projection.
    pub fn from_io(phase: TransportFailurePhase, source: io::Error) -> Self {
        Self {
            diagnostic: TransportDiagnostic::from_io(phase, &source),
            source: Some(source),
        }
    }

    /// Returns the bounded diagnostic retained by the connection owner.
    pub const fn diagnostic(&self) -> TransportDiagnostic {
        self.diagnostic
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.source.as_ref() {
            Some(source) => source.fmt(formatter),
            None => formatter.write_str("transport progression failed"),
        }
    }
}

impl core::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn core::error::Error + 'static))
    }
}
