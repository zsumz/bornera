//! Formatting and standard error conversions for production-owner failures.

use core::fmt;
use std::io;

use calandria_mio::MioError;

use super::{ConnectError, EngineCommitError, EngineError, EngineInvariant};

impl<E: fmt::Display> fmt::Display for ConnectError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(source) => source.fmt(formatter),
            Self::Mio(source) => source.fmt(formatter),
            Self::Decoder(source) => source.fmt(formatter),
            Self::ResourceAdmission => formatter.write_str("connection set capacity is exhausted"),
            Self::OwnerFailed(_) => formatter.write_str("shared selector owner previously failed"),
            Self::TransportCapacity { .. } => {
                formatter.write_str("transport exceeds its retained-memory bound")
            }
            Self::TransportLimit { .. } => {
                formatter.write_str("transport declares an incompatible retained-memory bound")
            }
        }
    }
}

impl<E> core::error::Error for ConnectError<E> where E: core::error::Error + 'static {}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mio(source) => source.fmt(formatter),
            Self::Core(source) => source.fmt(formatter),
            Self::Invariant(source) => source.fmt(formatter),
            Self::OwnerFailed(_) => formatter.write_str("connection owner previously failed"),
        }
    }
}

impl core::error::Error for EngineError {}

impl fmt::Display for EngineInvariant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResourceToken => "a proven-live transport resource disappeared",
            Self::UnexpectedDiscardEffect => "an internal write discard escaped the aggregate",
            Self::UnexpectedUnitReply => "unit policy transition published a reply",
            Self::DeadlineIndexCapacity => "deadline index exceeded operation capacity",
            Self::DeadlineSchedule(_) => "reserved operation deadline could not be scheduled",
            Self::OutcomePublication(_) => "reserved terminal outcome could not be published",
            Self::RecoveryOutcomePublication(_) => {
                "terminal outcome exceeded bounded recovery ownership"
            }
            Self::LifecyclePublication(_) => "bounded lifecycle event publication failed",
            Self::RecoveryLifecyclePublication(_) => {
                "lifecycle edge exceeded bounded recovery ownership"
            }
            Self::EventSequenceExhausted => "connection event sequence is exhausted",
            Self::MissingCloseReason => "closing connection retained no mechanical reason",
            Self::MissingShutdownDeadline => {
                "draining connection retained no graceful-shutdown deadline"
            }
            Self::UnsupportedCoreEffect => "connection core emitted an unsupported effect",
            Self::TransportReadContract { .. } => {
                "transport reported a read larger than the supplied buffer"
            }
            Self::TransportProgressContract { .. } => {
                "transport progression exceeded its supplied budget"
            }
            Self::TransportOpenedBeforeEstablishment => {
                "transport opened before bounded establishment accepted its policy"
            }
            Self::TransportRetainedCapacity { .. } => {
                "transport exceeded its retained-memory bound"
            }
            Self::TransportLimitContract { .. } => {
                "transport declared an incompatible retained-memory bound"
            }
            Self::TransportNoProgress => {
                "transport advertised immediate work without making progress"
            }
        })
    }
}

impl<F: fmt::Debug> fmt::Display for EngineCommitError<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(source) => source.fmt(formatter),
            Self::AcceptedOwnerFailure { source, .. } => source.fmt(formatter),
            Self::OwnerFailed { .. } => formatter.write_str("connection owner previously failed"),
        }
    }
}

impl<F: fmt::Debug> core::error::Error for EngineCommitError<F> {}

impl<E> From<io::Error> for ConnectError<E> {
    fn from(source: io::Error) -> Self {
        Self::Io(source)
    }
}

impl<E> From<MioError> for ConnectError<E> {
    fn from(source: MioError) -> Self {
        Self::Mio(source)
    }
}

impl From<MioError> for EngineError {
    fn from(source: MioError) -> Self {
        Self::Mio(source)
    }
}
