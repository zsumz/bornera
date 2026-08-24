//! Recoverable admission rejection and terminal decoder failures.

use core::fmt;

use calandria::RetainedBytes;

/// Why a bounded decoder driver could not admit or decode bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FrameDecodeError<E> {
    /// The decoder already observed a terminal adapter or accounting failure.
    DecoderFailed,
    /// The borrowed input chunk could not fit without exceeding retained bytes.
    RetainedByteCapacity {
        /// Bytes retained before the rejected input.
        retained: RetainedBytes,
        /// Bytes in the rejected borrowed input.
        incoming: usize,
        /// Configured retained-byte maximum.
        limit: RetainedBytes,
    },
    /// The platform input length could not fit the fixed-width byte domain.
    InputSizeOverflow,
    /// The protocol adapter reported a terminal decoder failure.
    Decoder(E),
    /// The adapter retained more memory than the configured mechanical bound.
    RetainedContractViolation {
        /// Retained bytes reported by the adapter.
        retained: RetainedBytes,
        /// Configured retained-byte maximum.
        limit: RetainedBytes,
    },
}

impl<E: fmt::Display> fmt::Display for FrameDecodeError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecoderFailed => formatter.write_str("the frame decoder has already failed"),
            Self::RetainedByteCapacity { .. } => {
                formatter.write_str("input would exceed decoder retained-byte capacity")
            }
            Self::InputSizeOverflow => {
                formatter.write_str("input length exceeds the retained-byte accounting domain")
            }
            Self::Decoder(error) => error.fmt(formatter),
            Self::RetainedContractViolation { .. } => {
                formatter.write_str("decoder violated its retained-byte reporting contract")
            }
        }
    }
}

impl<E: core::error::Error + 'static> core::error::Error for FrameDecodeError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Decoder(error) => Some(error),
            _ => None,
        }
    }
}
