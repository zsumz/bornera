//! Opaque complete-frame capability and one-time admission measurement.

use calandria::RetainedBytes;

/// Wire length and retained memory sampled once when a frame is committed.
///
/// Bornera owns this measurement for the remainder of the frame's lifetime. It
/// never re-samples retained memory or uses a later byte-view length for bounds
/// accounting or write completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameMeasure {
    wire_bytes: usize,
    retained_bytes: RetainedBytes,
}

impl FrameMeasure {
    /// Samples one frame's wire length and retained memory exactly once.
    pub fn capture<F>(frame: &F) -> Self
    where
        F: WriteFrame + ?Sized,
    {
        Self {
            wire_bytes: frame.bytes().len(),
            retained_bytes: frame.retained_bytes(),
        }
    }

    /// Returns the complete wire length observed at commit.
    pub const fn wire_bytes(self) -> usize {
        self.wire_bytes
    }

    /// Returns the retained-memory footprint observed at commit.
    pub const fn retained_bytes(self) -> RetainedBytes {
        self.retained_bytes
    }
}

/// A frame changed the byte-view shape promised at commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FrameContractViolation {
    /// The current byte-view length differs from the committed measurement.
    WireLengthChanged {
        /// Wire length sampled at commit.
        measured: usize,
        /// Wire length observed while borrowing the next write slice.
        observed: usize,
    },
    /// The cached progress range was unavailable in the current byte view.
    WriteRangeUnavailable {
        /// Wire length sampled at commit.
        measured: usize,
        /// Bytes already reported as written.
        written: usize,
        /// Current byte-view length.
        observed: usize,
    },
}

/// A complete outbound frame with explicit retained-memory accounting.
///
/// The byte contents and length must remain stable for the entire period that
/// Bornera owns the value. Retained memory is sampled once at commit. A later
/// byte-length change is rejected explicitly rather than indexed unchecked;
/// same-length content mutation remains a caller contract violation that
/// Bornera cannot mechanically detect without copying the frame.
pub trait WriteFrame {
    /// Returns the contiguous bytes still interpreted only by the transport.
    fn bytes(&self) -> &[u8];

    /// Returns all variable memory retained while this frame remains owned.
    fn retained_bytes(&self) -> RetainedBytes;
}

impl core::fmt::Display for FrameContractViolation {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::WireLengthChanged { .. } => "frame wire length changed after commit",
            Self::WriteRangeUnavailable { .. } => {
                "frame byte view no longer contains the committed write range"
            }
        })
    }
}

impl core::error::Error for FrameContractViolation {}
