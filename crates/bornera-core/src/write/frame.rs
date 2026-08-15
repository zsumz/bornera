//! Opaque complete-frame capability required by the bounded writer.

use calandria::RetainedBytes;

/// A complete outbound frame with explicit retained-memory accounting.
///
/// The byte view, its length, and retained footprint must remain stable for
/// the entire period that a `WriteQueue` owns the value.
pub trait WriteFrame {
    /// Returns the contiguous bytes still interpreted only by the transport.
    fn bytes(&self) -> &[u8];

    /// Returns all variable memory retained while this frame remains owned.
    fn retained_bytes(&self) -> RetainedBytes;
}
