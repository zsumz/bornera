//! Narrow protocol-owned classification of reply correlation identities.

use bornera_core::MatchKey;

/// Classifies one complete opaque frame as a correlated reply.
///
/// The first production matcher intentionally accepts only replies. Push
/// traffic remains absent until a second protocol proves its ownership model.
pub trait InboundClassifier<F> {
    /// Protocol-specific classification failure.
    type Error;

    /// Extracts the match key without taking ownership of the complete frame.
    fn reply_key(&mut self, frame: &F) -> Result<MatchKey, Self::Error>;
}
