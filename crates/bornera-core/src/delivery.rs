//! Conservative delivery certainty for terminal operation outcomes.

/// What the local connection can prove about delivery to the peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delivery {
    /// No byte of the operation entered transport write ownership.
    NotSent,
    /// Some or all bytes may have reached the peer.
    PossiblySent,
}

impl Delivery {
    /// Weakens certainty once transport write ownership begins.
    #[must_use]
    pub const fn possibly_sent(self) -> Self {
        Self::PossiblySent
    }
}
