//! Conservative delivery certainty for terminal operation outcomes.

/// What the local connection can prove after transferring application bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delivery {
    /// No application byte entered irreversible transport write ownership.
    NotSent,
    /// Some or all application bytes entered irreversible transport ownership and may
    /// have reached the peer.
    PossiblySent,
}

impl Delivery {
    /// Weakens certainty once transport write ownership begins.
    #[must_use]
    pub const fn possibly_sent(self) -> Self {
        Self::PossiblySent
    }
}
