//! Observable result of applying one machine command or input.

use crate::ConnectionEffect;

/// Whether an input changed the current epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputDisposition {
    /// The input changed current state or emitted a required effect.
    Applied,
    /// An old epoch produced the input; current state was untouched.
    IgnoredStaleEpoch,
    /// No retained slot owns the named operation.
    IgnoredUnknownOperation,
    /// The effect does not own the named operation's write.
    IgnoredStaleEffect,
    /// The operation already emitted its terminal outcome.
    AlreadyTerminal,
    /// The input is not valid in the operation or connection phase.
    IgnoredInvalidPhase,
    /// Current-epoch input poisoned the epoch and forced closure.
    Fault,
}

/// Result of explicit cancellation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    /// Queued work was removed before transport write ownership.
    CancelledNotSent,
    /// Local observation ended, but the peer may have received the work.
    ObservationCancelled {
        /// Conservative delivery certainty.
        delivery: crate::Delivery,
    },
    /// The operation was already terminal or is no longer retained.
    AlreadyTerminal,
}

/// Data-only effects and classification produced by one state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionTransition<F = ()> {
    disposition: InputDisposition,
    cancel_outcome: Option<CancelOutcome>,
    effects: Vec<ConnectionEffect<F>>,
}

impl<F> ConnectionTransition<F> {
    pub(crate) fn new(disposition: InputDisposition) -> Self {
        Self {
            disposition,
            cancel_outcome: None,
            effects: Vec::new(),
        }
    }

    pub(crate) fn cancelled(mut self, outcome: CancelOutcome) -> Self {
        self.cancel_outcome = Some(outcome);
        self
    }

    pub(crate) fn push(&mut self, effect: ConnectionEffect<F>) {
        self.effects.push(effect);
    }

    /// Returns how the machine classified the input.
    pub const fn disposition(&self) -> InputDisposition {
        self.disposition
    }

    /// Returns the explicit cancellation result, when applicable.
    pub const fn cancel_outcome(&self) -> Option<CancelOutcome> {
        self.cancel_outcome
    }

    /// Returns emitted effects in deterministic owner order.
    pub fn effects(&self) -> &[ConnectionEffect<F>] {
        &self.effects
    }

    /// Transfers the emitted effects to the caller.
    pub fn into_effects(self) -> Vec<ConnectionEffect<F>> {
        self.effects
    }
}
