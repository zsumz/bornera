//! Explicit conservative recovery after a production owner cannot continue.

use core::fmt;

use bornera_core::{DiscardedWrite, FrameDecoder, RecoveredOperation};
use calandria::Retained;

use crate::{
    ConnectionEngine, ConnectionEvent, EngineError, EngineOutcome, InboundClassifier, OutboundFrame,
};

/// Mechanical category explaining why normal owner finalization was abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerFailure {
    /// Deterministic aggregate ownership diverged or was poisoned.
    Core,
    /// The readiness backend failed.
    Readiness,
    /// A bounded production-owner invariant failed.
    OwnerInvariant,
}

impl From<&EngineError> for OwnerFailure {
    fn from(error: &EngineError) -> Self {
        match error {
            EngineError::Core(_) => Self::Core,
            EngineError::Mio(_) => Self::Readiness,
            EngineError::Invariant(_) => Self::OwnerInvariant,
            EngineError::OwnerFailed(reason) => *reason,
        }
    }
}

/// Bounded owner contents transferred to the parent after fatal failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryReport<F, R> {
    /// Exact failed socket lifetime.
    pub epoch: bornera_core::ConnectionEpoch,
    /// Mechanical fatal-owner category.
    pub reason: OwnerFailure,
    /// Nonterminal operations in original wire order.
    pub operations: Vec<RecoveredOperation<F>>,
    /// Outbound frames with no exact policy record at recovery.
    pub unmatched_writes: Vec<DiscardedWrite<F>>,
    /// Terminal outcomes published before recovery but not yet drained.
    pub outcomes: Vec<EngineOutcome<R>>,
    /// Lifecycle edges published before recovery but not yet drained.
    pub events: Vec<ConnectionEvent>,
    /// Policy, frame, or transport cleanup ownership disagreed.
    pub ownership_diverged: bool,
}

/// Rejected recovery attempt that still owns the healthy connection engine.
pub struct RecoveryWhileRunning<D, C>
where
    D: FrameDecoder,
{
    engine: Box<ConnectionEngine<D, C>>,
}

impl<D, C> fmt::Debug for RecoveryWhileRunning<D, C>
where
    D: FrameDecoder,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryWhileRunning")
            .finish_non_exhaustive()
    }
}

impl<D, C> RecoveryWhileRunning<D, C>
where
    D: FrameDecoder,
{
    /// Returns immutable access to the still-running owner.
    pub fn engine(&self) -> &ConnectionEngine<D, C> {
        &self.engine
    }

    /// Recovers the still-running owner without closing its capabilities.
    pub fn into_engine(self) -> ConnectionEngine<D, C> {
        *self.engine
    }
}

impl<D, C> ConnectionEngine<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Tries to consume a failed engine and transfer all recoverable ownership.
    ///
    /// A running engine is returned intact in the error variant. A failed fixed
    /// epoch cannot be resumed or reused after successful recovery.
    pub fn try_recover(
        mut self,
    ) -> Result<RecoveryReport<OutboundFrame, D::Frame>, RecoveryWhileRunning<D, C>> {
        let Some(reason) = self.state.failure() else {
            return Err(RecoveryWhileRunning {
                engine: Box::new(self),
            });
        };
        Ok(self.recover_owned(reason))
    }

    /// Explicitly abandons a running owner or recovers one using its latched failure.
    pub fn abandon(mut self, requested: OwnerFailure) -> RecoveryReport<OutboundFrame, D::Frame> {
        let reason = self.state.failure().unwrap_or(requested);
        self.recover_owned(reason)
    }

    fn recover_owned(&mut self, reason: OwnerFailure) -> RecoveryReport<OutboundFrame, D::Frame> {
        drop(self.commands.close());
        self.command_more_pending = false;
        let cleanup_failed = self.close_transport().is_err();
        let recovery = self.core.recover();
        let mut outcomes: Vec<_> = self.outcomes.drain().collect();
        outcomes.extend(self.recovery_outcomes.drain());
        let mut events: Vec<_> = self.lifecycle.drain().collect();
        events.extend(self.recovery_events.drain());
        RecoveryReport {
            epoch: recovery.epoch,
            reason,
            operations: recovery.operations,
            unmatched_writes: recovery.unmatched_writes,
            outcomes,
            events,
            ownership_diverged: recovery.ownership_diverged || cleanup_failed,
        }
    }
}
