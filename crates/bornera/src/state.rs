//! Fail-closed production-owner state after the first fatal error.

use bornera_core::FrameDecoder;

use crate::{CloseDirective, ConnectionSlot, EngineError, InboundClassifier, OwnerFailure};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EngineState {
    Running,
    Failed(OwnerFailure),
}

impl EngineState {
    pub(crate) const fn failure(self) -> Option<OwnerFailure> {
        match self {
            Self::Running => None,
            Self::Failed(reason) => Some(reason),
        }
    }
}

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: calandria::Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn ensure_running(&self) -> Result<(), EngineError> {
        self.state
            .failure()
            .map_or(Ok(()), |reason| Err(EngineError::OwnerFailed(reason)))
    }

    pub(crate) fn latch<T>(&mut self, result: Result<T, EngineError>) -> Result<T, EngineError> {
        if let Err(error) = &result {
            self.latch_failure(error);
        }
        result
    }

    pub(crate) fn latch_failure(&mut self, error: &EngineError) {
        if self.state.failure().is_some() || matches!(error, EngineError::OwnerFailed(_)) {
            return;
        }
        self.latch_owner_failure(OwnerFailure::from(error));
    }

    pub(crate) fn latch_owner_failure(&mut self, reason: OwnerFailure) {
        if self.state.failure().is_some() {
            return;
        }
        self.state = EngineState::Failed(reason);
        self.drain_deadline = None;
        self.close_request = Some(CloseDirective::Abort);
    }
}
