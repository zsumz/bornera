//! Set-wide fail-closed handling for an uncertain selector owner.

use bornera_core::FrameDecoder;
use calandria::{PollReport, Retained, Span};
use calandria_mio::MioError;

use crate::{ConnectionSet, EngineError, InboundClassifier, OwnerFailure};

impl<D, C> ConnectionSet<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn poll_selector(&mut self, maximum: Span) -> Result<PollReport, EngineError> {
        self.ensure_owner_running()?;
        let result = self.poller.poll(maximum, &mut self.poll_events);
        self.resolve_selector_poll(result)
    }

    pub(crate) fn ensure_owner_running(&self) -> Result<(), EngineError> {
        self.owner_failure
            .map_or(Ok(()), |reason| Err(EngineError::OwnerFailed(reason)))
    }

    pub(crate) fn latch_readiness_error(&mut self, error: &EngineError) {
        if let EngineError::Mio(source) = error {
            self.latch_selector_failure(source);
        }
    }

    pub(crate) fn latch_selector_failure(&mut self, source: &MioError) {
        if self.owner_failure.is_some() {
            return;
        }
        let reason = OwnerFailure::Readiness;
        self.owner_failure = Some(reason);
        drop(self.commands.close());
        self.ready.clear();
        self.scan.clear();
        self.scan
            .extend(self.resources.iter().map(|(token, _, _)| token));
        for token in self.scan.iter().copied() {
            let Ok((_, entry)) = self.resources.get_mut(token) else {
                continue;
            };
            if let MioError::Io(error) = source {
                entry
                    .slot
                    .record_transport_failure(crate::TransportDiagnostic::from_io(
                        crate::TransportFailurePhase::Readiness,
                        error,
                    ));
            }
            entry.slot.latch_owner_failure(reason);
            entry.ready_queued = false;
        }
    }

    pub(crate) fn resolve_selector_poll(
        &mut self,
        result: Result<PollReport, MioError>,
    ) -> Result<PollReport, EngineError> {
        match result {
            Ok(report) => Ok(report),
            Err(source) => {
                self.latch_selector_failure(&source);
                Err(EngineError::Mio(source))
            }
        }
    }
}
