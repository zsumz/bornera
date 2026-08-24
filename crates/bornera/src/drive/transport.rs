//! Bounded establishment and transport-local control progression.

use bornera_core::{CloseReason, FrameDecoder};
use calandria::Retained;

use crate::{
    ConnectionSlot, EngineError, EngineInvariant, InboundClassifier, SlotTransport,
    TransportBudget, TransportProgress, TransportState,
};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(super) fn drive_transport_once<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        remaining: usize,
    ) -> Result<Option<usize>, EngineError> {
        if self.is_connecting() && transport.is_open() {
            return Err(EngineError::Invariant(
                EngineInvariant::TransportOpenedBeforeEstablishment,
            ));
        }
        if remaining == 0 {
            return Ok(None);
        }
        let budget = TransportBudget::new(
            core::num::NonZeroUsize::MIN,
            self.limits.io_chunk_bytes(),
            self.limits.io_chunk_bytes(),
        );
        if self.is_connecting() {
            if !transport.can_establish() {
                return Ok(None);
            }
            let progress = match transport.drive_establishment(self.socket_policy, budget) {
                Ok(progress) => progress,
                Err(source) => {
                    self.record_transport_failure(source.diagnostic());
                    self.close_for(CloseReason::ConnectFailed)?;
                    return Ok(Some(1));
                }
            };
            Self::validate_transport_progress(budget, progress)?;
            if transport.is_open() {
                self.transport_state = TransportState::Open;
                self.publish_transport_opened()?;
            }
            return Ok(Some(progress.operations()));
        }
        if !transport.has_transport_work() {
            return Ok(None);
        }
        let progress = match transport.drive_transport(budget) {
            Ok(progress) => progress,
            Err(source) => {
                self.record_transport_failure(source.diagnostic());
                self.close_for(CloseReason::TransportLost)?;
                return Ok(Some(1));
            }
        };
        Self::validate_transport_progress(budget, progress)?;
        Ok(Some(progress.operations()))
    }

    fn validate_transport_progress(
        budget: TransportBudget,
        progress: TransportProgress,
    ) -> Result<(), EngineError> {
        if !progress.fits(budget) {
            return Err(EngineError::Invariant(
                EngineInvariant::TransportProgressContract {
                    budget,
                    reported: progress,
                },
            ));
        }
        if progress.is_idle() {
            return Err(EngineError::Invariant(EngineInvariant::TransportNoProgress));
        }
        Ok(())
    }
}
