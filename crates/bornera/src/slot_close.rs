//! Physical transport closure for one selector-free connection slot.

use bornera_core::{ConnectionInput, FrameDecoder};
use calandria::Retained;

use crate::{
    CloseDirective, ConnectionSlot, EngineError, InboundClassifier, TransportState,
    slot::ShutdownState,
};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn take_close_request(&mut self) -> Option<CloseDirective> {
        self.close_request
            .is_some_and(CloseDirective::settlement_ready)
            .then(|| self.close_request.take())
            .flatten()
    }

    pub(crate) fn restore_close_request(&mut self, directive: CloseDirective) {
        self.close_request = Some(directive);
    }

    pub(crate) fn abort_transport(&mut self) {
        self.transport_state = TransportState::Closed;
        self.drain_deadline = None;
        self.close_request = None;
    }

    /// Confirms that the owner released its physical capability after a close request.
    ///
    /// Returns `false` when no settle-ready close request was pending. A graceful
    /// request becomes settle-ready only after bounded shutdown progression completes
    /// or reaches its deadline. Publication failure latches the slot and conservatively
    /// aborts its transport state.
    pub fn settle_transport_closed(&mut self) -> bool {
        let Some(directive) = self.take_close_request() else {
            return false;
        };
        match directive {
            CloseDirective::Abort => self.abort_transport(),
            CloseDirective::Core {
                reason,
                shutdown: ShutdownState::Immediate | ShutdownState::Complete,
            } => {
                if let Err(error) = self.confirm_transport_closed(reason) {
                    self.latch_failure(&error);
                    self.abort_transport();
                }
            }
            CloseDirective::Core { .. } => return false,
        }
        true
    }

    fn confirm_transport_closed(
        &mut self,
        reason: bornera_core::CloseReason,
    ) -> Result<(), EngineError> {
        self.transport_state = TransportState::Closed;
        let transition = self
            .core
            .apply(ConnectionInput::EpochClosed {
                epoch: self.core.epoch(),
            })
            .map_err(EngineError::Core)?;
        self.interpret_unit(transition)?;
        self.publish_closed(reason)
    }
}
