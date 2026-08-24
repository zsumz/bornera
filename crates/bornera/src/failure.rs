//! Connection-local conversion of transport and adapter faults into core closure.

use bornera_core::{CloseReason, ConnectionInput, FrameDecodeError, FrameDecoder};
use calandria::Retained;

use crate::{ConnectionSlot, EngineError, InboundClassifier};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn close_decode_error(
        &mut self,
        error: &FrameDecodeError<D::Error>,
    ) -> Result<(), EngineError> {
        match error {
            FrameDecodeError::RetainedByteCapacity { .. }
            | FrameDecodeError::InputSizeOverflow
            | FrameDecodeError::RetainedContractViolation { .. } => {
                self.close_for(CloseReason::InboundRetainedCapacity)
            }
            FrameDecodeError::DecoderFailed | FrameDecodeError::Decoder(_) => {
                self.close_malformed()
            }
            _ => self.close_malformed(),
        }
    }

    pub(crate) fn close_malformed(&mut self) -> Result<(), EngineError> {
        let transition = self
            .core
            .apply(ConnectionInput::ReplyMalformed {
                epoch: self.core.epoch(),
            })
            .map_err(EngineError::Core)?;
        self.interpret_unit(transition)
    }

    pub(crate) fn close_for(&mut self, reason: CloseReason) -> Result<(), EngineError> {
        let transition = self
            .core
            .apply(ConnectionInput::CloseRequested {
                epoch: self.core.epoch(),
                reason,
            })
            .map_err(EngineError::Core)?;
        self.interpret_unit(transition)
    }
}
