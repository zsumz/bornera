//! Bounded publication of mechanical connection lifecycle edges.

use bornera_core::{CloseReason, FrameDecoder};
use calandria::Retained;

use crate::{ConnectionEngine, ConnectionEvent, EngineError, EngineInvariant, InboundClassifier};

impl<D, C> ConnectionEngine<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn publish_transport_opened(&mut self) -> Result<(), EngineError> {
        let sequence = self.next_event_sequence()?;
        self.publish_event(ConnectionEvent::TransportOpened {
            sequence,
            epoch: self.core.epoch(),
        })
    }

    pub(crate) fn publish_admission_opened(&mut self) -> Result<(), EngineError> {
        let sequence = self.next_event_sequence()?;
        self.publish_event(ConnectionEvent::AdmissionOpened {
            sequence,
            epoch: self.core.epoch(),
        })
    }

    pub(crate) fn publish_closing(&mut self, reason: CloseReason) -> Result<(), EngineError> {
        let sequence = self.next_event_sequence()?;
        self.publish_event(ConnectionEvent::Closing {
            sequence,
            epoch: self.core.epoch(),
            reason,
        })
    }

    pub(crate) fn publish_closed(&mut self, reason: CloseReason) -> Result<(), EngineError> {
        let sequence = self.next_event_sequence()?;
        self.publish_event(ConnectionEvent::Closed {
            sequence,
            epoch: self.core.epoch(),
            reason,
        })
    }

    fn next_event_sequence(&self) -> Result<u64, EngineError> {
        self.event_sequence
            .checked_add(1)
            .ok_or(EngineError::Invariant(
                EngineInvariant::EventSequenceExhausted,
            ))
    }

    fn publish_event(&mut self, event: ConnectionEvent) -> Result<(), EngineError> {
        if let Err(error) = self.lifecycle.try_push(event) {
            let (event, failure) = error.into_parts();
            self.event_sequence = event.sequence();
            self.recovery_events.try_push(event).map_err(|error| {
                EngineError::Invariant(EngineInvariant::RecoveryLifecyclePublication(
                    error.failure(),
                ))
            })?;
            return Err(EngineError::Invariant(
                EngineInvariant::LifecyclePublication(failure),
            ));
        }
        self.event_sequence = event.sequence();
        Ok(())
    }
}
