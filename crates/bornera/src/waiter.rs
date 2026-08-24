//! Calandria waiting delegated to the shared Mio selector.

use bornera_core::FrameDecoder;
use calandria::{Retained, Span, WaitOutcome, Waiter};

use crate::{ConnectionSet, EngineError, InboundClassifier, StandaloneConnection};

/// Waiter that polls readiness into a bounded connection set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionWaiter;

impl<D, C> Waiter<ConnectionSet<D, C>> for ConnectionWaiter
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    type Error = EngineError;

    fn wait(
        &mut self,
        duty: &mut ConnectionSet<D, C>,
        maximum: Span,
    ) -> Result<WaitOutcome, Self::Error> {
        duty.poll_io(maximum)
    }
}

impl<D, C> Waiter<StandaloneConnection<D, C>> for ConnectionWaiter
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    type Error = EngineError;

    fn wait(
        &mut self,
        duty: &mut StandaloneConnection<D, C>,
        maximum: Span,
    ) -> Result<WaitOutcome, Self::Error> {
        duty.poll_io(maximum)
    }
}
