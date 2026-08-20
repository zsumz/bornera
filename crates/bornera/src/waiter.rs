//! Dedicated-reactor waiting delegated directly to the engine's Mio poller.

use bornera_core::FrameDecoder;
use calandria::{Retained, Span, WaitOutcome, Waiter};

use crate::{ConnectionEngine, EngineError, InboundClassifier};

/// Calandria waiter that polls readiness into its owned connection engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionWaiter;

impl<D, C> Waiter<ConnectionEngine<D, C>> for ConnectionWaiter
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    type Error = EngineError;

    fn wait(
        &mut self,
        duty: &mut ConnectionEngine<D, C>,
        maximum: Span,
    ) -> Result<WaitOutcome, Self::Error> {
        duty.poll_io(maximum)
    }
}
