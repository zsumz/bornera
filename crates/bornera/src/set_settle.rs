//! Selector registration settlement and transport teardown for one slot.

use bornera_core::FrameDecoder;
use calandria::{ResourceToken, Retained};
use calandria_mio::{MioError, MioPoller};

use crate::{
    ConnectionEntry, ConnectionSet, EngineError, EngineInvariant, InboundClassifier,
    RegisteredTransport, TransportDiagnostic, TransportFailurePhase,
};

impl<D, C, T> ConnectionSet<D, C, T>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    pub(crate) fn settle_connection(
        &mut self,
        resource: ResourceToken,
    ) -> Result<usize, EngineError> {
        let result = {
            let (poller, resources) = (&mut self.poller, &mut self.resources);
            let Ok((_, entry)) = resources.get_mut(resource) else {
                return Err(EngineError::Invariant(EngineInvariant::ResourceToken));
            };
            settle_entry(poller, resource, entry)
        };
        let settled = match result {
            Ok(settled) => settled,
            Err(error) => {
                self.latch_readiness_error(&error);
                return Err(error);
            }
        };
        if settled != 0 {
            self.ready.retain(|token| *token != resource);
            if let Ok((_, entry)) = self.resources.get_mut(resource) {
                entry.ready_queued = false;
            }
        }
        if let Ok((_, entry)) = self.resources.get(resource)
            && let Some(reason) = entry.slot.state.failure()
        {
            return Err(EngineError::OwnerFailed(reason));
        }
        Ok(settled)
    }
}

pub(crate) fn settle_entry<D, C, T>(
    poller: &mut MioPoller,
    resource: ResourceToken,
    entry: &mut ConnectionEntry<D, C, T>,
) -> Result<usize, EngineError>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    let Some(directive) = entry.slot.take_close_request() else {
        return Ok(0);
    };
    if let Some(transport) = entry.transport.as_mut()
        && let Err(source) = poller.deregister(transport, resource)
    {
        record_mio_failure(&mut entry.slot, &source);
        entry.slot.restore_close_request(directive);
        let error = EngineError::Mio(source);
        entry.slot.latch_failure(&error);
        return Err(error);
    }
    entry.transport = None;
    entry.slot.restore_close_request(directive);
    if !entry.slot.settle_transport_closed() {
        let error = EngineError::Invariant(crate::EngineInvariant::MissingCloseReason);
        entry.slot.latch_failure(&error);
        entry.slot.abort_transport();
    }
    Ok(1)
}

pub(crate) fn sync_interest<D, C, T>(
    poller: &mut MioPoller,
    resource: ResourceToken,
    entry: &mut ConnectionEntry<D, C, T>,
) -> Result<usize, EngineError>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    let Some(transport) = entry.transport.as_mut() else {
        return Ok(0);
    };
    let desired = entry.slot.desired_interest(transport);
    if desired == entry.interest {
        return Ok(0);
    }
    if let Err(source) = poller.reregister(transport, resource, desired) {
        record_mio_failure(&mut entry.slot, &source);
        let error = EngineError::Mio(source);
        entry.slot.latch_failure(&error);
        return Err(error);
    }
    entry.interest = desired;
    Ok(1)
}

fn record_mio_failure<D, C>(slot: &mut crate::ConnectionSlot<D, C>, error: &MioError)
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    if let MioError::Io(source) = error {
        slot.record_transport_failure(TransportDiagnostic::from_io(
            TransportFailurePhase::Readiness,
            source,
        ));
    }
}
