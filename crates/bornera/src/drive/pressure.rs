//! Fail-closed observation of transport-owned retained memory.

use std::io;

use bornera_core::FrameDecoder;
use calandria::Retained;

use crate::{
    ConnectionSlot, EngineError, EngineInvariant, InboundClassifier, SlotTransport,
    TransportDiagnostic, TransportFailureKind, TransportFailurePhase,
};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn capture_transport_pressure<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &T,
    ) -> Result<(), EngineError> {
        let slot_limit = self.limits.transport_retained_bytes();
        let reported_limit = transport.pressure_limit().retained_bytes();
        let expected_limit = self.transport_retained_limit.unwrap_or(slot_limit);
        if reported_limit > slot_limit
            || self
                .transport_retained_limit
                .is_some_and(|bound| bound != reported_limit)
        {
            self.observe_transport_pressure(transport);
            self.transport_contract_diverged = true;
            self.record_transport_failure(TransportDiagnostic::new(
                TransportFailurePhase::Pressure,
                TransportFailureKind::Contract,
                io::ErrorKind::Other,
                None,
            ));
            return Err(EngineError::Invariant(
                EngineInvariant::TransportLimitContract {
                    limit: expected_limit,
                    reported: reported_limit,
                },
            ));
        }
        self.transport_retained_limit = Some(reported_limit);
        let pressure = self.observe_transport_pressure(transport);
        if pressure.total() <= reported_limit {
            return Ok(());
        }
        self.transport_contract_diverged = true;
        self.record_transport_failure(TransportDiagnostic::new(
            TransportFailurePhase::Pressure,
            TransportFailureKind::Capacity,
            io::ErrorKind::Other,
            None,
        ));
        Err(EngineError::Invariant(
            EngineInvariant::TransportRetainedCapacity {
                limit: reported_limit,
                reported: pressure,
            },
        ))
    }

    pub(crate) fn observe_transport_pressure<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &T,
    ) -> crate::TransportPressure {
        let pressure = transport.pressure();
        self.transport_pressure = Some(pressure);
        pressure
    }
}
