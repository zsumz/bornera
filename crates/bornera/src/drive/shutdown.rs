//! Bounded graceful transport shutdown and forced-settlement deadlines.

use bornera_core::FrameDecoder;
use calandria::{Moment, Retained};

use crate::{
    CloseDirective, ConnectionSlot, EngineError, InboundClassifier, SlotTransport, TransportBudget,
    TransportDiagnostic, TransportFailureKind, TransportFailurePhase, slot::ShutdownState,
};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(super) fn drive_shutdown_once<T: SlotTransport + ?Sized>(
        &mut self,
        now: Moment,
        transport: &mut T,
        remaining: usize,
    ) -> Result<Option<usize>, EngineError> {
        let Some(CloseDirective::Core { reason, shutdown }) = self.close_request else {
            return Ok(None);
        };
        let deadline = match shutdown {
            ShutdownState::Pending { deadline } | ShutdownState::Started { deadline } => deadline,
            ShutdownState::Immediate | ShutdownState::Complete => return Ok(None),
        };
        if deadline.is_elapsed_at(now) {
            self.shutdown_timed_out(reason);
            return Ok(Some(1));
        }
        if remaining == 0 {
            return Ok(None);
        }
        match shutdown {
            ShutdownState::Pending { .. } => {
                let budget = self.transport_budget();
                let result = transport.begin_shutdown(budget);
                let pressure = self.capture_transport_pressure(transport);
                let progress = match result {
                    Ok(progress) => progress,
                    Err(source) => {
                        self.record_transport_failure(
                            source
                                .diagnostic()
                                .in_phase(TransportFailurePhase::Shutdown),
                        );
                        self.force_shutdown_settlement(reason);
                        pressure?;
                        return Ok(Some(1));
                    }
                };
                if let Err(error) = pressure {
                    self.force_shutdown_settlement(reason);
                    return Err(error);
                }
                if let Err(error) = Self::validate_transport_progress(budget, progress) {
                    self.force_shutdown_settlement(reason);
                    return Err(error);
                }
                let shutdown = if transport.is_shutdown_complete() {
                    ShutdownState::Complete
                } else {
                    ShutdownState::Started { deadline }
                };
                self.close_request = Some(CloseDirective::Core { reason, shutdown });
                Ok(Some(progress.operations()))
            }
            ShutdownState::Started { .. } => {
                if transport.is_shutdown_complete() {
                    self.complete_shutdown(reason);
                    return Ok(None);
                }
                if !transport.has_transport_work() {
                    return Ok(None);
                }
                self.progress_shutdown(reason, deadline, transport)
            }
            ShutdownState::Immediate | ShutdownState::Complete => Ok(None),
        }
    }

    fn progress_shutdown<T: SlotTransport + ?Sized>(
        &mut self,
        reason: bornera_core::CloseReason,
        deadline: calandria::Deadline,
        transport: &mut T,
    ) -> Result<Option<usize>, EngineError> {
        let budget = self.transport_budget();
        let result = transport.drive_transport(budget);
        let pressure = self.capture_transport_pressure(transport);
        let progress = match result {
            Ok(progress) => progress,
            Err(source) => {
                self.record_transport_failure(
                    source
                        .diagnostic()
                        .in_phase(TransportFailurePhase::Shutdown),
                );
                self.force_shutdown_settlement(reason);
                pressure?;
                return Ok(Some(1));
            }
        };
        if let Err(error) = pressure {
            self.force_shutdown_settlement(reason);
            return Err(error);
        }
        if let Err(error) = Self::validate_transport_progress(budget, progress) {
            self.force_shutdown_settlement(reason);
            return Err(error);
        }
        let shutdown = if transport.is_shutdown_complete() {
            ShutdownState::Complete
        } else {
            ShutdownState::Started { deadline }
        };
        self.close_request = Some(CloseDirective::Core { reason, shutdown });
        Ok(Some(progress.operations()))
    }

    pub(crate) fn observe_shutdown_complete<T: SlotTransport + ?Sized>(&mut self, transport: &T) {
        let Some(CloseDirective::Core { reason, shutdown }) = self.close_request else {
            return;
        };
        if matches!(shutdown, ShutdownState::Started { .. }) && transport.is_shutdown_complete() {
            self.complete_shutdown(reason);
        }
    }

    pub(crate) fn has_runnable_shutdown<T: SlotTransport + ?Sized>(
        &self,
        now: Moment,
        transport: &T,
    ) -> bool {
        match self.close_request {
            Some(CloseDirective::Core {
                shutdown: ShutdownState::Pending { .. },
                ..
            }) => true,
            Some(CloseDirective::Core {
                shutdown: ShutdownState::Started { deadline },
                ..
            }) => {
                deadline.is_elapsed_at(now)
                    || transport.is_shutdown_complete()
                    || transport.has_transport_work()
            }
            Some(
                CloseDirective::Core {
                    shutdown: ShutdownState::Immediate | ShutdownState::Complete,
                    ..
                }
                | CloseDirective::Abort,
            )
            | None => false,
        }
    }

    pub(crate) fn force_shutdown(&mut self) -> bool {
        let Some(CloseDirective::Core { reason, shutdown }) = self.close_request else {
            return false;
        };
        if !matches!(
            shutdown,
            ShutdownState::Pending { .. } | ShutdownState::Started { .. }
        ) {
            return false;
        }
        self.force_shutdown_settlement(reason);
        true
    }

    fn shutdown_timed_out(&mut self, reason: bornera_core::CloseReason) {
        self.record_transport_failure(TransportDiagnostic::new(
            TransportFailurePhase::Shutdown,
            TransportFailureKind::TimedOut,
            std::io::ErrorKind::TimedOut,
            None,
        ));
        self.force_shutdown_settlement(reason);
    }

    fn complete_shutdown(&mut self, reason: bornera_core::CloseReason) {
        self.close_request = Some(CloseDirective::Core {
            reason,
            shutdown: ShutdownState::Complete,
        });
    }

    fn force_shutdown_settlement(&mut self, reason: bornera_core::CloseReason) {
        self.close_request = Some(CloseDirective::Core {
            reason,
            shutdown: ShutdownState::Immediate,
        });
    }

    fn transport_budget(&self) -> TransportBudget {
        TransportBudget::new(
            core::num::NonZeroUsize::MIN,
            self.limits.io_chunk_bytes(),
            self.limits.io_chunk_bytes(),
        )
    }
}
