//! Bounded selector-free deadline and transport progression for one slot.

use bornera_core::{CloseReason, ConnectionInput, FrameDecoder};
use calandria::{Moment, Retained};

use crate::{
    ConnectProgress, ConnectionSlot, EngineError, InboundClassifier, IoPreference, SlotTransport,
    TransportDiagnostic, TransportFailurePhase, TransportState,
};

/// Bounded work and remaining-runnable state from one slot quantum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SlotProgress {
    pub(crate) work: usize,
    pub(crate) saturated: bool,
}

impl SlotProgress {
    /// Returns the number of deadline, decode, connect, read, or write steps completed.
    pub const fn work(self) -> usize {
        self.work
    }

    /// Returns whether more immediately runnable work remained at the hard quantum bound.
    pub const fn saturated(self) -> bool {
        self.saturated
    }
}

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Drives deadlines and a backend-neutral transport under the slot's fixed work bound.
    pub fn drive_quantum<T: SlotTransport + ?Sized>(
        &mut self,
        now: Moment,
        transport: Option<&mut T>,
    ) -> Result<SlotProgress, EngineError> {
        self.ensure_running()?;
        let result = self.drive_quantum_inner(now, transport);
        self.latch(result)
    }

    fn drive_quantum_inner<T: SlotTransport + ?Sized>(
        &mut self,
        now: Moment,
        transport: Option<&mut T>,
    ) -> Result<SlotProgress, EngineError> {
        let budget = self.limits.io_operations().get();
        let mut work = self.drive_deadlines(now, budget)?;
        if self.close_request.is_some() || work == budget {
            return Ok(SlotProgress {
                work,
                saturated: work == budget && self.has_due_deadline(now),
            });
        }
        let Some(transport) = transport else {
            return Ok(SlotProgress {
                work,
                saturated: false,
            });
        };
        let io = self.drive_io(transport, budget - work)?;
        work = work.saturating_add(io.work);
        Ok(SlotProgress {
            work,
            saturated: io.saturated,
        })
    }

    fn drive_deadlines(&mut self, now: Moment, budget: usize) -> Result<usize, EngineError> {
        let mut work = 0;
        while work < budget {
            let operation = self.timers.next_deadline();
            let connect_first = self.is_connecting()
                && self.connect_deadline.is_elapsed_at(now)
                && operation.is_none_or(|deadline| self.connect_deadline <= deadline);
            if connect_first {
                self.close_for(CloseReason::ConnectTimedOut)?;
                work += 1;
                break;
            }
            let Some(timer) = self.timers.pop_due(now) else {
                break;
            };
            let token = timer.token();
            if let Some(index) = self.deadlines.iter().position(|entry| entry.token == token) {
                self.deadlines.swap_remove(index);
            }
            let event = timer.into_value();
            let transition = self
                .core
                .apply(ConnectionInput::DeadlineElapsed {
                    epoch: event.epoch,
                    operation: event.operation,
                    now,
                })
                .map_err(EngineError::Core)?;
            self.interpret_unit(transition)?;
            work += 1;
            if self.close_request.is_some() {
                break;
            }
        }
        Ok(work)
    }

    fn drive_io<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        budget: usize,
    ) -> Result<SlotProgress, EngineError> {
        let mut work = 0;
        while work < budget && self.close_request.is_none() {
            if self.decoder_pending {
                self.drive_decoder_once()?;
                work += 1;
                continue;
            }
            if self.is_connecting() && transport.can_finish_connect() {
                self.drive_connect_once(transport)?;
                work += 1;
                continue;
            }
            let progressed = match self.io_preference {
                IoPreference::Read => match self.drive_read_once(transport)? {
                    Some(progressed) => Some(progressed),
                    None => self.drive_write_once(transport)?,
                },
                IoPreference::Write => match self.drive_write_once(transport)? {
                    Some(progressed) => Some(progressed),
                    None => self.drive_read_once(transport)?,
                },
            };
            if progressed.is_none() {
                break;
            }
            self.io_preference = match self.io_preference {
                IoPreference::Read => IoPreference::Write,
                IoPreference::Write => IoPreference::Read,
            };
            work += 1;
        }
        Ok(SlotProgress {
            work,
            saturated: work == budget
                && self.close_request.is_none()
                && self.has_runnable_io(transport),
        })
    }

    fn drive_connect_once<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &mut T,
    ) -> Result<(), EngineError> {
        match transport.finish_connect() {
            Ok(ConnectProgress::Opened | ConnectProgress::AlreadyOpen) => {
                if let Err(source) = transport.apply_policy(self.socket_policy) {
                    self.record_transport_failure(TransportDiagnostic::from_io(
                        TransportFailurePhase::SocketPolicy,
                        &source,
                    ));
                    return self.close_for(CloseReason::ConnectFailed);
                }
                self.transport_state = TransportState::Open;
                self.publish_transport_opened()
            }
            Ok(ConnectProgress::Pending) => Ok(()),
            Err(source) => {
                self.record_transport_failure(TransportDiagnostic::from_io(
                    TransportFailurePhase::Connect,
                    &source,
                ));
                self.close_for(CloseReason::ConnectFailed)
            }
        }
    }

    fn has_due_deadline(&self, now: Moment) -> bool {
        (self.is_connecting() && self.connect_deadline.is_elapsed_at(now))
            || self
                .timers
                .next_deadline()
                .is_some_and(|deadline| deadline.is_elapsed_at(now))
    }
}
