//! Bounded selector-free deadline and transport progression for one slot.

use bornera_core::FrameDecoder;
use calandria::{Moment, Retained};

use crate::{ConnectionSlot, EngineError, InboundClassifier, IoPreference, SlotTransport};

/// Bounded work and remaining-runnable state from one slot quantum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SlotProgress {
    pub(crate) work: usize,
    pub(crate) saturated: bool,
}

impl SlotProgress {
    /// Returns the number of deadline, transport, decode, read, or write steps completed.
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
        if let Some(transport) = transport.as_deref() {
            self.capture_transport_pressure(transport)?;
        }
        let budget = self.limits.io_operations().get();
        let mut work = self.drive_deadlines(now, budget)?;
        if self.close_request.is_some() {
            return Ok(SlotProgress {
                work,
                saturated: false,
            });
        }
        if work == budget {
            let runnable_io = self.decoder_pending
                || transport
                    .as_deref()
                    .is_some_and(|transport| self.has_runnable_io(transport));
            return Ok(SlotProgress {
                work,
                saturated: self.has_due_deadline(now) || runnable_io,
            });
        }
        let io = self.drive_io(transport, budget - work)?;
        work = work.saturating_add(io.work);
        Ok(SlotProgress {
            work,
            saturated: io.saturated,
        })
    }

    fn drive_io<T: SlotTransport + ?Sized>(
        &mut self,
        mut transport: Option<&mut T>,
        budget: usize,
    ) -> Result<SlotProgress, EngineError> {
        let mut work = 0;
        while work < budget && self.close_request.is_none() {
            let progressed = match transport.as_deref_mut() {
                Some(transport) => self.drive_ready_once(transport, budget - work)?,
                None if self.decoder_pending => {
                    self.drive_decoder_once()?;
                    self.io_preference = IoPreference::Read;
                    Some(1)
                }
                None => None,
            };
            let Some(progressed) = progressed else {
                break;
            };
            work += progressed;
        }
        Ok(SlotProgress {
            work,
            saturated: work == budget
                && self.close_request.is_none()
                && (self.decoder_pending
                    || transport
                        .as_deref()
                        .is_some_and(|transport| self.has_runnable_io(transport))),
        })
    }

    fn drive_ready_once<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        remaining: usize,
    ) -> Result<Option<usize>, EngineError> {
        let mut preference = self.io_preference;
        for _ in 0..4 {
            let current = preference;
            preference = preference.next();
            let result = match current {
                IoPreference::Transport => self.drive_transport_once(transport, remaining),
                IoPreference::Decode => {
                    if self.decoder_pending {
                        self.drive_decoder_once().map(|()| Some(1))
                    } else {
                        Ok(None)
                    }
                }
                IoPreference::Read => {
                    if self.is_transport_open() {
                        self.drive_read_once(transport)
                            .map(|progressed| progressed.map(|()| 1))
                    } else {
                        Ok(None)
                    }
                }
                IoPreference::Write => {
                    if self.is_transport_open() {
                        self.drive_write_once(transport)
                            .map(|progressed| progressed.map(|()| 1))
                    } else {
                        Ok(None)
                    }
                }
            };
            let pressure = self.capture_transport_pressure(transport);
            let progressed = result?;
            pressure?;
            if progressed.is_some() {
                self.io_preference = preference;
                return Ok(progressed);
            }
        }
        Ok(None)
    }
}
