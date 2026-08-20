//! Bounded readiness ingestion, deadline delivery, and complete turn interest.

use bornera_core::{CloseReason, ConnectionInput, ConnectionPhase, FrameDecoder};
use calandria::{Next, PollEvent, Retained, Span, Turn, WorkCount};

use crate::{
    ConnectProgress, ConnectionEngine, EngineError, EngineInvariant, InboundClassifier,
    IoPreference, to_u64,
};

impl<D, C> ConnectionEngine<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn drive_turn(&mut self, now: calandria::Moment) -> Result<Turn, EngineError> {
        let mut work = self.drive_commands()?;
        work = work.saturating_add(self.ingest_poll_batches()?);
        work = work.saturating_add(self.drive_deadlines(now)?);
        let (io_work, io_saturated) = self.drive_io()?;
        work = work.saturating_add(io_work);
        work = work.saturating_add(self.sync_interest()?);

        let next = if self.core.snapshot().phase == ConnectionPhase::Closed {
            Next::Stop
        } else if self.command_more_pending
            || self.poll_saturated
            || io_saturated
            || self.decoder_pending
        {
            Next::Now
        } else if let Some(deadline) = self.timers.next_deadline() {
            Next::WakeOr(deadline)
        } else {
            Next::Wake
        };
        Ok(Turn::new(WorkCount::new(to_u64(work)), next))
    }

    fn ingest_poll_batches(&mut self) -> Result<usize, EngineError> {
        let mut work = self.ingest_readiness();
        if self.poll_saturated {
            let report = self.poller.poll(Span::ZERO, &mut self.poll_events)?;
            self.observe_poll(report);
            work = work.saturating_add(self.ingest_readiness());
        }
        Ok(work)
    }

    fn ingest_readiness(&mut self) -> usize {
        let mut work = 0_usize;
        for event in self.poll_events.drain() {
            work = work.saturating_add(1);
            let PollEvent::Resource { token, readiness } = event else {
                continue;
            };
            match self.resources.get_mut(token) {
                Ok((_, transport)) => transport.observe(readiness),
                Err(_) => {
                    self.stale_resource_events = self.stale_resource_events.saturating_add(1);
                }
            }
        }
        work
    }

    fn drive_deadlines(&mut self, now: calandria::Moment) -> Result<usize, EngineError> {
        let mut work = 0;
        while work < self.limits.io_operations().get() {
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
        }
        Ok(work)
    }

    pub(crate) fn drive_io(&mut self) -> Result<(usize, bool), EngineError> {
        let budget = self.limits.io_operations().get();
        let mut work = 0;
        while work < budget && self.transport.is_some() {
            if self.decoder_pending {
                self.drive_decoder_once()?;
                work += 1;
                continue;
            }
            if self.can_finish_connect()? {
                self.drive_connect_once()?;
                work += 1;
                continue;
            }
            let progressed = match self.io_preference {
                IoPreference::Read => match self.drive_read_once()? {
                    Some(progressed) => Some(progressed),
                    None => self.drive_write_once()?,
                },
                IoPreference::Write => match self.drive_write_once()? {
                    Some(progressed) => Some(progressed),
                    None => self.drive_read_once()?,
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
        Ok((work, work == budget && self.has_runnable_io()))
    }

    fn can_finish_connect(&mut self) -> Result<bool, EngineError> {
        let Some(token) = self.transport else {
            return Ok(false);
        };
        let (_, transport) = self.resource_mut(token)?;
        Ok(transport.can_finish_connect())
    }

    fn drive_connect_once(&mut self) -> Result<(), EngineError> {
        let Some(token) = self.transport else {
            return Ok(());
        };
        let result = {
            let (_, transport) = self.resource_mut(token)?;
            transport.finish_connect()
        };
        match result {
            Ok(ConnectProgress::Opened) => self.publish_transport_opened(),
            Ok(ConnectProgress::Pending | ConnectProgress::AlreadyOpen) => Ok(()),
            Err(_) => self.close_for(CloseReason::TransportLost),
        }
    }

    pub(crate) fn resource_mut(
        &mut self,
        token: calandria::ResourceToken,
    ) -> Result<(&bornera_core::ConnectionId, &mut crate::PlaintextTransport), EngineError> {
        self.resources
            .get_mut(token)
            .map_err(|_| EngineError::Invariant(EngineInvariant::ResourceToken))
    }
}
