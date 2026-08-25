//! Bounded command, readiness, deadline, and fair connection-set turns.

use bornera_core::{CloseReason, FrameDecoder};
use calandria::{
    DrainStatus, Moment, Next, PollEvent, PollReport, Readiness, Retained, Span, Turn, WorkCount,
};

use crate::{
    ConnectionCommand, ConnectionSet, ConnectionSlot, EngineError, InboundClassifier,
    RegisteredTransport,
    set_settle::{settle_entry, sync_interest},
    to_u64,
};

impl<D, C, T> ConnectionSet<D, C, T>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    /// Performs one complete bounded set turn with fair per-slot progression.
    pub fn turn_component(&mut self, now: Moment) -> Result<Turn, EngineError> {
        self.ensure_owner_running()?;
        let mut work = self.drive_commands();
        work = work.saturating_add(self.ingest_poll_batches()?);
        self.enqueue_candidates(now);
        work = work.saturating_add(self.drive_ready(now)?);

        let next = if self.command_more_pending || self.poll_saturated || !self.ready.is_empty() {
            Next::Now
        } else {
            self.earliest_deadline().map_or(Next::Wake, Next::WakeOr)
        };
        Ok(Turn::new(WorkCount::new(to_u64(work)), next))
    }

    pub(crate) fn observe_poll(&mut self, report: PollReport) {
        self.stale_backend_events = self
            .stale_backend_events
            .saturating_add(to_u64(report.stale()));
        self.poll_saturated = report.saturated();
    }

    fn drive_commands(&mut self) -> usize {
        let report = self
            .commands
            .drain_into(&mut self.command_buffer, self.limits.commands_per_turn());
        self.command_more_pending = report.status() == DrainStatus::MorePending;
        let drained = report.drained();
        for index in 0..self.command_buffer.len() {
            let Some(command) = self.command_buffer.get(index).copied() else {
                continue;
            };
            self.apply_command(command);
        }
        self.command_buffer.clear();
        drained
    }

    fn apply_command(&mut self, command: ConnectionCommand) {
        let connection = command.connection();
        let resource = connection.resource();
        let Ok(entry) = self.entry_mut(connection) else {
            self.stale_commands = self.stale_commands.saturating_add(1);
            return;
        };
        let result = match command {
            ConnectionCommand::OpenAdmission { .. } => entry.slot.open_admission().map(drop),
            ConnectionCommand::Cancel { operation, .. } => entry.slot.cancel(operation).map(drop),
            ConnectionCommand::BeginDrain { .. } => entry.slot.begin_drain().map(drop),
            ConnectionCommand::Close { .. } => {
                entry.slot.finalize(CloseReason::Requested).map(drop)
            }
        };
        if let Err(error) = result {
            entry.slot.latch_failure(&error);
        }
        self.enqueue(resource);
    }

    fn ingest_poll_batches(&mut self) -> Result<usize, EngineError> {
        let mut work = self.ingest_readiness();
        if self.poll_saturated {
            let report = self.poll_selector(Span::ZERO)?;
            self.observe_poll(report);
            work = work.saturating_add(self.ingest_readiness());
        }
        Ok(work)
    }

    fn ingest_readiness(&mut self) -> usize {
        let mut work = 0_usize;
        let (events, resources, ready, stale) = (
            &mut self.poll_events,
            &mut self.resources,
            &mut self.ready,
            &mut self.stale_resource_events,
        );
        for event in events.drain() {
            work = work.saturating_add(1);
            let PollEvent::Resource { token, readiness } = event else {
                continue;
            };
            let Ok((_, entry)) = resources.get_mut(token) else {
                *stale = stale.saturating_add(1);
                continue;
            };
            let Some(transport) = entry.transport.as_mut() else {
                *stale = stale.saturating_add(1);
                continue;
            };
            observe_transport_readiness(&mut entry.slot, transport, readiness);
            if !entry.ready_queued {
                entry.ready_queued = true;
                ready.push_back(token);
            }
        }
        work
    }

    fn enqueue_candidates(&mut self, now: Moment) {
        self.scan.clear();
        self.scan
            .extend(self.resources.iter().map(|(token, _, _)| token));
        for index in 0..self.scan.len() {
            let resource = self.scan[index];
            let should_enqueue = self.resources.get(resource).is_ok_and(|(_, entry)| {
                entry.slot.close_request.is_some()
                    || entry
                        .slot
                        .next_deadline()
                        .is_some_and(|deadline| deadline.is_elapsed_at(now))
                    || entry
                        .transport
                        .as_ref()
                        .is_some_and(|transport| entry.slot.has_runnable_io(transport))
            });
            if should_enqueue {
                self.enqueue(resource);
            }
        }
    }

    fn drive_ready(&mut self, now: Moment) -> Result<usize, EngineError> {
        let mut work = 0_usize;
        'ready: for _ in 0..self.limits.ready_connections_per_turn().get() {
            let Some(resource) = self.ready.pop_front() else {
                break;
            };
            let result = 'entry: {
                let (poller, resources) = (&mut self.poller, &mut self.resources);
                let Ok((_, entry)) = resources.get_mut(resource) else {
                    self.stale_resource_events = self.stale_resource_events.saturating_add(1);
                    continue 'ready;
                };
                entry.ready_queued = false;
                let progress = match entry.slot.drive_quantum(now, entry.transport.as_mut()) {
                    Ok(progress) => progress,
                    Err(error) => {
                        entry.slot.latch_failure(&error);
                        crate::SlotProgress {
                            work: 0,
                            saturated: false,
                        }
                    }
                };
                let settled = match settle_entry(poller, resource, entry) {
                    Ok(settled) => settled,
                    Err(error) => break 'entry Err(error),
                };
                let interest = if entry.slot.close_request.is_none() {
                    match sync_interest(poller, resource, entry) {
                        Ok(interest) => interest,
                        Err(error) => break 'entry Err(error),
                    }
                } else {
                    0
                };
                let runnable = entry.slot.close_request.is_some()
                    || (entry.slot.state.failure().is_none()
                        && entry.transport.as_ref().is_some_and(|transport| {
                            progress.saturated || entry.slot.has_runnable_io(transport)
                        }));
                Ok((
                    progress
                        .work
                        .saturating_add(settled)
                        .saturating_add(interest),
                    runnable,
                ))
            };
            let (progress, requeue) = match result {
                Ok(progress) => progress,
                Err(error) => {
                    self.latch_readiness_error(&error);
                    return Err(error);
                }
            };
            work = work.saturating_add(progress);
            if requeue {
                self.enqueue(resource);
            }
        }
        Ok(work)
    }

    fn earliest_deadline(&self) -> Option<calandria::Deadline> {
        self.resources
            .iter()
            .filter_map(|(_, _, entry)| {
                (entry.slot.state.failure().is_none()).then(|| entry.slot.next_deadline())?
            })
            .min()
    }
}

pub(crate) fn observe_transport_readiness<D, C, T>(
    slot: &mut ConnectionSlot<D, C>,
    transport: &mut T,
    readiness: Readiness,
) where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    transport.observe_readiness(readiness);
    if let Err(error) = slot.capture_transport_pressure(transport) {
        slot.latch_failure(&error);
    }
}

#[cfg(test)]
mod readiness_tests;
