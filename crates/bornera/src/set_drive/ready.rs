//! Fair runnable-slot scanning and bounded ready-queue progression.

use bornera_core::FrameDecoder;
use calandria::{Moment, Retained};

use crate::{
    ConnectionSet, EngineError, InboundClassifier, RegisteredTransport,
    set_settle::{settle_entry, sync_interest},
};

impl<D, C, T> ConnectionSet<D, C, T>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    pub(super) fn enqueue_candidates(&mut self, now: Moment) {
        self.scan.clear();
        self.scan
            .extend(self.resources.iter().map(|(token, _, _)| token));
        for index in 0..self.scan.len() {
            let resource = self.scan[index];
            let should_enqueue = self.resources.get(resource).is_ok_and(|(_, entry)| {
                entry
                    .slot
                    .next_deadline()
                    .is_some_and(|deadline| deadline.is_elapsed_at(now))
                    || entry.slot.transport_release_ready()
                    || entry.transport.as_ref().is_some_and(|transport| {
                        if entry.slot.close_request.is_some() {
                            entry.slot.has_runnable_shutdown(now, transport)
                        } else {
                            entry.slot.has_runnable_io(transport)
                        }
                    })
            });
            if should_enqueue {
                self.enqueue(resource);
            }
        }
    }

    pub(super) fn drive_ready(&mut self, now: Moment) -> Result<usize, EngineError> {
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
                let interest = match sync_interest(poller, resource, entry) {
                    Ok(interest) => interest,
                    Err(error) => break 'entry Err(error),
                };
                let runnable = entry.slot.state.failure().is_none()
                    && entry.transport.as_ref().is_some_and(|transport| {
                        progress.saturated
                            || entry.slot.transport_release_ready()
                            || if entry.slot.close_request.is_some() {
                                entry.slot.has_runnable_shutdown(now, transport)
                            } else {
                                entry.slot.has_runnable_io(transport)
                            }
                    });
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

    pub(super) fn earliest_deadline(&self) -> Option<calandria::Deadline> {
        self.resources
            .iter()
            .filter_map(|(_, _, entry)| {
                entry
                    .slot
                    .state
                    .failure()
                    .is_none()
                    .then(|| entry.slot.next_deadline())?
            })
            .min()
    }
}
