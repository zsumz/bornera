//! Shared Mio selector and generation-fenced ownership for bounded connections.

use std::collections::VecDeque;

use bornera_core::FrameDecoder;
use calandria::{
    Duty, MailboxReceiver, MailboxSender, Moment, PollEvents, ResourceTable, ResourceToken,
    Retained, Span, Turn, WaitOutcome,
};
use calandria_mio::MioPoller;

use crate::{
    ConnectionAccessError, ConnectionCommand, ConnectionIdentity, ConnectionPort,
    ConnectionSetConfig, ConnectionSetLimits, ConnectionSetSnapshot, ConnectionSlot,
    ConnectionToken, EngineError, InboundClassifier, PlaintextTransport,
};

/// One connection slot plus the private capability registered for its generation.
pub(crate) struct ConnectionEntry<D, C>
where
    D: FrameDecoder,
{
    pub(crate) slot: ConnectionSlot<D, C>,
    pub(crate) transport: Option<PlaintextTransport>,
    pub(crate) ready_queued: bool,
}

impl<D, C> core::fmt::Debug for ConnectionEntry<D, C>
where
    D: FrameDecoder,
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ConnectionEntry")
            .field("transport", &self.transport)
            .field("ready_queued", &self.ready_queued)
            .finish_non_exhaustive()
    }
}

/// Bounded owner for many connection epochs sharing one Mio selector.
#[derive(Debug)]
pub struct ConnectionSet<D, C>
where
    D: FrameDecoder,
{
    pub(crate) limits: ConnectionSetLimits,
    pub(crate) poller: MioPoller,
    pub(crate) poll_events: PollEvents,
    pub(crate) resources: ResourceTable<ConnectionIdentity, ConnectionEntry<D, C>>,
    pub(crate) ready: VecDeque<ResourceToken>,
    pub(crate) scan: Vec<ResourceToken>,
    pub(crate) commands: MailboxReceiver<ConnectionCommand>,
    pub(crate) command_buffer: Vec<ConnectionCommand>,
    pub(crate) sender: MailboxSender<ConnectionCommand>,
    pub(crate) command_more_pending: bool,
    pub(crate) poll_saturated: bool,
    pub(crate) stale_backend_events: u64,
    pub(crate) stale_resource_events: u64,
    pub(crate) stale_commands: u64,
    pub(crate) owner_failure: Option<crate::OwnerFailure>,
}

impl<D, C> ConnectionSet<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Creates an empty bounded set and its sole readiness selector.
    pub fn new(
        config: ConnectionSetConfig,
        limits: ConnectionSetLimits,
    ) -> Result<Self, calandria_mio::MioError> {
        let poller = MioPoller::new(limits.poller())?;
        let poll_events = poller.event_batch();
        let (sender, commands) = calandria::mailbox(limits.commands(), poller.wake_handle());
        Ok(Self {
            limits,
            poller,
            poll_events,
            resources: ResourceTable::new(config.resource_owner(), limits.max_connections()),
            ready: VecDeque::with_capacity(limits.max_connections().get()),
            scan: Vec::with_capacity(limits.max_connections().get()),
            commands,
            command_buffer: Vec::with_capacity(limits.commands_per_turn().get()),
            sender,
            command_more_pending: false,
            poll_saturated: false,
            stale_backend_events: 0,
            stale_resource_events: 0,
            stale_commands: 0,
            owner_failure: None,
        })
    }

    /// Returns a bounded command producer tied to one exact live generation.
    pub fn port(
        &self,
        connection: ConnectionToken,
    ) -> Result<ConnectionPort, ConnectionAccessError> {
        self.ensure_owner_running()
            .map_err(ConnectionAccessError::Owner)?;
        let _entry = self.entry(connection)?;
        Ok(ConnectionPort::new(connection, self.sender.clone()))
    }

    /// Creates an independent coalesced wake domain for the shared selector.
    pub fn wake_handle(&self) -> calandria::WakeHandle {
        self.poller.wake_handle()
    }

    /// Returns immutable shared-selector pressure and stale-event observations.
    pub fn snapshot(&self) -> ConnectionSetSnapshot {
        ConnectionSetSnapshot {
            connections: self.resources.snapshot(),
            poller: self.poller.snapshot(),
            ready_connections: self.ready.len(),
            commands: self.commands.snapshot(),
            stale_backend_events: self.stale_backend_events,
            stale_resource_events: self.stale_resource_events,
            stale_commands: self.stale_commands,
            owner_failure: self.owner_failure,
        }
    }

    /// Polls the sole selector into the set's bounded observation batch.
    pub fn poll_io(&mut self, maximum: Span) -> Result<WaitOutcome, EngineError> {
        let report = self.poll_selector(maximum)?;
        self.observe_poll(report);
        Ok(if report.observed() == 0 {
            WaitOutcome::Idle
        } else {
            WaitOutcome::Notified
        })
    }

    pub(crate) fn entry(
        &self,
        connection: ConnectionToken,
    ) -> Result<&ConnectionEntry<D, C>, ConnectionAccessError> {
        let (identity, entry) = self
            .resources
            .get(connection.resource())
            .map_err(|_| ConnectionAccessError::StaleConnection)?;
        if *identity != connection.identity() {
            return Err(ConnectionAccessError::StaleConnection);
        }
        Ok(entry)
    }

    pub(crate) fn entry_mut(
        &mut self,
        connection: ConnectionToken,
    ) -> Result<&mut ConnectionEntry<D, C>, ConnectionAccessError> {
        let (identity, entry) = self
            .resources
            .get_mut(connection.resource())
            .map_err(|_| ConnectionAccessError::StaleConnection)?;
        if *identity != connection.identity() {
            return Err(ConnectionAccessError::StaleConnection);
        }
        Ok(entry)
    }

    pub(crate) fn enqueue(&mut self, resource: ResourceToken) {
        let Ok((_, entry)) = self.resources.get_mut(resource) else {
            return;
        };
        if entry.ready_queued {
            return;
        }
        entry.ready_queued = true;
        self.ready.push_back(resource);
    }
}

impl<D, C> Duty for ConnectionSet<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    type Error = EngineError;

    fn turn(&mut self, now: Moment) -> Result<Turn, Self::Error> {
        self.turn_component(now)
    }
}

impl<D, C> Drop for ConnectionSet<D, C>
where
    D: FrameDecoder,
{
    fn drop(&mut self) {
        drop(self.commands.close());
        self.scan.clear();
        self.scan
            .extend(self.resources.iter().map(|(token, _, _)| token));
        for token in self.scan.iter().copied() {
            let Ok((_, entry)) = self.resources.get_mut(token) else {
                continue;
            };
            let Some(transport) = entry.transport.as_mut() else {
                continue;
            };
            let _deregistered = self.poller.deregister(transport, token);
        }
    }
}
