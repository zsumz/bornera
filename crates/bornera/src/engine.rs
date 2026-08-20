//! Calandria-hosted ownership of one exact plaintext connection epoch.

use std::num::NonZeroUsize;

use bornera_core::{ConnectionCore, FrameDecoder, FrameDriver, OperationId, RetainedBytes};
use calandria::{
    Duty, EventBatch, MailboxReceiver, Moment, PollEvents, ResourceTable, ResourceToken, Retained,
    TimerQueue, TimerToken, Turn,
};
use calandria_mio::MioPoller;

use crate::{
    ConnectError, ConnectionEvent, EngineCommand, EngineConfig, EngineError, EngineLimits,
    EngineOutcome, EnginePort, EngineState, InboundClassifier, OutboundFrame, PlaintextTransport,
};

/// One mutable production owner for one exact connection epoch.
#[derive(Debug)]
pub struct ConnectionEngine<D, C>
where
    D: FrameDecoder,
{
    pub(crate) core: ConnectionCore<OutboundFrame>,
    pub(crate) decoder: FrameDriver<D>,
    pub(crate) classifier: C,
    pub(crate) limits: EngineLimits,
    pub(crate) poller: MioPoller,
    pub(crate) poll_events: PollEvents,
    pub(crate) resources: ResourceTable<bornera_core::ConnectionId, PlaintextTransport>,
    pub(crate) transport: Option<ResourceToken>,
    pub(crate) timers: TimerQueue<DeadlineEvent>,
    pub(crate) deadlines: Vec<DeadlineEntry>,
    pub(crate) outcomes: EventBatch<EngineOutcome<D::Frame>>,
    pub(crate) lifecycle: EventBatch<ConnectionEvent>,
    pub(crate) recovery_outcomes: EventBatch<EngineOutcome<D::Frame>>,
    pub(crate) recovery_events: EventBatch<ConnectionEvent>,
    pub(crate) event_sequence: u64,
    pub(crate) commands: MailboxReceiver<EngineCommand>,
    pub(crate) command_buffer: Vec<EngineCommand>,
    pub(crate) port: EnginePort,
    pub(crate) read_buffer: Vec<u8>,
    pub(crate) decoder_pending: bool,
    pub(crate) poll_saturated: bool,
    pub(crate) stale_backend_events: u64,
    pub(crate) stale_resource_events: u64,
    pub(crate) stale_commands: u64,
    pub(crate) command_more_pending: bool,
    pub(crate) io_preference: IoPreference,
    pub(crate) state: EngineState,
}

impl<D, C> ConnectionEngine<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Begins one nonblocking plaintext TCP connection and registers its exact generation.
    pub fn connect(
        config: EngineConfig,
        limits: EngineLimits,
        decoder: D,
        classifier: C,
    ) -> Result<Self, ConnectError<D::Error>> {
        let decoder =
            FrameDriver::new(decoder, limits.decoder_bytes()).map_err(ConnectError::Decoder)?;
        let mut poller = MioPoller::new(limits.poller())?;
        let poll_events = poller.event_batch();
        let mut resources = ResourceTable::new(config.resource_owner, NonZeroUsize::MIN);
        let transport = PlaintextTransport::connect(config.address)?;
        let token = resources
            .admit(config.connection, transport)
            .map_err(|_| ConnectError::ResourceAdmission)?;
        let (_, transport) = resources
            .get_mut(token)
            .map_err(|_| ConnectError::ResourceAdmission)?;
        poller.register(transport, token, calandria::Interest::READ_WRITE)?;
        let (sender, commands) = calandria::mailbox(limits.commands(), poller.wake_handle());

        let read_buffer = std::iter::repeat_n(0_u8, limits.io_chunk_bytes().get()).collect();
        Ok(Self {
            core: ConnectionCore::new(
                config.endpoint,
                config.lane,
                config.connection,
                config.epoch,
                limits.connection(),
            ),
            decoder,
            classifier,
            limits,
            poller,
            poll_events,
            resources,
            transport: Some(token),
            timers: TimerQueue::new(config.timer_owner, limits.timers()),
            deadlines: Vec::with_capacity(limits.connection().max_operations()),
            outcomes: EventBatch::new(limits.outcomes()),
            lifecycle: EventBatch::new(limits.lifecycle()),
            recovery_outcomes: EventBatch::new(limits.outcomes()),
            recovery_events: EventBatch::new(limits.recovery_lifecycle()),
            event_sequence: 0,
            commands,
            command_buffer: Vec::with_capacity(limits.command_operations().get()),
            port: EnginePort::new(sender),
            read_buffer,
            decoder_pending: false,
            poll_saturated: false,
            stale_backend_events: 0,
            stale_resource_events: 0,
            stale_commands: 0,
            command_more_pending: false,
            io_preference: IoPreference::Read,
            state: EngineState::Running,
        })
    }
}

impl<D, C> Duty for ConnectionEngine<D, C>
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

impl<D, C> Drop for ConnectionEngine<D, C>
where
    D: FrameDecoder,
{
    fn drop(&mut self) {
        drop(self.commands.close());
        let Some(token) = self.transport.take() else {
            return;
        };
        if let Ok((_, transport)) = self.resources.get_mut(token) {
            let _deregistered = self.poller.deregister(transport, token);
        }
        let _removed = self.resources.remove(token);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeadlineEvent {
    pub(crate) epoch: bornera_core::ConnectionEpoch,
    pub(crate) operation: OperationId,
}

impl Retained for DeadlineEvent {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeadlineEntry {
    pub(crate) operation: OperationId,
    pub(crate) token: TimerToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IoPreference {
    Read,
    Write,
}

pub(crate) fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
