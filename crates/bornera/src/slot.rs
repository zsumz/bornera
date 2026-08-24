//! Selector-free production state for one exact application-transport epoch.

use bornera_core::{
    ConnectionCore, FrameDecodeError, FrameDecoder, FrameDriver, OperationId, RetainedBytes,
};
use calandria::{Deadline, EventBatch, Retained, TimerQueue, TimerToken};

use crate::{
    ConnectionEvent, ConnectionSlotConfig, ConnectionSlotLimits, EngineOutcome, EngineState,
    InboundClassifier, OutboundFrame, TcpSocketPolicy, TransportDiagnostic, TransportState,
};

/// One selector-free mutable owner for one exact connection epoch.
#[derive(Debug)]
pub struct ConnectionSlot<D, C>
where
    D: FrameDecoder,
{
    pub(crate) core: ConnectionCore<OutboundFrame>,
    pub(crate) decoder: FrameDriver<D>,
    pub(crate) classifier: C,
    pub(crate) limits: ConnectionSlotLimits,
    pub(crate) connect_deadline: Deadline,
    pub(crate) socket_policy: TcpSocketPolicy,
    pub(crate) timers: TimerQueue<DeadlineEvent>,
    pub(crate) deadlines: Vec<DeadlineEntry>,
    pub(crate) outcomes: EventBatch<EngineOutcome<D::Frame>>,
    pub(crate) lifecycle: EventBatch<ConnectionEvent>,
    pub(crate) recovery_outcomes: EventBatch<EngineOutcome<D::Frame>>,
    pub(crate) recovery_events: EventBatch<ConnectionEvent>,
    pub(crate) event_sequence: u64,
    pub(crate) read_buffer: Vec<u8>,
    pub(crate) decoder_pending: bool,
    pub(crate) io_preference: IoPreference,
    pub(crate) transport_state: TransportState,
    pub(crate) transport_diagnostic: Option<TransportDiagnostic>,
    pub(crate) close_request: Option<CloseDirective>,
    pub(crate) state: EngineState,
}

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Creates selector-independent state for one exact connection epoch.
    pub fn new(
        config: ConnectionSlotConfig,
        limits: ConnectionSlotLimits,
        decoder: D,
        classifier: C,
    ) -> Result<Self, FrameDecodeError<D::Error>> {
        let decoder = FrameDriver::new(decoder, limits.decoder_bytes())?;
        let identity = config.identity();
        let read_buffer = std::iter::repeat_n(0_u8, limits.io_chunk_bytes().get()).collect();
        Ok(Self {
            core: ConnectionCore::new(
                identity.endpoint(),
                identity.lane(),
                identity.connection(),
                identity.epoch(),
                limits.connection(),
            ),
            decoder,
            classifier,
            limits,
            connect_deadline: config.connect_deadline(),
            socket_policy: config.tcp_policy(),
            timers: TimerQueue::new(config.timer_owner(), limits.timers()),
            deadlines: Vec::with_capacity(limits.connection().max_operations()),
            outcomes: EventBatch::new(limits.outcomes()),
            lifecycle: EventBatch::new(limits.lifecycle()),
            recovery_outcomes: EventBatch::new(limits.outcomes()),
            recovery_events: EventBatch::new(limits.recovery_lifecycle()),
            event_sequence: 0,
            read_buffer,
            decoder_pending: false,
            io_preference: IoPreference::Transport,
            transport_state: TransportState::Connecting,
            transport_diagnostic: None,
            close_request: None,
            state: EngineState::Running,
        })
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
    Transport,
    Decode,
    Read,
    Write,
}

impl IoPreference {
    pub(crate) const fn next(self) -> Self {
        match self {
            Self::Transport => Self::Decode,
            Self::Decode => Self::Read,
            Self::Read => Self::Write,
            Self::Write => Self::Transport,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloseDirective {
    Core(bornera_core::CloseReason),
    Abort,
}

pub(crate) fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
