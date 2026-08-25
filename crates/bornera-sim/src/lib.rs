//! Exact, bounded replay of Bornera connection-policy event traces.

mod config;
mod frame;
mod replay;
mod report;
mod slot_apply;
mod slot_config;
mod slot_replay;
mod slot_report;
mod slot_trace;
mod slot_trace_error;
mod slot_transport;
mod slot_transport_state;
mod trace;
mod wire;

pub use config::SimulationConfig;
pub use frame::{SimFrame, SimFrameError};
pub use replay::Simulator;
pub use report::{ReplayReport, StepObservation, StepResult};
pub use slot_config::SlotSimulationConfig;
pub use slot_replay::SlotSimulator;
pub use slot_report::{
    SlotActionFailure, SlotActionResult, SlotObservationKind, SlotReplayError, SlotReplayReport,
    SlotStepObservation,
};
pub use slot_trace::{SlotAction, SlotTrace, SlotTraceLimits, TimedSlotAction};
pub use slot_trace_error::{SlotTraceAdmissionError, SlotTraceAdmissionFailure};
pub use trace::{
    EpochTarget, OperationIndex, Trace, TraceAction, TraceAdmissionError, TraceAdmissionFailure,
    TraceLimits,
};
pub use wire::{SimReply, SimWireError};

pub(crate) use slot_replay::{Accepted, ReplayOwner, SimSlot};
pub(crate) use slot_transport::SimTransport;
pub(crate) use slot_transport_state::{Phase, ShutdownState, transport_pressure};
pub(crate) use wire::{SimClassifier, SimDecoder, encode_reply};
