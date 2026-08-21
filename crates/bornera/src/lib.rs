//! Production connection ownership for native protocol clients.
//!
//! This crate privately owns native plaintext TCP capabilities while Calandria
//! supplies time, readiness, timers, resources, wakes, and hosting. Protocol
//! crates retain codecs, session meaning, routing, retry, and public APIs.

mod admission;
mod classifier;
mod command;
mod config;
mod control;
mod drive;
mod engine;
mod error;
mod event;
mod failure;
mod frame;
mod io;
mod lifecycle;
mod outcome;
mod port;
mod recovery;
mod snapshot;
mod state;
mod transition;
#[cfg(test)]
mod transition_test;
mod transport;
mod waiter;

pub use classifier::InboundClassifier;
pub use command::EngineCommand;
pub use config::{
    DecoderLimits, EngineConfig, EngineLimits, EngineLimitsError, PublicationLimits, TurnLimits,
};
pub use engine::ConnectionEngine;
pub use error::{ConnectError, EngineCommitError, EngineError, EngineInvariant};
pub use event::ConnectionEvent;
pub use frame::{OutboundFrame, OutboundFrameError};
pub use outcome::EngineOutcome;
pub use port::EnginePort;
pub use recovery::{OwnerFailure, RecoveryReport, RecoveryWhileRunning};
pub use snapshot::{EngineSnapshot, TransportState};
pub use waiter::ConnectionWaiter;

pub use bornera_core::{FrameDecoder, OperationOptions};

pub(crate) use engine::{DeadlineEntry, DeadlineEvent, IoPreference, to_u64};
pub(crate) use state::EngineState;
pub(crate) use transport::{ConnectProgress, PlaintextTransport};
