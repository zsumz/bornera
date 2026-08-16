//! One-owner connection-epoch state machine.

mod cancellation;
mod close;
mod commit;
mod core;
mod deadline;
mod drive;
mod effect;
#[cfg(test)]
mod fault_test;
mod input;
mod integrity;
mod journal;
mod lifecycle;
mod machine;
mod progress;
mod recovery;
mod reserve;
mod response;
mod snapshot;
mod transition;

pub use core::ConnectionCore;
pub use effect::{CloseReason, ConnectionEffect};
pub use input::{ConnectionInput, InboundReply};
pub use integrity::{ConnectionCoreError, ConnectionCoreInvariant};
pub use machine::{ConnectionMachine, ConnectionPhase};
pub use recovery::{ConnectionRecovery, RecoveredOperation};
pub use snapshot::ConnectionSnapshot;
pub use transition::{CancelOutcome, ConnectionTransition, InputDisposition};
