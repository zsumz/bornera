//! One-owner connection-epoch state machine.

mod cancellation;
mod close;
mod commit;
mod core;
mod core_error;
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
mod recovery_item;
mod reserve;
mod response;
mod snapshot;
mod transition;

pub use core::ConnectionCore;
pub use core_error::{ConnectionCoreError, ConnectionCoreInvariant};
pub use effect::{CloseReason, ConnectionEffect};
pub use input::{ConnectionInput, InboundReply};
pub use machine::{ConnectionMachine, ConnectionPhase};
pub use recovery::{ConnectionRecovery, RecoveredOperation};
pub use snapshot::ConnectionSnapshot;
pub use transition::{CancelOutcome, ConnectionTransition, InputDisposition};
