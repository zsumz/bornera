//! Production connection ownership for native protocol clients.
//!
//! Bornera owns native transports; protocol crates own session meaning and policy.
mod admission;
mod classifier;
mod command;
mod config;
mod connection_error;
mod control;
mod drive;
mod error;
mod event;
mod failure;
mod frame;
mod io;
mod lifecycle;
mod outcome;
mod port;
mod recovery;
mod set;
mod set_access;
mod set_connect;
#[cfg(test)]
mod set_connect_test;
mod set_drive;
mod set_failure;
#[cfg(test)]
mod set_failure_test;
mod set_recovery;
mod set_settle;
#[cfg(test)]
mod set_settlement_test;
mod slot;
mod slot_close;
mod snapshot;
mod socket;
mod standalone;
mod state;
mod token;
mod transition;
#[cfg(test)]
mod transition_test;
mod transport;
mod waiter;

pub use bornera_core::{CompletionMode, FrameDecoder, OperationOptions};
pub use classifier::InboundClassifier;
pub use command::ConnectionCommand;
pub use config::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSetLimits,
    ConnectionSlotConfig, ConnectionSlotLimits, ConnectionSlotLimitsError, DecoderLimits, IoLimits,
    PublicationLimits, StandaloneConnectionConfig,
};
pub use connection_error::{
    ConnectionAccessError, ConnectionCommitError, ConnectionRecoveryError, ConnectionReserveError,
    ConnectionRetireError,
};
pub use drive::SlotProgress;
pub use error::{ConnectError, EngineCommitError, EngineError, EngineInvariant};
pub use event::ConnectionEvent;
pub use frame::{OutboundFrame, OutboundFrameError};
pub use outcome::EngineOutcome;
pub use port::{ConnectionPort, ConnectionPulseHandle};
pub use recovery::{OwnerFailure, RecoveryReport, RecoveryWhileRunning};
pub(crate) use set::ConnectionEntry;
pub use set::ConnectionSet;
pub use slot::ConnectionSlot;
pub(crate) use slot::{CloseDirective, DeadlineEntry, DeadlineEvent, IoPreference, to_u64};
pub use snapshot::{
    ConnectionSetSnapshot, ConnectionSlotSnapshot, TransportDiagnostic, TransportFailurePhase,
    TransportState,
};
pub use socket::{SocketPolicyError, TcpKeepalivePolicy, TcpNoDelay, TcpSocketPolicy};
pub use standalone::StandaloneConnection;
pub(crate) use state::EngineState;
pub use token::ConnectionToken;
pub use transport::{
    ConnectProgress, RegisteredTransport, SlotTransport, TcpTransport, TransportConnector,
};
pub use waiter::ConnectionWaiter;
