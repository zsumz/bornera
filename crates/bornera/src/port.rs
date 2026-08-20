//! Cloneable bounded producer for mechanical dedicated-owner commands.

use bornera_core::{ConnectionEpoch, OperationId};
use calandria::{MailboxSender, TrySendError};

use crate::EngineCommand;

/// Cross-thread producer for a bounded Calandria command mailbox.
#[derive(Clone, Debug)]
pub struct EnginePort {
    sender: MailboxSender<EngineCommand>,
}

impl EnginePort {
    pub(crate) const fn new(sender: MailboxSender<EngineCommand>) -> Self {
        Self { sender }
    }

    /// Requests regular admission for one exact established epoch.
    pub fn open_admission(
        &self,
        epoch: ConnectionEpoch,
    ) -> Result<(), TrySendError<EngineCommand>> {
        self.sender
            .try_send_control(EngineCommand::OpenAdmission { epoch })
    }

    /// Requests explicit local cancellation for one exact accepted operation.
    pub fn cancel(
        &self,
        epoch: ConnectionEpoch,
        operation: OperationId,
    ) -> Result<(), TrySendError<EngineCommand>> {
        self.sender
            .try_send(EngineCommand::Cancel { epoch, operation })
    }

    /// Requests admission closure followed by ordered draining.
    pub fn begin_drain(&self, epoch: ConnectionEpoch) -> Result<(), TrySendError<EngineCommand>> {
        self.sender
            .try_send_control(EngineCommand::BeginDrain { epoch })
    }

    /// Requests forced mechanical closure of one exact epoch.
    pub fn close(&self, epoch: ConnectionEpoch) -> Result<(), TrySendError<EngineCommand>> {
        self.sender.try_send_control(EngineCommand::Close { epoch })
    }
}
