//! Fixed-size, epoch-fenced commands for a dedicated connection owner.

use bornera_core::{CloseReason, ConnectionEpoch, ConnectionInput, FrameDecoder, OperationId};
use calandria::{DrainStatus, Retained, RetainedBytes};

use crate::{ConnectionEngine, EngineError, InboundClassifier};

/// One mechanical command submitted to an exact connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineCommand {
    /// Opens regular admission after protocol-owned session establishment.
    OpenAdmission {
        /// Exact connection lifetime authorized by the sender.
        epoch: ConnectionEpoch,
    },
    /// Explicitly cancels local observation of one accepted operation.
    Cancel {
        /// Exact connection lifetime authorized by the sender.
        epoch: ConnectionEpoch,
        /// Operation to cancel.
        operation: OperationId,
    },
    /// Closes admission before draining accepted work.
    BeginDrain {
        /// Exact connection lifetime authorized by the sender.
        epoch: ConnectionEpoch,
    },
    /// Forces requested closure of one exact connection lifetime.
    Close {
        /// Exact connection lifetime authorized by the sender.
        epoch: ConnectionEpoch,
    },
}

impl EngineCommand {
    pub(crate) const fn epoch(self) -> ConnectionEpoch {
        match self {
            Self::OpenAdmission { epoch }
            | Self::Cancel { epoch, .. }
            | Self::BeginDrain { epoch }
            | Self::Close { epoch } => epoch,
        }
    }
}

impl Retained for EngineCommand {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

impl<D, C> ConnectionEngine<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn drive_commands(&mut self) -> Result<usize, EngineError> {
        let report = self
            .commands
            .drain_into(&mut self.command_buffer, self.limits.command_operations());
        self.command_more_pending = report.status() == DrainStatus::MorePending;
        let drained = report.drained();
        for index in 0..self.command_buffer.len() {
            self.apply_command(self.command_buffer[index])?;
        }
        self.command_buffer.clear();
        Ok(drained)
    }

    fn apply_command(&mut self, command: EngineCommand) -> Result<(), EngineError> {
        if command.epoch() != self.core.epoch() {
            self.stale_commands = self.stale_commands.saturating_add(1);
            return Ok(());
        }
        let opens_admission = matches!(command, EngineCommand::OpenAdmission { .. });
        let transition = match command {
            EngineCommand::OpenAdmission { .. } if !self.is_transport_open() => return Ok(()),
            EngineCommand::OpenAdmission { epoch } => {
                self.core.apply(ConnectionInput::OpenAdmission { epoch })
            }
            EngineCommand::Cancel { epoch, operation } => self
                .core
                .apply(ConnectionInput::Cancel { epoch, operation }),
            EngineCommand::BeginDrain { epoch } => {
                self.core.apply(ConnectionInput::BeginDrain { epoch })
            }
            EngineCommand::Close { epoch } => self.core.apply(ConnectionInput::CloseRequested {
                epoch,
                reason: CloseReason::Requested,
            }),
        }
        .map_err(EngineError::Core)?;
        let disposition = transition.disposition();
        self.interpret_unit(transition)?;
        if opens_admission && disposition == bornera_core::InputDisposition::Applied {
            self.publish_admission_opened()?;
        }
        Ok(())
    }
}
