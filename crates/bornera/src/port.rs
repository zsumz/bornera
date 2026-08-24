//! Cloneable producer for one exact connection's bounded command mailbox.

use bornera_core::OperationId;
use calandria::{MailboxSender, TrySendError};

use crate::{ConnectionCommand, ConnectionToken};

/// Cross-thread producer bound to one generation-fenced connection.
///
/// Successful enqueue means only that the bounded set mailbox owns the
/// command. The authoritative confirmation for admission is
/// [`crate::ConnectionEvent::AdmissionOpened`].
#[derive(Clone, Debug)]
pub struct ConnectionPort {
    connection: ConnectionToken,
    sender: MailboxSender<ConnectionCommand>,
}

impl ConnectionPort {
    pub(crate) const fn new(
        connection: ConnectionToken,
        sender: MailboxSender<ConnectionCommand>,
    ) -> Self {
        Self { connection, sender }
    }

    /// Returns the exact set generation targeted by this producer.
    pub const fn connection(&self) -> ConnectionToken {
        self.connection
    }

    /// Queues admission opening after the caller has established its protocol session.
    pub fn open_admission(&self) -> Result<(), TrySendError<ConnectionCommand>> {
        self.sender
            .try_send_control(ConnectionCommand::OpenAdmission {
                connection: self.connection,
            })
    }

    /// Queues explicit local cancellation of one accepted operation.
    pub fn cancel(&self, operation: OperationId) -> Result<(), TrySendError<ConnectionCommand>> {
        self.sender.try_send(ConnectionCommand::Cancel {
            connection: self.connection,
            operation,
        })
    }

    /// Queues admission closure followed by ordered draining.
    pub fn begin_drain(&self) -> Result<(), TrySendError<ConnectionCommand>> {
        self.sender.try_send_control(ConnectionCommand::BeginDrain {
            connection: self.connection,
        })
    }

    /// Queues forced mechanical closure of the exact epoch.
    pub fn close(&self) -> Result<(), TrySendError<ConnectionCommand>> {
        self.sender.try_send_control(ConnectionCommand::Close {
            connection: self.connection,
        })
    }
}
