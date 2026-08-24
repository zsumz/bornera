//! Hard bounds for one selector and its fair multi-connection turn.

use core::num::NonZeroUsize;

use calandria::{LaneLimits, MailboxLimits, RetainedBytes};
use calandria_mio::MioPollerLimits;

use super::ConnectionSlotLimits;

/// Hard bounds for one shared selector and its fair owner turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSetLimits {
    max_connections: NonZeroUsize,
    poll_events: NonZeroUsize,
    command_capacity: NonZeroUsize,
    commands_per_turn: NonZeroUsize,
    ready_connections_per_turn: NonZeroUsize,
}

impl ConnectionSetLimits {
    /// Creates explicit connection, selector, mailbox, and fairness bounds.
    pub const fn new(
        max_connections: NonZeroUsize,
        poll_events: NonZeroUsize,
        command_capacity: NonZeroUsize,
        commands_per_turn: NonZeroUsize,
        ready_connections_per_turn: NonZeroUsize,
    ) -> Self {
        Self {
            max_connections,
            poll_events,
            command_capacity,
            commands_per_turn,
            ready_connections_per_turn,
        }
    }

    pub(crate) const fn max_connections(self) -> NonZeroUsize {
        self.max_connections
    }

    pub(crate) const fn poller(self) -> MioPollerLimits {
        MioPollerLimits::new(self.poll_events, self.max_connections)
    }

    pub(crate) const fn commands(self) -> MailboxLimits {
        let lane = LaneLimits::new(self.command_capacity, RetainedBytes::ZERO);
        MailboxLimits::new(lane, lane)
    }

    pub(crate) const fn commands_per_turn(self) -> NonZeroUsize {
        self.commands_per_turn
    }

    pub(crate) const fn ready_connections_per_turn(self) -> NonZeroUsize {
        self.ready_connections_per_turn
    }

    pub(crate) const fn standalone(slot: ConnectionSlotLimits) -> Self {
        Self::new(
            NonZeroUsize::MIN,
            slot.io_operations(),
            slot.io_operations(),
            slot.io_operations(),
            NonZeroUsize::MIN,
        )
    }
}
