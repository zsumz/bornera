//! Backend-neutral transport contract for one connection slot.

use std::{
    io::{self, Read, Write},
    net::SocketAddr,
};

use calandria::{Interest, Readiness};
use mio::event::Source;

use crate::{TcpSocketPolicy, TransportBudget, TransportError, TransportProgress};

/// Backend-neutral nonblocking capability driven by a [`crate::ConnectionSlot`].
///
/// `Read` and `Write` exchange application bytes. A positive application write
/// irreversibly transfers exactly that prefix into transport ownership; it need not
/// mean that encoded transport bytes reached the operating system. A nonempty write
/// must not return `Ok(0)`: implementations expose local backpressure through
/// [`SlotTransport::can_write`] or `WouldBlock`.
///
/// Implementations own readiness observations and must clear a readiness class
/// when its corresponding operation reports [`io::ErrorKind::WouldBlock`].
pub trait SlotTransport: Read + Write {
    /// Performs bounded establishment work, including socket policy, toward application readiness.
    ///
    /// [`SlotTransport::can_establish`] must cover all immediately runnable work while
    /// establishment remains incomplete. A successful call must report nonzero work
    /// within `budget`.
    fn drive_establishment(
        &mut self,
        policy: TcpSocketPolicy,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError>;
    /// Performs bounded transport-local work independent of application frames.
    ///
    /// A successful call made after [`SlotTransport::has_transport_work`] returned
    /// `true` must report nonzero work within `budget`.
    fn drive_transport(
        &mut self,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError>;
    /// Returns whether establishment can make immediate progress now.
    fn can_establish(&self) -> bool;
    /// Returns whether transport-local work can make immediate progress without a new edge.
    fn has_transport_work(&self) -> bool;
    /// Returns whether complete establishment allows application-byte exchange.
    ///
    /// This must remain `false` until `drive_establishment` has accepted the exact
    /// supplied socket policy and completed every transport-specific handshake.
    fn is_open(&self) -> bool;
    /// Returns whether a read can be attempted now.
    fn can_read(&self) -> bool;
    /// Returns whether a write can be attempted now.
    fn can_write(&self) -> bool;
    /// Returns the readiness interest required for the current phase and write ownership.
    fn desired_interest(&self, has_writes: bool) -> Interest;
    /// Clears the current readable observation after a would-block result.
    fn clear_read(&mut self);
    /// Clears the current writable observation after a would-block result.
    fn clear_write(&mut self);
}

/// One backend-neutral transport that can be registered with a Mio selector.
///
/// Readiness observations belong to the exact registered transport generation.
/// Implementations must retain them until the corresponding nonblocking operation
/// consumes the observation or reports `WouldBlock`.
pub trait RegisteredTransport: SlotTransport + Source {
    /// Merges one readiness observation into this transport generation.
    fn observe_readiness(&mut self, readiness: Readiness);
}

/// Capacity-first construction of one exact nonblocking registered transport.
///
/// A [`crate::ConnectionSet`] invokes the connector only after reserving its bounded
/// resource slot. Implementations must initiate at most one nonblocking address attempt.
pub trait TransportConnector {
    /// Concrete transport produced for this homogeneous connection set.
    type Transport: RegisteredTransport;

    /// Initiates one exact nonblocking attempt to the already-resolved address.
    fn connect(self, address: SocketAddr) -> io::Result<Self::Transport>;
}
