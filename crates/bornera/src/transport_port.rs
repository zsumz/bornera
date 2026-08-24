//! Backend-neutral transport contract for one connection slot.

use std::io::{self, Read, Write};

use calandria::Interest;

use crate::TcpSocketPolicy;

/// Backend-neutral nonblocking capability driven by a [`crate::ConnectionSlot`].
///
/// Implementations own readiness observations and must clear a readiness class
/// when its corresponding operation reports [`io::ErrorKind::WouldBlock`].
pub trait SlotTransport: Read + Write {
    /// Resolves one readiness-observed nonblocking connect attempt.
    fn finish_connect(&mut self) -> io::Result<ConnectProgress>;
    /// Applies the configured mechanical socket policy after establishment.
    fn apply_policy(&mut self, policy: TcpSocketPolicy) -> io::Result<()>;
    /// Returns whether connect completion can make progress now.
    fn can_finish_connect(&self) -> bool;
    /// Returns whether the transport is established.
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

/// Result of resolving one readiness-observed nonblocking connection attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectProgress {
    /// Establishment remains pending after the observation was consumed.
    Pending,
    /// This attempt transitioned from connecting to open.
    Opened,
    /// The capability was already open.
    AlreadyOpen,
}
