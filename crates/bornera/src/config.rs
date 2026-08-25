//! Private-field configuration for attempts, slots, and shared selectors.

mod identity;
mod set_limits;
mod slot_limits;

pub use identity::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSlotConfig,
    StandaloneConnectionConfig,
};
pub use set_limits::ConnectionSetLimits;
pub use slot_limits::{
    ConnectionSlotLimits, ConnectionSlotLimitsError, DecoderLimits, IoLimits, PublicationLimits,
    TransportLimits,
};
