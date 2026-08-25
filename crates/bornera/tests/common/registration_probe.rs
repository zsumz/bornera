//! Per-fixture selector-call observations without process-global test state.

use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use bornera::TransportLimits;
use bornera_core::RetainedBytes;

#[derive(Clone, Debug)]
pub(crate) struct RegistrationProbe {
    state: Arc<ProbeState>,
}

#[derive(Debug)]
struct ProbeState {
    connector_calls: AtomicUsize,
    connector_limit: AtomicU64,
    registrations: AtomicUsize,
    reregistrations: AtomicUsize,
    deregistrations: AtomicUsize,
}

impl RegistrationProbe {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(ProbeState {
                connector_calls: AtomicUsize::new(0),
                connector_limit: AtomicU64::new(0),
                registrations: AtomicUsize::new(0),
                reregistrations: AtomicUsize::new(0),
                deregistrations: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn connected(&self, limits: TransportLimits) {
        self.state
            .connector_limit
            .store(limits.retained_bytes().get(), Ordering::Relaxed);
        self.state.connector_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn registered(&self) {
        self.state.registrations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reregistered(&self) {
        self.state.reregistrations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn deregistered(&self) {
        self.state.deregistrations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn connector_limits(&self) -> Option<TransportLimits> {
        (self.state.connector_calls.load(Ordering::Relaxed) != 0).then(|| {
            TransportLimits::new(RetainedBytes::new(
                self.state.connector_limit.load(Ordering::Relaxed),
            ))
        })
    }

    pub(crate) fn registrations(&self) -> usize {
        self.state.registrations.load(Ordering::Relaxed)
    }

    pub(crate) fn reregistrations(&self) -> usize {
        self.state.reregistrations.load(Ordering::Relaxed)
    }

    pub(crate) fn deregistrations(&self) -> usize {
        self.state.deregistrations.load(Ordering::Relaxed)
    }
}
