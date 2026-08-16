//! Admission drain and physical close confirmation.

use crate::{
    AdmissionGate, CloseReason, ConnectionMachine, ConnectionPhase, ConnectionTransition,
    InputDisposition,
};

impl ConnectionMachine {
    pub(super) fn begin_drain(&mut self, epoch: crate::ConnectionEpoch) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        if self.phase != ConnectionPhase::Live
            || matches!(self.gate, AdmissionGate::Closed | AdmissionGate::Draining)
        {
            return ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase);
        }
        self.gate = AdmissionGate::Draining;
        let mut transition = ConnectionTransition::new(InputDisposition::Applied);
        self.finish_drain(&mut transition);
        transition
    }

    pub(super) fn close_requested(
        &mut self,
        epoch: crate::ConnectionEpoch,
        reason: CloseReason,
    ) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        if self.phase != ConnectionPhase::Live {
            return ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase);
        }
        let mut transition = ConnectionTransition::new(InputDisposition::Applied);
        self.close_into(reason, &mut transition);
        transition
    }

    pub(super) fn epoch_closed(&mut self, epoch: crate::ConnectionEpoch) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        if self.phase != ConnectionPhase::Closing {
            return ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase);
        }
        self.phase = ConnectionPhase::Closed;
        self.gate = AdmissionGate::Closed;
        ConnectionTransition::new(InputDisposition::Applied)
    }
}
