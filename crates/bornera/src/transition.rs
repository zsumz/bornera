//! Interpretation of data-only core effects through bounded Calandria owners.

use bornera_core::{ConnectionEffect, ConnectionInput, ConnectionTransition, OperationOutcome};
use calandria::Retained;

use crate::{
    ConnectionEngine, DeadlineEntry, DeadlineEvent, EngineError, EngineInvariant, EngineOutcome,
    InboundClassifier,
};

impl<D, C> ConnectionEngine<D, C>
where
    D: bornera_core::FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn interpret_unit(
        &mut self,
        transition: ConnectionTransition,
    ) -> Result<(), EngineError> {
        let mut close_reason = None;
        let mut failure = None;
        for effect in transition.into_effects() {
            match effect {
                ConnectionEffect::ScheduleDeadline {
                    epoch,
                    operation,
                    deadline,
                } => retain_first(
                    &mut failure,
                    self.schedule_deadline(DeadlineEvent { epoch, operation }, deadline),
                ),
                ConnectionEffect::DiscardWrite { effect, epoch, .. } => {
                    let _ = (effect, epoch);
                    retain_first(
                        &mut failure,
                        Err(invariant(EngineInvariant::UnexpectedDiscardEffect)),
                    );
                }
                ConnectionEffect::CancelDeadline { operation, .. } => {
                    self.cancel_deadline(operation);
                }
                ConnectionEffect::CloseEpoch { reason, .. } => {
                    close_reason = Some(reason);
                }
                ConnectionEffect::PublishOutcome {
                    epoch,
                    operation,
                    outcome,
                } => {
                    let outcome = match outcome {
                        OperationOutcome::Reply(()) => {
                            retain_first(
                                &mut failure,
                                Err(invariant(EngineInvariant::UnexpectedUnitReply)),
                            );
                            continue;
                        }
                        OperationOutcome::Failed { failure, delivery } => {
                            OperationOutcome::Failed { failure, delivery }
                        }
                        OperationOutcome::Cancelled { delivery } => {
                            OperationOutcome::Cancelled { delivery }
                        }
                    };
                    retain_first(
                        &mut failure,
                        self.publish(EngineOutcome::new(epoch, operation, outcome)),
                    );
                }
            }
        }
        self.finish_close(close_reason, &mut failure);
        failure.map_or(Ok(()), Err)
    }

    pub(crate) fn interpret_reply(
        &mut self,
        transition: ConnectionTransition<D::Frame>,
    ) -> Result<(), EngineError> {
        let mut close_reason = None;
        let mut failure = None;
        for effect in transition.into_effects() {
            match effect {
                ConnectionEffect::ScheduleDeadline {
                    epoch,
                    operation,
                    deadline,
                } => retain_first(
                    &mut failure,
                    self.schedule_deadline(DeadlineEvent { epoch, operation }, deadline),
                ),
                ConnectionEffect::DiscardWrite { effect, epoch, .. } => {
                    let _ = (effect, epoch);
                    retain_first(
                        &mut failure,
                        Err(invariant(EngineInvariant::UnexpectedDiscardEffect)),
                    );
                }
                ConnectionEffect::CancelDeadline { operation, .. } => {
                    self.cancel_deadline(operation);
                }
                ConnectionEffect::CloseEpoch { reason, .. } => {
                    close_reason = Some(reason);
                }
                ConnectionEffect::PublishOutcome {
                    epoch,
                    operation,
                    outcome,
                } => retain_first(
                    &mut failure,
                    self.publish(EngineOutcome::new(epoch, operation, outcome)),
                ),
            }
        }
        self.finish_close(close_reason, &mut failure);
        failure.map_or(Ok(()), Err)
    }

    fn schedule_deadline(
        &mut self,
        event: DeadlineEvent,
        deadline: calandria::Deadline,
    ) -> Result<(), EngineError> {
        self.cancel_deadline(event.operation);
        if self.deadlines.len() >= self.limits.operation_capacity().get() {
            return Err(invariant(EngineInvariant::DeadlineIndexCapacity));
        }
        let token = self
            .timers
            .schedule(deadline, event)
            .map_err(|error| invariant(EngineInvariant::DeadlineSchedule(error.failure())))?;
        self.deadlines.push(DeadlineEntry {
            operation: event.operation,
            token,
        });
        Ok(())
    }

    fn cancel_deadline(&mut self, operation: bornera_core::OperationId) {
        let Some(index) = self
            .deadlines
            .iter()
            .position(|entry| entry.operation == operation)
        else {
            return;
        };
        let entry = self.deadlines.swap_remove(index);
        let _cancelled = self.timers.cancel(entry.token);
    }

    fn publish(&mut self, outcome: EngineOutcome<D::Frame>) -> Result<(), EngineError> {
        if let Err(error) = self.outcomes.try_push(outcome) {
            let (outcome, failure) = error.into_parts();
            self.recovery_outcomes.try_push(outcome).map_err(|error| {
                invariant(EngineInvariant::RecoveryOutcomePublication(error.failure()))
            })?;
            return Err(invariant(EngineInvariant::OutcomePublication(failure)));
        }
        Ok(())
    }

    pub(crate) fn close_transport(&mut self) -> Result<(), EngineError> {
        drop(self.commands.close());
        self.command_more_pending = false;
        let Some(token) = self.transport else {
            return Ok(());
        };
        let (poller, resources) = (&mut self.poller, &mut self.resources);
        let (_, transport) = resources
            .get_mut(token)
            .map_err(|_| invariant(EngineInvariant::ResourceToken))?;
        poller.deregister(transport, token)?;
        resources
            .remove(token)
            .map_err(|_| invariant(EngineInvariant::ResourceToken))?;
        self.transport = None;
        Ok(())
    }

    fn finish_close(
        &mut self,
        reason: Option<bornera_core::CloseReason>,
        failure: &mut Option<EngineError>,
    ) {
        let Some(reason) = reason else {
            return;
        };
        retain_first(failure, self.publish_closing(reason));
        let cleanup = self.close_transport();
        if let Err(error) = cleanup {
            retain_first(failure, Err(error));
            return;
        }
        let reason = self
            .core
            .snapshot()
            .close_reason
            .ok_or_else(|| invariant(EngineInvariant::MissingCloseReason));
        let Ok(reason) = reason else {
            retain_first(failure, reason.map(|_| ()));
            return;
        };
        let transition = self
            .core
            .apply(ConnectionInput::EpochClosed {
                epoch: self.core.epoch(),
            })
            .map_err(EngineError::Core);
        match transition {
            Ok(transition) => {
                if let Err(error) = self.interpret_unit(transition) {
                    retain_first(failure, Err(error));
                    return;
                }
            }
            Err(error) => {
                retain_first(failure, Err(error));
                return;
            }
        }
        retain_first(failure, self.publish_closed(reason));
    }
}

fn retain_first(failure: &mut Option<EngineError>, result: Result<(), EngineError>) {
    if let Err(error) = result {
        failure.get_or_insert(error);
    }
}

fn invariant(source: EngineInvariant) -> EngineError {
    EngineError::Invariant(source)
}
