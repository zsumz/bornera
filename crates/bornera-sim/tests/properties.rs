//! Deterministic generated traces and aggregate ownership properties.

mod common;

use std::{collections::BTreeSet, error::Error};

use bornera_core::{
    CompletionMode, ConnectionEffect, ConnectionTransition, Delivery, OperationId, OperationOutcome,
};
use bornera_sim::{
    EpochTarget, OperationIndex, SimFrame, Simulator, StepResult, Trace, TraceAction,
};

use common::{options, simulator_config, submit, trace};

#[test]
fn generated_bounded_traces_replay_exactly_and_preserve_invariants() -> Result<(), Box<dyn Error>> {
    for seed in 0..128_u64 {
        let trace = generated_trace(seed)?;
        let simulator = Simulator::new(simulator_config()?);
        let first = simulator.replay(&trace);
        let second = simulator.replay(&trace);
        assert_eq!(first, second, "seed {seed}");
        assert_report_properties(&first, seed);
    }
    Ok(())
}

fn generated_trace(seed: u64) -> Result<Trace, Box<dyn Error>> {
    let mut random = Random::new(seed);
    let mut trace = trace(100, 1_024)?;
    submit(&mut trace, CompletionMode::ReplyExpected, &[0])?;
    let mut submissions = 1_usize;
    trace.try_push(TraceAction::OpenAdmission {
        epoch: EpochTarget::Current,
    })?;
    for _ in 0..96 {
        let selector = random.next() % 8;
        let target = if random.next().is_multiple_of(7) {
            EpochTarget::Stale
        } else {
            EpochTarget::Current
        };
        let action = match selector {
            0..=2 => {
                let length = usize::try_from(random.next() % 9)?;
                let byte = u8::try_from(random.next() & 0xff)?;
                let bytes: Vec<_> = core::iter::repeat_n(byte, length).collect();
                let mode = if random.next().is_multiple_of(3) {
                    CompletionMode::WriteComplete
                } else {
                    CompletionMode::ReplyExpected
                };
                submissions = submissions.saturating_add(1);
                TraceAction::Submit {
                    now: bornera_core::Moment::ORIGIN,
                    options: options(mode, length)?,
                    frame: SimFrame::copy_from_slice(&bytes)?,
                }
            }
            3 => TraceAction::AdvanceWrite {
                bytes: usize::try_from(random.next() % 10)?,
            },
            4 => TraceAction::Cancel {
                operation: operation_index(&mut random, submissions),
                epoch: target,
            },
            5 => TraceAction::Deadline {
                operation: operation_index(&mut random, submissions),
                now: bornera_core::Moment::from_nanos(if random.next().is_multiple_of(2) {
                    999
                } else {
                    1_000
                }),
                epoch: target,
            },
            6 => TraceAction::Reply {
                operation: operation_index(&mut random, submissions),
                frame: SimFrame::copy_from_slice(&[u8::try_from(random.next() & 0xff)?])?,
                epoch: target,
            },
            _ => TraceAction::OpenAdmission { epoch: target },
        };
        trace.try_push(action)?;
    }
    trace.try_push(TraceAction::Recover)?;
    Ok(trace)
}

fn operation_index(random: &mut Random, submissions: usize) -> OperationIndex {
    let domain = submissions.saturating_add(2);
    let candidate = usize::try_from(random.next()).unwrap_or(usize::MAX) % domain;
    OperationIndex::new(candidate)
}

fn assert_report_properties(report: &bornera_sim::ReplayReport, seed: u64) {
    let mut terminal = BTreeSet::new();
    let mut progressed = BTreeSet::new();
    let mut recovered = BTreeSet::new();
    for observation in report.observations() {
        let snapshot = observation.snapshot;
        assert!(snapshot.owned_operations <= 16, "seed {seed}");
        assert!(snapshot.buffered_write_frames <= 16, "seed {seed}");
        assert!(snapshot.retained_bytes <= bornera_core::RetainedBytes::new(256));
        assert!(snapshot.buffered_write_retained_bytes <= bornera_core::RetainedBytes::new(256));
        assert_eq!(
            snapshot.owned_operations,
            snapshot
                .reserved_permits
                .saturating_add(snapshot.active_operations)
                .saturating_add(snapshot.terminal_slots),
            "seed {seed}"
        );
        assert_eq!(
            snapshot.active_match_keys, snapshot.owned_operations,
            "seed {seed}"
        );

        match &observation.result {
            StepResult::Submitted { transition, .. } | StepResult::UnitTransition(transition) => {
                record_effects(transition, &progressed, &mut terminal, seed);
            }
            StepResult::WriteTransition {
                operation,
                bytes,
                transition,
            } => {
                if *bytes != 0 {
                    progressed.insert(*operation);
                }
                record_effects(transition, &progressed, &mut terminal, seed);
            }
            StepResult::ReplyTransition(transition) => {
                record_effects(transition, &progressed, &mut terminal, seed);
            }
            StepResult::Recovered(recovery) => {
                for operation in &recovery.operations {
                    assert!(!terminal.contains(&operation.operation), "seed {seed}");
                    assert!(recovered.insert(operation.operation), "seed {seed}");
                    if progressed.contains(&operation.operation) {
                        assert_eq!(operation.delivery, Delivery::PossiblySent, "seed {seed}");
                    }
                }
            }
            _ => {}
        }
    }
}

fn record_effects<F>(
    transition: &ConnectionTransition<F>,
    progressed: &BTreeSet<OperationId>,
    terminal: &mut BTreeSet<OperationId>,
    seed: u64,
) {
    for effect in transition.effects() {
        let ConnectionEffect::PublishOutcome {
            operation, outcome, ..
        } = effect
        else {
            continue;
        };
        assert!(
            terminal.insert(*operation),
            "duplicate terminal publication, seed {seed}"
        );
        let delivery = match outcome {
            OperationOutcome::WriteComplete { delivery }
            | OperationOutcome::Failed { delivery, .. }
            | OperationOutcome::Cancelled { delivery } => Some(*delivery),
            _ => None,
        };
        if progressed.contains(operation) {
            assert_eq!(delivery, Some(Delivery::PossiblySent), "seed {seed}");
        }
    }
}

#[derive(Debug)]
struct Random(u64);

impl Random {
    const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}
