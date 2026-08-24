//! Measures owner admission and mutation paths at representative live depths.

use std::{error::Error, hint::black_box, io::Write, time::Instant};

use bornera_core::{
    CloseReason, ConnectionCore, ConnectionEpoch, ConnectionId, ConnectionInput, ConnectionLimits,
    Deadline, EffectId, EndpointId, InboundReply, LaneId, MatchKey, MatchKeySpace, Moment,
    OperationId, OperationOptions, RetainedBytes, WriteFrame,
};
use calandria::Retained;

const DEPTHS: [usize; 4] = [1, 64, 256, 4_096];
const SAMPLES: usize = 5;

#[derive(Debug)]
struct Frame([u8; 1]);

impl WriteFrame for Frame {
    fn bytes(&self) -> &[u8] {
        &self.0
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::new(1)
    }
}

#[derive(Debug)]
struct Reply;

impl Retained for Reply {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Clone, Copy, Debug)]
struct Accepted {
    operation: OperationId,
    key: MatchKey,
    effect: EffectId,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut output = String::new();
    for depth in DEPTHS {
        report(&mut output, "reserve", depth, || measure_reserve(depth))?;
        report(&mut output, "commit", depth, || measure_commit(depth))?;
        report(&mut output, "reply", depth, || measure_reply(depth))?;
        report(&mut output, "cancel", depth, || measure_cancel(depth))?;
        report(&mut output, "deadline", depth, || measure_deadline(depth))?;
        report(&mut output, "close", depth, || measure_close(depth))?;
    }
    std::io::stdout().write_all(output.as_bytes())?;
    Ok(())
}

fn report(
    output: &mut String,
    path: &str,
    depth: usize,
    mut sample: impl FnMut() -> Result<u128, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut total = 0_u128;
    for _ in 0..SAMPLES {
        total = total.saturating_add(sample()?);
    }
    output.push_str("owner_path path=");
    output.push_str(path);
    output.push_str(" depth=");
    output.push_str(&depth.to_string());
    output.push_str(" ns=");
    output.push_str(&(total / SAMPLES as u128).to_string());
    output.push('\n');
    Ok(())
}

fn measure_reserve(depth: usize) -> Result<u128, Box<dyn Error>> {
    let mut core = fixture(depth)?;
    fill(&mut core, depth.saturating_sub(1), far_deadline())?;
    let started = Instant::now();
    let permit = black_box(core.reserve(Moment::ORIGIN, options(far_deadline()))?);
    let elapsed = started.elapsed().as_nanos();
    drop(permit);
    Ok(elapsed)
}

fn measure_commit(depth: usize) -> Result<u128, Box<dyn Error>> {
    let mut core = fixture(depth)?;
    fill(&mut core, depth.saturating_sub(1), far_deadline())?;
    let permit = core.reserve(Moment::ORIGIN, options(far_deadline()))?;
    let started = Instant::now();
    let _committed = black_box(core.commit(permit, Frame([0]))?);
    Ok(started.elapsed().as_nanos())
}

fn measure_reply(depth: usize) -> Result<u128, Box<dyn Error>> {
    let mut core = fixture(depth)?;
    let accepted = fill(&mut core, depth, far_deadline())?;
    complete_writes(&mut core, &accepted)?;
    let first = accepted
        .first()
        .ok_or_else(|| std::io::Error::other("reply benchmark has no operation"))?;
    let started = Instant::now();
    let _transition =
        black_box(core.apply_reply(InboundReply::new(core.epoch(), first.key, Reply))?);
    Ok(started.elapsed().as_nanos())
}

fn measure_cancel(depth: usize) -> Result<u128, Box<dyn Error>> {
    let mut core = fixture(depth)?;
    let accepted = fill(&mut core, depth, far_deadline())?;
    let last = accepted
        .last()
        .ok_or_else(|| std::io::Error::other("cancel benchmark has no operation"))?;
    let started = Instant::now();
    let _transition = black_box(core.apply(ConnectionInput::Cancel {
        epoch: core.epoch(),
        operation: last.operation,
    })?);
    Ok(started.elapsed().as_nanos())
}

fn measure_deadline(depth: usize) -> Result<u128, Box<dyn Error>> {
    let mut core = fixture(depth)?;
    fill(&mut core, depth.saturating_sub(1), far_deadline())?;
    let deadline = Deadline::at(Moment::from_nanos(1));
    let permit = core.reserve(Moment::ORIGIN, options(deadline))?;
    let operation = permit.operation_id();
    let _committed = black_box(core.commit(permit, Frame([0]))?);
    let now = deadline.moment();
    let started = Instant::now();
    let _transition = black_box(core.apply(ConnectionInput::DeadlineElapsed {
        epoch: core.epoch(),
        operation,
        now,
    })?);
    Ok(started.elapsed().as_nanos())
}

fn measure_close(depth: usize) -> Result<u128, Box<dyn Error>> {
    let mut core = fixture(depth)?;
    fill(&mut core, depth, far_deadline())?;
    let started = Instant::now();
    let _transition = black_box(core.apply(ConnectionInput::CloseRequested {
        epoch: core.epoch(),
        reason: CloseReason::Requested,
    })?);
    Ok(started.elapsed().as_nanos())
}

fn fixture(depth: usize) -> Result<ConnectionCore<Frame>, Box<dyn Error>> {
    let maximum_key = u32::try_from(depth.saturating_sub(1))?;
    let retained = RetainedBytes::new(u64::try_from(depth)?.saturating_mul(2));
    let limits = ConnectionLimits::new(
        depth,
        retained,
        depth,
        retained,
        MatchKeySpace::new(0, maximum_key)?,
    )?;
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        limits,
    ))
}

fn fill(
    core: &mut ConnectionCore<Frame>,
    count: usize,
    deadline: Deadline,
) -> Result<Vec<Accepted>, Box<dyn Error>> {
    let mut accepted = Vec::with_capacity(count);
    for _ in 0..count {
        let permit = core.reserve(Moment::ORIGIN, options(deadline))?;
        let key = permit.match_key();
        let (operation, _transition) = core.commit(permit, Frame([0]))?;
        let effect = core
            .write_effect(operation)
            .ok_or_else(|| std::io::Error::other("accepted frame has no write identity"))?;
        accepted.push(Accepted {
            operation,
            key,
            effect,
        });
    }
    Ok(accepted)
}

fn complete_writes(
    core: &mut ConnectionCore<Frame>,
    accepted: &[Accepted],
) -> Result<(), Box<dyn Error>> {
    for operation in accepted {
        let _transition = core.advance_write(core.epoch(), operation.effect, 1)?;
    }
    Ok(())
}

fn options(deadline: Deadline) -> OperationOptions {
    OperationOptions::until(deadline)
        .session()
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1))
}

fn far_deadline() -> Deadline {
    Deadline::at(Moment::from_nanos(u64::MAX))
}
