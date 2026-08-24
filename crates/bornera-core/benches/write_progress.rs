//! Measures exact write progress with different in-flight ownership depths.

use std::{error::Error, hint::black_box, io::Write, time::Instant};

use bornera_core::{
    ConnectionCore, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EffectId,
    EndpointId, LaneId, MatchKeySpace, Moment, OperationOptions, RetainedBytes, WriteFrame,
};

const PROGRESS_STEPS: usize = 16_384;
const SAMPLES: usize = 5;

#[derive(Debug)]
struct Frame(Vec<u8>);

impl WriteFrame for Frame {
    fn bytes(&self) -> &[u8] {
        &self.0
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::new(u64::try_from(self.0.len()).unwrap_or(u64::MAX))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    run()
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut output = String::new();
    for in_flight in [1, 64, 256, 4_096] {
        let mut total_nanos = 0_u128;
        for _ in 0..SAMPLES {
            total_nanos = total_nanos.saturating_add(measure(in_flight)?);
        }
        let operations = u128::try_from(PROGRESS_STEPS.saturating_mul(SAMPLES))?;
        output.push_str("write_progress in_flight=");
        output.push_str(&in_flight.to_string());
        output.push_str(" ns_per_progress=");
        output.push_str(&(total_nanos / operations).to_string());
        output.push('\n');
    }
    std::io::stdout().write_all(output.as_bytes())?;
    Ok(())
}

fn measure(in_flight: usize) -> Result<u128, Box<dyn Error>> {
    let (mut core, effect) = fixture(in_flight)?;
    let epoch = core.epoch();
    let started = Instant::now();
    for _ in 0..PROGRESS_STEPS {
        let _transition = black_box(core.advance_write(epoch, effect, 1)?);
    }
    Ok(started.elapsed().as_nanos())
}

fn fixture(in_flight: usize) -> Result<(ConnectionCore<Frame>, EffectId), Box<dyn Error>> {
    let maximum_key = u32::try_from(in_flight.saturating_sub(1))?;
    let retained_limit = RetainedBytes::new(1_000_000);
    let limits = ConnectionLimits::new(
        in_flight,
        retained_limit,
        in_flight,
        retained_limit,
        MatchKeySpace::new(0, maximum_key)?,
    )?;
    let mut core = ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        limits,
    );
    let mut first_effect = None;
    for index in 0..in_flight {
        let length = if index == 0 { PROGRESS_STEPS + 1 } else { 1 };
        let retained = RetainedBytes::new(u64::try_from(length)?);
        let permit = core.reserve(
            Moment::ORIGIN,
            OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
                .session()
                .retained_bytes(retained)
                .write_retained_bytes(retained),
        )?;
        let bytes = std::iter::repeat_n(0, length).collect();
        let (operation, _) = core.commit(permit, Frame(bytes))?;
        if index == 0 {
            first_effect = core.write_effect(operation);
        }
    }
    let effect =
        first_effect.ok_or_else(|| std::io::Error::other("benchmark created no write identity"))?;
    Ok((core, effect))
}
