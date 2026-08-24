//! Kafka-driver persona over the production capacity-one owner.

#[path = "support/io_engine.rs"]
mod io_engine;
mod support;

use std::{
    error::Error,
    io::{Read, Write},
    net::TcpListener,
    num::NonZeroUsize,
    sync::mpsc,
    thread,
};

use bornera::{OutboundFrame, TransportState};
use bornera_core::{
    CancelOutcome, CloseReason, CompletionMode, Delivery, InputDisposition, OperationOptions,
    OperationOutcome, RetainedBytes,
};
use calandria::{Deadline, Moment, Span};

use io_engine::engine_with_io;
use support::framing::{FRAME_BYTES, request};
use support::{TestEngine, engine};

#[test]
fn session_correlated_requests_and_acks_zero_share_one_healthy_epoch() -> Result<(), Box<dyn Error>>
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        echo_one(&mut stream)?;
        echo_one(&mut stream)?;
        let mut no_reply = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut no_reply)?;
        hold.recv()
            .map_err(|_| std::io::Error::other("persona release was dropped"))
    });

    let mut engine = engine(address)?;
    let session_permit = engine.reserve(
        Moment::ORIGIN,
        options(CompletionMode::ReplyExpected).session(),
    )?;
    let session_frame = request(session_permit.match_key(), 18);
    let session = engine.commit(
        session_permit,
        OutboundFrame::copy_from_slice(&session_frame)?,
    )?;
    run_until_outcomes(&mut engine, 1)?;
    let session_outcomes: Vec<_> = engine.drain_outcomes()?.collect();
    assert!(matches!(
        session_outcomes.as_slice(),
        [outcome]
            if outcome.operation() == session
                && matches!(outcome.outcome(), OperationOutcome::Reply(frame) if frame.0 == session_frame)
    ));

    assert_eq!(engine.open_admission()?, InputDisposition::Applied);
    let request_permit = engine.reserve(Moment::ORIGIN, options(CompletionMode::ReplyExpected))?;
    let request_frame = request(request_permit.match_key(), 3);
    let correlated = engine.commit(
        request_permit,
        OutboundFrame::copy_from_slice(&request_frame)?,
    )?;
    let no_reply_permit = engine.reserve(Moment::ORIGIN, options(CompletionMode::WriteComplete))?;
    let no_reply_frame = request(no_reply_permit.match_key(), 0);
    let no_reply = engine.commit(
        no_reply_permit,
        OutboundFrame::copy_from_slice(&no_reply_frame)?,
    )?;
    run_until_outcomes(&mut engine, 2)?;

    let outcomes: Vec<_> = engine.drain_outcomes()?.collect();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().any(|outcome| {
        outcome.operation() == correlated
            && matches!(outcome.outcome(), OperationOutcome::Reply(frame) if frame.0 == request_frame)
    }));
    assert!(outcomes.iter().any(|outcome| {
        outcome.operation() == no_reply
            && matches!(
                outcome.outcome(),
                OperationOutcome::WriteComplete {
                    delivery: Delivery::PossiblySent
                }
            )
    }));
    assert_eq!(engine.snapshot()?.transport, TransportState::Open);
    assert_eq!(engine.begin_drain()?, InputDisposition::Applied);
    assert_eq!(
        engine.snapshot()?.connection.close_reason,
        Some(CloseReason::Drained)
    );
    release.send(())?;
    join(server)?;
    Ok(())
}

#[test]
fn cancellation_distinguishes_queued_from_partially_written_work() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        hold.recv()
            .map_err(|_| std::io::Error::other("cancellation release was dropped"))
    });
    let one = NonZeroUsize::MIN;
    let mut engine = engine_with_io(address, one, one)?;
    run_until_open(&mut engine)?;
    assert_eq!(engine.open_admission()?, InputDisposition::Applied);

    let queued_permit = engine.reserve(Moment::ORIGIN, options(CompletionMode::ReplyExpected))?;
    let queued_frame = request(queued_permit.match_key(), 1);
    let queued = engine.commit(
        queued_permit,
        OutboundFrame::copy_from_slice(&queued_frame)?,
    )?;
    assert_eq!(engine.cancel(queued)?, CancelOutcome::CancelledNotSent);

    let writing_permit = engine.reserve(Moment::ORIGIN, options(CompletionMode::WriteComplete))?;
    let writing_frame = request(writing_permit.match_key(), 0);
    let writing = engine.commit(
        writing_permit,
        OutboundFrame::copy_from_slice(&writing_frame)?,
    )?;
    engine.turn_component(Moment::ORIGIN)?;
    assert_eq!(engine.snapshot()?.queued_write_frames, 1);
    assert_eq!(
        engine.cancel(writing)?,
        CancelOutcome::ObservationCancelled {
            delivery: Delivery::PossiblySent
        }
    );
    run_until_written(&mut engine)?;

    let outcomes: Vec<_> = engine.drain_outcomes()?.collect();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().any(|outcome| {
        outcome.operation() == queued
            && matches!(
                outcome.outcome(),
                OperationOutcome::Cancelled {
                    delivery: Delivery::NotSent
                }
            )
    }));
    assert!(outcomes.iter().any(|outcome| {
        outcome.operation() == writing
            && matches!(
                outcome.outcome(),
                OperationOutcome::Cancelled {
                    delivery: Delivery::PossiblySent
                }
            )
    }));
    assert_eq!(engine.snapshot()?.connection.owned_operations, 0);
    release.send(())?;
    join(server)?;
    Ok(())
}

fn options(completion: CompletionMode) -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(100)))
        .retained_bytes(RetainedBytes::new(FRAME_BYTES as u64))
        .write_retained_bytes(RetainedBytes::new(FRAME_BYTES as u64))
        .completion_mode(completion)
}

fn echo_one(stream: &mut std::net::TcpStream) -> std::io::Result<()> {
    let mut frame = [0_u8; FRAME_BYTES];
    stream.read_exact(&mut frame)?;
    stream.write_all(&frame)
}

fn run_until_open(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        engine.turn_component(Moment::ORIGIN)?;
        if engine.is_transport_open()? {
            return Ok(());
        }
        engine.poll_io(Span::from_nanos(10_000_000))?;
    }
    Err(std::io::Error::other("transport did not open within bounded persona turns").into())
}

fn run_until_written(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        engine.turn_component(Moment::ORIGIN)?;
        if engine.snapshot()?.queued_write_frames == 0 {
            return Ok(());
        }
        engine.poll_io(Span::from_nanos(10_000_000))?;
    }
    Err(std::io::Error::other("frame did not finish within bounded persona turns").into())
}

fn run_until_outcomes(engine: &mut TestEngine, count: usize) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        engine.turn_component(Moment::ORIGIN)?;
        if engine.snapshot()?.pending_outcomes == count {
            return Ok(());
        }
        engine.poll_io(Span::from_nanos(10_000_000))?;
    }
    Err(std::io::Error::other("outcomes did not arrive within bounded persona turns").into())
}

fn join(handle: thread::JoinHandle<std::io::Result<()>>) -> Result<(), Box<dyn Error>> {
    Ok(handle
        .join()
        .map_err(|_| std::io::Error::other("persona server panicked"))??)
}
