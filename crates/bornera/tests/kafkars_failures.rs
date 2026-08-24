//! Kafka-driver failure-boundary persona over production loopback TCP.

#[path = "support/io_engine.rs"]
mod io_engine;
mod support;

use std::{
    error::Error,
    io::Read,
    net::{Shutdown, TcpListener},
    num::NonZeroUsize,
    sync::mpsc,
    thread,
};

use bornera::{OutboundFrame, OwnerFailure};
use bornera_core::{
    CloseReason, Delivery, OperationFailure, OperationOptions, OperationOutcome, RetainedBytes,
};
use calandria::{Deadline, Moment, Next, Span};

use io_engine::engine_with_io;
use support::framing::{FRAME_BYTES, request};
use support::{TestEngine, engine};

#[test]
fn partial_write_deadline_closes_with_conservative_delivery() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut first_byte = [0_u8; 1];
        stream.read_exact(&mut first_byte)?;
        hold.recv()
            .map_err(|_| std::io::Error::other("deadline release was dropped"))
    });
    let one = NonZeroUsize::MIN;
    let mut engine = engine_with_io(address, one, one)?;
    run_until_open(&mut engine)?;
    let deadline = Deadline::at(Moment::from_nanos(100));
    let permit = engine.reserve(Moment::ORIGIN, options(deadline).session())?;
    let frame = request(permit.match_key(), 7);
    engine.commit(permit, OutboundFrame::copy_from_slice(&frame)?)?;

    engine.turn_component(Moment::ORIGIN)?;
    assert_eq!(engine.snapshot()?.queued_write_frames, 1);
    let turn = engine.turn_component(deadline.moment())?;
    assert_eq!(turn.next(), Next::Stop);
    let outcomes: Vec<_> = engine.drain_outcomes()?.collect();
    assert!(matches!(
        outcomes.as_slice(),
        [outcome]
            if matches!(
                outcome.outcome(),
                OperationOutcome::Failed {
                    failure: OperationFailure::ConnectionClosed(
                        CloseReason::DeadlineAfterPossibleSend
                    ),
                    delivery: Delivery::PossiblySent,
                }
            )
    ));
    release.send(())?;
    join(server)?;
    Ok(())
}

#[test]
fn peer_close_is_observed_without_protocol_retry_semantics() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (stream, _) = listener.accept()?;
        stream.shutdown(Shutdown::Both)
    });
    let mut engine = engine(address)?;
    run_until_stopped(&mut engine)?;
    assert_eq!(
        engine.snapshot()?.connection.close_reason,
        Some(CloseReason::TransportLost)
    );
    join(server)?;
    Ok(())
}

#[test]
fn explicit_owner_recovery_returns_the_unsent_kafka_frame() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let engine = engine(listener.local_addr()?)?;
    let mut engine = engine;
    let permit = engine.reserve(
        Moment::ORIGIN,
        options(Deadline::at(Moment::from_nanos(100))).session(),
    )?;
    let bytes = request(permit.match_key(), 11);
    let operation = engine.commit(permit, OutboundFrame::copy_from_slice(&bytes)?)?;
    let report = engine.abandon(OwnerFailure::OwnerInvariant);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(report.operations[0].operation, operation);
    assert_eq!(report.operations[0].delivery, Delivery::NotSent);
    assert_eq!(
        report.operations[0]
            .frame
            .as_ref()
            .map(OutboundFrame::as_bytes),
        Some(&bytes[..])
    );
    assert!(!report.ownership_diverged);
    Ok(())
}

fn options(deadline: Deadline) -> OperationOptions {
    OperationOptions::until(deadline)
        .retained_bytes(RetainedBytes::new(FRAME_BYTES as u64))
        .write_retained_bytes(RetainedBytes::new(FRAME_BYTES as u64))
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

fn run_until_stopped(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        let turn = engine.turn_component(Moment::ORIGIN)?;
        if turn.next() == Next::Stop {
            return Ok(());
        }
        if turn.next() != Next::Now {
            engine.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("engine did not stop within bounded persona turns").into())
}

fn join(handle: thread::JoinHandle<std::io::Result<()>>) -> Result<(), Box<dyn Error>> {
    Ok(handle
        .join()
        .map_err(|_| std::io::Error::other("persona server panicked"))??)
}
