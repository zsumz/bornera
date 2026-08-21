//! Constructs and runs a Calandria reactor entirely on its dedicated owner thread.

mod support;

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener},
    thread,
};

use bornera::ConnectionWaiter;
use calandria::{MonotonicClock, Reactor, ReactorOutcome};

use support::{BoxError, FRAME_BYTES, prepared_engine};

fn main() -> Result<(), BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let peer = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        stream.write_all(&frame)?;
        stream.shutdown(Shutdown::Both)
    });

    let owner = thread::Builder::new().name("bornera-owner".into()).spawn(
        move || -> Result<bool, BoxError> {
            let (engine, expected) = prepared_engine(address)?;
            let termination_wake = engine.wake_handle();
            let reactor = Reactor::new(
                engine,
                MonotonicClock::new(),
                ConnectionWaiter,
                termination_wake,
            );
            let exit = reactor.run();
            if !matches!(exit.outcome(), ReactorOutcome::Stopped) {
                return Err(std::io::Error::other("dedicated reactor failed").into());
            }
            let mut engine = exit.into_duty();
            let outcome = engine
                .drain_outcomes()
                .next()
                .ok_or_else(|| std::io::Error::other("dedicated example received no outcome"))?;
            Ok(outcome.into_outcome()
                == bornera_core::OperationOutcome::Reply(support::ExampleFrame(expected.to_vec())))
        },
    )?;

    let matched = owner
        .join()
        .map_err(|_| std::io::Error::other("dedicated owner panicked"))??;
    peer.join()
        .map_err(|_| std::io::Error::other("dedicated peer panicked"))??;
    if !matched {
        return Err(std::io::Error::other("dedicated reply did not match").into());
    }
    Ok(())
}
