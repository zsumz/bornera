<p align="center">
  <img src="https://raw.githubusercontent.com/zsumz/bornera/main/bornera-logo.svg" alt="bornera" width="720">
</p>

<p align="center">
  <strong>Deterministic connection ownership for native protocol clients.</strong>
</p>

<p align="center">
  Bornera owns bounded connection lifecycles between protocol codecs and
  sockets without taking over protocol semantics, retry policy, or public APIs.
</p>

<p align="center">
  <a href="#model">Model</a>
  <span> · </span>
  <a href="#crates">Crates</a>
  <span> · </span>
  <a href="#start">Start</a>
  <span> · </span>
  <a href="#qualification">Qualification</a>
</p>

<br />

## Model

```text
bornera       production connection ownership hosted by Calandria
bornera-core  deterministic sans-I/O connection policy
bornera-sim   deterministic bounded trace replay (currently unpublished)
```

Operations follow `reserve -> prepare -> commit`. Bornera reserves bounded
capacity before an adapter encodes a frame, then takes ownership only when the
complete frame is committed. Every accepted operation belongs to one connection
epoch and ends exactly once while its owner is driven to completion or consumed
through explicit fatal-owner recovery. Arbitrary Rust `drop`, process failure,
or a panic in adapter code can discard observations that were not drained.

Production ownership is split deliberately:

```text
ConnectionSlot         selector-free state for one exact connection epoch
ConnectionSet          one selector and bounded fair progression for many slots
StandaloneConnection   capacity-one convenience wrapper around ConnectionSet
```

`bornera-sim` drives that same selector-free `ConnectionSlot`—including its
decoder, classifier, deadlines, publications, and recovery path—through
Calandria virtual time and a bounded simulated transport. It remains an
unpublished qualification crate rather than a peer production capability.

Delivery certainty is deliberately limited to `NotSent` and `PossiblySent`.
A local socket write cannot prove remote receipt or processing. Protocol crates
retain codecs, routing, session semantics, topology, errors, and retry policy.

## Crates

| Crate | Purpose |
| --- | --- |
| `bornera` | Shared-selector production connection ownership using Calandria hosting and private Mio TCP capabilities |
| `bornera-core` | Bounded admission, framing, matching, deadlines, delivery certainty, and recovery policy |
| `bornera-sim` | Unpublished bounded trace capture, exact replay, and generated policy properties |

The crates begin at the same version and remain lockstepped during pre-alpha.
Use only the layer you need.

## Start

Add only the layers you need:

```toml
[dependencies]
bornera = "=0.0.1-rc.2"
bornera-core = "=0.0.1-rc.2"
calandria = { version = "=0.0.1-rc.2", features = ["std"] }
```

Run either production hosting model from a checkout:

```sh
cargo run -p bornera --example embedded --locked --offline
cargo run -p bornera --example dedicated --locked --offline
```

Public configuration and host contracts use Bornera-Core and Calandria value
types, so production consumers should declare all three layers explicitly.
Mailbox success means a command is queued, not applied. The sequenced
`AdmissionOpened` lifecycle event is the authoritative confirmation that
regular admission opened.

The owner exposes bounded, generation- and epoch-fenced mechanical control. It
does not expose an async runtime, protocol-semantic work, or automatic retry
decisions.

## Qualification

```sh
zcheck
```

The zcheck graph is the complete local gate for formatting, tests, examples,
Clippy, rustdoc, source shape, zrail architecture, clean diffs, package
contents, packaged-crate smoke compilation, and publish ordering.

Bornera requires Rust 1.88 or newer. Checked-in CI additionally qualifies the
latest stable toolchain, macOS, Windows, cargo-deny, selected Miri tests, and a
workspace coverage report. The production loopback persona covers session
establishment, correlated request/reply, Kafka-style no-reply writes, partial
writes at deadline, cancellation on both sides of first write progress, peer
loss, and explicit owner recovery. DNS ownership, address selection, reconnect
policy, and optional TLS remain future work. A prerelease version in source is
not release proof, and package publication must occur in dependency order:
`bornera-core` before `bornera`.

## License

Apache-2.0. See [LICENSE](LICENSE).
