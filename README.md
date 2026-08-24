<h1 align="center">Bornera</h1>

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
bornera-sim   deterministic network capabilities for simulation
```

Operations follow `reserve -> prepare -> commit`. Bornera reserves bounded
capacity before an adapter encodes a frame, then takes ownership only when the
complete frame is committed. Every accepted operation belongs to one connection
epoch and ends exactly once or transfers through explicit fatal-owner recovery.

Delivery certainty is deliberately limited to `NotSent` and `PossiblySent`.
A local socket write cannot prove remote receipt or processing. Protocol crates
retain codecs, routing, session semantics, topology, errors, and retry policy.

## Crates

| Crate | Purpose |
| --- | --- |
| `bornera` | Production connection engine using Calandria hosting and private Mio TCP capabilities |
| `bornera-core` | Bounded admission, framing, matching, deadlines, delivery certainty, and recovery policy |
| `bornera-sim` | Deterministic network capabilities for future simulation support |

The crates begin at the same version and remain lockstepped during pre-alpha.
Use only the layer you need.

## Start

Run either production hosting model from a checkout:

```sh
cargo run -p bornera --example embedded --locked --offline
cargo run -p bornera --example dedicated --locked --offline
```

The engine exposes bounded, epoch-fenced mechanical control. It does not expose
an async runtime, protocol-semantic work, or automatic retry decisions.

## Qualification

```sh
zcheck
```

The zcheck graph is the complete gate for formatting, tests, examples, Clippy,
rustdoc, source shape, zrail architecture, and clean diffs.

Bornera requires Rust 1.88 or newer. The repository is pre-alpha; DNS ownership,
reconnect policy, optional TLS, and deterministic network simulation remain
future work.

## License

Apache-2.0. See [LICENSE](LICENSE).
