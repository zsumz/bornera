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

Bornera reserves bounded capacity before a protocol adapter prepares a frame,
then takes ownership only when the complete frame is committed:

```text
reserve -> prepare -> commit -> drive -> complete or recover
```

The production owner runs many connection slots through one selector. The same
selector-free policy can also run under deterministic simulation.

## Crates

| Crate | Purpose |
| --- | --- |
| `bornera` | Shared-selector production ownership for native transports |
| `bornera-core` | Sans-I/O admission, framing, deadlines, delivery, and recovery policy |
| `bornera-rustls` | Bounded rustls client transport |
| `bornera-sim` | Unpublished deterministic trace replay and generated properties |

The crates remain version-locked during pre-alpha. Use only the layers your
integration needs.

## Start

```toml
[dependencies]
bornera = "=0.0.1-rc.3"
bornera-core = "=0.0.1-rc.3"
calandria = { version = "=0.0.1-rc.2", features = ["std"] }
```

Run the embedded production example from a checkout:

```sh
cargo run -p bornera --example embedded --locked --offline
```

See the [dedicated-owner example](https://github.com/zsumz/bornera/blob/main/crates/bornera/examples/dedicated.rs)
for the thread-owned hosting model and the [architecture guide](https://github.com/zsumz/bornera/blob/main/docs/architecture.md)
for TLS setup and integration boundaries.

## Guarantees

- Admission is bounded before ownership transfers.
- Accepted work completes exactly once or transfers through explicit recovery.
- Delivery certainty is intentionally limited to `NotSent` and `PossiblySent`.
- Connection generations and epochs fence stale work.
- Protocol semantics, routing, retries, DNS, and address selection stay with the caller.

## Documentation

- [Architecture and ownership](https://github.com/zsumz/bornera/blob/main/docs/architecture.md)
- [Qualification and release proof](https://github.com/zsumz/bornera/blob/main/docs/qualification.md)
- [API documentation](https://docs.rs/bornera)

## Qualification

```sh
zcheck
```

The canonical gate covers architecture, formatting, Clippy, rustdoc, tests,
package contents, and packaged-crate smoke compilation. Bornera requires Rust
1.88 or newer; the full evidence matrix is documented in
[qualification](https://github.com/zsumz/bornera/blob/main/docs/qualification.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
