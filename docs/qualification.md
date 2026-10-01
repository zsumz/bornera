# Qualification and release proof

## Canonical local gate

Run the complete local qualification graph from the repository root:

```sh
zcheck
```

The graph covers:

- source guardrails and zrail architecture policy;
- formatting, Clippy, and rustdoc with warnings denied;
- the complete workspace test suite and examples;
- clean staged and unstaged diffs;
- package contents, registry-shaped compilation, and extracted-package Kafka
  admission, deadline, completion, recovery, and client TLS personas.

Bornera requires Rust 1.88 or newer.

## Hosted qualification

Checked-in CI additionally qualifies:

- the latest stable Rust toolchain;
- macOS and Windows builds;
- cargo-deny policy;
- selected Miri tests;
- a workspace coverage report.

## Behavioral evidence

The production loopback persona covers session establishment, correlated
request/reply, Kafka-style no-reply writes, partial writes at deadline,
cancellation on both sides of first write progress, peer loss, and explicit
owner recovery.

The rustls persona covers handshake read/write alternation, buffered ciphertext,
local decrypted plaintext, SNI and certificate failures, truncation, graceful
close, logical buffer ceilings, and connection-local TLS failure isolation.
It explicitly qualifies TLS 1.2 request/reply and rejects TLS 1.3 handshake
messages crossing a key-change boundary before transport or admission opens.

These tests do not assign Bornera ownership of DNS, address selection, protocol
semantics, or reconnect policy.

## Release proof

A prerelease version in source is not publication proof. Publish the lockstepped
crates in dependency order:

```text
bornera-core -> bornera -> bornera-rustls
```

Each package must pass the archive-content check and compile from its normalized,
registry-shaped dependency graph before publication. The extracted packages must
also pass the Kafka-used behavioral tests; repository-only architecture probes
remain in the source gate because their sibling manifests are not published.
