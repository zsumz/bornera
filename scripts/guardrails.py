#!/usr/bin/env python3
"""Fail-closed source checks that complement zrail's semantic policy."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parent.parent
CRATES = REPOSITORY / "crates"
CORE = CRATES / "bornera-core" / "src"
ENGINE = CRATES / "bornera" / "src"
CARGO_CONFIGS = (REPOSITORY / ".cargo" / "config", REPOSITORY / ".cargo" / "config.toml")

TEST_NAMES = ("tests.rs", "_test.rs", "_tests.rs")
CORE_VOCABULARY = ("kafka", "cassandra", "postgres", "postgresql", "redis")
CORE_CAPABILITIES = (
    "calandria_mio",
    "mio::",
    "tokio::",
    "std::net",
    "std::process",
    "std::sync::Mutex",
    "std::sync::RwLock",
    "std::thread",
)
COLLECTION = re.compile(
    r"\b(?:BTreeMap|BTreeSet|BinaryHeap|HashMap|HashSet|LinkedList|Vec|VecDeque)\b"
)
PROTOCOL_ENUM = re.compile(r"\benum\s+[A-Za-z0-9_]*Protocol[A-Za-z0-9_]*\b")
CALLBACK_TRAIT = re.compile(r"\b(?:Fn|FnMut|FnOnce)\s*(?:\(|<|\+)")
TEST_MODULE_EDGE = re.compile(r'#\[cfg\(test\)\]\s*mod\s+[A-Za-z0-9_]+\s*;')
BOUNDED_COLLECTION_OWNERS = {
    "crates/bornera-core/src/admission/ledger.rs",
    "crates/bornera-core/src/admission/key_set.rs",
    "crates/bornera-core/src/connection/transition.rs",
    "crates/bornera-core/src/connection/recovery.rs",
    "crates/bornera-core/src/connection/recovery_item.rs",
    "crates/bornera-core/src/connection/journal.rs",
    "crates/bornera-core/src/matching/ordered_verified.rs",
    "crates/bornera-core/src/write/queue.rs",
    "crates/bornera-core/src/write/state.rs",
}


def relative(path: Path) -> str:
    return path.relative_to(REPOSITORY).as_posix()


def is_test(path: Path) -> bool:
    return "tests" in path.parts or path.name == "tests.rs" or path.name.endswith(TEST_NAMES[1:])


def report(violations: list[str], path: Path, rule: str) -> None:
    violations.append(f"{relative(path)}: {rule}")


def inspect(path: Path, violations: list[str]) -> None:
    source = path.read_text(encoding="utf-8")
    test_source = is_test(path)

    implementation = TEST_MODULE_EDGE.sub("", source)
    if not test_source and ("#[test]" in implementation or "#[cfg(test)]" in implementation):
        report(violations, path, "tests must live in sibling test files")

    if path.is_relative_to(CORE):
        lowered = source.lower()
        for name in CORE_VOCABULARY:
            if re.search(rf"\b{re.escape(name)}\b", lowered):
                report(violations, path, f"protocol vocabulary {name!r} is forbidden in core")
        for capability in CORE_CAPABILITIES:
            if capability in source:
                report(violations, path, f"capability {capability!r} is forbidden in core")
        if PROTOCOL_ENUM.search(source):
            report(violations, path, "protocol-kind enums are forbidden in core")
        if (
            not test_source
            and COLLECTION.search(source)
            and relative(path) not in BOUNDED_COLLECTION_OWNERS
        ):
            report(
                violations,
                path,
                "direct variable-size collections require a reviewed bounded owner",
            )

    if path.is_relative_to(ENGINE) and not test_source and CALLBACK_TRAIT.search(source):
        report(violations, path, "callback traits are forbidden on production owner paths")

    if relative(path).endswith("/matching/keyed.rs"):
        report(violations, path, "Keyed is deferred until the Cassandra proof")


def main() -> int:
    violations: list[str] = []
    for path in CARGO_CONFIGS:
        if path.exists() or path.is_symlink():
            report(violations, path, "repository-local Cargo configuration is not permitted")

    for path in sorted(CRATES.rglob("*.rs")):
        inspect(path, violations)

    if violations:
        print("guardrail violations:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1

    print("guardrails: pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
