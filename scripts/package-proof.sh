#!/bin/sh
set -eu
export LANG=C
export LC_ALL=C
# Archive locations below belong to this proof's isolated scratch workspace.
unset CARGO_TARGET_DIR

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
temporary_root=${TMPDIR:-/tmp}
scratch=$(mktemp -d "$temporary_root/bornera-package.XXXXXX")

cleanup() {
    if [ -n "${scratch:-}" ] && [ -d "$scratch" ]; then
        rm -rf -- "$scratch"
    fi
}
trap cleanup EXIT HUP INT TERM

workspace="$scratch/repository"
mkdir -p "$workspace/crates"
cp "$repository/Cargo.toml" "$repository/Cargo.lock" "$workspace/"
cp "$repository/LICENSE" "$repository/README.md" "$workspace/"
cp -R "$repository/crates/bornera-core" "$workspace/crates/"
cp -R "$repository/crates/bornera" "$workspace/crates/"
cp -R "$repository/crates/bornera-rustls" "$workspace/crates/"
cp -R "$repository/crates/bornera-sim" "$workspace/crates/"

version=$(awk -F '"' '/^version = "/ { print $2; exit }' "$workspace/Cargo.toml")
if [ -z "$version" ]; then
    echo "package proof: workspace version is absent" >&2
    exit 1
fi

cd "$workspace"
cargo package --list -p bornera-core --locked --offline > "$scratch/bornera-core.list"
cargo package --list -p bornera --locked --offline > "$scratch/bornera.list"
cargo package --list -p bornera-rustls --locked --offline > "$scratch/bornera-rustls.list"

for required in Cargo.toml LICENSE README.md src/lib.rs; do
    grep -F -x "$required" "$scratch/bornera-core.list" > /dev/null
    grep -F -x "$required" "$scratch/bornera.list" > /dev/null
    grep -F -x "$required" "$scratch/bornera-rustls.list" > /dev/null
done

cargo package -p bornera-core --locked --offline
core_source="$workspace/target/package/bornera-core-$version"
# The source workspace uses its path edge directly; this patch becomes active
# only when Cargo validates the normalized registry-only dependency graph.
cargo package -p bornera --offline --no-verify --quiet \
    --config "patch.crates-io.bornera-core.path='$core_source'"
production_archive="$workspace/target/package/bornera-$version.crate"
tar -xzf "$production_archive" -C "$workspace/target/package"
production_source="$workspace/target/package/bornera-$version"
cargo package -p bornera-rustls --offline --no-verify --quiet \
    --config "patch.crates-io.bornera-core.path='$core_source'" \
    --config "patch.crates-io.bornera.path='$production_source'"

core_archive="$workspace/target/package/bornera-core-$version.crate"
rustls_archive="$workspace/target/package/bornera-rustls-$version.crate"
test -f "$core_archive"
test -f "$production_archive"
test -f "$rustls_archive"

smoke="$scratch/smoke"
mkdir -p "$smoke"
tar -xzf "$core_archive" -C "$smoke"
tar -xzf "$production_archive" -C "$smoke"
tar -xzf "$rustls_archive" -C "$smoke"

normalized="$smoke/bornera-$version/Cargo.toml"
dependency_block="$scratch/bornera-core.dependency"
sed -n '/^\[dependencies\.bornera-core\]$/,/^\[/p' "$normalized" > "$dependency_block"
grep -F -x '[dependencies.bornera-core]' "$dependency_block" > /dev/null
grep -F -x "version = \"=$version\"" "$dependency_block" > /dev/null
if grep -F 'path =' "$dependency_block" > /dev/null; then
    echo "package proof: normalized bornera dependency retained a local path" >&2
    exit 1
fi

rustls_normalized="$smoke/bornera-rustls-$version/Cargo.toml"
rustls_dependency_block="$scratch/bornera.dependency"
sed -n '/^\[dependencies\.bornera\]$/,/^\[/p' "$rustls_normalized" \
    > "$rustls_dependency_block"
grep -F -x '[dependencies.bornera]' "$rustls_dependency_block" > /dev/null
grep -F -x "version = \"=$version\"" "$rustls_dependency_block" > /dev/null
rustls_core_block="$scratch/bornera-rustls-core.dependency"
sed -n '/^\[dev-dependencies\.bornera-core\]$/,/^\[/p' "$rustls_normalized" \
    > "$rustls_core_block"
grep -F -x '[dev-dependencies.bornera-core]' "$rustls_core_block" > /dev/null
grep -F -x "version = \"=$version\"" "$rustls_core_block" > /dev/null
rustls_tls_block="$scratch/rustls.dependency"
sed -n '/^\[dependencies\.rustls\]$/,/^\[/p' "$rustls_normalized" \
    > "$rustls_tls_block"
grep -F -x '[dependencies.rustls]' "$rustls_tls_block" > /dev/null
grep -F -x 'version = "=0.23.43"' "$rustls_tls_block" > /dev/null
if grep -F 'path =' \
    "$rustls_dependency_block" "$rustls_core_block" "$rustls_tls_block" > /dev/null; then
    echo "package proof: normalized bornera-rustls package retained a local path" >&2
    exit 1
fi

{
    echo '[workspace]'
    echo 'resolver = "3"'
    echo 'members = ['
    echo "  \"bornera-core-$version\","
    echo "  \"bornera-$version\","
    echo "  \"bornera-rustls-$version\","
    echo ']'
    echo
    echo '[patch.crates-io]'
    echo "bornera-core = { path = \"bornera-core-$version\" }"
    echo "bornera = { path = \"bornera-$version\" }"
} > "$smoke/Cargo.toml"
cp "$workspace/Cargo.lock" "$smoke/Cargo.lock"
# Normalize only the removed unpublished workspace member from the checked
# root lock. Offline resolution preserves every compatible locked selection;
# the compilation that follows is fail-closed under that derived lock.
cargo metadata --manifest-path "$smoke/Cargo.toml" --offline --format-version 1 > /dev/null

cargo check --manifest-path "$smoke/Cargo.toml" --workspace --all-targets --locked --offline
echo "package proof: bornera-core $version -> bornera $version -> bornera-rustls $version"
