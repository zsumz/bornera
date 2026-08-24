#!/bin/sh
set -eu
export LANG=C
export LC_ALL=C

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
cp -R "$repository/crates/bornera-sim" "$workspace/crates/"

version=$(awk -F '"' '/^version = "/ { print $2; exit }' "$workspace/Cargo.toml")
if [ -z "$version" ]; then
    echo "package proof: workspace version is absent" >&2
    exit 1
fi

cd "$workspace"
cargo package --list -p bornera-core --locked --offline > "$scratch/bornera-core.list"
cargo package --list -p bornera --locked --offline > "$scratch/bornera.list"

for required in Cargo.toml LICENSE README.md src/lib.rs; do
    grep -F -x "$required" "$scratch/bornera-core.list" > /dev/null
    grep -F -x "$required" "$scratch/bornera.list" > /dev/null
done

cargo package -p bornera-core --locked --offline
core_source="$workspace/target/package/bornera-core-$version"
# The source workspace uses its path edge directly; this patch becomes active
# only when Cargo validates the normalized registry-only dependency graph.
cargo package -p bornera --offline --no-verify --quiet \
    --config "patch.crates-io.bornera-core.path='$core_source'"

core_archive="$workspace/target/package/bornera-core-$version.crate"
production_archive="$workspace/target/package/bornera-$version.crate"
test -f "$core_archive"
test -f "$production_archive"

smoke="$scratch/smoke"
mkdir -p "$smoke"
tar -xzf "$core_archive" -C "$smoke"
tar -xzf "$production_archive" -C "$smoke"

normalized="$smoke/bornera-$version/Cargo.toml"
dependency_block="$scratch/bornera-core.dependency"
sed -n '/^\[dependencies\.bornera-core\]$/,/^\[/p' "$normalized" > "$dependency_block"
grep -F -x '[dependencies.bornera-core]' "$dependency_block" > /dev/null
grep -F -x "version = \"=$version\"" "$dependency_block" > /dev/null
if grep -F 'path =' "$dependency_block" > /dev/null; then
    echo "package proof: normalized bornera dependency retained a local path" >&2
    exit 1
fi

{
    echo '[workspace]'
    echo 'resolver = "3"'
    echo 'members = ['
    echo "  \"bornera-core-$version\","
    echo "  \"bornera-$version\","
    echo ']'
    echo
    echo '[patch.crates-io]'
    echo "bornera-core = { path = \"bornera-core-$version\" }"
} > "$smoke/Cargo.toml"
cp "$workspace/Cargo.lock" "$smoke/Cargo.lock"
# Normalize only the removed unpublished workspace member from the checked
# root lock. Offline resolution preserves every compatible locked selection;
# the compilation that follows is fail-closed under that derived lock.
cargo metadata --manifest-path "$smoke/Cargo.toml" --offline --format-version 1 > /dev/null

cargo check --manifest-path "$smoke/Cargo.toml" --workspace --all-targets --locked --offline
echo "package proof: bornera-core $version -> bornera $version"
