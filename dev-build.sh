#!/bin/bash
#
# dev-build.sh - run cargo against local sibling checkouts, reliably.
#
# Why this exists: this repo depends on sibling bridge crates as *git*
# dependencies, with gitignored [patch] overrides in .cargo/config.toml
# redirecting them to local checkouts during development. That combination
# is a trap for bare cargo:
#
#   * When the local crate's version EQUALS the locked one, any resolving
#     cargo command (build/test/check/run) applies the patch immediately and
#     silently REWRITES Cargo.lock with local-path entries that must never
#     be committed (CI has no sibling checkouts).
#   * When the versions differ, the patch is silently IGNORED and you build
#     the GitHub revisions instead of your local edits.
#
# Either way bare cargo does the wrong thing, so always go through this
# script. It keeps two lockfiles and swaps them around the cargo call:
#
#   Cargo.lock       committed lock, pinned to git sources (CI truth)
#   .cargo/dev.lock  local-only lock, resolved with the patches applied
#
# and verifies every patched crate in the dependency graph actually resolved
# to a local path, failing loudly if not. The committed Cargo.lock is never
# touched.
#
# Usage:
#   ./dev-build.sh                  # cargo build, against local checkouts
#   ./dev-build.sh test             # cargo test, against local checkouts
#   ./dev-build.sh build --release  # any cargo subcommand + args
#   ./dev-build.sh --ci test        # CI-parity: patches disabled, committed
#                                   # lock's git pins, lock rewrite guarded
#
set -euo pipefail
cd "$(dirname "$0")"

CONFIG=.cargo/config.toml
CONFIG_OFF=.cargo/config.toml.ci-off
DEV_LOCK=.cargo/dev.lock
CI_LOCK_STASH=.cargo/ci.lock.swap

ci_mode=""
if [[ ${1:-} == --ci ]]; then
    ci_mode=1
    shift
fi
[[ $# -eq 0 ]] && set -- build

# No local patch overrides: behave exactly like cargo.
if [[ ! -f $CONFIG ]] || ! grep -q '^\[patch\.' "$CONFIG"; then
    exec cargo "$@"
fi

# --- CI-parity mode: disable the patches, build with the committed lock ---
if [[ -n $ci_mode ]]; then
    lock_before=""
    [[ -f Cargo.lock ]] && lock_before=$(cksum < Cargo.lock)
    mv "$CONFIG" "$CONFIG_OFF"
    restore_ci() { [[ -f $CONFIG_OFF ]] && mv "$CONFIG_OFF" "$CONFIG"; }
    trap restore_ci EXIT
    cargo "$@"
    if [[ -n $lock_before && $(cksum < Cargo.lock) != "$lock_before" ]]; then
        echo "dev-build: NOTE: Cargo.lock was re-resolved during this CI-parity run." >&2
        echo "dev-build: review 'git diff Cargo.lock' — internal crates must keep their" >&2
        echo "dev-build: source = \"git+https://...\" lines before committing." >&2
    fi
    exit 0
fi

# --- dev mode: swap in the dev lock, build against local checkouts ---

# Crate names the config patches to local paths.
patched=$(sed -n 's/^\([A-Za-z0-9_-]*\) *= *{ *path *=.*/\1/p' "$CONFIG")

swapped=""
restore() {
    if [[ -n $swapped ]]; then
        [[ -f Cargo.lock ]] && mv Cargo.lock "$DEV_LOCK"
        [[ -f $CI_LOCK_STASH ]] && mv "$CI_LOCK_STASH" Cargo.lock
    fi
}
trap restore EXIT

# If the committed (CI) lock is tracked, set it aside and use the dev lock;
# cargo re-creates the dev lock from scratch if it doesn't exist yet, and a
# fresh resolve does honor the config patches.
if git ls-files --error-unmatch Cargo.lock >/dev/null 2>&1; then
    swapped=1
    mv Cargo.lock "$CI_LOCK_STASH"
    [[ -f $DEV_LOCK ]] && mv "$DEV_LOCK" Cargo.lock
fi

# True when every patched crate that appears in the lock is path-resolved
# (path-resolved entries are the only ones without a `source =` line).
verify() {
    local ok=0 crate
    for crate in $patched; do
        grep -q "^name = \"$crate\"\$" Cargo.lock 2>/dev/null || continue
        if grep -A2 "^name = \"$crate\"\$" Cargo.lock | grep -q '^source ='; then
            echo "dev-build: $crate still resolves to a remote source" >&2
            ok=1
        fi
    done
    return $ok
}

cargo "$@"

if [[ -f Cargo.lock ]] && ! verify; then
    # Stale dev lock from before the patches existed; it is disposable —
    # discard it and re-resolve fresh, which applies the patches.
    echo "dev-build: discarding stale dev lock and re-resolving..." >&2
    rm Cargo.lock
    cargo "$@"
    verify || {
        echo "dev-build: ERROR: patched crates still resolve to remote sources." >&2
        echo "dev-build: check that the sibling checkouts in $CONFIG exist." >&2
        exit 1
    }
fi

for crate in $patched; do
    if grep -q "^name = \"$crate\"\$" Cargo.lock 2>/dev/null; then
        echo "dev-build: ✓ $crate → local checkout"
    fi
done
