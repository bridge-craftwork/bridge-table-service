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
# Config discovery: cargo merges .cargo/config.toml from every *ancestor* of
# the invocation directory, so the overrides that apply here are not
# necessarily next to this script. In a git worktree under
# .claude/worktrees/<name>/ there is no local .cargo/ at all, yet the main
# checkout's config three levels up still patches the build. Looking only
# beside the script made this script fall through to bare cargo in exactly
# that case — the one place bare cargo silently corrupts Cargo.lock, and with
# --ci not even reaching the guard that would have said so. So we walk up the
# way cargo does and manage whichever config we find. Lockfiles stay
# per-worktree (Cargo.lock is), while the config, and therefore the --ci
# move-aside, may be shared: don't run two --ci builds against the same
# config concurrently.
#
# Usage:
#   ./dev-build.sh                  # cargo build, against local checkouts
#   ./dev-build.sh test             # cargo test, against local checkouts
#   ./dev-build.sh build --release  # any cargo subcommand + args
#   ./dev-build.sh --ci test        # CI-parity: patches disabled, committed
#                                   # lock's git pins, lock rewrite guarded

set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR"

# Both spellings cargo accepts, newest first.
CONFIG_NAMES=(config.toml config)

# Nearest ancestor .cargo/ config that actually carries [patch.] overrides.
find_patch_config() {
    local dir=$SCRIPT_DIR name
    while :; do
        for name in "${CONFIG_NAMES[@]}"; do
            if [[ -f $dir/.cargo/$name ]] && grep -q '^\[patch\.' "$dir/.cargo/$name"; then
                printf '%s\n' "$dir/.cargo/$name"
                return 0
            fi
        done
        if [[ $dir == / ]]; then
            return 1
        fi
        dir=$(dirname "$dir")
    done
}

# Nearest ancestor marker left by an in-flight (or crashed) --ci run.
find_disabled_config() {
    local dir=$SCRIPT_DIR name
    while :; do
        for name in "${CONFIG_NAMES[@]}"; do
            if [[ -f $dir/.cargo/$name.ci-off ]]; then
                printf '%s\n' "$dir/.cargo/$name.ci-off"
                return 0
            fi
        done
        if [[ $dir == / ]]; then
            return 1
        fi
        dir=$(dirname "$dir")
    done
}

ci_mode=""
if [[ ${1:-} == --ci ]]; then
    ci_mode=1
    shift
fi
[[ $# -eq 0 ]] && set -- build

CONFIG=$(find_patch_config) || CONFIG=""

# No local patch overrides anywhere above us: behave exactly like cargo.
if [[ -z $CONFIG ]]; then
    stray=$(find_disabled_config) || stray=""
    if [[ -n $stray ]]; then
        echo "dev-build: ERROR: $stray exists." >&2
        echo "dev-build: another --ci run has the patch overrides moved aside, or one" >&2
        echo "dev-build: crashed before restoring them. Wait for it, or rename that file" >&2
        echo "dev-build: back to ${stray%.ci-off} if nothing else is running." >&2
        exit 1
    fi
    exec cargo "$@"
fi

CONFIG_DIR=$(dirname "$CONFIG")
CONFIG_OFF="$CONFIG.ci-off"

# We have to be able to move the config aside (--ci) and read the patch list
# (dev). Falling back to bare cargo here is not safe: cargo would still apply
# these overrides and rewrite Cargo.lock with local-path entries.
if [[ ! -w $CONFIG || ! -w $CONFIG_DIR ]]; then
    echo "dev-build: ERROR: $CONFIG carries [patch] overrides but is not writable" >&2
    echo "dev-build: (neither is $CONFIG_DIR), so this script cannot disable or" >&2
    echo "dev-build: inspect them. Refusing to run bare cargo, which would apply the" >&2
    echo "dev-build: patches and rewrite Cargo.lock with local-path entries." >&2
    exit 1
fi

if [[ $CONFIG_DIR != "$SCRIPT_DIR/.cargo" ]]; then
    echo "dev-build: patch overrides from $CONFIG" >&2
fi

# Lockfiles are per-manifest, so they always live beside *this* checkout even
# when the config is shared with the main worktree.
LOCAL_CARGO_DIR=$SCRIPT_DIR/.cargo
DEV_LOCK=$LOCAL_CARGO_DIR/dev.lock
CI_LOCK_STASH=$LOCAL_CARGO_DIR/ci.lock.swap

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

mkdir -p "$LOCAL_CARGO_DIR"

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
