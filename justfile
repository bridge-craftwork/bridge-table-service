# justfile
#
# This file reflects the maintainer's specific workflow: Apple Silicon
# Mac builds via Colima/Rosetta, deployed to a private host alias
# `bridge-droplet`. If you're not the maintainer, the sections below are
# scoped so you can keep what's useful and ignore (or replace) the rest:
#
#   1. UNIVERSAL          — works on any platform with cargo + git.
#   2. APPLE SILICON MAC  — image build pipeline. Skip if you build via
#                           `docker build .` directly or via GitHub Actions.
#   3. MAINTAINER DROPLET — ssh-based deploy to a private host. Skip
#                           or replace if you deploy somewhere else.
#
# The Dockerfile and .github/workflows/ci.yml are deliberately platform-
# agnostic and don't depend on this justfile.

SERVICE := "bridge-table-service"
# ghcr.io paths must be lowercase. The GitHub user is Rick-Wilson but the
# image registry rejects mixed case ("repository name must be lowercase").
IMAGE   := "ghcr.io/rick-wilson/" + SERVICE

default:
    @just --list

# =============================================================================
# 1. UNIVERSAL
# =============================================================================

# Run locally without docker (fastest iteration loop). Goes through
# dev-build.sh so the gitignored local-checkout patches in
# .cargo/config.toml take effect without dirtying the committed Cargo.lock
# (see CLAUDE.md).
dev:
    ./dev-build.sh run

# Run cargo tests against local sibling checkouts.
test:
    ./dev-build.sh test

# Format and lint (CI parity: patches disabled, committed lock's git pins).
check:
    cargo fmt --check
    ./dev-build.sh --ci clippy -- -D warnings

# Tag and push a release. CI builds and pushes the versioned image.
release VERSION:
    git tag {{VERSION}}
    git push origin {{VERSION}}
    @echo "GitHub Actions will build {{VERSION}}. Once CI is green:"
    @echo "  just deploy-version {{VERSION}}"

# =============================================================================
# 2. APPLE SILICON MAC + COLIMA
#
# These recipes assume Colima with --vz-rosetta is set up locally for
# amd64 cross-builds. On other platforms, build via `docker build .`
# directly (native architecture), or push to GitHub and let CI build.
# =============================================================================

# Ensure colima is running (no-op if already up).
_colima-up:
    @colima status >/dev/null 2>&1 || (echo "Starting colima..." && colima start)

# Build the droplet-bound linux/amd64 image.
#
# This is the *only* image-producing recipe. It always targets amd64
# because the droplet is x86_64; there's no native-arch build because
# Rust testing happens in cargo (`just dev`, `cargo test`) without
# needing a container, and any droplet-targeted smoke test needs amd64
# anyway.
#
# Forced through the `colima` builder (host daemon, uses Apple Rosetta
# on Apple Silicon) instead of buildx's default `docker-container`
# driver. The default driver carries its own QEMU emulator inside the
# buildkit container, which segfaults running rustc x86_64. The colima
# builder uses the colima VM directly, where Rosetta-via-VZ emulates
# amd64 at near-native speed and rustc compiles cleanly.
#
# Prereq: `colima start --vz-rosetta` (one-time; persists across reboots).
build: _colima-up
    docker buildx --builder colima build --platform linux/amd64 -t {{IMAGE}}:dev --load .

# Push the dev image to ghcr.io.
push: build
    docker push {{IMAGE}}:dev

# =============================================================================
# 3. MAINTAINER DROPLET
#
# These recipes ssh to `bridge-droplet`, a local SSH alias the
# maintainer defines in their personal ~/.ssh/config — it doesn't
# resolve for anyone else and contains no IP or hostname information
# in this repo. Forks should either (a) define their own SSH alias
# with the same name pointing at their own host, or (b) replace these
# recipes with whatever their deploy mechanism is.
# =============================================================================

DROPLET := "bridge-droplet"

# Build, push, and trigger a droplet pull+restart.
deploy: push
    ssh {{DROPLET}} '/opt/bridge-craftwork/scripts/deploy.sh {{SERVICE}}'

# Promote a tagged version on the droplet.
deploy-version VERSION:
    ssh {{DROPLET}} 'sed -i "s/^{{SERVICE}}_TAG=.*/{{SERVICE}}_TAG={{VERSION}}/" /opt/bridge-craftwork/.env && \
        /opt/bridge-craftwork/scripts/deploy.sh {{SERVICE}}'

# Tail logs from the droplet.
logs:
    ssh {{DROPLET}} 'cd /opt/bridge-craftwork && docker compose logs -f --tail 100 {{SERVICE}}'

# Shell into the running container.
shell:
    ssh -t {{DROPLET}} 'cd /opt/bridge-craftwork && docker compose exec {{SERVICE}} /bin/sh'
