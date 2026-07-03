# bridge-table-service

Realtime multiplayer bridge table manager: server-authoritative bidding and play over WebSockets, with bot seats (BBA bidding, BEN cardplay).

This service follows the [bridge-craftwork service contract](https://github.com/bridge-craftwork/bridge-craftwork-platform/blob/main/docs/bridge-craftwork-plan.md): JSON logs, `/healthz`, `/metrics`, gated `/dashboard`, env-var config.

## What it does

Hosts live bridge tables for [Bridge Classroom](https://bridge-classroom.com): teacher-managed table sets (Shark-Bridge-style class sessions) and ad-hoc social tables. The server is the referee — auction legality, follow-suit enforcement, trick resolution, and per-viewer hand redaction all happen here; browsers are thin mirrors driven by WebSocket events.

- **Event-sourced tables**: each table's state is a fold over its action log; undo (unlimited, Shark-style) is truncate-and-refold.
- **Join tickets**: clients authenticate the WebSocket with a short-lived HMAC ticket minted by the bridge-classroom API (`TICKET_SECRET` shared between the two services); this service verifies offline.
- **Sister crates**: bridge primitives come from [`bridge-types`](https://github.com/bridge-craftwork/bridge-types), PBN parsing from [`bridge-encodings`](https://github.com/bridge-craftwork/bridge-encodings), and rule-based cardplay from [`bridge-rulebot`](https://github.com/bridge-craftwork/bridge-rulebot) — only auction/play *legality* logic lives here (see `src/engine/`).

## Local development

Rust testing happens directly in cargo on the host — no container needed:

```sh
cp .env.example .env             # then edit
just dev                          # runs the service on localhost:8004
just test                         # cargo test against local sibling checkouts
```

The internal bridge crates (`bridge-types`, `bridge-encodings`, `bridge-rulebot`) are git dependencies pinned by `Cargo.lock`. For local development against sister-directory checkouts (`../bridge-types` etc.), gitignored `[patch]` overrides live in `.cargo/config.toml` and take effect through `./dev-build.sh` (which `just dev` / `just test` use) — see "Local development against sibling crates" below.

> **About the `justfile`:** it's organized into three audience-scoped sections. *Universal* recipes (`dev`, `test`, `check`, `release`) work on any platform. *Apple Silicon Mac* recipes (`build`, `push`) drive a Colima/Rosetta cross-build pipeline for the maintainer's amd64 droplet. *Maintainer droplet* recipes (`deploy`, `logs`, `shell`, …) ssh to a local SSH alias (`bridge-droplet`) defined in the maintainer's personal `~/.ssh/config`. If you're forking this service, you can ignore sections 2 and 3 — `docker build .` works directly, [`.github/workflows/ci.yml`](.github/workflows/ci.yml) is platform-agnostic and produces images on GitHub's amd64 runners.

Endpoints once running:

- `http://localhost:8004/healthz`
- `http://localhost:8004/metrics`
- `http://localhost:8004/dashboard?key=<DASHBOARD_SECRET>`
- `ws://localhost:8004/ws` — game traffic (first message must be `{"t":"hello","ticket":"…"}`)

## Deploy

The Docker pipeline exists to build droplet-bound images; there is no native-arch build. Every `just build` produces a linux/amd64 image suitable for the droplet.

```sh
just deploy           # build amd64, push :dev to ghcr.io, restart on droplet
just logs             # tail droplet logs
```

For tagged releases:

```sh
just release v0.1.0
# wait for CI (.github/workflows/ci.yml) to build and push
just deploy-version v0.1.0
```

### Apple Silicon prereq

`just build` cross-compiles to linux/amd64. On Apple Silicon Macs, this needs Rosetta-via-VZ enabled in colima:

```sh
colima stop && colima start --vz-rosetta   # one-time; persists across reboots
```

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `PORT` | `8004` | HTTP listen port |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT` | `json` | `json` for prod, `pretty` for local dev |
| `DATABASE_PATH` | `./data/bridge-table-service.db` | SQLite file path (observability events) |
| `DASHBOARD_SECRET` | *required* | Gate for `/dashboard?key=…` |
| `TICKET_SECRET` | *required* | HMAC key for join tickets, shared with the bridge-classroom API |
| `BEN_URL` | `https://ben.bridge-craftwork.com` | BEN cardplay engine. **On the droplet set `BEN_URL=http://ben:8085`** in the compose block — BEN runs on the same `bridge-net` docker network, so calls should stay internal instead of hairpinning through the public edge. |
| `BBA_URL` | `https://bba.harmonicsystems.com` | BBA bidding engine (Windows-VM hosted; public URL is the only route) |
| `BOT_TIMEOUT_MS` | `20000` | Per-call budget for BEN cardplay requests — opening leads and early plays can legitimately take >8s even on a warm BEN |

Bot seats use BBA for bidding (with a per-room predicted-auction prefix cache; divergence or undo re-requests with `auctionPrefix`) and BEN for cardplay (encodings mirror the frontend's `benClient.js`). Every suggestion is validated through the legality engine; on timeout (`BOT_TIMEOUT_MS` for BEN, 8s for BBA), error, or an illegal suggestion the seat falls back to Pass / a [`bridge-rulebot`](https://github.com/bridge-craftwork/bridge-rulebot) card (deterministic rule-based play — it also covers BEN's cold-start window), so a slow bot never freezes a table. BEN is pre-warmed at startup to absorb its ~20s model cold start. For testing, a client can send `"bot":"random"` (Pass bidding + instant deterministic RandomLegal, no BBA/BEN) or `"bot":"rules"` (real BBA auctions + instant bridge-rulebot cardplay, no BEN) in its `hello` frame to switch the whole room to that backend; the `welcome` frame reports the active mode as `"bot_mode":"real"|"random"|"rules"`.

[`bridge-rulebot`](https://github.com/bridge-craftwork/bridge-rulebot) (sibling crate) is the cardplay fallback and the `"bot":"rules"` backend: a deterministic rule-based player (opening leads, second/third-hand play, defensive signals) whose every decision carries a reason code — see that repo's `docs/requirements.md`. Each decision is logged as a `rulebot_decision` event with its rule slug and duration.

## Local development against sibling crates

This service depends on `bridge-types`, `bridge-encodings`, and `bridge-rulebot` as git dependencies pinned by the committed `Cargo.lock`; Docker and CI always build those pinned, pushed revisions (no sibling checkouts or build-contexts). For local iteration against sister-directory checkouts, gitignored `[patch]` overrides in `.cargo/config.toml` redirect them to `../<sibling>` — but only through `./dev-build.sh` (see CLAUDE.md): bare cargo either ignores the patches or silently rewrites `Cargo.lock` with local paths that must never be committed. To pick up local sibling changes in a container image, push the sibling first and run `cargo update -p <crate>` via `./dev-build.sh --ci` to re-pin.

When you add a new internal crate: add the git dependency in `Cargo.toml` and a matching `[patch]` entry in `.cargo/config.toml`. Nothing in the `Dockerfile`, `justfile`, or CI needs to change.

Use [`record_event()`](src/observability/events.rs) for any domain-significant event — it writes to the SQLite `events` table *and* emits a JSON log line, so it shows up in both the dashboard and ops tooling.
