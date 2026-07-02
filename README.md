# bridge-table-service

Realtime multiplayer bridge table manager: server-authoritative bidding and play over WebSockets, with bot seats (BBA bidding, BEN cardplay).

This service follows the [bridge-craftwork service contract](https://github.com/bridge-craftwork/bridge-craftwork-platform/blob/main/docs/bridge-craftwork-plan.md): JSON logs, `/healthz`, `/metrics`, gated `/dashboard`, env-var config.

## What it does

Hosts live bridge tables for [Bridge Classroom](https://bridge-classroom.com): teacher-managed table sets (Shark-Bridge-style class sessions) and ad-hoc social tables. The server is the referee — auction legality, follow-suit enforcement, trick resolution, and per-viewer hand redaction all happen here; browsers are thin mirrors driven by WebSocket events.

- **Event-sourced tables**: each table's state is a fold over its action log; undo (unlimited, Shark-style) is truncate-and-refold.
- **Join tickets**: clients authenticate the WebSocket with a short-lived HMAC ticket minted by the bridge-classroom API (`TICKET_SECRET` shared between the two services); this service verifies offline.
- **Sister crates**: bridge primitives come from [`bridge-types`](https://github.com/bridge-craftwork/bridge-types) and PBN parsing from [`bridge-encodings`](https://github.com/bridge-craftwork/bridge-encodings) — only auction/play *legality* logic lives here (see `src/engine/`).

## Local development

Rust testing happens directly in cargo on the host — no container needed:

```sh
cp .env.example .env             # then edit
just dev                          # cargo run, listens on localhost:8004
cargo test
```

Sibling crates are expected as sister directories (`../bridge-types`, `../bridge-encodings`) via the `[patch]` entries in `Cargo.toml` — see "Sibling crate path-deps" below.

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

Bot seats use BBA for bidding (with a per-room predicted-auction prefix cache; divergence or undo re-requests with `auctionPrefix`) and BEN for cardplay (encodings mirror the frontend's `benClient.js`). Every suggestion is validated through the legality engine; on timeout (`BOT_TIMEOUT_MS` for BEN, 8s for BBA), error, or an illegal suggestion the seat falls back to Pass / a random legal card, so a slow bot never freezes a table. BEN is pre-warmed at startup to absorb its ~20s model cold start. For testing, a client can send `"bot":"random"` in its `hello` frame to switch the whole room to instant RandomLegal bots (Pass for bidding, deterministic legal card for play — no BBA/BEN calls); the `welcome` frame reports the active mode as `"bot_mode":"real"|"random"`.

## Sibling crate path-deps

This service depends on `bridge-types` and `bridge-encodings` as sibling repos via the buildx multi-context pattern. The container layout mirrors the developer-Mac layout — siblings live as sister directories of the service — so a single `[patch]` path works in both native cargo and inside Docker.

The wiring lives in four places (already done for both siblings): `Cargo.toml` (`[patch]` entries), `justfile` (`SIBLING_CONTEXTS`), `Dockerfile` (`COPY --from=` lines), and `.github/workflows/ci.yml` (checkout steps in both jobs + `build-contexts:`).

Use [`record_event()`](src/observability/events.rs) for any domain-significant event — it writes to the SQLite `events` table *and* emits a JSON log line, so it shows up in both the dashboard and ops tooling.
