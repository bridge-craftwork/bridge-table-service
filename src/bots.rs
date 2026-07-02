//! Bot seats.
//!
//! Any unoccupied seat is a bot (with one exception: a human declarer plays
//! the dummy's cards, so the dummy seat is driven by the declarer, not a
//! bot). This mirrors the frontend's pluggable-bot design (`cardplayBots.js`):
//! bidding is BBA (prefix-cached, see `bba`), cardplay is BEN (see `ben`),
//! and every suggestion is validated through the engine with `RandomLegal`
//! (Pass, for bidding) as the always-available fallback — a slow or wrong
//! bot can never freeze or corrupt a table.
//!
//! The driver is undo-safe by construction: every iteration captures the
//! folded state under the lock, releases it for the (possibly slow) HTTP
//! call, then re-locks and applies only if the table's seq is unchanged —
//! a human action or undo that lands mid-think just discards the stale
//! suggestion and the next iteration re-decides. Exactly one driver task
//! runs per room (`bot_running` flag); every accepted action re-kicks it.

pub mod bba;
pub mod ben;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bridge_types::{Call, Card, Direction, Vulnerability};
use once_cell::sync::OnceCell;
use serde_json::json;

use crate::engine::auction;
use crate::rooms::Room;
use crate::table::{Action, BoardSetup, Phase};

/// Pacing, matching the solo client's feel (useCardPlay.js): a beat between
/// plays, a longer beat after a completed trick.
const BETWEEN_PLAYS: Duration = Duration::from_millis(300);
const AFTER_TRICK: Duration = Duration::from_millis(1000);

/// Process-wide bot backends, set once at startup. Absent (e.g. in unit
/// tests) every choice falls back to RandomLegal / Pass.
static CLIENTS: OnceCell<Clients> = OnceCell::new();

struct Clients {
    ben: ben::BenClient,
    bba: bba::BbaClient,
}

/// Initialize the HTTP bot backends and fire the BEN pre-warm (the first
/// BEN call after idle costs ~10-20s; warming at startup absorbs it).
pub fn init(ben_url: &str, bba_url: &str) {
    let clients = Clients {
        ben: ben::BenClient::new(ben_url),
        bba: bba::BbaClient::new(bba_url),
    };
    clients.ben.warm();
    if CLIENTS.set(clients).is_err() {
        tracing::warn!(event = "bots_already_initialized", "");
    }
    tracing::info!(event = "bots_initialized", ben_url, bba_url, "");
}

fn clients() -> Option<&'static Clients> {
    CLIENTS.get()
}

/// Deterministic "random" pick: multiplicative hash of the action seq.
/// Deterministic fallbacks make failures replayable.
fn pick<T: Copy>(options: &[T], seq: usize) -> T {
    options[(seq.wrapping_mul(2654435761)) % options.len()]
}

/// Kick the bot driver for a room. Cheap to call after every state change;
/// only one task runs at a time.
pub fn kick(room: Arc<Room>) {
    if room.bot_running.swap(true, Ordering::SeqCst) {
        return; // already running
    }
    tokio::spawn(async move {
        loop {
            let acted = act_once(&room).await;
            if !acted {
                room.bot_running.store(false, Ordering::SeqCst);
                // Re-check: a human action may have made it a bot's turn
                // again between our last look and clearing the flag.
                if bot_turn_pending(&room).await && !room.bot_running.swap(true, Ordering::SeqCst) {
                    continue;
                }
                return;
            }
        }
    });
}

async fn bot_turn_pending(room: &Room) -> bool {
    let inner = room.state.lock().await;
    let f = inner.table.fold();
    match f.next_to_act {
        Some(seat) => is_bot_seat(&inner, &f, seat),
        None => false,
    }
}

/// Whether the bot driver should act for `seat`. Control of the dummy
/// belongs to whoever controls the declarer seat (bridge: declarer plays
/// dummy's cards) — so the dummy is bot-driven iff the declarer seat is
/// unoccupied, regardless of who sits at dummy. Every other seat is
/// bot-driven iff unoccupied.
fn is_bot_seat(inner: &crate::rooms::RoomInner, f: &crate::table::Folded, seat: Direction) -> bool {
    if let Some(c) = &f.contract {
        if seat == c.dummy() {
            return !inner.seats.contains_key(&c.declarer);
        }
    }
    !inner.seats.contains_key(&seat)
}

/// Everything a bot needs to decide, captured under the state lock so the
/// HTTP call can run without holding it.
enum Pending {
    Bid {
        seat: Direction,
        seq: usize,
        board: BoardSetup,
        calls: Vec<Call>,
    },
    Play(Box<PlayPending>),
}

struct PlayPending {
    seat: Direction,
    seq: usize,
    /// Engine-legal cards for `seat` — the validation set and the fallback pool.
    legal: Vec<Card>,
    opening_lead: bool,
    /// BEN's `seat`/`hand` are the decision-maker's: the declarer for
    /// declarer-side plays (incl. dummy's cards), the defender itself
    /// otherwise (see bots/ben.rs).
    decision_maker: Direction,
    declarer: Direction,
    hand_pbn: String,
    dummy_pbn: String,
    dealer: Direction,
    vul: Vulnerability,
    ctx: String,
    played: String,
}

/// Make one bot action if it's a bot's turn. Returns false when it isn't
/// (human's turn, board complete, or empty room with nothing to do).
async fn act_once(room: &Room) -> bool {
    // Pace before acting so bots don't feel instant. The lock is NOT held
    // during the sleep — humans can act (or undo) meanwhile; we re-fold after.
    tokio::time::sleep(BETWEEN_PLAYS).await;

    // Phase A: under the lock, decide whether a bot must act and capture
    // what it needs. The lock is dropped before any HTTP.
    let pending = {
        let inner = room.state.lock().await;
        let f = inner.table.fold();
        let Some(seat) = f.next_to_act else {
            return false;
        };
        if !is_bot_seat(&inner, &f, seat) {
            return false;
        }
        let seq = inner.table.seq();
        match f.phase {
            Phase::Bidding => Pending::Bid {
                seat,
                seq,
                board: inner.table.board.clone(),
                calls: f.calls.clone(),
            },
            Phase::Play => {
                let remaining = inner.table.remaining(seat, &f);
                let lead = f
                    .current_trick
                    .as_ref()
                    .and_then(|(_, plays)| plays.first().map(|(_, c)| c.suit));
                let legal = crate::engine::play::legal_cards(&remaining, lead);
                if legal.is_empty() {
                    return false;
                }
                let c = f.contract.as_ref().expect("play phase has a contract");
                let decision_maker = if seat == c.declarer || seat == c.dummy() {
                    c.declarer
                } else {
                    seat
                };
                let board = &inner.table.board;
                Pending::Play(Box::new(PlayPending {
                    seat,
                    seq,
                    legal,
                    opening_lead: f.played.is_empty(),
                    decision_maker,
                    declarer: c.declarer,
                    hand_pbn: board.deal.hand(decision_maker).to_pbn(),
                    dummy_pbn: board.deal.hand(c.dummy()).to_pbn(),
                    dealer: board.dealer,
                    vul: board.vulnerable,
                    ctx: ben::calls_to_ctx(&f.calls),
                    played: ben::played_to_string(&f.played),
                }))
            }
            Phase::Complete => return false,
        }
    };

    // Phase B: choose. May take seconds (BBA/BEN over HTTP); the state
    // lock is not held, so humans keep acting and undo keeps working.
    let (seq, action, via, opening_lead) = match pending {
        Pending::Bid {
            seat,
            seq,
            board,
            calls,
        } => {
            let (call, via) = choose_call(room, &board, &calls).await;
            (seq, Action::Call { seat, call }, via, false)
        }
        Pending::Play(p) => {
            let (card, via) = choose_card(&p).await;
            (
                p.seq,
                Action::Play { seat: p.seat, card },
                via,
                p.opening_lead,
            )
        }
    };

    // Phase C: re-lock and apply — unless the world moved while we were
    // thinking (human action or undo). Then the suggestion is stale:
    // discard it and let the next iteration re-decide from a fresh fold.
    let mut inner = room.state.lock().await;
    if inner.table.seq() != seq {
        tracing::debug!(event = "bot_suggestion_stale", "state changed mid-think");
        return true;
    }
    let new_seq = match inner.table.apply(action.clone()) {
        Ok(s) => s,
        Err(e) => {
            // Unreachable in principle: suggestions are validated against
            // the same fold the seq guard just pinned.
            tracing::error!(event = "bot_action_rejected", error = %e, "");
            return false;
        }
    };
    let f2 = inner.table.fold();
    let event = match &action {
        Action::Call { seat, call } => json!({
            "t": "event",
            "table_id": room.id,
            "seq": new_seq,
            "kind": "bid_made",
            "seat": seat.to_char().to_string(),
            "call": call.to_pbn(),
            "by_bot": true,
            "via": via,
            "next_to_act": f2.next_to_act.map(|d| d.to_char().to_string()),
        }),
        Action::Play { seat, card } => {
            let trick_done = match &f2.current_trick {
                None => true,
                Some((_, plays)) => plays.is_empty(),
            };
            let winner = f2
                .completed_tricks
                .last()
                .filter(|_| trick_done)
                .map(|(_, _, w)| w.to_char().to_string());
            json!({
                "t": "event",
                "table_id": room.id,
                "seq": new_seq,
                "kind": "card_played",
                "seat": seat.to_char().to_string(),
                "card": format!("{}{}", card.suit.to_char(), card.rank.to_char()),
                "by_bot": true,
                "via": via,
                "next_to_act": f2.next_to_act.map(|d| d.to_char().to_string()),
                "trick_winner": winner,
                "tricks": { "ns": f2.tricks_ns, "ew": f2.tricks_ew },
            })
        }
    };

    let trick_pause = event["trick_winner"].is_string();
    drop(inner);
    room.broadcast(event.to_string());
    // A bot's opening lead reveals dummy to everyone — trigger the same
    // per-viewer resync a human opening lead (or an undo) does.
    if opening_lead {
        room.broadcast_resync();
    }
    if trick_pause {
        tokio::time::sleep(AFTER_TRICK - BETWEEN_PLAYS).await;
    }
    true
}

/// Pick a bot call: BBA via the room's prefix cache, falling back to Pass
/// (always legal mid-auction) on any failure. Holding the cache lock across
/// the HTTP call serializes BBA requests per room — deliberate: concurrent
/// bot turns must not double-request. The state mutex is NOT held here.
async fn choose_call(room: &Room, board: &BoardSetup, calls: &[Call]) -> (Call, &'static str) {
    let mut cache = room.bba_cache.lock().await;
    if let Some(call) = bba::continuation(&cache, calls) {
        if auction::is_legal(board.dealer, calls, &call) {
            return (call, "bba");
        }
        // A cached prediction the engine rejects can only recur — drop it.
        tracing::warn!(event = "bba_cached_call_illegal", call = %call.to_pbn(), "");
        *cache = None;
    }
    if let Some(clients) = clients() {
        match clients.bba.generate(board, calls).await {
            Ok(predicted) => {
                *cache = Some(predicted);
                match bba::continuation(&cache, calls) {
                    Some(call) if auction::is_legal(board.dealer, calls, &call) => {
                        return (call, "bba");
                    }
                    Some(call) => {
                        tracing::warn!(event = "bba_call_illegal", call = %call.to_pbn(), "")
                    }
                    None => tracing::warn!(event = "bba_no_continuation", ""),
                }
            }
            Err(e) => {
                tracing::warn!(event = "bba_failed", error = %format!("{e:#}"), "falling back to Pass")
            }
        }
    }
    (Call::Pass, "fallback")
}

/// Pick a bot card: BEN (/lead for the opening lead, /play otherwise),
/// falling back to a deterministic legal pick on timeout, error, an illegal
/// suggestion, or a `player` that maps to the wrong seat.
async fn choose_card(p: &PlayPending) -> (Card, &'static str) {
    if let Some(clients) = clients() {
        let vul = ben::vul_to_ben(p.vul, p.decision_maker);
        let result = if p.opening_lead {
            clients
                .ben
                .lead(&p.hand_pbn, p.seat, p.dealer, vul, &p.ctx)
                .await
                .map(|c| (c, None))
        } else {
            clients
                .ben
                .play(
                    &p.hand_pbn,
                    &p.dummy_pbn,
                    p.decision_maker,
                    p.dealer,
                    vul,
                    &p.ctx,
                    &p.played,
                )
                .await
        };
        match result {
            Ok((card, player)) => {
                // Live BEN names the DECISION-MAKER in `player`, not the
                // physical seat: dummy plays come back as 3 (declarer), not
                // 1 (benClient.js's comment assumes 1 — wrong empirically,
                // observed 2026-07-01). Accept either mapping; the legality
                // check against the acting seat's cards is the real guard.
                let mismatched = player
                    .map(|pl| ben::player_to_seat(pl, p.declarer))
                    .filter(|s| *s != p.seat && *s != p.decision_maker);
                if let Some(mapped) = mismatched {
                    tracing::warn!(
                        event = "ben_player_mismatch",
                        expected = %p.seat.to_char(),
                        got = %mapped.to_char(),
                        ""
                    );
                } else if p.legal.contains(&card) {
                    return (card, "ben");
                } else {
                    tracing::warn!(
                        event = "ben_card_illegal",
                        card = %format!("{}{}", card.suit.to_char(), card.rank.to_char()),
                        ""
                    );
                }
            }
            Err(e) => {
                // {:#} shows the anyhow cause chain (e.g. "operation timed
                // out"), which is what distinguishes a slow BEN from a down one.
                tracing::warn!(event = "ben_failed", error = %format!("{e:#}"), "falling back to RandomLegal")
            }
        }
    }
    (pick(&p.legal, p.seq), "random")
}
