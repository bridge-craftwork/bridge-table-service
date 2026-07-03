//! Bot seats.
//!
//! Any unoccupied seat is a bot, and a seat whose human has been
//! disconnected past the zombie-seat grace period (see
//! `RoomInner::seat_bot_controlled`) is bot-driven until the human
//! reconnects. One exception: a human declarer plays the dummy's cards, so
//! the dummy seat is driven by whoever controls the declarer seat, not a
//! bot. This mirrors the frontend's pluggable-bot design (`cardplayBots.js`):
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
use std::time::{Duration, Instant};

use bridge_types::{Call, Card, Direction, Vulnerability};
use once_cell::sync::OnceCell;
use serde_json::json;

use crate::engine::auction;
use crate::rooms::{BotMode, Room};
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
/// `ben_timeout` is the per-call BEN budget (`BOT_TIMEOUT_MS`); BBA keeps
/// its own fixed budget (bidding shouldn't stall an auction that long).
pub fn init(ben_url: &str, bba_url: &str, ben_timeout: Duration) {
    let clients = Clients {
        ben: ben::BenClient::new(ben_url, ben_timeout),
        bba: bba::BbaClient::new(bba_url),
    };
    clients.ben.warm();
    if CLIENTS.set(clients).is_err() {
        tracing::warn!(event = "bots_already_initialized", "");
    }
    tracing::info!(
        event = "bots_initialized",
        ben_url,
        bba_url,
        ben_timeout_ms = ben_timeout.as_millis() as u64,
        ""
    );
}

fn clients() -> Option<&'static Clients> {
    CLIENTS.get()
}

/// Deterministic "random" pick: multiplicative hash of the action seq.
/// Deterministic fallbacks make failures replayable.
fn pick<T: Copy>(options: &[T], seq: usize) -> T {
    options[(seq.wrapping_mul(2654435761)) % options.len()]
}

/// Keepalive watchdog cadence — how often an idle room re-checks whether a
/// bot should act. This is what makes zombie-seat takeover happen without
/// another human action: the grace period expires, the next tick kicks the
/// driver, the bot plays.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Spawn the per-room keepalive watchdog (idempotent — at most one task per
/// room). Rooms live for the process lifetime (in-memory registry, no
/// eviction), so the task never needs to stop; each tick is one cheap
/// lock-and-fold when nothing is pending.
pub fn ensure_keepalive(room: Arc<Room>) {
    if room.keepalive_running.swap(true, Ordering::SeqCst) {
        return; // already watching
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(KEEPALIVE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            kick(room.clone());
        }
    });
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
        Some(seat) => mode_allows_bot(&inner, &f) && is_bot_seat(&inner, &f, seat),
        None => false,
    }
}

/// Board-mode gate for the driver: bid-only boards have no bot cardplay
/// (the board is over at the contract); play-only boards let bots bid for
/// EVERY seat (the auction is automatic — see rooms::BoardMode).
fn mode_allows_bot(inner: &crate::rooms::RoomInner, f: &crate::table::Folded) -> bool {
    !(inner.board_mode == crate::rooms::BoardMode::BidOnly && f.phase == Phase::Play)
}

fn mode_forces_bot(inner: &crate::rooms::RoomInner, f: &crate::table::Folded) -> bool {
    inner.board_mode == crate::rooms::BoardMode::PlayOnly && f.phase == Phase::Bidding
}

/// Whether the bot driver should act for `seat`. Control of the dummy
/// belongs to whoever controls the declarer seat (bridge: declarer plays
/// dummy's cards) — so the dummy is bot-driven iff the *declarer* seat is
/// bot-controlled, regardless of who sits at dummy. A seat is
/// bot-controlled when empty or when its human has been disconnected past
/// the zombie-seat grace period (`seat_bot_controlled`). This is only ever
/// asked about the seat on turn, so the on-turn grace period applies.
fn is_bot_seat(inner: &crate::rooms::RoomInner, f: &crate::table::Folded, seat: Direction) -> bool {
    let controller = match &f.contract {
        // Declarer-side seats resolve through the controller chain: human
        // declarer plays both; else a human dummy plays the hand for a bot
        // declarer; else the bot drives.
        Some(c) if seat == c.dummy() || seat == c.declarer => {
            inner.declarer_side_controller(c.declarer, true, Instant::now())
        }
        _ => seat,
    };
    inner.seat_bot_controlled(controller, true, Instant::now())
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
    // Rulebot inputs (bridge-rulebot is stateless: full history per call).
    /// The acting seat's remaining cards (superset of `legal`).
    remaining: Vec<Card>,
    /// Dummy's ORIGINAL 13 cards.
    dummy_cards: Vec<Card>,
    /// The auction, in call order from the dealer.
    calls: Vec<Call>,
    /// Chronological play history, all seats.
    played_pairs: Vec<(Direction, Card)>,
    /// PBN contract string, e.g. "4S", "3N", "2HX".
    contract_pbn: String,
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
        if !mode_allows_bot(&inner, &f) {
            return false;
        }
        if !mode_forces_bot(&inner, &f) && !is_bot_seat(&inner, &f, seat) {
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
                    remaining,
                    dummy_cards: board.deal.hand(c.dummy()).cards().to_vec(),
                    calls: f.calls.clone(),
                    played_pairs: f.played.clone(),
                    contract_pbn: c.to_pbn(),
                }))
            }
            Phase::Complete => return false,
        }
    };

    // Phase B: choose. May take seconds (BBA/BEN over HTTP); the state
    // lock is not held, so humans keep acting and undo keeps working.
    // Random and Rules modes are instant and never call BBA/BEN.
    let mode = room.bot_mode();
    let (seq, action, via, opening_lead) = match pending {
        Pending::Bid {
            seat,
            seq,
            board,
            calls,
        } => {
            let (call, via) = match mode {
                // Rules/Slow modes still bid with BBA — the rulebot is
                // cardplay only, and all-pass auctions made the mode useless
                // for watching real contracts (Rick, 2026-07-02).
                BotMode::Real | BotMode::Rules | BotMode::Slow => {
                    choose_call(room, &board, &calls).await
                }
                BotMode::Random => (Call::Pass, "random"),
            };
            (seq, Action::Call { seat, call }, via, false)
        }
        Pending::Play(p) => {
            let (card, via) = match mode {
                BotMode::Real => choose_card(&p).await,
                BotMode::Random => (pick(&p.legal, p.seq), "random"),
                BotMode::Rules => (rules_card(&p), "rules"),
                // Slow mode mimics humans at N/S: rulebot card, then a
                // random 3-10s think before it lands. E/W stay fast. The
                // state lock is NOT held during the sleep and the seq guard
                // below discards the play if a human acted meanwhile.
                BotMode::Slow => {
                    let card = rules_card(&p);
                    if matches!(p.seat, Direction::North | Direction::South) {
                        let think_ms = 3000 + (rand::random::<u64>() % 7000);
                        tokio::time::sleep(Duration::from_millis(think_ms)).await;
                    }
                    (card, "rules")
                }
            };
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
    // A bot action can end the board (last card, or the 4th pass of a
    // passed-out deal): broadcast the result like a human action would.
    let complete = inner.result_json().map(|result| {
        let mut ev = json!({
            "t": "event",
            "table_id": room.id,
            "seq": new_seq,
            "kind": "board_complete",
        });
        ev.as_object_mut()
            .unwrap()
            .extend(result.as_object().unwrap().clone());
        ev.to_string()
    });
    drop(inner);
    room.broadcast(event.to_string());
    // A bot's opening lead reveals dummy to everyone — trigger the same
    // per-viewer resync a human opening lead (or an undo) does.
    if opening_lead {
        room.broadcast_resync();
    }
    if let Some(complete) = complete {
        room.broadcast(complete);
        // A finished board reveals the original deal to everyone for review
        // (snapshot() un-redacts at Phase::Complete) — push snapshots.
        room.broadcast_resync();
    }
    // Keep the teacher's lobby grid live (phase / trick counts / turn).
    room.notify_session_lobby();
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
                tracing::warn!(event = "ben_failed", error = %format!("{e:#}"), "falling back to rulebot")
            }
        }
    }
    (rules_card(p), "rules")
}

/// Rule-based card via bridge-rulebot: deterministic, sub-millisecond,
/// with a teachable reason for every pick. Serves as the Rules bot mode
/// and as the cardplay fallback while BEN is cold/slow/wrong. The rulebot
/// is stateless — the full play history goes in on every call — and only
/// ever returns a member of `legal`, so no re-validation is needed (the
/// deterministic pick guards the impossible error path anyway).
fn rules_card(p: &PlayPending) -> Card {
    let config = bridge_rulebot::SignalConfig::default();
    let decision = if p.opening_lead {
        bridge_rulebot::choose_opening_lead(
            &bridge_rulebot::LeadContext {
                seat: p.seat,
                hand: p.remaining.clone(),
                declarer: p.declarer,
                dealer: p.dealer,
                contract: p.contract_pbn.clone(),
                auction: p.calls.clone(),
                vulnerability: p.vul,
                legal: p.legal.clone(),
            },
            &config,
        )
    } else {
        bridge_rulebot::choose_card(
            &bridge_rulebot::PlayContext {
                seat: p.seat,
                hand: p.remaining.clone(),
                dummy: p.dummy_cards.clone(),
                declarer: p.declarer,
                dealer: p.dealer,
                contract: p.contract_pbn.clone(),
                auction: p.calls.clone(),
                vulnerability: p.vul,
                played: p
                    .played_pairs
                    .iter()
                    .map(|(seat, card)| bridge_rulebot::PlayedCard {
                        seat: *seat,
                        card: *card,
                    })
                    .collect(),
                legal: p.legal.clone(),
            },
            &config,
        )
    };
    match decision {
        Ok(d) => {
            // Info, not debug: this is the audit trail for evaluating the
            // bot's play (the droplet runs LOG_LEVEL=info). Low volume — at
            // most one line per bot card.
            tracing::info!(
                event = "rulebot_decision",
                rule = d.rule,
                legal_count = d.legal_count,
                micros = d.duration_micros,
                explanation = %d.explanation,
                ""
            );
            d.card
        }
        Err(e) => {
            // Only reachable with an empty legal set, which act_once
            // already screens out.
            tracing::error!(event = "rulebot_failed", error = %e, "");
            pick(&p.legal, p.seq)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_types::Direction::*;
    use bridge_types::{Rank, Suit};

    /// The rulebot path returns a legal card deterministically, and its
    /// choice is a *reasoned* one: third hand (partner led a low spade,
    /// dummy played low) goes up with the king, which no random pick can
    /// be trusted to do. Exercises the full PlayPending → PlayContext
    /// translation including the played-history mapping.
    #[test]
    fn rules_card_plays_third_hand_high() {
        let c = |s: Suit, r: Rank| Card::new(s, r);
        let legal = vec![
            c(Suit::Spades, Rank::King),
            c(Suit::Spades, Rank::Eight),
            c(Suit::Spades, Rank::Two),
        ];
        let p = PlayPending {
            seat: East,
            seq: 7,
            legal: legal.clone(),
            opening_lead: false,
            decision_maker: East,
            declarer: South,
            hand_pbn: String::new(),
            dummy_pbn: String::new(),
            dealer: North,
            vul: Vulnerability::None,
            ctx: String::new(),
            played: String::new(),
            remaining: legal,
            dummy_cards: vec![
                c(Suit::Spades, Rank::Seven),
                c(Suit::Spades, Rank::Four),
                c(Suit::Spades, Rank::Three),
            ],
            calls: vec![],
            played_pairs: vec![
                (West, c(Suit::Spades, Rank::Five)),
                (North, c(Suit::Spades, Rank::Three)),
            ],
            contract_pbn: "3N".to_string(),
        };
        let first = rules_card(&p);
        assert_eq!(first, c(Suit::Spades, Rank::King), "third hand high");
        assert_eq!(rules_card(&p), first, "deterministic");
    }

    /// Zombie-seat takeover through the driver's own gate: a fully human
    /// table has no pending bot turn until the seat on turn has been
    /// disconnected past the grace period, and a reconnect reclaims control
    /// instantly. The clock is injected by editing `disconnected_at`
    /// directly — no sleeps.
    #[tokio::test]
    async fn disconnected_seat_becomes_bot_driven_after_grace() {
        let room = crate::rooms::Room::new_for_test("t");
        {
            let mut inner = room.state.lock().await;
            for sub in ["u1", "u2", "u3", "u4"] {
                inner.seat_or_rebind(sub, sub); // S, W, N, E
            }
        }
        // Demo board's dealer is North (= "u3"), so North is on turn.
        assert!(!bot_turn_pending(&room).await, "all humans connected");

        room.state.lock().await.mark_disconnected("u3");
        assert!(
            !bot_turn_pending(&room).await,
            "freshly disconnected: still within grace"
        );

        // Backdate the disconnect past the on-turn grace period.
        room.state
            .lock()
            .await
            .seats
            .get_mut(&North)
            .unwrap()
            .disconnected_at =
            Some(Instant::now() - crate::rooms::TAKEOVER_AFTER_ON_TURN - Duration::from_secs(1));
        assert!(
            bot_turn_pending(&room).await,
            "grace expired: bot takes over"
        );

        // Reconnect: same sub rebinds the same seat, takeover ends.
        assert_eq!(
            room.state.lock().await.seat_or_rebind("u3", "u3"),
            Some(North)
        );
        assert!(!bot_turn_pending(&room).await, "human reclaimed the seat");
    }
}
