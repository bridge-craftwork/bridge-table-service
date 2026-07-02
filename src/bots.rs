//! Bot seats.
//!
//! Any unoccupied seat is a bot (with one exception: a human declarer plays
//! the dummy's cards, so the dummy seat is driven by the declarer, not a
//! bot). This mirrors the frontend's pluggable-bot design (`cardplayBots.js`):
//! `RandomLegal` is the always-available fallback; `BbaBidder`/`BenPlayer`
//! (async, HTTP) slot in behind the same interface in the next phase — the
//! driver below already tolerates awaiting between lock acquisitions.
//!
//! The driver is undo-safe by construction: every iteration re-folds the
//! action log from scratch, so a rewind that happens mid-pacing-sleep just
//! changes what the next iteration sees. Exactly one driver task runs per
//! room (`bot_running` flag); every accepted action re-kicks it.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bridge_types::{Call, Card, Direction};
use serde_json::json;

use crate::rooms::Room;
use crate::table::{Action, Phase};

/// Pacing, matching the solo client's feel (useCardPlay.js): a beat between
/// plays, a longer beat after a completed trick.
const BETWEEN_PLAYS: Duration = Duration::from_millis(300);
const AFTER_TRICK: Duration = Duration::from_millis(1000);

/// Deterministic "random" pick: multiplicative hash of the action seq.
/// Deterministic bots make failures replayable; real randomness (or BEN)
/// can replace this per-bot later.
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

/// Make one bot action if it's a bot's turn. Returns false when it isn't
/// (human's turn, board complete, or empty room with nothing to do).
async fn act_once(room: &Room) -> bool {
    // Pace before acting so bots don't feel instant. The lock is NOT held
    // during the sleep — humans can act (or undo) meanwhile; we re-fold after.
    tokio::time::sleep(BETWEEN_PLAYS).await;

    let mut inner = room.state.lock().await;
    let f = inner.table.fold();
    let Some(seat) = f.next_to_act else {
        return false;
    };
    if !is_bot_seat(&inner, &f, seat) {
        return false;
    }

    let seq = inner.table.seq();
    let event = match f.phase {
        Phase::Bidding => {
            // RandomLegal bidding policy: always pass. (Keeps auctions sane
            // until BbaBidder lands — the human's calls set the contract.)
            let call = Call::Pass;
            match inner.table.apply(Action::Call {
                seat,
                call: call.clone(),
            }) {
                Ok(new_seq) => {
                    let f2 = inner.table.fold();
                    json!({
                        "t": "event",
                        "table_id": room.id,
                        "seq": new_seq,
                        "kind": "bid_made",
                        "seat": seat.to_char().to_string(),
                        "call": call.to_pbn(),
                        "by_bot": true,
                        "next_to_act": f2.next_to_act.map(|d| d.to_char().to_string()),
                    })
                }
                Err(e) => {
                    tracing::error!(event = "bot_bid_rejected", error = %e, "");
                    return false;
                }
            }
        }
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
            let card: Card = pick(&legal, seq);
            match inner.table.apply(Action::Play { seat, card }) {
                Ok(new_seq) => {
                    let f2 = inner.table.fold();
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
                        "next_to_act": f2.next_to_act.map(|d| d.to_char().to_string()),
                        "trick_winner": winner,
                        "tricks": { "ns": f2.tricks_ns, "ew": f2.tricks_ew },
                    })
                }
                Err(e) => {
                    tracing::error!(event = "bot_play_rejected", error = %e, "");
                    return false;
                }
            }
        }
        Phase::Complete => return false,
    };

    let trick_pause = event["trick_winner"].is_string();
    drop(inner);
    room.broadcast(event.to_string());
    if trick_pause {
        tokio::time::sleep(AFTER_TRICK - BETWEEN_PLAYS).await;
    }
    true
}
