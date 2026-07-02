//! The WebSocket endpoint — all game traffic flows through here.
//!
//! Protocol (JSON, one message per frame):
//!   client → server: {"t":"hello","ticket":"…"} must be the FIRST message
//!                    (the ticket travels in-band, not in the URL, so it
//!                    stays out of proxy logs; optional "bot":"random"
//!                    switches the room to RandomLegal bots for testing —
//!                    the welcome echoes "bot_mode":"real"|"random"), then
//!                    {"t":"bid","call":"1H"} | {"t":"play","card":"SA"} |
//!                    {"t":"undo","to_seq":N} | {"t":"pong"}
//!   server → client: {"t":"welcome"…} then {"t":"snapshot"…} on join,
//!                    after any undo, and after the opening lead,
//!                    {"t":"event"…} per action, {"t":"error"…}, {"t":"ping"}
//!
//! Every client gets a full redacted snapshot on (re)join — state is tiny,
//! so there is no event-replay protocol. `seq` lets clients drop stale
//! events. Undo broadcasts a fresh per-viewer snapshot because a rewind
//! can re-hide information (e.g. undoing the opening lead hides dummy);
//! the opening lead broadcasts one because it reveals dummy to everyone.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use bridge_types::{Call, Card, Direction, Rank, Suit};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde_json::{json, Value};

use crate::auth::{verify_ticket, Ticket};
use crate::rooms::RESYNC;
use crate::table::Action;
use crate::SharedState;

pub fn router() -> Router<SharedState> {
    Router::new().route("/ws", get(upgrade))
}

async fn upgrade(ws: WebSocketUpgrade, State(state): State<SharedState>) -> Response {
    ws.on_upgrade(move |socket| handle(socket, state))
}

fn parse_card(s: &str) -> Option<Card> {
    let mut ch = s.chars();
    let suit = Suit::from_char(ch.next()?)?;
    let rank = Rank::from_char(ch.next()?)?;
    if ch.next().is_some() {
        return None;
    }
    Some(Card::new(suit, rank))
}

fn err_msg(code: &str, msg: &str) -> String {
    json!({ "t": "error", "code": code, "msg": msg }).to_string()
}

/// One viewer's snapshot frame: redacted table state plus the seats map, so
/// a (re)joining client learns who is human/connected without waiting for
/// the next seat_update event.
fn snapshot_msg(
    room_id: &str,
    inner: &crate::rooms::RoomInner,
    seat: Option<Direction>,
    see_all: bool,
) -> String {
    json!({
        "t": "snapshot",
        "table_id": room_id,
        "state": inner.table.snapshot(seat, see_all),
        "seats": inner.seats_json(),
    })
    .to_string()
}

async fn handle(mut socket: WebSocket, state: SharedState) {
    // First frame must be hello{ticket}.
    let hello = match wait_for_hello(&mut socket, &state).await {
        Some(h) => h,
        None => return, // error already sent / socket gone
    };
    let ticket = hello.ticket;

    // Until session routing lands, every connection joins the demo room.
    let room = state.rooms.demo_room().await;

    // Testing convenience: hello{"bot":"random"} flips the whole room to
    // RandomLegal bots (sticky; absent field leaves the mode untouched).
    if hello.random_bots {
        room.random_bots
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(event = "bot_mode_random", room = %room.id, sub = %ticket.sub, "");
    }
    let bot_mode = if room.random_bots.load(std::sync::atomic::Ordering::Relaxed) {
        "random"
    } else {
        "real"
    };

    let (seat, welcome, snapshot, seats_event) = {
        let mut inner = room.state.lock().await;
        let seat = inner.seat_or_rebind(&ticket.sub, &ticket.name);
        let see_all = ticket.role == "teacher";
        let welcome = json!({
            "t": "welcome",
            "table_id": room.id,
            "name": ticket.name,
            "role": ticket.role,
            "seat": seat.map(|s| s.to_char().to_string()),
            "bot_mode": bot_mode,
        })
        .to_string();
        let snapshot = snapshot_msg(&room.id, &inner, seat, see_all);
        let seats_event = json!({
            "t": "event",
            "table_id": room.id,
            "seq": inner.table.seq(),
            "kind": "seat_update",
            "seats": inner.seats_json(),
        })
        .to_string();
        (seat, welcome, snapshot, seats_event)
    };

    let _ = crate::observability::events::record_event(
        &state.db,
        "ws_joined",
        json!({ "sub": ticket.sub, "seat": seat.map(|s| s.to_char().to_string()), "room": room.id }),
    )
    .await;
    room.broadcast(seats_event);
    // Watchdog so bot takeover of a later-disconnected seat fires without
    // another human action, plus an immediate kick for this join.
    crate::bots::ensure_keepalive(room.clone());
    crate::bots::kick(room.clone());

    let mut events = room.events.subscribe();
    let (mut tx, mut rx) = socket.split();
    if tx.send(Message::Text(welcome)).await.is_err() {
        return;
    }
    if tx.send(Message::Text(snapshot)).await.is_err() {
        return;
    }

    // Main loop: interleave room broadcasts with this client's messages.
    loop {
        tokio::select! {
            broadcasted = events.recv() => {
                match broadcasted {
                    // RESYNC is an internal marker (never sent to clients):
                    // snapshots are per-viewer (redaction), so a broadcast
                    // can't carry one. Each connection builds its own here.
                    Ok(msg) if msg == RESYNC => {
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, seat, ticket.role == "teacher");
                        drop(inner);
                        if tx.send(Message::Text(snap)).await.is_err() {
                            break;
                        }
                    }
                    Ok(msg) => {
                        if tx.send(Message::Text(msg)).await.is_err() {
                            break;
                        }
                    }
                    // Lagged: resync with a fresh snapshot.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, seat, ticket.role == "teacher");
                        drop(inner);
                        if tx.send(Message::Text(snap)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            incoming = rx.next() => {
                let Some(Ok(msg)) = incoming else { break };
                let Message::Text(text) = msg else { continue };
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    let _ = tx.send(Message::Text(err_msg("bad_json", "unparseable message"))).await;
                    continue;
                };
                if let Some(reply) = handle_client_msg(&room, &ticket, seat, &v).await {
                    if tx.send(Message::Text(reply)).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    // Socket gone: mark the seat disconnected. The seat stays bound to this
    // sub (a reconnect rebinds it); after the zombie-seat grace period the
    // keepalive-kicked bot driver plays for it (rooms::seat_bot_controlled).
    {
        let mut inner = room.state.lock().await;
        inner.mark_disconnected(&ticket.sub);
        let seats_event = json!({
            "t": "event",
            "table_id": room.id,
            "seq": inner.table.seq(),
            "kind": "seat_update",
            "seats": inner.seats_json(),
        })
        .to_string();
        drop(inner);
        room.broadcast(seats_event);
    }
    tracing::info!(event = "ws_left", sub = %ticket.sub, room = %room.id, "");
}

/// The parsed hello frame: the verified ticket plus protocol options.
struct Hello {
    ticket: Ticket,
    /// True iff the hello carried `"bot":"random"` (testing convenience:
    /// switches the room's bot driver to RandomLegal, skipping BBA/BEN).
    random_bots: bool,
}

async fn wait_for_hello(socket: &mut WebSocket, state: &SharedState) -> Option<Hello> {
    let msg = socket.recv().await?.ok()?;
    let Message::Text(text) = msg else {
        let _ = socket
            .send(Message::Text(err_msg(
                "expected_hello",
                "first frame must be hello",
            )))
            .await;
        return None;
    };
    let v: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => {
            let _ = socket
                .send(Message::Text(err_msg("bad_json", "unparseable hello")))
                .await;
            return None;
        }
    };
    if v["t"] != "hello" {
        let _ = socket
            .send(Message::Text(err_msg(
                "expected_hello",
                "first frame must be hello",
            )))
            .await;
        return None;
    }
    let Some(token) = v["ticket"].as_str() else {
        let _ = socket
            .send(Message::Text(err_msg("no_ticket", "hello.ticket missing")))
            .await;
        return None;
    };
    match verify_ticket(token, &state.ticket_secret) {
        Ok(ticket) => Some(Hello {
            ticket,
            random_bots: v["bot"].as_str() == Some("random"),
        }),
        Err(e) => {
            let _ = socket
                .send(Message::Text(err_msg("bad_ticket", &e.to_string())))
                .await;
            None
        }
    }
}

/// Apply one client message to the room. Returns an error frame to send back
/// to *this* client only (successful actions broadcast to the whole room).
async fn handle_client_msg(
    room: &std::sync::Arc<crate::rooms::Room>,
    ticket: &Ticket,
    seat: Option<Direction>,
    v: &Value,
) -> Option<String> {
    match v["t"].as_str() {
        Some("pong") => None,
        Some("bid") => {
            let Some(seat) = seat else {
                return Some(err_msg("not_seated", "kibitzers cannot bid"));
            };
            let Some(call) = v["call"].as_str().and_then(Call::from_pbn) else {
                return Some(err_msg("bad_call", "unrecognized call"));
            };
            let mut inner = room.state.lock().await;
            match inner.table.apply(Action::Call {
                seat,
                call: call.clone(),
            }) {
                Ok(seq) => {
                    let ev = json!({
                        "t": "event",
                        "table_id": room.id,
                        "seq": seq,
                        "kind": "bid_made",
                        "seat": seat.to_char().to_string(),
                        "call": call.to_pbn(),
                        "next_to_act": inner.table.fold().next_to_act.map(|d| d.to_char().to_string()),
                    })
                    .to_string();
                    drop(inner);
                    room.broadcast(ev);
                    crate::bots::kick(room.clone());
                    None
                }
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        Some("play") => {
            let Some(seat) = seat else {
                return Some(err_msg("not_seated", "kibitzers cannot play"));
            };
            let Some(card) = v["card"].as_str().and_then(parse_card) else {
                return Some(err_msg("bad_card", "unrecognized card"));
            };
            let mut inner = room.state.lock().await;
            // Server-authoritative seat resolution: a play always targets the
            // seat on turn. The sender is entitled if that's their own seat,
            // or if they're the declarer and the seat on turn is the dummy
            // (declarer plays dummy's cards).
            let f = inner.table.fold();
            let Some(turn_seat) = f.next_to_act else {
                return Some(err_msg("rejected", "no one is on turn"));
            };
            let entitled = turn_seat == seat
                || f.contract
                    .as_ref()
                    .is_some_and(|c| c.declarer == seat && turn_seat == c.dummy());
            if !entitled {
                return Some(err_msg("rejected", "not your turn"));
            }
            let was_opening_lead = f.played.is_empty();
            match inner.table.apply(Action::Play {
                seat: turn_seat,
                card,
            }) {
                Ok(seq) => {
                    let f = inner.table.fold();
                    // A trick just completed iff this play was a trick's 4th
                    // card: the current trick is now empty (or play is done).
                    let trick_just_completed = match &f.current_trick {
                        None => true,
                        Some((_, plays)) => plays.is_empty(),
                    };
                    let trick_winner = f
                        .completed_tricks
                        .last()
                        .filter(|_| trick_just_completed)
                        .map(|(_, _, w)| w.to_char().to_string());
                    let ev = json!({
                        "t": "event",
                        "table_id": room.id,
                        "seq": seq,
                        "kind": "card_played",
                        "seat": turn_seat.to_char().to_string(),
                        "card": v["card"].as_str(),
                        "next_to_act": f.next_to_act.map(|d| d.to_char().to_string()),
                        "trick_winner": trick_winner,
                        "tricks": { "ns": f.tricks_ns, "ew": f.tricks_ew },
                    })
                    .to_string();
                    drop(inner);
                    room.broadcast(ev);
                    // The opening lead reveals dummy to everyone — each
                    // connection rebuilds its own redacted snapshot.
                    if was_opening_lead {
                        room.broadcast_resync();
                    }
                    crate::bots::kick(room.clone());
                    None
                }
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        Some("undo") => {
            // Unlimited, any-actor undo (Shark-style) per the project plan;
            // per-table policy knobs come with teacher sessions.
            let Some(to_seq) = v["to_seq"].as_u64() else {
                return Some(err_msg("bad_undo", "undo.to_seq missing"));
            };
            let mut inner = room.state.lock().await;
            match inner.table.undo_to(to_seq as usize) {
                Ok(seq) => {
                    let ev = json!({
                        "t": "event",
                        "table_id": room.id,
                        "seq": seq,
                        "kind": "undo",
                        "by": ticket.name,
                    })
                    .to_string();
                    drop(inner);
                    room.broadcast(ev);
                    // A rewind can re-hide info (e.g. undoing the opening
                    // lead hides dummy again), so every connection rebuilds
                    // its own redacted snapshot.
                    room.broadcast_resync();
                    crate::bots::kick(room.clone());
                    None
                }
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        _ => Some(err_msg("unknown", "unknown message type")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_includes_state_and_seats() {
        let room = crate::rooms::Room::new_for_test("t1");
        let mut inner = room.state.try_lock().unwrap();
        inner.seat_or_rebind("u1", "Alice");
        let msg = snapshot_msg("t1", &inner, Some(Direction::South), false);
        let v: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["t"], "snapshot");
        assert_eq!(v["table_id"], "t1");
        assert_eq!(v["state"]["your_seat"], "S");
        // Seats travel with every snapshot so late joiners see who is
        // human vs bot without waiting for a seat_update event.
        assert_eq!(v["seats"]["S"]["kind"], "human");
        assert_eq!(v["seats"]["S"]["name"], "Alice");
        assert_eq!(v["seats"]["W"]["kind"], "empty");
    }
}
