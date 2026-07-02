//! The WebSocket endpoint — all game traffic flows through here.
//!
//! Protocol (JSON, one message per frame):
//!   client → server: {"t":"hello","ticket":"…"} must be the FIRST message
//!                    (the ticket travels in-band, not in the URL, so it
//!                    stays out of proxy logs; optional "bot":"random"
//!                    switches the table to RandomLegal bots for testing —
//!                    the welcome echoes "bot_mode":"real"|"random"), then
//!                    {"t":"bid","call":"1H"} | {"t":"play","card":"SA"} |
//!                    {"t":"undo","to_seq":N} | {"t":"ready_next_board"} |
//!                    {"t":"pong"};
//!                    teacher only: {"t":"open_boards","count":N} |
//!                    {"t":"assign_seat","table":ID,"seat":"S","sub":SUB|null} |
//!                    {"t":"boot","table":ID,"seat":"S"} |
//!                    {"t":"force_advance","table":ID} |
//!                    {"t":"kibitz","table":ID}
//!   server → client: {"t":"welcome"…} then {"t":"snapshot"…} on join,
//!                    after any undo/board change, and after the opening
//!                    lead; {"t":"event"…} per action; {"t":"lobby"…}
//!                    (teachers, on join and on any session change);
//!                    {"t":"error"…}; {"t":"ping"}
//!
//! Routing: the verified ticket's `session_id` selects a live session
//! (created via POST /admin/sessions); the literal id "demo" falls through
//! to the standing dev room. Within a session, arrivals are seated by the
//! session's seat policy; the session owner (or any teacher-role ticket)
//! joins as the see-all teacher — lobby feed, kibitz-any-table, seat and
//! round control.
//!
//! A player's `welcome` can arrive again mid-connection: when the teacher
//! reseats them (assign_seat/boot) their connection re-resolves its table
//! and seat and announces the new binding with a fresh welcome + snapshot.

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
use std::sync::Arc;

use crate::auth::{verify_ticket, Ticket};
use crate::rooms::{Room, RESYNC};
use crate::sessions::{Session, RECHECK};
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

fn seats_event(room: &Room, inner: &crate::rooms::RoomInner) -> String {
    json!({
        "t": "event",
        "table_id": room.id,
        "seq": inner.table.seq(),
        "kind": "seat_update",
        "seats": inner.seats_json(),
    })
    .to_string()
}

/// The board_complete event for `inner`'s table, if the board just ended.
/// Built under the room lock; broadcast by the caller after dropping it.
/// Results are not persisted (v1) — this broadcast is the whole delivery.
fn board_complete_msg(room_id: &str, inner: &crate::rooms::RoomInner) -> Option<String> {
    inner.table.board_result_json().map(|result| {
        let mut ev = json!({
            "t": "event",
            "table_id": room_id,
            "seq": inner.table.seq(),
            "kind": "board_complete",
        });
        ev.as_object_mut()
            .unwrap()
            .extend(result.as_object().unwrap().clone());
        ev.to_string()
    })
}

async fn handle(mut socket: WebSocket, state: SharedState) {
    // First frame must be hello{ticket}.
    let hello = match wait_for_hello(&mut socket, &state).await {
        Some(h) => h,
        None => return, // error already sent / socket gone
    };

    // Session routing: the ticket names the session; "demo" is the standing
    // dev room (no admin setup required).
    if let Some(session) = state.rooms.get_session(&hello.ticket.session_id).await {
        let is_teacher =
            hello.ticket.sub == session.owner_sub || hello.ticket.role == "teacher";
        if is_teacher {
            handle_teacher(socket, state, session, hello).await;
        } else {
            handle_player(socket, state, session, hello).await;
        }
        return;
    }
    if hello.ticket.session_id == "demo" {
        handle_demo(socket, state, hello).await;
        return;
    }
    let _ = socket
        .send(Message::Text(err_msg(
            "unknown_session",
            "no such session (it may have been closed)",
        )))
        .await;
}

// ---------------------------------------------------------------------------
// Session player connections
// ---------------------------------------------------------------------------

async fn handle_player(
    socket: WebSocket,
    state: SharedState,
    session: Arc<Session>,
    hello: Hello,
) {
    let ticket = hello.ticket;
    let placement = session.place(&ticket.sub, &ticket.name).await;
    let mut room: Arc<Room> = session.rooms[placement.room_idx].clone();
    let mut seat = placement.seat;

    if hello.random_bots {
        room.random_bots
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(event = "bot_mode_random", room = %room.id, sub = %ticket.sub, "");
    }

    let (welcome, snapshot, seats_ev) = {
        let inner = room.state.lock().await;
        (
            welcome_msg(&session, &room, &ticket, seat),
            snapshot_msg(&room.id, &inner, seat, false),
            seats_event(&room, &inner),
        )
    };

    let _ = crate::observability::events::record_event(
        &state.db,
        "ws_joined",
        json!({
            "sub": ticket.sub,
            "seat": seat.map(|s| s.to_char().to_string()),
            "room": room.id,
            "session": session.id,
        }),
    )
    .await;
    room.broadcast(seats_ev);
    session.notify_lobby();
    crate::bots::ensure_keepalive(room.clone());
    crate::bots::kick(room.clone());

    let mut room_events = room.events.subscribe();
    let mut session_events = session.notify.subscribe();
    let (mut tx, mut rx) = socket.split();
    if tx.send(Message::Text(welcome)).await.is_err()
        || tx.send(Message::Text(snapshot)).await.is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            broadcasted = room_events.recv() => {
                match broadcasted {
                    Ok(msg) if msg == RESYNC => {
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, seat, false);
                        drop(inner);
                        if tx.send(Message::Text(snap)).await.is_err() {
                            break;
                        }
                    }
                    Ok(msg) => {
                        let closing = msg.contains("\"session_closed\"");
                        if tx.send(Message::Text(msg)).await.is_err() || closing {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, seat, false);
                        drop(inner);
                        if tx.send(Message::Text(snap)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            notice = session_events.recv() => {
                match notice {
                    Ok(m) if m == RECHECK => {
                        // The teacher reseated someone — possibly us.
                        match resolve_binding(&session, &ticket, &mut room, &mut seat, &mut room_events).await {
                            Rebind::Unchanged => {}
                            Rebind::Changed => {
                                let inner = room.state.lock().await;
                                let welcome = welcome_msg(&session, &room, &ticket, seat);
                                let snap = snapshot_msg(&room.id, &inner, seat, false);
                                drop(inner);
                                if tx.send(Message::Text(welcome)).await.is_err()
                                    || tx.send(Message::Text(snap)).await.is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(_) => {} // LOBBY etc.: teacher-only concerns
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
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
                if let Some(reply) = handle_client_msg(&room, Some(&session), &ticket, seat, &v).await {
                    if tx.send(Message::Text(reply)).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    // Socket gone. A seated player keeps their seat (zombie-seat policy:
    // reconnect rebinds; bots take over after the grace period). A kibitzer
    // is simply forgotten.
    if session.find_seated(&ticket.sub).await.is_some() {
        let mut inner = room.state.lock().await;
        inner.mark_disconnected(&ticket.sub);
        let ev = seats_event(&room, &inner);
        drop(inner);
        room.broadcast(ev);
    } else {
        session.remove_kibitzer(&ticket.sub).await;
    }
    session.notify_lobby();
    tracing::info!(event = "ws_left", sub = %ticket.sub, room = %room.id, session = %session.id, "");
}

fn welcome_msg(
    session: &Session,
    room: &Room,
    ticket: &Ticket,
    seat: Option<Direction>,
) -> String {
    let bot_mode = if room
        .random_bots
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        "random"
    } else {
        "real"
    };
    json!({
        "t": "welcome",
        "session_id": session.id,
        "table_id": room.id,
        "name": ticket.name,
        "role": ticket.role,
        "seat": seat.map(|s| s.to_char().to_string()),
        "bot_mode": bot_mode,
    })
    .to_string()
}

enum Rebind {
    Unchanged,
    Changed,
}

/// Re-resolve this connection's table/seat after a teacher reseat. Booted
/// players become kibitzers of the table they were removed from.
async fn resolve_binding(
    session: &Arc<Session>,
    ticket: &Ticket,
    room: &mut Arc<Room>,
    seat: &mut Option<Direction>,
    room_events: &mut tokio::sync::broadcast::Receiver<String>,
) -> Rebind {
    match session.find_seated(&ticket.sub).await {
        Some((idx, s)) => {
            let target = &session.rooms[idx];
            if target.id == room.id && *seat == Some(s) {
                return Rebind::Unchanged;
            }
            if target.id != room.id {
                *room = target.clone();
                *room_events = room.events.subscribe();
            }
            *seat = Some(s);
            session.remove_kibitzer(&ticket.sub).await;
            Rebind::Changed
        }
        None => {
            if seat.is_none() {
                return Rebind::Unchanged; // still the same kibitzer
            }
            // Booted: watch the table we were removed from.
            *seat = None;
            let idx = session
                .rooms
                .iter()
                .position(|r| r.id == room.id)
                .unwrap_or(0);
            session.add_kibitzer(&ticket.sub, &ticket.name, idx).await;
            Rebind::Changed
        }
    }
}

// ---------------------------------------------------------------------------
// Session teacher connections
// ---------------------------------------------------------------------------

async fn handle_teacher(
    socket: WebSocket,
    state: SharedState,
    session: Arc<Session>,
    hello: Hello,
) {
    let ticket = hello.ticket;
    if hello.random_bots {
        for room in &session.rooms {
            room.random_bots
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        tracing::info!(event = "bot_mode_random", session = %session.id, sub = %ticket.sub, "");
    }

    let welcome = json!({
        "t": "welcome",
        "session_id": session.id,
        "name": ticket.name,
        "role": "teacher",
        "seat": null,
        "see_all": true,
    })
    .to_string();
    let lobby = session.lobby_json().await;

    let _ = crate::observability::events::record_event(
        &state.db,
        "ws_joined",
        json!({ "sub": ticket.sub, "role": "teacher", "session": session.id }),
    )
    .await;
    for room in &session.rooms {
        crate::bots::ensure_keepalive(room.clone());
    }

    let mut session_events = session.notify.subscribe();
    // The table the teacher is currently kibitzing, if any ({"t":"kibitz"}).
    let mut kibitz: Option<Arc<Room>> = None;
    let mut kibitz_events: Option<tokio::sync::broadcast::Receiver<String>> = None;

    let (mut tx, mut rx) = socket.split();
    if tx.send(Message::Text(welcome)).await.is_err()
        || tx.send(Message::Text(lobby)).await.is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            notice = session_events.recv() => {
                match notice {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Any session change → fresh lobby frame. Also how a
                        // closed session wakes us to hang up.
                        if state.rooms.get_session(&session.id).await.is_none() {
                            let _ = tx.send(Message::Text(json!({
                                "t": "event", "kind": "session_closed",
                            }).to_string())).await;
                            break;
                        }
                        let lobby = session.lobby_json().await;
                        if tx.send(Message::Text(lobby)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            broadcasted = recv_opt(&mut kibitz_events) => {
                let Some(room) = &kibitz else { continue };
                match broadcasted {
                    Ok(msg) if msg == RESYNC => {
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, None, true);
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
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, None, true);
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
                match v["t"].as_str() {
                    Some("kibitz") => {
                        let Some(table_id) = v["table"].as_str() else {
                            let _ = tx.send(Message::Text(err_msg("bad_table", "kibitz.table missing"))).await;
                            continue;
                        };
                        let Some(target) = session.room_by_id(table_id) else {
                            let _ = tx.send(Message::Text(err_msg("bad_table", "unknown table"))).await;
                            continue;
                        };
                        kibitz_events = Some(target.events.subscribe());
                        kibitz = Some(target.clone());
                        let inner = target.state.lock().await;
                        let snap = snapshot_msg(&target.id, &inner, None, true);
                        drop(inner);
                        if tx.send(Message::Text(snap)).await.is_err() {
                            break;
                        }
                    }
                    _ => {
                        if let Some(reply) = handle_teacher_msg(&session, kibitz.as_ref(), &ticket, &v).await {
                            if tx.send(Message::Text(reply)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    tracing::info!(event = "ws_left", sub = %ticket.sub, role = "teacher", session = %session.id, "");
}

/// Await a broadcast on an optional receiver; pends forever when there is
/// none (so the select! arm simply never fires).
async fn recv_opt(
    rx: &mut Option<tokio::sync::broadcast::Receiver<String>>,
) -> Result<String, tokio::sync::broadcast::error::RecvError> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

/// Teacher control messages. Returns an error frame for this client only;
/// state changes announce themselves via broadcasts / lobby refreshes.
async fn handle_teacher_msg(
    session: &Arc<Session>,
    kibitz: Option<&Arc<Room>>,
    ticket: &Ticket,
    v: &Value,
) -> Option<String> {
    match v["t"].as_str() {
        Some("pong") => None,
        Some("open_boards") => {
            let Some(count) = v["count"].as_u64() else {
                return Some(err_msg("bad_count", "open_boards.count missing"));
            };
            let open = session.open_boards_to(count as usize);
            // Tell every table (players see "next board available"), then
            // move any table whose humans are already all ready.
            for room in &session.rooms {
                room.broadcast(
                    json!({
                        "t": "event",
                        "table_id": room.id,
                        "kind": "boards_open",
                        "open": open,
                        "total": session.boards.len(),
                    })
                    .to_string(),
                );
            }
            for room in &session.rooms {
                session.try_advance(room, false).await;
            }
            session.notify_lobby();
            None
        }
        Some("force_advance") => {
            let Some(room) = v["table"].as_str().and_then(|id| session.room_by_id(id)) else {
                return Some(err_msg("bad_table", "force_advance.table unknown"));
            };
            if session.try_advance(room, true).await {
                None
            } else {
                Some(err_msg("rejected", "no more boards"))
            }
        }
        Some("assign_seat") | Some("boot") => {
            let Some(table_id) = v["table"].as_str() else {
                return Some(err_msg("bad_table", "table missing"));
            };
            let Some(seat) = v["seat"]
                .as_str()
                .and_then(|s| s.chars().next())
                .and_then(Direction::from_char)
            else {
                return Some(err_msg("bad_seat", "seat missing or unrecognized"));
            };
            // boot ≡ assign_seat with sub:null.
            let sub = if v["t"] == "boot" { None } else { v["sub"].as_str() };
            match session.assign_seat(table_id, seat, sub).await {
                Ok(changed) => {
                    for idx in changed {
                        let room = &session.rooms[idx];
                        let inner = room.state.lock().await;
                        let ev = seats_event(room, &inner);
                        drop(inner);
                        room.broadcast(ev);
                        // A vacated seat is a bot now; a filled one may
                        // unblock the table.
                        crate::bots::kick(room.clone());
                    }
                    session.notify_recheck();
                    session.notify_lobby();
                    None
                }
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        Some("undo") => {
            // Teacher undo applies to the kibitzed table.
            let Some(room) = kibitz else {
                return Some(err_msg("no_table", "kibitz a table before undoing"));
            };
            handle_client_msg(room, Some(session), ticket, None, v).await
        }
        _ => Some(err_msg("unknown", "unknown message type")),
    }
}

// ---------------------------------------------------------------------------
// Demo room (dev): pre-session behavior, unchanged
// ---------------------------------------------------------------------------

async fn handle_demo(socket: WebSocket, state: SharedState, hello: Hello) {
    let ticket = hello.ticket;
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

    let (seat, welcome, snapshot, seats_ev) = {
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
        let seats_ev = seats_event(&room, &inner);
        (seat, welcome, snapshot, seats_ev)
    };

    let _ = crate::observability::events::record_event(
        &state.db,
        "ws_joined",
        json!({ "sub": ticket.sub, "seat": seat.map(|s| s.to_char().to_string()), "room": room.id }),
    )
    .await;
    room.broadcast(seats_ev);
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
                if let Some(reply) = handle_client_msg(&room, None, &ticket, seat, &v).await {
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
        let ev = seats_event(&room, &inner);
        drop(inner);
        room.broadcast(ev);
    }
    tracing::info!(event = "ws_left", sub = %ticket.sub, room = %room.id, "");
}

/// The parsed hello frame: the verified ticket plus protocol options.
struct Hello {
    ticket: Ticket,
    /// True iff the hello carried `"bot":"random"` (testing convenience:
    /// switches the table's bot driver to RandomLegal, skipping BBA/BEN).
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
/// `session` is None for the demo room (no board rounds there).
async fn handle_client_msg(
    room: &Arc<Room>,
    session: Option<&Arc<Session>>,
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
                    // A 4th pass completes a passed-out board.
                    let complete = board_complete_msg(&room.id, &inner);
                    drop(inner);
                    room.broadcast(ev);
                    if let Some(complete) = complete {
                        room.broadcast(complete);
                    }
                    room.notify_session_lobby();
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
                    let complete = board_complete_msg(&room.id, &inner);
                    drop(inner);
                    room.broadcast(ev);
                    // The opening lead reveals dummy to everyone — each
                    // connection rebuilds its own redacted snapshot.
                    if was_opening_lead {
                        room.broadcast_resync();
                    }
                    if let Some(complete) = complete {
                        room.broadcast(complete);
                    }
                    room.notify_session_lobby();
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
                    room.notify_session_lobby();
                    crate::bots::kick(room.clone());
                    None
                }
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        Some("ready_next_board") => {
            let Some(seat) = seat else {
                return Some(err_msg("not_seated", "kibitzers cannot ready up"));
            };
            let Some(session) = session else {
                return Some(err_msg("no_session", "the demo table has no board rounds"));
            };
            let ev = {
                let mut inner = room.state.lock().await;
                inner.ready.insert(seat);
                json!({
                    "t": "event",
                    "table_id": room.id,
                    "seq": inner.table.seq(),
                    "kind": "ready_update",
                    "ready": inner
                        .ready
                        .iter()
                        .map(|d| d.to_char().to_string())
                        .collect::<Vec<_>>(),
                })
                .to_string()
            };
            room.broadcast(ev);
            session.try_advance(room, false).await;
            session.notify_lobby();
            None
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

    #[test]
    fn board_complete_msg_flattens_the_result() {
        use crate::table::{Action, TableState};
        use bridge_types::Direction::*;
        let room = crate::rooms::Room::new_for_test("t1");
        let mut inner = room.state.try_lock().unwrap();
        assert!(board_complete_msg("t1", &inner).is_none(), "in progress");
        // Pass the demo board out (dealer N).
        for seat in [North, East, South, West] {
            inner
                .table
                .apply(Action::Call {
                    seat,
                    call: Call::Pass,
                })
                .unwrap();
        }
        let msg = board_complete_msg("t1", &inner).unwrap();
        let v: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["t"], "event");
        assert_eq!(v["kind"], "board_complete");
        assert_eq!(v["board_no"], 1);
        assert_eq!(v["passed_out"], true);
        assert!(v["contract"].is_null());
        // Fresh state for the next board would come via board_advanced.
        let _ = TableState::new(inner.table.board.clone());
    }
}
