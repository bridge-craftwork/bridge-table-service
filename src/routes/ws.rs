//! The WebSocket endpoint — all game traffic flows through here.
//!
//! Protocol (JSON, one message per frame):
//!   client → server: {"t":"hello","ticket":"…"} must be the FIRST message
//!                    (the ticket travels in-band, not in the URL, so it
//!                    stays out of proxy logs; optional "bot":"random" or
//!                    "bot":"rules" switches the table to RandomLegal or
//!                    bridge-rulebot bots for testing — the welcome echoes
//!                    "bot_mode":"real"|"random"|"rules"; optional
//!                    "as_player":true lets the session owner join SEATED
//!                    (host-as-player) instead of the see-all controller,
//!                    keeping the host-control frames from their seat), then
//!                    {"t":"bid","call":"1H"} | {"t":"play","card":"SA"} |
//!                    {"t":"undo","to_seq":N} | {"t":"ready_next_board"} |
//!                    {"t":"pong"};
//!                    host only: {"t":"open_boards","count":N} |
//!                    seat/token-addressed seat control (friends-table model):
//!                    {"t":"assign_seat","table"?:ID,"seat":"S"|null,
//!                        "from"?:"N","token"?:TOKEN}  (move | vacate | place) |
//!                    {"t":"boot","table"?:ID,"seat":"S"}  (vacate alias) |
//!                    {"t":"force_advance","table":ID} |
//!                    {"t":"kibitz","table":ID} |
//!                    runtime deal source + lockstep board nav (§Phase 3.1):
//!                    {"t":"load_boards","boards_pbn":"…","label":"…"} |
//!                    {"t":"goto_board","index":N} (1-based) |
//!                    {"t":"next_board"} | {"t":"prev_board"} |
//!                    dynamic tables + lobby (§Phase 3.2):
//!                    {"t":"seat_students"} | {"t":"wait_to_seat","on":bool} |
//!                    {"t":"add_tables","count":N} |
//!                    {"t":"set_seat_policy","policy":"first_free|one_per_south|
//!                        two_per_ns|pairs_ns|manual"} |
//!                    {"t":"set_bot_mode","mode":"rules|real|random|slow"} |
//!                    {"t":"pause_bots","on":bool}
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

/// A random opaque per-connection token (unrelated to the ticket `sub`,
/// which the client never receives). Keyed into the session roster.
fn new_token() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// The snapshot frame: FULL (un-redacted) table state — every viewer gets
/// all four hands, the client redacts locally — plus the per-table seats map
/// and the session `roster`, so a (re)joining client learns the whole
/// arrangement without waiting for the next seat_update/roster_update event.
/// `roster` is computed by the caller (session-wide; must not be built while
/// holding a room lock). Pass `&json!([])` for the demo room.
fn snapshot_msg(room_id: &str, inner: &crate::rooms::RoomInner, roster: &Value) -> String {
    json!({
        "t": "snapshot",
        "table_id": room_id,
        "state": inner
            .table
            .snapshot(inner.board_mode == crate::rooms::BoardMode::BidOnly),
        "board_mode": inner.board_mode.as_str(),
        "seats": inner.seats_json(),
        "roster": roster,
    })
    .to_string()
}

/// Broadcast the session-wide `roster_update` event to every table (all
/// seated players and kibitzing hosts receive it; unseated hosts get the
/// roster via their lobby frame). Computes the roster once.
async fn broadcast_roster(session: &Session) {
    let roster = session.roster_json().await;
    let frame = json!({
        "t": "event",
        "kind": "roster_update",
        "roster": roster,
    })
    .to_string();
    for room in session.rooms_snapshot().await {
        room.broadcast(frame.clone());
    }
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
    inner.result_json().map(|result| {
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
        let is_controller = hello.ticket.sub == session.owner_sub || hello.ticket.role == "teacher";
        // The owner/teacher normally joins as the see-all controller. `as_player`
        // lets them instead take a seat and play (host-as-player) while still
        // holding the host control frames (dispatched in handle_player).
        if is_controller && !hello.as_player {
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

async fn handle_player(socket: WebSocket, state: SharedState, session: Arc<Session>, hello: Hello) {
    let ticket = hello.ticket;
    let token = new_token();
    let is_host = ticket.sub == session.owner_sub || ticket.role == "teacher";
    let placement = session
        .place_seat(&ticket.sub, &ticket.name, hello.requested_seat)
        .await;
    let mut room: Arc<Room> = placement.room;
    // A connection may hold MULTIPLE seats (host multi-seat). Arrival binds
    // at most one; RECHECK rebinding recomputes the full set.
    let mut seats: Vec<Direction> = placement.seat.into_iter().collect();

    // Register this connection so it appears in the roster and the host can
    // address it by token (place/Sit).
    session
        .register_conn(&token, &ticket.sub, &ticket.name)
        .await;

    if let Some(mode) = hello.bot_override {
        room.set_bot_mode(mode);
        tracing::info!(event = "bot_mode_set", mode = mode.as_str(), room = %room.id, sub = %ticket.sub, "");
    }

    // Deck status + roster fetched before the room lock (they lock the deck /
    // other rooms; no nested locks while we hold this room).
    let status = session.deck_status().await;
    let roster = session.roster_json().await;
    let (welcome, snapshot, seats_ev) = {
        let inner = room.state.lock().await;
        (
            welcome_msg(
                &session, &room, &ticket, is_host, &token, &seats, &roster, &status,
            ),
            snapshot_msg(&room.id, &inner, &roster),
            seats_event(&room, &inner),
        )
    };

    let _ = crate::observability::events::record_event(
        &state.db,
        "ws_joined",
        json!({
            "sub": ticket.sub,
            "seat": seats.first().map(|s| s.to_char().to_string()),
            "room": room.id,
            "session": session.id,
        }),
    )
    .await;
    room.broadcast(seats_ev);
    broadcast_roster(&session).await;
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
                        let roster = session.roster_json().await;
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, &roster);
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
                        let roster = session.roster_json().await;
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, &roster);
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
                        // The host reseated someone — possibly us. Re-resolve
                        // our table + full seat set (a connection may hold
                        // several seats now).
                        match resolve_seats(&session, &ticket, &mut room, &mut seats, &mut room_events).await {
                            Rebind::Unchanged => {}
                            Rebind::Changed => {
                                let status = session.deck_status().await;
                                let roster = session.roster_json().await;
                                let inner = room.state.lock().await;
                                let welcome = welcome_msg(&session, &room, &ticket, is_host, &token, &seats, &roster, &status);
                                let snap = snapshot_msg(&room.id, &inner, &roster);
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
                // A seated HOST (owner or teacher-role): bid/play/undo run
                // through their seat(s), but host control frames (deal source,
                // seating, bots) dispatch to the same handler the console uses.
                // A seated host isn't kibitzing, so pass no kibitz room.
                let reply = if is_host && is_host_control(&v) {
                    handle_teacher_msg(&session, None, &ticket, &v).await
                } else {
                    handle_client_msg(&room, Some(&session), &ticket, &seats, &v).await
                };
                if let Some(reply) = reply {
                    if tx.send(Message::Text(reply)).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    // Socket gone. A seated player keeps their seat (zombie-seat policy:
    // reconnect rebinds; bots take over after the grace period). A waiter is
    // simply forgotten. Either way the connection leaves the roster's active
    // set (seated → greyed via connected=false; waiter → removed).
    if session.find_seated(&ticket.sub).await.is_some() {
        session.mark_conn_disconnected(&token).await;
        let mut inner = room.state.lock().await;
        inner.mark_disconnected(&ticket.sub);
        let ev = seats_event(&room, &inner);
        drop(inner);
        room.broadcast(ev);
    } else {
        session.remove_conn(&token).await;
        session.remove_kibitzer(&ticket.sub).await;
    }
    broadcast_roster(&session).await;
    session.notify_lobby();
    tracing::info!(event = "ws_left", sub = %ticket.sub, room = %room.id, session = %session.id, "");
}

/// `status` is the session's `deck_status()` trio (label, current 1-based
/// board, total) — passed in (not read here) so the caller can fetch it
/// before locking the room, keeping the no-nested-locks rule. `total == 0`
/// means no deal is loaded yet → the client shows a "waiting" overlay.
#[allow(clippy::too_many_arguments)]
fn welcome_msg(
    session: &Session,
    room: &Room,
    ticket: &Ticket,
    is_host: bool,
    token: &str,
    your_seats: &[Direction],
    roster: &Value,
    status: &(Option<String>, usize, usize),
) -> String {
    let bot_mode = room.bot_mode().as_str();
    let (label, current, total) = status;
    let seats: Vec<String> = your_seats.iter().map(|s| s.to_char().to_string()).collect();
    json!({
        "t": "welcome",
        "session_id": session.id,
        "table_id": room.id,
        "name": ticket.name,
        "role": ticket.role,
        // Every seat this connection occupies (may be several — multi-seat).
        "your_seats": seats,
        // Back-compat single seat = the first of your_seats (or null).
        "seat": seats.first(),
        // Host = session owner or any teacher-role ticket (drives seating).
        "is_host": is_host,
        // Opaque per-connection token (the client never sees `sub`). The host
        // addresses this connection by token in assign_seat.
        "token": token,
        // Shared roster (also on every snapshot + roster_update).
        "roster": roster,
        "bot_mode": bot_mode,
        // Deal-set status so the client renders the board or a "waiting for
        // the teacher to load a deal" overlay (roadmap §Phase 3.1).
        "loaded": total > &0,
        "set_label": label,
        "board": current,
        "board_total": total,
    })
    .to_string()
}

enum Rebind {
    Unchanged,
    Changed,
}

/// Re-resolve this connection's table + full seat set after a host reseat.
/// A connection may now hold MULTIPLE seats; a vacated connection becomes a
/// parked waiter of the table it watched. When the connection is seated in a
/// different room, rebind to it (single-table friends sessions never hit that
/// branch).
async fn resolve_seats(
    session: &Arc<Session>,
    ticket: &Ticket,
    room: &mut Arc<Room>,
    seats: &mut Vec<Direction>,
    room_events: &mut tokio::sync::broadcast::Receiver<String>,
) -> Rebind {
    let all = session.find_all_seated(&ticket.sub).await;
    if all.is_empty() {
        if seats.is_empty() {
            return Rebind::Unchanged; // still the same waiter
        }
        // Vacated: assign_seat already parked us watching this table; just
        // drop our seat view (keep the current room).
        seats.clear();
        return Rebind::Changed;
    }
    // Bind to the room holding (the first of) our seats; collect every seat we
    // hold there.
    let target = all[0].0.clone();
    let mut new_seats: Vec<Direction> = all
        .iter()
        .filter(|(r, _)| r.id == target.id)
        .map(|(_, s)| *s)
        .collect();
    new_seats.sort_by_key(|d| d.to_char());
    let mut cur = seats.clone();
    cur.sort_by_key(|d| d.to_char());
    if target.id == room.id && cur == new_seats {
        return Rebind::Unchanged;
    }
    if target.id != room.id {
        *room = target.clone();
        *room_events = room.events.subscribe();
    }
    *seats = new_seats;
    session.remove_kibitzer(&ticket.sub).await;
    Rebind::Changed
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
    let token = new_token();
    if let Some(mode) = hello.bot_override {
        // Session-level so every table (and future ones) uses it.
        session.set_bot_mode(mode).await;
        tracing::info!(event = "bot_mode_set", mode = mode.as_str(), session = %session.id, sub = %ticket.sub, "");
    }

    // The host is a live connection in the roster too (unseated → empty
    // seats), so it can Sit itself by its own token.
    session
        .register_conn(&token, &ticket.sub, &ticket.name)
        .await;
    let roster = session.roster_json().await;
    let welcome = json!({
        "t": "welcome",
        "session_id": session.id,
        "name": ticket.name,
        "role": "teacher",
        "seat": null,
        "your_seats": [],
        "is_host": true,
        "token": token,
        "roster": roster,
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
    for room in session.rooms_snapshot().await {
        crate::bots::ensure_keepalive(room.clone());
    }
    broadcast_roster(&session).await;

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
                        let roster = session.roster_json().await;
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, &roster);
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
                        let roster = session.roster_json().await;
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, &roster);
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
                        let Some(target) = session.room_by_id(table_id).await else {
                            let _ = tx.send(Message::Text(err_msg("bad_table", "unknown table"))).await;
                            continue;
                        };
                        kibitz_events = Some(target.events.subscribe());
                        kibitz = Some(target.clone());
                        let roster = session.roster_json().await;
                        let inner = target.state.lock().await;
                        let snap = snapshot_msg(&target.id, &inner, &roster);
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
    // Host connection gone: drop it from the roster.
    session.remove_conn(&token).await;
    broadcast_roster(&session).await;
    session.notify_lobby();
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
/// The session-control frames a seated host (owner) may send in addition to the
/// player frames (bid/play/undo/pong) — the same ones the console uses. Anything
/// not listed here falls through to the normal player message handler.
fn is_host_control(v: &Value) -> bool {
    matches!(
        v["t"].as_str(),
        Some("load_boards")
            | Some("goto_board")
            | Some("next_board")
            | Some("prev_board")
            | Some("open_boards")
            | Some("force_advance")
            | Some("seat_students")
            | Some("wait_to_seat")
            | Some("add_tables")
            | Some("set_seat_policy")
            | Some("set_bot_mode")
            | Some("pause_bots")
            | Some("assign_seat")
            | Some("boot")
    )
}

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
            let open = session.open_boards_to(count as usize).await;
            let total = session.deck_status().await.2;
            // Tell every table (players see "next board available"), then
            // move any table whose humans are already all ready.
            let rooms = session.rooms_snapshot().await;
            for room in &rooms {
                room.broadcast(
                    json!({
                        "t": "event",
                        "table_id": room.id,
                        "kind": "boards_open",
                        "open": open,
                        "total": total,
                    })
                    .to_string(),
                );
            }
            for room in &rooms {
                session.try_advance(room, false).await;
            }
            session.notify_lobby();
            None
        }
        // ---- Runtime deal source + lockstep board navigation (Shark model,
        // roadmap §Phase 3.1). All force EVERY table together. ----
        Some("load_boards") => {
            let Some(pbn) = v["boards_pbn"].as_str() else {
                return Some(err_msg("bad_pbn", "load_boards.boards_pbn missing"));
            };
            let boards = match crate::sessions::parse_boards(pbn) {
                Ok(b) => b,
                Err(e) => return Some(err_msg("bad_pbn", &e)),
            };
            let label = v["label"].as_str().map(|s| s.to_string());
            match session.load_boards(boards, label).await {
                Ok(_) => None,
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        Some("goto_board") => {
            let Some(n) = v["index"].as_u64() else {
                return Some(err_msg("bad_index", "goto_board.index missing"));
            };
            if !session.loaded().await {
                return Some(err_msg("no_set", "no deal set loaded"));
            }
            session.goto_board(n as usize).await;
            None
        }
        Some("next_board") => {
            if !session.loaded().await {
                return Some(err_msg("no_set", "no deal set loaded"));
            }
            session.next_board().await;
            None
        }
        Some("prev_board") => {
            if !session.loaded().await {
                return Some(err_msg("no_set", "no deal set loaded"));
            }
            session.prev_board().await;
            None
        }
        Some("force_advance") => {
            // A seated host may send bare `{t:"force_advance"}` — default to
            // the session's first table when none is named.
            let room = match v["table"].as_str() {
                Some(id) => session.room_by_id(id).await,
                None => session.rooms_snapshot().await.first().cloned(),
            };
            let Some(room) = room else {
                return Some(err_msg("bad_table", "force_advance.table unknown"));
            };
            if session.try_advance(&room, true).await {
                None
            } else {
                Some(err_msg("rejected", "no more boards"))
            }
        }
        // ---- Dynamic tables + lobby (roadmap §Phase 3.2) ----
        Some("seat_students") => {
            for room in session.seat_students().await {
                let inner = room.state.lock().await;
                let ev = seats_event(&room, &inner);
                drop(inner);
                room.broadcast(ev);
                crate::bots::kick(room.clone());
            }
            broadcast_roster(session).await;
            session.notify_recheck();
            session.notify_lobby();
            None
        }
        Some("wait_to_seat") => {
            session.set_wait_to_seat(v["on"].as_bool().unwrap_or(false));
            None
        }
        Some("add_tables") => {
            let count = v["count"].as_u64().unwrap_or(1).clamp(1, 20) as usize;
            session.add_tables(count).await;
            None
        }
        Some("set_seat_policy") => {
            let Some(policy) = v["policy"]
                .as_str()
                .and_then(crate::sessions::SeatPolicy::from_key)
            else {
                return Some(err_msg("bad_policy", "unknown seat policy"));
            };
            session.set_seat_policy(policy);
            None
        }
        Some("set_bot_mode") => {
            let Some(mode) = v["mode"].as_str().and_then(crate::rooms::BotMode::parse) else {
                return Some(err_msg("bad_mode", "unknown bot mode"));
            };
            session.set_bot_mode(mode).await;
            None
        }
        Some("pause_bots") => {
            session
                .set_bots_paused(v["on"].as_bool().unwrap_or(true))
                .await;
            None
        }
        // Seat/token-addressed host seat control (friends-table model):
        //   {t:"assign_seat", table?, seat:<dir|null>, from?:<dir>, token?:<str>}
        //   move : from + seat        vacate : from + seat:null
        //   place: token + seat       (Sit self = own token)
        // `boot` is a thin alias for vacate: {t:"boot", table?, seat:<dir>}.
        Some("assign_seat") | Some("boot") => {
            let parse_dir = |val: &Value| {
                val.as_str()
                    .and_then(|s| s.chars().next())
                    .and_then(Direction::from_char)
            };
            let table = v["table"].as_str();
            let (target, from, token) = if v["t"] == "boot" {
                // boot{seat} vacates that seat.
                (None, parse_dir(&v["seat"]), None)
            } else {
                (
                    parse_dir(&v["seat"]),
                    parse_dir(&v["from"]),
                    v["token"].as_str(),
                )
            };
            match session.assign_seat(table, target, from, token).await {
                Ok(changed) => {
                    for room in changed {
                        let inner = room.state.lock().await;
                        let ev = seats_event(&room, &inner);
                        drop(inner);
                        room.broadcast(ev);
                        // A vacated seat is a bot now; a filled one may
                        // unblock the table.
                        crate::bots::kick(room.clone());
                    }
                    broadcast_roster(session).await;
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
            handle_client_msg(room, Some(session), ticket, &[], v).await
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

    // Testing convenience: hello{"bot":"random"|"rules"} flips the whole
    // room to that backend (sticky; absent field leaves the mode untouched).
    if let Some(mode) = hello.bot_override {
        room.set_bot_mode(mode);
        tracing::info!(event = "bot_mode_set", mode = mode.as_str(), room = %room.id, sub = %ticket.sub, "");
    }
    let bot_mode = room.bot_mode().as_str();
    let token = new_token();
    // The demo room has no session/host; roster is empty (dev harness only).
    let demo_roster = json!([]);

    let (seat, welcome, snapshot, seats_ev) = {
        let mut inner = room.state.lock().await;
        let seat = inner.seat_or_rebind(&ticket.sub, &ticket.name);
        let your_seats: Vec<String> = seat.iter().map(|s| s.to_char().to_string()).collect();
        let welcome = json!({
            "t": "welcome",
            "table_id": room.id,
            "name": ticket.name,
            "role": ticket.role,
            "seat": your_seats.first(),
            "your_seats": your_seats,
            "is_host": ticket.role == "teacher",
            "token": token,
            "roster": demo_roster,
            "bot_mode": bot_mode,
        })
        .to_string();
        let snapshot = snapshot_msg(&room.id, &inner, &demo_roster);
        let seats_ev = seats_event(&room, &inner);
        (seat, welcome, snapshot, seats_ev)
    };
    // Demo tracks a single seat as a one-element slice for handle_client_msg.
    let seats: Vec<Direction> = seat.into_iter().collect();

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
                    // each connection rebuilds its own snapshot frame here.
                    Ok(msg) if msg == RESYNC => {
                        let inner = room.state.lock().await;
                        let snap = snapshot_msg(&room.id, &inner, &demo_roster);
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
                        let snap = snapshot_msg(&room.id, &inner, &demo_roster);
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
                if let Some(reply) = handle_client_msg(&room, None, &ticket, &seats, &v).await {
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
    /// Set iff the hello carried `"bot":"random"` or `"bot":"rules"`
    /// (testing convenience: switches the table's bot backend, skipping
    /// BBA/BEN — see `rooms::BotMode`).
    bot_override: Option<crate::rooms::BotMode>,
    /// Set iff the hello carried `"as_player": true`. Lets the session OWNER
    /// (or a teacher-role ticket) join as a seated player — host-as-player —
    /// instead of the see-all controller. A seated owner still holds the host
    /// control frames from their seat (see handle_player).
    as_player: bool,
    /// Optional `"seat":"N|E|S|W"` — the `?seat=` invite link. Under Manual
    /// policy a free requested seat is taken directly instead of waiting.
    requested_seat: Option<Direction>,
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
            bot_override: v["bot"].as_str().and_then(crate::rooms::BotMode::parse),
            as_player: v["as_player"].as_bool().unwrap_or(false),
            requested_seat: v["seat"]
                .as_str()
                .and_then(|s| s.chars().next())
                .and_then(Direction::from_char),
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
    seats: &[Direction],
    v: &Value,
) -> Option<String> {
    match v["t"].as_str() {
        Some("pong") => None,
        // Host-only frames are routed away for hosts; if a non-host sends one
        // it lands here → refuse.
        Some("assign_seat") | Some("boot") | Some("force_advance") => {
            Some(err_msg("not_host", "seat management is host-only"))
        }
        Some("bid") => {
            if seats.is_empty() {
                return Some(err_msg("not_seated", "kibitzers cannot bid"));
            }
            let Some(call) = v["call"].as_str().and_then(Call::from_pbn) else {
                return Some(err_msg("bad_call", "unrecognized call"));
            };
            let mut inner = room.state.lock().await;
            if inner.board_mode == crate::rooms::BoardMode::PlayOnly {
                return Some(err_msg(
                    "play_only",
                    "the auction is automatic on this board",
                ));
            }
            // Membership turn-auth: bid as whichever of our seats is on turn.
            let Some(turn_seat) = inner.table.fold().next_to_act else {
                return Some(err_msg("rejected", "the auction is over"));
            };
            if !seats.contains(&turn_seat) {
                return Some(err_msg("rejected", "not your turn to call"));
            }
            match inner.table.apply(Action::Call {
                seat: turn_seat,
                call: call.clone(),
            }) {
                Ok(seq) => {
                    let ev = json!({
                        "t": "event",
                        "table_id": room.id,
                        "seq": seq,
                        "kind": "bid_made",
                        "seat": turn_seat.to_char().to_string(),
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
                        // A finished board reveals the original deal for review
                        // (snapshot shows the complete state) — push a fresh
                        // snapshot to everyone.
                        room.broadcast_resync();
                    }
                    room.notify_session_lobby();
                    crate::bots::kick(room.clone());
                    None
                }
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        Some("play") => {
            if seats.is_empty() {
                return Some(err_msg("not_seated", "kibitzers cannot play"));
            }
            let Some(card) = v["card"].as_str().and_then(parse_card) else {
                return Some(err_msg("bad_card", "unrecognized card"));
            };
            let mut inner = room.state.lock().await;
            if inner.board_mode == crate::rooms::BoardMode::BidOnly {
                return Some(err_msg(
                    "bid_only",
                    "this board ends at the contract — no cardplay",
                ));
            }
            // Server-authoritative seat resolution: a play always targets the
            // seat on turn. The sender is entitled if they occupy that seat
            // (membership — multi-seat), or if they control the declarer side
            // and the seat on turn is a declarer-side seat (declarer plays
            // dummy's cards; a human dummy plays the hand for a bot declarer).
            let f = inner.table.fold();
            let Some(turn_seat) = f.next_to_act else {
                return Some(err_msg("rejected", "no one is on turn"));
            };
            let entitled = seats.contains(&turn_seat)
                || f.contract.as_ref().is_some_and(|c| {
                    (turn_seat == c.declarer || turn_seat == c.dummy())
                        && seats.contains(&inner.declarer_side_controller(
                            c.declarer,
                            true,
                            std::time::Instant::now(),
                        ))
                });
            if !entitled {
                return Some(err_msg("rejected", "not your turn"));
            }
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
                    // Note: the old opening-lead resync (dummy reveal) is gone —
                    // hands are un-redacted now, so the opening lead is an
                    // ordinary card_played with no visibility change.
                    let complete = board_complete_msg(&room.id, &inner);
                    drop(inner);
                    room.broadcast(ev);
                    if let Some(complete) = complete {
                        room.broadcast(complete);
                        // A finished board reveals the original deal for review
                        // (snapshot shows the complete state) — push a fresh
                        // snapshot to everyone.
                        room.broadcast_resync();
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
            //
            // Two shapes:
            //   {t:"undo", to_seq:N} — explicit rewind to seq N (unchanged;
            //     used by teacher/kibitz paths).
            //   {t:"undo"}           — rewind to the last human action: the
            //     state just before the most recent bid/play by a seat that a
            //     human currently occupies, so the table waits on that human
            //     instead of a bot instantly replaying a single-ply undo. Any
            //     human at the table may trigger it (table-wide, not
            //     sender-scoped).
            let mut inner = room.state.lock().await;
            let to_seq = match v["to_seq"].as_u64() {
                Some(n) => n as usize,
                None => match inner.last_human_action_seq(std::time::Instant::now()) {
                    Some(seq) => seq,
                    None => {
                        return Some(err_msg(
                            "no_human_action",
                            "no human has acted on this board yet",
                        ))
                    }
                },
            };
            match inner.table.undo_to(to_seq) {
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
                    // A rewind changes the board state (seq / auction /
                    // remaining cards), so every connection reloads a fresh
                    // snapshot.
                    room.broadcast_resync();
                    room.notify_session_lobby();
                    crate::bots::kick(room.clone());
                    None
                }
                Err(e) => Some(err_msg("rejected", &e)),
            }
        }
        Some("ready_next_board") => {
            if seats.is_empty() {
                return Some(err_msg("not_seated", "kibitzers cannot ready up"));
            }
            let Some(session) = session else {
                return Some(err_msg("no_session", "the demo table has no board rounds"));
            };
            let ev = {
                let mut inner = room.state.lock().await;
                // A connection may hold several seats — ready them all.
                for s in seats {
                    inner.ready.insert(*s);
                }
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
        Some("deal") => {
            // Deal-source controls (roadmap Phase 2). Demo room only: any
            // seated player may re-deal; session boards stay owned by the
            // teacher console's round flow.
            if seats.is_empty() {
                return Some(err_msg("not_seated", "kibitzers cannot deal"));
            }
            if session.is_some() {
                return Some(err_msg(
                    "session_table",
                    "session boards are managed by the teacher console",
                ));
            }
            let source = v["source"].as_str().unwrap_or("");
            // Script deals hit the dealer service over HTTP — resolve them
            // to PBN BEFORE taking the state lock.
            let script_pbn = if source == "script" {
                let Some(script) = v["script"].as_str() else {
                    return Some(err_msg("bad_script", "deal.script missing"));
                };
                if !crate::dealer::available() {
                    return Some(err_msg(
                        "dealer_unavailable",
                        "script deals aren't set up on this server yet",
                    ));
                }
                match crate::dealer::generate_board_pbn(script).await {
                    Ok(pbn) => Some(pbn),
                    Err(e) => {
                        tracing::warn!(event = "dealer_failed", error = %format!("{e:#}"), "");
                        return Some(err_msg("dealer_failed", &format!("{e:#}")));
                    }
                }
            } else {
                None
            };
            let ev = {
                let mut inner = room.state.lock().await;
                // Optional sticky board mode rides along with the deal.
                if let Some(m) = v["mode"].as_str().and_then(crate::rooms::BoardMode::parse) {
                    inner.board_mode = m;
                }
                match source {
                    // Same board again: the undo machinery truncated to 0.
                    "replay" => {
                        let _ = inner.table.undo_to(0);
                    }
                    "random" => {
                        let number = inner.table.board.number + 1;
                        inner.table =
                            crate::table::TableState::new(crate::rooms::random_board(number));
                    }
                    // Client-fetched deals (scenario/lesson PBNs, pasted PBN,
                    // URL-parameter deals) all come through this one door.
                    "pbn" => {
                        let Some(pbn) = v["pbn"].as_str() else {
                            return Some(err_msg("bad_pbn", "deal.pbn missing"));
                        };
                        if pbn.len() > 64 * 1024 {
                            return Some(err_msg("bad_pbn", "PBN too large"));
                        }
                        let rotate = (v["rotate"].as_u64().unwrap_or(0) % 4) as u8;
                        let number = inner.table.board.number + 1;
                        match crate::rooms::board_from_pbn(pbn, rotate, number) {
                            Ok(board) => inner.table = crate::table::TableState::new(board),
                            Err(e) => return Some(err_msg("bad_pbn", &e)),
                        }
                    }
                    // Dealer-script deals: the PBN was generated above.
                    "script" => {
                        let pbn = script_pbn.as_deref().unwrap_or("");
                        let rotate = (v["rotate"].as_u64().unwrap_or(0) % 4) as u8;
                        let number = inner.table.board.number + 1;
                        match crate::rooms::board_from_pbn(pbn, rotate, number) {
                            Ok(board) => inner.table = crate::table::TableState::new(board),
                            Err(e) => return Some(err_msg("bad_pbn", &e)),
                        }
                    }
                    _ => {
                        return Some(err_msg(
                            "bad_source",
                            "source must be random, replay, pbn, or script",
                        ))
                    }
                }
                inner.ready.clear();
                json!({
                    "t": "event",
                    "table_id": room.id,
                    "seq": 0,
                    "kind": "board_advanced",
                    "board_no": inner.table.board.number,
                    "source": source,
                    "by": ticket.name,
                })
                .to_string()
            };
            tracing::info!(event = "deal_applied", source, room = %room.id, sub = %ticket.sub, "");
            // A new (or rewound) board invalidates the BBA prediction.
            *room.bba_cache.lock().await = None;
            room.broadcast(ev);
            // Per-viewer snapshots for the fresh board.
            room.broadcast_resync();
            crate::bots::kick(room.clone());
            None
        }
        _ => Some(err_msg("unknown", "unknown message type")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_host_control_covers_session_frames_not_play_frames() {
        // A seated host (owner) may send these session-control frames…
        for t in [
            "load_boards",
            "goto_board",
            "next_board",
            "prev_board",
            "open_boards",
            "force_advance",
            "seat_students",
            "wait_to_seat",
            "add_tables",
            "set_seat_policy",
            "set_bot_mode",
            "pause_bots",
            "assign_seat",
            "boot",
        ] {
            assert!(
                is_host_control(&json!({ "t": t })),
                "{t} should be host-control"
            );
        }
        // …but play frames route through the normal seated-player path.
        for t in ["bid", "play", "undo", "pong", "kibitz", "hello", "nonsense"] {
            assert!(
                !is_host_control(&json!({ "t": t })),
                "{t} must NOT be host-control"
            );
        }
    }

    #[test]
    fn snapshot_includes_state_and_seats() {
        let room = crate::rooms::Room::new_for_test("t1");
        let mut inner = room.state.try_lock().unwrap();
        inner.seat_or_rebind("u1", "Alice");
        let msg = snapshot_msg("t1", &inner, &json!([]));
        let v: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["t"], "snapshot");
        assert_eq!(v["table_id"], "t1");
        // Un-redacted: every seat is fully visible now.
        assert_eq!(v["state"]["hands"]["S"]["visible"], true);
        assert_eq!(v["state"]["hands"]["N"]["visible"], true);
        // Seats travel with every snapshot so late joiners see who is
        // human vs bot without waiting for a seat_update event.
        assert_eq!(v["seats"]["S"]["kind"], "human");
        assert_eq!(v["seats"]["S"]["name"], "Alice");
        assert_eq!(v["seats"]["W"]["kind"], "empty");
        // And the roster rides along.
        assert!(v["roster"].is_array());
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
