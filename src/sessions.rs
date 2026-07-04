//! Multiplayer sessions: a set of tables playing the same ordered boards.
//!
//! A `Session` is created by the bridge-classroom API via
//! `POST /admin/sessions` (see routes/admin.rs) and holds N rooms
//! (`"<session>-t1"`…), the parsed board list, the seat policy, and the
//! teacher-gated round counter. Sessions are in-memory only, like rooms —
//! v1 persists nothing.
//!
//! # Seat policy (JSON, from the classroom API)
//!
//! - `{"mode":"manual"}` — arrivals kibitz (attached round-robin to a table
//!   so they see play) until the teacher `assign_seat`s them.
//! - `{"mode":"auto","pattern":"one_per_seat","seats":["S"]}` — arrivals are
//!   dealt round-robin across tables into the listed seats (first arrival →
//!   table 1's South, second → table 2's South, …); when those fill,
//!   later arrivals kibitz.
//! - `{"mode":"auto","pattern":"pairs","sides":["NS"]}` — fills a table's
//!   whole side before moving to the next table (partners sit together):
//!   S, N of table 1, then S, N of table 2, …
//! - `{"mode":"auto","pattern":"first_free"}` (the default) — first free
//!   seat (S, W, N, E) round-robin across tables.
//!
//! Empty seats are always bots. The session owner (and any teacher-role
//! ticket) is never seated by policy — they join as the see-all teacher.
//!
//! # Board flow
//!
//! `boards[0..open_boards]` are open. `teacher_set` sessions start with 1
//! open board and the teacher widens the window with `open_boards{count}`
//! (Shark-style rounds); `adhoc` sessions open everything up front so the
//! only gate is all-humans-ready. A table advances (fresh `TableState`,
//! cleared BBA cache, per-viewer snapshot resync) when every *connected
//! seated human* has sent `ready_next_board` and the next board is open —
//! or when the teacher `force_advance`s it (which skips both checks).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use bridge_types::Direction;
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex};

use crate::rooms::Room;
use crate::table::{BoardSetup, TableState};

/// Session-channel marker: lobby-relevant state changed; teacher
/// connections rebuild and resend their lobby frame. Never sent to clients.
pub const LOBBY: &str = "__lobby__";
/// Session-channel marker: seat assignments changed (teacher assign/boot);
/// every player connection re-resolves which room/seat it belongs to.
/// Never sent to clients.
pub const RECHECK: &str = "__recheck__";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionKind {
    /// Teacher-run class set: rounds are teacher-gated (starts with board 1
    /// open).
    TeacherSet,
    /// Friends' table(s): every board is open; tables advance on all-ready.
    Adhoc,
}

impl SessionKind {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "teacher_set" => Ok(SessionKind::TeacherSet),
            "adhoc" => Ok(SessionKind::Adhoc),
            other => Err(format!("unknown session kind '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    NS,
    EW,
}

impl Side {
    /// Seat fill order within the side (South/West first — the "primary"
    /// student seats).
    fn seats(self) -> [Direction; 2] {
        match self {
            Side::NS => [Direction::South, Direction::North],
            Side::EW => [Direction::West, Direction::East],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SeatPolicy {
    Manual,
    OnePerSeat(Vec<Direction>),
    Pairs(Vec<Side>),
    FirstFree,
}

impl SeatPolicy {
    /// Parse the seat-policy JSON documented in the module header.
    /// Missing/empty → the first-free default.
    pub fn from_value(v: &Value) -> Result<Self, String> {
        if v.is_null() {
            return Ok(SeatPolicy::FirstFree);
        }
        let mode = v["mode"].as_str().unwrap_or("auto");
        match mode {
            "manual" => Ok(SeatPolicy::Manual),
            "auto" => match v["pattern"].as_str().unwrap_or("first_free") {
                "first_free" => Ok(SeatPolicy::FirstFree),
                "one_per_seat" => {
                    let seats = v["seats"]
                        .as_array()
                        .ok_or("one_per_seat requires seats:[...]")?
                        .iter()
                        .map(|s| {
                            s.as_str()
                                .and_then(|s| s.chars().next())
                                .and_then(Direction::from_char)
                                .ok_or_else(|| format!("bad seat {s}"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if seats.is_empty() {
                        return Err("one_per_seat requires at least one seat".into());
                    }
                    Ok(SeatPolicy::OnePerSeat(seats))
                }
                "pairs" => {
                    let sides = v["sides"]
                        .as_array()
                        .ok_or("pairs requires sides:[...]")?
                        .iter()
                        .map(|s| match s.as_str() {
                            Some("NS") => Ok(Side::NS),
                            Some("EW") => Ok(Side::EW),
                            other => Err(format!("bad side {other:?}")),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if sides.is_empty() {
                        return Err("pairs requires at least one side".into());
                    }
                    Ok(SeatPolicy::Pairs(sides))
                }
                other => Err(format!("unknown seat pattern '{other}'")),
            },
            other => Err(format!("unknown seat mode '{other}'")),
        }
    }
}

/// Parse raw PBN text into the session's ordered board list.
/// Boards without a `[Board]` number get 1-based sequence numbers; boards
/// without a dealer get the standard rotation (board 1 → N, 2 → E, …).
pub fn parse_boards(pbn: &str) -> Result<Vec<BoardSetup>, String> {
    let boards = bridge_encodings::pbn::read_pbn(pbn).map_err(|e| format!("bad PBN: {e}"))?;
    let mut out = Vec::with_capacity(boards.len());
    for (i, b) in boards.iter().enumerate() {
        if b.deal.total_cards() != 52 {
            return Err(format!(
                "board {} has {} cards (need 52)",
                i + 1,
                b.deal.total_cards()
            ));
        }
        let number = b.number.unwrap_or(i as u32 + 1);
        let dealer = b.dealer.unwrap_or_else(|| standard_dealer(number));
        out.push(BoardSetup {
            number,
            dealer,
            vulnerable: b.vulnerable,
            deal: b.deal.clone(),
        });
    }
    if out.is_empty() {
        return Err("no boards in PBN".into());
    }
    Ok(out)
}

/// Standard duplicate dealer rotation: board 1 → N, 2 → E, 3 → S, 4 → W, …
fn standard_dealer(board_number: u32) -> Direction {
    match (board_number.wrapping_sub(1)) % 4 {
        0 => Direction::North,
        1 => Direction::East,
        2 => Direction::South,
        _ => Direction::West,
    }
}

/// A kibitzer known to the session: connected but unseated (manual policy,
/// or all policy seats taken). Tracked so the teacher's lobby lists them
/// and `assign_seat` can find them by sub.
#[derive(Debug, Clone)]
pub struct Kibitzer {
    pub name: String,
    /// Which table their connection is attached to (they watch this one).
    pub room_idx: usize,
}

/// The session's loaded board set. `index` is the lockstep teacher pointer
/// (0-based current board, forced onto every table by `deal_index_to_all`).
/// An empty `boards` = idle: no deal loaded yet, rooms hold a throwaway
/// placeholder board and bots stay off until `load_boards`.
pub struct Deck {
    pub boards: Arc<Vec<BoardSetup>>,
    /// Brief human-readable description of the set (picker-generated), for
    /// the teacher console + server logs. None while idle.
    pub label: Option<String>,
    /// Lockstep teacher pointer: 0-based index of the board all teacher_set
    /// tables are on. Reset to 0 on load.
    pub index: usize,
}

pub struct Session {
    pub id: String,
    pub kind: SessionKind,
    pub owner_sub: String,
    pub seat_policy: SeatPolicy,
    /// The loaded set + label + lockstep pointer. Swappable at runtime
    /// (`load_boards`); empty = idle. Never held across a room lock.
    pub deck: Mutex<Deck>,
    pub rooms: Vec<Arc<Room>>,
    /// Adhoc open-board window (all boards open once a set is loaded).
    /// teacher_set ignores it — it's teacher-driven lockstep (`deal_index_to_all`).
    pub open_boards: AtomicUsize,
    /// Session-level notifications (LOBBY / RECHECK markers). Subscribed by
    /// every connection in the session; players ignore LOBBY, teachers
    /// rebuild their lobby frame on anything.
    pub notify: broadcast::Sender<String>,
    /// Round-robin cursor distributing arrivals across tables.
    next_arrival: AtomicUsize,
    pub kibitzers: Mutex<HashMap<String, Kibitzer>>,
}

/// Where an arriving connection ends up.
pub struct Placement {
    pub room_idx: usize,
    /// None → kibitzer (manual policy or no policy seat free).
    pub seat: Option<Direction>,
}

impl Session {
    pub fn new(
        id: String,
        kind: SessionKind,
        owner_sub: String,
        seat_policy: SeatPolicy,
        boards: Vec<BoardSetup>,
        table_count: usize,
    ) -> Arc<Self> {
        let open = match kind {
            SessionKind::TeacherSet => boards.len().min(1),
            SessionKind::Adhoc => boards.len(),
        };
        // Idle sessions (no boards yet) seed rooms with a throwaway placeholder
        // so `TableState` stays valid; `loaded()` is false and bots stay off
        // until a set is loaded, and the client shows a "waiting" overlay.
        let seed = boards
            .first()
            .cloned()
            .unwrap_or_else(crate::rooms::placeholder_board);
        let (notify, _) = broadcast::channel(64);
        Arc::new_cyclic(|weak: &Weak<Session>| {
            let rooms = (1..=table_count)
                .map(|i| Room::new(format!("{id}-t{i}"), seed.clone(), Some(weak.clone())))
                .collect();
            Session {
                id,
                kind,
                owner_sub,
                seat_policy,
                deck: Mutex::new(Deck {
                    boards: Arc::new(boards),
                    label: None,
                    index: 0,
                }),
                rooms,
                open_boards: AtomicUsize::new(open),
                notify,
                next_arrival: AtomicUsize::new(0),
                kibitzers: Mutex::new(HashMap::new()),
            }
        })
    }

    pub fn notify_lobby(&self) {
        let _ = self.notify.send(LOBBY.to_string());
    }

    pub fn notify_recheck(&self) {
        let _ = self.notify.send(RECHECK.to_string());
    }

    pub fn room_by_id(&self, table_id: &str) -> Option<&Arc<Room>> {
        self.rooms.iter().find(|r| r.id == table_id)
    }

    /// Find the room+seat currently bound to `sub`, if any. Locks rooms one
    /// at a time (no lock is ever held across another lock in this module).
    pub async fn find_seated(&self, sub: &str) -> Option<(usize, Direction)> {
        for (idx, room) in self.rooms.iter().enumerate() {
            let inner = room.state.lock().await;
            if let Some((&seat, _)) = inner.seats.iter().find(|(_, o)| o.sub == sub) {
                return Some((idx, seat));
            }
        }
        None
    }

    /// Place an arriving participant per the seat policy. Reconnects rebind
    /// their existing seat (and mark it connected); returning kibitzers keep
    /// their table. New arrivals get a seat per the policy or kibitz.
    pub async fn place(&self, sub: &str, name: &str) -> Placement {
        // Reconnect: an existing seat always wins.
        for (idx, room) in self.rooms.iter().enumerate() {
            let mut inner = room.state.lock().await;
            if let Some((&seat, _)) = inner.seats.iter().find(|(_, o)| o.sub == sub) {
                let occ = inner.seats.get_mut(&seat).unwrap();
                occ.connected = true;
                occ.disconnected_at = None;
                occ.name = name.to_string();
                return Placement {
                    room_idx: idx,
                    seat: Some(seat),
                };
            }
        }
        // Returning kibitzer keeps their table.
        {
            let mut kib = self.kibitzers.lock().await;
            if let Some(k) = kib.get_mut(sub) {
                k.name = name.to_string();
                let room_idx = k.room_idx;
                return Placement {
                    room_idx,
                    seat: None,
                };
            }
        }

        let placement = match &self.seat_policy {
            SeatPolicy::Manual => None,
            SeatPolicy::OnePerSeat(seats) => {
                let start = self.next_arrival.fetch_add(1, Ordering::SeqCst);
                self.seat_round_robin(start, seats, sub, name).await
            }
            SeatPolicy::Pairs(sides) => {
                // Fill a table's whole side before the next table, so
                // partners land together.
                let mut found = None;
                'outer: for (idx, room) in self.rooms.iter().enumerate() {
                    for side in sides {
                        for seat in side.seats() {
                            let mut inner = room.state.lock().await;
                            if inner.try_seat(seat, sub, name) {
                                found = Some((idx, seat));
                                break 'outer;
                            }
                        }
                    }
                }
                found
            }
            SeatPolicy::FirstFree => {
                // South first — the traditional student seat (matches the
                // demo room's seat_or_rebind order).
                const FILL_ORDER: [Direction; 4] = [
                    Direction::South,
                    Direction::West,
                    Direction::North,
                    Direction::East,
                ];
                let start = self.next_arrival.fetch_add(1, Ordering::SeqCst);
                self.seat_round_robin(start, &FILL_ORDER, sub, name).await
            }
        };

        match placement {
            Some((room_idx, seat)) => Placement {
                room_idx,
                seat: Some(seat),
            },
            None => {
                // Kibitz: attach round-robin so viewers spread out.
                let room_idx = self.next_arrival.fetch_add(1, Ordering::SeqCst) % self.rooms.len();
                self.kibitzers.lock().await.insert(
                    sub.to_string(),
                    Kibitzer {
                        name: name.to_string(),
                        room_idx,
                    },
                );
                Placement {
                    room_idx,
                    seat: None,
                }
            }
        }
    }

    /// Try tables starting at `start % n`, taking the first free seat from
    /// `seats` (in the given order) at each.
    async fn seat_round_robin(
        &self,
        start: usize,
        seats: &[Direction],
        sub: &str,
        name: &str,
    ) -> Option<(usize, Direction)> {
        let n = self.rooms.len();
        for offset in 0..n {
            let idx = (start + offset) % n;
            let mut inner = self.rooms[idx].state.lock().await;
            for &seat in seats {
                if inner.try_seat(seat, sub, name) {
                    return Some((idx, seat));
                }
            }
        }
        None
    }

    /// A kibitzer disconnected: forget them (a rejoin re-places them).
    pub async fn remove_kibitzer(&self, sub: &str) {
        self.kibitzers.lock().await.remove(sub);
    }

    /// Register `sub` as a kibitzer of table `room_idx` (booted players
    /// keep watching the table they were removed from).
    pub async fn add_kibitzer(&self, sub: &str, name: &str, room_idx: usize) {
        self.kibitzers.lock().await.insert(
            sub.to_string(),
            Kibitzer {
                name: name.to_string(),
                room_idx,
            },
        );
    }

    /// Teacher seat control. `sub = None` vacates the seat (boot); a booted
    /// connected human becomes a kibitzer of the same table. `sub = Some`
    /// moves a known participant (seated anywhere in the session, or
    /// kibitzing) into the seat, preserving their connected state.
    /// Returns the indices of the rooms whose seats changed (for
    /// seat_update broadcasts); the caller sends RECHECK + lobby refresh.
    pub async fn assign_seat(
        &self,
        table_id: &str,
        seat: Direction,
        sub: Option<&str>,
    ) -> Result<Vec<usize>, String> {
        let Some(target_idx) = self.rooms.iter().position(|r| r.id == table_id) else {
            return Err(format!("unknown table {table_id}"));
        };
        let target = &self.rooms[target_idx];

        let Some(sub) = sub else {
            // Vacate (boot). The seat becomes a bot; a booted live human
            // keeps kibitzing this table.
            let removed = target.state.lock().await.vacate(seat);
            let Some(occ) = removed else {
                return Ok(vec![]); // already empty — nothing changed
            };
            if occ.connected {
                self.add_kibitzer(&occ.sub, &occ.name, target_idx).await;
            }
            return Ok(vec![target_idx]);
        };

        // Fast pre-check so we don't yank someone out of their seat only to
        // find the target taken (the re-check at insert handles races).
        if target.state.lock().await.seats.contains_key(&seat) {
            return Err("seat is occupied — boot it first".into());
        }

        // Pull the participant out of wherever they are.
        let (occ, mut changed) = if let Some((old_idx, old_seat)) = self.find_seated(sub).await {
            match self.rooms[old_idx].state.lock().await.vacate(old_seat) {
                Some(occ) => (occ, vec![old_idx]),
                None => return Err(format!("participant {sub} moved concurrently")),
            }
        } else if let Some(k) = self.kibitzers.lock().await.remove(sub) {
            (
                crate::rooms::Occupant {
                    sub: sub.to_string(),
                    name: k.name,
                    connected: true,
                    disconnected_at: None,
                },
                vec![],
            )
        } else {
            return Err(format!("unknown participant {sub}"));
        };

        let mut inner = target.state.lock().await;
        if inner.try_place(seat, occ.clone()) {
            drop(inner);
            changed.push(target_idx);
            Ok(changed)
        } else {
            // Lost the race for the seat: park them as a kibitzer so they
            // aren't dropped from the session.
            drop(inner);
            self.add_kibitzer(&occ.sub, &occ.name, target_idx).await;
            Err("seat is occupied — boot it first".into())
        }
    }

    /// Widen the open-board window to `count` (absolute, clamped to the
    /// board list, never narrowing). Returns the new count. Adhoc only —
    /// teacher_set is lockstep. `count` is clamped to the loaded set size.
    pub async fn open_boards_to(&self, count: usize) -> usize {
        let total = self.deck.lock().await.boards.len();
        let clamped = count.min(total);
        self.open_boards
            .fetch_max(clamped, Ordering::SeqCst)
            .max(clamped)
    }

    /// Advance `room` to its next board if allowed (see module docs), or
    /// unconditionally when `force`. Returns true if the board changed.
    /// **teacher_set never self-advances** — it's teacher-driven lockstep
    /// (`deal_index_to_all`); the auto path here is the adhoc round flow.
    /// On advance: fresh TableState, cleared ready set + BBA cache,
    /// `board_advanced` event + per-viewer snapshot resync, lobby refresh,
    /// bot kick.
    pub async fn try_advance(&self, room: &Arc<Room>, force: bool) -> bool {
        // Snapshot the loaded set (cheap Arc clone) so we never hold the deck
        // lock across a room lock — keeps the module's no-nested-locks rule.
        let boards = self.deck.lock().await.boards.clone();
        let event = {
            let mut inner = room.state.lock().await;
            let next = inner.board_index + 1;
            if next >= boards.len() {
                return false;
            }
            if !force {
                // Lockstep: teacher_set tables move only on a teacher command.
                if self.kind == SessionKind::TeacherSet {
                    return false;
                }
                if next >= self.open_boards.load(Ordering::SeqCst) {
                    return false; // waiting for the teacher's next round
                }
                // Every connected seated human must be ready — and there
                // must be at least one (an all-bot table only force-advances).
                let humans: Vec<Direction> = inner
                    .seats
                    .iter()
                    .filter(|(_, o)| o.connected)
                    .map(|(&s, _)| s)
                    .collect();
                if humans.is_empty() || !humans.iter().all(|s| inner.ready.contains(s)) {
                    return false;
                }
            }
            inner.board_index = next;
            inner.table = TableState::new(boards[next].clone());
            inner.ready.clear();
            json!({
                "t": "event",
                "table_id": room.id,
                "seq": 0,
                "kind": "board_advanced",
                "board_no": boards[next].number,
                "board_index": next,
                "forced": force,
            })
            .to_string()
        };
        // A new board invalidates the BBA auction prediction.
        *room.bba_cache.lock().await = None;
        room.broadcast(event);
        // Snapshots are per-viewer; everyone rebuilds for the new board.
        room.broadcast_resync();
        self.notify_lobby();
        crate::bots::kick(room.clone());
        true
    }

    /// True once a real board set is loaded (idle sessions have none).
    pub async fn loaded(&self) -> bool {
        !self.deck.lock().await.boards.is_empty()
    }

    /// Reconnect/status trio: (set label, current 1-based board, total).
    /// Idle → (None, 0, 0). This is what a reconnecting teacher renders as
    /// "‹label› — board 3 of 8".
    pub async fn deck_status(&self) -> (Option<String>, usize, usize) {
        let d = self.deck.lock().await;
        let total = d.boards.len();
        let current = if total == 0 { 0 } else { d.index + 1 };
        (d.label.clone(), current, total)
    }

    /// Replace the loaded set ("change deal source"): store boards + label,
    /// reset the pointer to board 1, and deal it to every table (lockstep).
    /// Errors on an empty set. Returns the board total.
    pub async fn load_boards(
        &self,
        boards: Vec<BoardSetup>,
        label: Option<String>,
    ) -> Result<usize, String> {
        if boards.is_empty() {
            return Err("no boards to load".into());
        }
        let total = boards.len();
        {
            let mut d = self.deck.lock().await;
            d.boards = Arc::new(boards);
            d.label = label.clone();
            d.index = 0;
        }
        // Adhoc opens the whole set; teacher_set ignores this (lockstep).
        self.open_boards.store(total, Ordering::SeqCst);
        tracing::info!(
            event = "load_boards",
            session = %self.id,
            label = label.as_deref().unwrap_or(""),
            total,
            "loaded deal set"
        );
        self.deal_index_to_all(0).await;
        Ok(total)
    }

    /// Force every table to board `index` (0-based, clamped to the set).
    /// Lockstep — both directions, teacher-driven. Fresh TableState, cleared
    /// ready + BBA cache, `board_advanced` (forced) + resync + lobby + bot
    /// kick per room. Returns the new 1-based board number (0 if idle).
    async fn deal_index_to_all(&self, index: usize) -> usize {
        let (idx, board) = {
            let mut d = self.deck.lock().await;
            if d.boards.is_empty() {
                return 0;
            }
            let idx = index.min(d.boards.len() - 1);
            d.index = idx;
            (idx, d.boards[idx].clone())
        };
        for room in &self.rooms {
            {
                let mut inner = room.state.lock().await;
                inner.board_index = idx;
                inner.table = TableState::new(board.clone());
                inner.ready.clear();
            }
            *room.bba_cache.lock().await = None;
            room.broadcast(
                json!({
                    "t": "event",
                    "table_id": room.id,
                    "seq": 0,
                    "kind": "board_advanced",
                    "board_no": board.number,
                    "board_index": idx,
                    "forced": true,
                })
                .to_string(),
            );
            room.broadcast_resync();
            crate::bots::kick(room.clone());
        }
        self.notify_lobby();
        idx + 1
    }

    /// Jump all tables to a 1-based board number (Shark "go to"). Clamped.
    pub async fn goto_board(&self, one_based: usize) -> usize {
        self.deal_index_to_all(one_based.saturating_sub(1)).await
    }

    /// Advance all tables one board (Shark "Next Deal"). Clamped at the end.
    pub async fn next_board(&self) -> usize {
        let cur = self.deck.lock().await.index;
        self.deal_index_to_all(cur + 1).await
    }

    /// Back all tables up one board (Shark "Replay/redo" at the set level).
    pub async fn prev_board(&self) -> usize {
        let cur = self.deck.lock().await.index;
        self.deal_index_to_all(cur.saturating_sub(1)).await
    }

    /// The teacher's lobby frame: every table's seats/board/phase/tricks
    /// plus the kibitzer roster. Sent on teacher join and on any change.
    pub async fn lobby_json(&self) -> String {
        // Deck snapshot first (locked + dropped) so we never nest it under a
        // room lock in the loop below.
        let (set_label, cur_board, total) = self.deck_status().await;
        let mut tables = Vec::with_capacity(self.rooms.len());
        for room in &self.rooms {
            let inner = room.state.lock().await;
            let f = inner.table.fold();
            let hands = {
                let mut hands = serde_json::Map::new();
                let complete = f.phase == crate::table::Phase::Complete;
                for seat in bridge_types::Direction::ALL {
                    let cards = if complete {
                        inner.table.board.deal.hand(seat).cards().to_vec()
                    } else {
                        inner.table.remaining(seat, &f)
                    };
                    hands.insert(
                        seat.to_char().to_string(),
                        json!(cards
                            .iter()
                            .map(|c| format!("{}{}", c.suit.to_char(), c.rank.to_char()))
                            .collect::<Vec<_>>()),
                    );
                }
                Value::Object(hands)
            };
            tables.push(json!({
                "table_id": room.id,
                "board_no": inner.table.board.number,
                "board_index": inner.board_index,
                "phase": match f.phase {
                    crate::table::Phase::Bidding => "bidding",
                    crate::table::Phase::Play => "play",
                    crate::table::Phase::Complete => "complete",
                },
                "tricks": { "ns": f.tricks_ns, "ew": f.tricks_ew },
                "next_to_act": f.next_to_act.map(|d| d.to_char().to_string()),
                "seats": inner.seats_json(),
                "ready": inner
                    .ready
                    .iter()
                    .map(|d| d.to_char().to_string())
                    .collect::<Vec<_>>(),
                // Multi-table monitor payload (teacher-only feed): enough to
                // render a live mini-table per room — dealer/vul, the
                // auction, contract, each seat's remaining cards, and the
                // trick in progress. ~1-2KB per table per update; the lobby
                // is already rebuilt on every action at any table.
                "dealer": inner.table.board.dealer.to_char().to_string(),
                "vulnerable": inner.table.board.vulnerable.to_pbn(),
                "auction": f.calls.iter().map(|c| c.to_pbn()).collect::<Vec<_>>(),
                "contract": f.contract.as_ref().map(|c| json!({
                    "text": c.to_pbn(),
                    "declarer": c.declarer.to_char().to_string(),
                })),
                "hands": hands,
                "current_trick": f.current_trick.as_ref().map(|(leader, plays)| json!({
                    "leader": leader.to_char().to_string(),
                    "plays": plays.iter().map(|(s, c)| json!({
                        "seat": s.to_char().to_string(),
                        "card": format!("{}{}", c.suit.to_char(), c.rank.to_char()),
                    })).collect::<Vec<_>>(),
                })),
                "result": inner.table.board_result_json(),
            }));
        }
        let kibitzers: Vec<Value> = {
            let kib = self.kibitzers.lock().await;
            kib.iter()
                .map(|(sub, k)| {
                    json!({
                        "sub": sub,
                        "name": k.name,
                        "table_id": self.rooms[k.room_idx].id,
                    })
                })
                .collect()
        };
        json!({
            "t": "lobby",
            "session_id": self.id,
            // Reconnect status: a teacher rejoining renders
            // "‹set_label› — board {index} of {total}" straight from here.
            "set_label": set_label,
            "loaded": total > 0,
            "boards": {
                "total": total,
                "open": self.open_boards.load(Ordering::SeqCst),
                "index": cur_board,
            },
            "tables": tables,
            "kibitzers": kibitzers,
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_types::Direction::*;

    // Two full deals (PBN example deal, boards 1 and 2).
    pub const TWO_BOARDS: &str = concat!(
        "[Board \"1\"]\n[Dealer \"N\"]\n[Vulnerable \"None\"]\n",
        "[Deal \"N:K843.T542.J6.863 AQJ7.K.Q75.AT942 962.AJ7.KT82.J75 T5.Q9863.A943.KQ\"]\n\n",
        "[Board \"2\"]\n[Dealer \"E\"]\n[Vulnerable \"NS\"]\n",
        "[Deal \"N:AQJ7.K.Q75.AT942 962.AJ7.KT82.J75 T5.Q9863.A943.KQ K843.T542.J6.863\"]\n",
    );

    fn session(kind: SessionKind, policy: SeatPolicy, tables: usize) -> Arc<Session> {
        Session::new(
            "s1".into(),
            kind,
            "owner-1".into(),
            policy,
            parse_boards(TWO_BOARDS).unwrap(),
            tables,
        )
    }

    #[test]
    fn parses_boards_with_numbers_and_dealers() {
        let boards = parse_boards(TWO_BOARDS).unwrap();
        assert_eq!(boards.len(), 2);
        assert_eq!(boards[0].number, 1);
        assert_eq!(boards[0].dealer, North);
        assert_eq!(boards[1].number, 2);
        assert_eq!(boards[1].dealer, East);
        assert!(parse_boards("not pbn at all").is_err());
    }

    #[test]
    fn seat_policy_parsing() {
        use serde_json::json;
        assert_eq!(
            SeatPolicy::from_value(&json!({"mode":"manual"})).unwrap(),
            SeatPolicy::Manual
        );
        assert_eq!(
            SeatPolicy::from_value(&json!({
                "mode":"auto","pattern":"one_per_seat","seats":["S"]
            }))
            .unwrap(),
            SeatPolicy::OnePerSeat(vec![South])
        );
        assert_eq!(
            SeatPolicy::from_value(&json!({"mode":"auto","pattern":"pairs","sides":["NS"]}))
                .unwrap(),
            SeatPolicy::Pairs(vec![Side::NS])
        );
        assert_eq!(
            SeatPolicy::from_value(&json!({})).unwrap(),
            SeatPolicy::FirstFree
        );
        assert_eq!(
            SeatPolicy::from_value(&Value::Null).unwrap(),
            SeatPolicy::FirstFree
        );
        assert!(SeatPolicy::from_value(&json!({"mode":"auto","pattern":"nope"})).is_err());
        assert!(SeatPolicy::from_value(&json!({"mode":"auto","pattern":"one_per_seat"})).is_err());
    }

    #[tokio::test]
    async fn one_per_seat_deals_arrivals_across_tables() {
        let s = session(
            SessionKind::TeacherSet,
            SeatPolicy::OnePerSeat(vec![South]),
            2,
        );
        let p1 = s.place("g1", "Ann").await;
        let p2 = s.place("g2", "Ben").await;
        assert_eq!((p1.room_idx, p1.seat), (0, Some(South)));
        assert_eq!((p2.room_idx, p2.seat), (1, Some(South)));
        // Policy seats full: third arrival kibitzes.
        let p3 = s.place("g3", "Cyd").await;
        assert_eq!(p3.seat, None);
        assert!(s.kibitzers.lock().await.contains_key("g3"));
        // Reconnect rebinds the same seat.
        let p1b = s.place("g1", "Ann").await;
        assert_eq!((p1b.room_idx, p1b.seat), (0, Some(South)));
    }

    #[tokio::test]
    async fn pairs_fills_a_side_per_table() {
        let s = session(SessionKind::Adhoc, SeatPolicy::Pairs(vec![Side::NS]), 2);
        let p1 = s.place("g1", "Ann").await;
        let p2 = s.place("g2", "Ben").await;
        let p3 = s.place("g3", "Cyd").await;
        let p4 = s.place("g4", "Dee").await;
        assert_eq!((p1.room_idx, p1.seat), (0, Some(South)));
        assert_eq!((p2.room_idx, p2.seat), (0, Some(North)));
        assert_eq!((p3.room_idx, p3.seat), (1, Some(South)));
        assert_eq!((p4.room_idx, p4.seat), (1, Some(North)));
        assert_eq!(s.place("g5", "Eve").await.seat, None);
    }

    #[tokio::test]
    async fn manual_policy_kibitzes_until_assigned() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::Manual, 2);
        let p1 = s.place("g1", "Ann").await;
        assert_eq!(p1.seat, None);
        assert!(s.kibitzers.lock().await.contains_key("g1"));
        // Simulate teacher assignment, then a reconnect rebinds the seat.
        {
            let mut inner = s.rooms[1].state.lock().await;
            assert!(inner.try_seat(South, "g1", "Ann"));
        }
        s.remove_kibitzer("g1").await;
        let p1b = s.place("g1", "Ann").await;
        assert_eq!((p1b.room_idx, p1b.seat), (1, Some(South)));
    }

    #[tokio::test]
    async fn first_free_round_robins_tables() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 2);
        let p1 = s.place("g1", "Ann").await;
        let p2 = s.place("g2", "Ben").await;
        let p3 = s.place("g3", "Cyd").await;
        assert_eq!((p1.room_idx, p1.seat), (0, Some(South)));
        assert_eq!((p2.room_idx, p2.seat), (1, Some(South)));
        assert_eq!((p3.room_idx, p3.seat), (0, Some(West)));
    }

    #[tokio::test]
    async fn teacher_set_is_lockstep_not_self_advancing() {
        let s = session(
            SessionKind::TeacherSet,
            SeatPolicy::OnePerSeat(vec![South]),
            2,
        );
        s.place("g1", "Ann").await; // t0 South
        let room = s.rooms[0].clone();

        // Even ready + a widened window: teacher_set NEVER self-advances
        // (it's teacher-driven lockstep).
        room.state.lock().await.ready.insert(South);
        assert_eq!(s.open_boards_to(2).await, 2);
        assert!(
            !s.try_advance(&room, false).await,
            "teacher_set is lockstep"
        );

        // The teacher drives EVERY table together with next_board.
        assert_eq!(s.next_board().await, 2, "1-based board number");
        for room in &s.rooms {
            let inner = room.state.lock().await;
            assert_eq!(inner.board_index, 1);
            assert_eq!(inner.table.board.number, 2);
            assert!(inner.ready.is_empty());
            assert_eq!(inner.table.seq(), 0, "fresh action log on the new board");
        }

        // And can back everyone up (Shark redo).
        assert_eq!(s.prev_board().await, 1);
        assert_eq!(s.rooms[0].state.lock().await.board_index, 0);
        assert_eq!(s.rooms[1].state.lock().await.board_index, 0);

        // next past the end clamps (stays on the last board).
        s.next_board().await;
        assert_eq!(s.next_board().await, 2, "clamped at the last board");
    }

    #[tokio::test]
    async fn load_boards_swaps_set_resets_to_one_and_labels() {
        // Start idle (no boards), then load a labelled set.
        let s = Session::new(
            "s1".into(),
            SessionKind::TeacherSet,
            "o".into(),
            SeatPolicy::Manual,
            vec![],
            2,
        );
        assert!(!s.loaded().await);
        assert_eq!(s.deck_status().await, (None, 0, 0));

        assert_eq!(
            s.load_boards(
                parse_boards(TWO_BOARDS).unwrap(),
                Some("Scenarios - Transfers".into())
            )
            .await
            .unwrap(),
            2
        );
        assert!(s.loaded().await);
        let (label, cur, total) = s.deck_status().await;
        assert_eq!(label.as_deref(), Some("Scenarios - Transfers"));
        assert_eq!((cur, total), (1, 2));
        for room in &s.rooms {
            assert_eq!(room.state.lock().await.table.board.number, 1);
        }

        // Move forward, then load a fresh set → pointer resets to board 1.
        s.next_board().await;
        assert_eq!(s.deck_status().await.1, 2);
        s.load_boards(parse_boards(TWO_BOARDS).unwrap(), Some("Mix".into()))
            .await
            .unwrap();
        assert_eq!(s.deck_status().await.1, 1, "load resets the pointer");

        // An empty load is rejected.
        assert!(s.load_boards(vec![], None).await.is_err());
    }

    #[tokio::test]
    async fn goto_clamps_and_moves_every_table() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::FirstFree, 2);
        assert_eq!(s.goto_board(99).await, 2, "clamped to the last board");
        for room in &s.rooms {
            assert_eq!(room.state.lock().await.board_index, 1);
        }
        assert_eq!(s.goto_board(1).await, 1);
        for room in &s.rooms {
            assert_eq!(room.state.lock().await.board_index, 0);
        }
        assert_eq!(s.goto_board(0).await, 1, "board 0 clamps to board 1");
    }

    #[tokio::test]
    async fn idle_session_is_valid_and_reports_unloaded() {
        let s = Session::new(
            "s1".into(),
            SessionKind::TeacherSet,
            "o".into(),
            SeatPolicy::FirstFree,
            vec![],
            1,
        );
        assert!(!s.loaded().await);
        // Placeholder board keeps TableState valid pre-load (seating works).
        assert_eq!(
            s.rooms[0].state.lock().await.table.board.deal.total_cards(),
            52
        );
        // No set → nothing advances, even forced.
        assert!(!s.try_advance(&s.rooms[0].clone(), true).await);
        // Lobby reports the idle state.
        let lobby: Value = serde_json::from_str(&s.lobby_json().await).unwrap();
        assert_eq!(lobby["loaded"], false);
        assert_eq!(lobby["set_label"], Value::Null);
        assert_eq!(lobby["boards"]["total"], 0);
        assert_eq!(lobby["boards"]["index"], 0);
    }

    #[tokio::test]
    async fn adhoc_opens_everything_advance_needs_all_ready() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        s.place("g1", "Ann").await; // South
        s.place("g2", "Ben").await; // West
        let room = s.rooms[0].clone();
        assert_eq!(s.open_boards.load(Ordering::SeqCst), 2);

        room.state.lock().await.ready.insert(South);
        assert!(
            !s.try_advance(&room, false).await,
            "one of two humans ready is not enough"
        );
        room.state.lock().await.ready.insert(West);
        assert!(s.try_advance(&room, false).await);
    }

    #[tokio::test]
    async fn disconnected_humans_do_not_block_advance() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        s.place("g1", "Ann").await;
        s.place("g2", "Ben").await;
        let room = s.rooms[0].clone();
        {
            let mut inner = room.state.lock().await;
            inner.mark_disconnected("g2");
            inner.ready.insert(South);
        }
        assert!(s.try_advance(&room, false).await);
    }

    #[tokio::test]
    async fn all_bot_table_only_force_advances() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        let room = s.rooms[0].clone();
        assert!(
            !s.try_advance(&room, false).await,
            "no humans: no auto-advance"
        );
        assert!(s.try_advance(&room, true).await, "teacher force works");
    }

    #[tokio::test]
    async fn force_advance_ignores_the_round_gate() {
        let s = session(
            SessionKind::TeacherSet,
            SeatPolicy::OnePerSeat(vec![South]),
            1,
        );
        s.place("g1", "Ann").await;
        let room = s.rooms[0].clone();
        assert!(!s.try_advance(&room, false).await);
        assert!(s.try_advance(&room, true).await);
    }

    #[tokio::test]
    async fn open_boards_never_narrows() {
        let s = session(
            SessionKind::TeacherSet,
            SeatPolicy::OnePerSeat(vec![South]),
            1,
        );
        assert_eq!(s.open_boards_to(2).await, 2);
        assert_eq!(s.open_boards_to(1).await, 2, "cannot close a round");
        assert_eq!(s.open_boards_to(99).await, 2, "clamped to the board count");
    }

    #[tokio::test]
    async fn assign_seat_moves_kibitzers_and_seated_players() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::Manual, 2);
        s.place("g1", "Ann").await; // kibitzer

        // Kibitzer → seat.
        let changed = s.assign_seat("s1-t2", South, Some("g1")).await.unwrap();
        assert_eq!(changed, vec![1]);
        assert_eq!(s.find_seated("g1").await, Some((1, South)));
        assert!(!s.kibitzers.lock().await.contains_key("g1"));

        // Seated → different table (both rooms report a change).
        let changed = s.assign_seat("s1-t1", North, Some("g1")).await.unwrap();
        assert_eq!(changed, vec![1, 0]);
        assert_eq!(s.find_seated("g1").await, Some((0, North)));

        // Occupied target rejected without unseating anyone.
        s.place("g2", "Ben").await; // kibitzer
        let err = s.assign_seat("s1-t1", North, Some("g2")).await.unwrap_err();
        assert!(err.contains("occupied"), "{err}");
        assert_eq!(s.find_seated("g1").await, Some((0, North)));

        // Unknown participant / table rejected.
        assert!(s.assign_seat("s1-t1", South, Some("nope")).await.is_err());
        assert!(s.assign_seat("s1-t9", South, Some("g2")).await.is_err());
    }

    #[tokio::test]
    async fn boot_vacates_and_keeps_the_human_as_kibitzer() {
        let s = session(
            SessionKind::TeacherSet,
            SeatPolicy::OnePerSeat(vec![South]),
            1,
        );
        s.place("g1", "Ann").await;
        {
            // A ready flag must not survive the boot.
            s.rooms[0].state.lock().await.ready.insert(South);
        }
        let changed = s.assign_seat("s1-t1", South, None).await.unwrap();
        assert_eq!(changed, vec![0]);
        assert_eq!(s.find_seated("g1").await, None);
        assert!(s.kibitzers.lock().await.contains_key("g1"));
        let inner = s.rooms[0].state.lock().await;
        assert!(inner.seats.is_empty());
        assert!(inner.ready.is_empty());
        drop(inner);
        // Booting an empty seat is a no-op.
        assert_eq!(
            s.assign_seat("s1-t1", South, None).await.unwrap(),
            Vec::<usize>::new()
        );
    }

    #[tokio::test]
    async fn lobby_json_lists_tables_and_kibitzers() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::Manual, 2);
        s.place("g1", "Ann").await; // kibitzer (manual)
        let lobby: Value = serde_json::from_str(&s.lobby_json().await).unwrap();
        assert_eq!(lobby["t"], "lobby");
        assert_eq!(lobby["session_id"], "s1");
        assert_eq!(lobby["boards"]["total"], 2);
        assert_eq!(lobby["boards"]["open"], 1);
        let tables = lobby["tables"].as_array().unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0]["table_id"], "s1-t1");
        assert_eq!(tables[0]["board_no"], 1);
        assert_eq!(tables[0]["phase"], "bidding");
        let kib = lobby["kibitzers"].as_array().unwrap();
        assert_eq!(kib.len(), 1);
        assert_eq!(kib[0]["sub"], "g1");
        assert_eq!(kib[0]["name"], "Ann");
    }
}
