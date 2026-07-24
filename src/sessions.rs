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
//!   table 1's South, …); when every existing table's seats fill, a new table
//!   is **auto-created** (student-fill — §Phase 3.2).
//! - `{"mode":"auto","pattern":"pairs","sides":["NS"]}` — fills a table's
//!   whole side before the next (partners together), auto-creating as needed.
//! - `{"mode":"auto","pattern":"first_free"}` (the default) — first free
//!   seat (S, W, N, E) round-robin, auto-creating as needed.
//! - `{"mode":"manual"}` — arrivals wait in the lobby until the teacher seats
//!   them (`assign_seat` / `seat_students`).
//!
//! Sessions can start with **0 tables** (student-fill creates them) and with
//! **`wait_to_seat`** on (Zoom-style waiting room: arrivals wait until
//! `seat_students`). A lobby member is *waiting* (auto-fill eligible) or
//! *parked* (benched via boot/drag — never auto-reseated, even on reconnect).
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

    /// The side a seat belongs to.
    pub fn of(dir: Direction) -> Side {
        match dir {
            Direction::North | Direction::South => Side::NS,
            Direction::East | Direction::West => Side::EW,
        }
    }

    pub fn from_key(key: &str) -> Option<Side> {
        match key {
            "NS" => Some(Side::NS),
            "EW" => Some(Side::EW),
            _ => None,
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Side::NS => "NS",
            Side::EW => "EW",
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

    /// Short key ↔ policy for the console dropdown (a curated subset of the
    /// full JSON policy space).
    pub fn from_key(key: &str) -> Option<SeatPolicy> {
        use Direction::{North, South};
        match key {
            "first_free" => Some(SeatPolicy::FirstFree),
            "one_per_south" => Some(SeatPolicy::OnePerSeat(vec![South])),
            "two_per_ns" => Some(SeatPolicy::OnePerSeat(vec![South, North])),
            "pairs_ns" => Some(SeatPolicy::Pairs(vec![Side::NS])),
            "manual" => Some(SeatPolicy::Manual),
            _ => None,
        }
    }

    pub fn key(&self) -> &'static str {
        use Direction::{North, South};
        match self {
            SeatPolicy::FirstFree => "first_free",
            SeatPolicy::Manual => "manual",
            SeatPolicy::OnePerSeat(s) if s.as_slice() == [South] => "one_per_south",
            SeatPolicy::OnePerSeat(s) if s.as_slice() == [South, North] => "two_per_ns",
            SeatPolicy::Pairs(s) if s.as_slice() == [Side::NS] => "pairs_ns",
            _ => "custom",
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
            play_line: crate::table::PlayLine::from_sequence(b.play.as_ref()),
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

/// A participant in the lobby: connected but unseated (waiting to be seated,
/// benched/parked, or a manual-policy watcher). Tracked so the teacher's lobby
/// lists them and `assign_seat` / `seat_students` can find them by sub.
#[derive(Debug, Clone)]
pub struct Kibitzer {
    pub name: String,
    /// The table their connection watches, if any (`None` = pure lobby, e.g. a
    /// 0-table session). Id-keyed so it survives table add/remove.
    pub room_id: Option<String>,
    /// Timeout corner: `true` = benched, never auto-seated (drag-to-lobby /
    /// boot). `false` = waiting, eligible for auto-fill / `seat_students`.
    pub parked: bool,
}

/// One live websocket connection, keyed in `Session::connections` by its
/// opaque per-connection `token` (a random id minted at connect, unrelated
/// to `sub` — the client never sees `sub`). Tracked so the host roster can
/// list every human connection (seated, waiting, OR the unseated host), and
/// so `assign_seat`'s token-addressed "place/Sit" can resolve a token back
/// to the participant it should seat.
#[derive(Debug, Clone)]
pub struct ConnMeta {
    /// Ticket subject (stable participant identity — used for seats/reconnect).
    pub sub: String,
    pub name: String,
    /// Socket currently live? A seated participant whose socket dropped stays
    /// in the registry (connected=false) so the roster greys them while the
    /// zombie-seat grace runs.
    pub connected: bool,
}

/// The bridge-classroom account id behind a ticket subject, or `None` for a
/// guest.
///
/// Guest subjects are minted as `guest-<uuid>` by the classroom API's
/// ticket endpoint; every other subject IS the account's `users.id`.
///
/// **Why this is exposed in the roster.** Friend requests must be addressed by
/// account id, and the friendship ADR deliberately provides no user-search: the
/// *only* way to befriend someone is to have shared a table with them. So the
/// roster is the one place that handle can legitimately come from. What travels
/// is an opaque UUID — not a name or email beyond what the roster already
/// shows — and it grants exactly one capability: "ask this person to be
/// friends", which is the intended bootstrap. Guests get `null`, matching the
/// rule that a guest cannot be friended.
pub fn account_id_of(sub: &str) -> Option<&str> {
    if sub.starts_with("guest-") {
        None
    } else {
        Some(sub)
    }
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
    /// Auto-seat shape, live-settable from the console (`set_seat_policy`).
    /// Read via `seat_policy()` (cheap clone; never held across an await).
    seat_policy: std::sync::Mutex<SeatPolicy>,
    /// Serializes placement so simultaneous arrivals fill deterministically
    /// (without it, concurrent tabs race to create tables / grab seats).
    seating: Mutex<()>,
    /// Session-default bot backend (`BotMode as u8`); new tables inherit it,
    /// `set_bot_mode` applies it to all. Surfaced on the console.
    bot_mode: std::sync::atomic::AtomicU8,
    /// Session-default board mode (`BoardMode as u8`); new tables inherit it,
    /// `set_board_mode` applies it to all. Carried in from the client (the solo
    /// "play the hand after bidding" preference, via `load_boards`'s optional
    /// `mode`), so an in-place solo→served upgrade keeps bidding-only vs
    /// bid-and-play instead of always defaulting to play.
    board_mode: std::sync::atomic::AtomicU8,
    /// Teacher pause: bots stop acting until resumed (Shark "Stop play").
    bots_paused: std::sync::atomic::AtomicBool,
    /// The loaded set + label + lockstep pointer. Swappable at runtime
    /// (`load_boards`); empty = idle. Never held across a room lock.
    pub deck: Mutex<Deck>,
    /// Tables. **Append-only for now** (grow on student-fill / add_tables;
    /// removal is a later phase). Snapshot-and-drop the guard before locking a
    /// room — never hold this across a room lock.
    pub rooms: tokio::sync::RwLock<Vec<Arc<Room>>>,
    /// Weak back-ref so we can build new rooms after construction (auto-create
    /// on student-fill, add_tables).
    weak_self: Weak<Session>,
    /// Next table number for `<id>-t<n>` ids (monotonic; ids stay unique even
    /// once removal lands).
    next_table_no: AtomicUsize,
    /// Zoom-style waiting room: when true, arrivals wait in the lobby instead
    /// of auto-seating (teacher runs `seat_students`).
    pub wait_to_seat: std::sync::atomic::AtomicBool,
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
    /// Live connections keyed by opaque per-connection token. The roster is
    /// built from this (one entry per human connection, seated or waiting or
    /// the unseated host); `assign_seat` place-by-token resolves here. Leaf
    /// lock — never held across a room lock.
    pub connections: Mutex<HashMap<String, ConnMeta>>,
}

/// Where an arriving connection ends up. Always bound to a room (a lobby member
/// watches a table; one is auto-created if none exist yet).
pub struct Placement {
    pub room: Arc<Room>,
    /// None → in the lobby (waiting / parked / manual).
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
        // Friends (adhoc) tables hand off to bots fast; classroom sets keep the
        // generous reconnect grace (see TakeoverGrace).
        let grace = if matches!(kind, SessionKind::Adhoc) {
            crate::rooms::TakeoverGrace::FAST
        } else {
            crate::rooms::TakeoverGrace::DEFAULT
        };
        Arc::new_cyclic(|weak: &Weak<Session>| {
            let rooms: Vec<Arc<Room>> = (1..=table_count)
                .map(|i| {
                    Room::new(
                        format!("{id}-t{i}"),
                        seed.clone(),
                        Some(weak.clone()),
                        grace,
                    )
                })
                .collect();
            Session {
                id,
                kind,
                owner_sub,
                seat_policy: std::sync::Mutex::new(seat_policy),
                seating: Mutex::new(()),
                bot_mode: std::sync::atomic::AtomicU8::new(crate::rooms::BotMode::default() as u8),
                board_mode: std::sync::atomic::AtomicU8::new(
                    crate::rooms::BoardMode::default() as u8
                ),
                bots_paused: std::sync::atomic::AtomicBool::new(false),
                deck: Mutex::new(Deck {
                    boards: Arc::new(boards),
                    label: None,
                    index: 0,
                }),
                rooms: tokio::sync::RwLock::new(rooms),
                weak_self: weak.clone(),
                next_table_no: AtomicUsize::new(table_count),
                wait_to_seat: std::sync::atomic::AtomicBool::new(false),
                open_boards: AtomicUsize::new(open),
                notify,
                next_arrival: AtomicUsize::new(0),
                kibitzers: Mutex::new(HashMap::new()),
                connections: Mutex::new(HashMap::new()),
            }
        })
    }

    pub fn notify_lobby(&self) {
        let _ = self.notify.send(LOBBY.to_string());
    }

    pub fn notify_recheck(&self) {
        let _ = self.notify.send(RECHECK.to_string());
    }

    /// Snapshot the current tables (cheap Arc clones) so callers never hold the
    /// rooms lock across a room lock — preserves the module's no-nested-locks rule.
    pub async fn rooms_snapshot(&self) -> Vec<Arc<Room>> {
        self.rooms.read().await.clone()
    }

    pub async fn table_count(&self) -> usize {
        self.rooms.read().await.len()
    }

    pub async fn room_by_id(&self, table_id: &str) -> Option<Arc<Room>> {
        self.rooms
            .read()
            .await
            .iter()
            .find(|r| r.id == table_id)
            .cloned()
    }

    /// Add a table (student-fill auto-create / add_tables). Starts on the
    /// current board (placeholder while idle) and kicks its bots.
    pub async fn add_room(&self) -> Arc<Room> {
        let n = self.next_table_no.fetch_add(1, Ordering::SeqCst) + 1;
        let board = {
            let d = self.deck.lock().await;
            if d.boards.is_empty() {
                crate::rooms::placeholder_board()
            } else {
                d.boards[d.index].clone()
            }
        };
        let grace = if matches!(self.kind, SessionKind::Adhoc) {
            crate::rooms::TakeoverGrace::FAST
        } else {
            crate::rooms::TakeoverGrace::DEFAULT
        };
        let room = Room::new(
            format!("{}-t{}", self.id, n),
            board,
            Some(self.weak_self.clone()),
            grace,
        );
        // New tables inherit the session's bot backend + board mode.
        room.set_bot_mode(self.bot_mode());
        room.state.lock().await.board_mode = self.board_mode();
        self.rooms.write().await.push(room.clone());
        crate::bots::ensure_keepalive(room.clone());
        if !self.bots_paused() {
            crate::bots::kick(room.clone());
        }
        self.notify_lobby();
        room
    }

    /// Add `count` tables at once (testing path: fill with bots). Returns the
    /// new table total.
    pub async fn add_tables(&self, count: usize) -> usize {
        for _ in 0..count {
            self.add_room().await;
        }
        self.table_count().await
    }

    /// Zoom-style waiting room toggle.
    pub fn set_wait_to_seat(&self, on: bool) {
        self.wait_to_seat.store(on, Ordering::SeqCst);
        self.notify_lobby();
    }

    // ── Live console settings: seat policy + bot controls (Phase 3.2) ────────
    pub fn seat_policy(&self) -> SeatPolicy {
        self.seat_policy.lock().unwrap().clone()
    }
    pub fn set_seat_policy(&self, p: SeatPolicy) {
        *self.seat_policy.lock().unwrap() = p;
        self.notify_lobby();
    }
    pub fn bot_mode(&self) -> crate::rooms::BotMode {
        crate::rooms::BotMode::from_u8(self.bot_mode.load(Ordering::SeqCst))
    }
    /// Change the bot backend for every table (and future ones).
    pub async fn set_bot_mode(&self, mode: crate::rooms::BotMode) {
        self.bot_mode.store(mode as u8, Ordering::SeqCst);
        for room in self.rooms_snapshot().await {
            room.set_bot_mode(mode);
            if !self.bots_paused() {
                crate::bots::kick(room);
            }
        }
        self.notify_lobby();
    }
    pub fn board_mode(&self) -> crate::rooms::BoardMode {
        crate::rooms::BoardMode::from_u8(self.board_mode.load(Ordering::SeqCst))
    }
    /// Change how boards run (bid-and-play / bid-only / play-only) for every
    /// table — and future ones. Applied under each room's lock (board mode is
    /// RoomInner state), then bots are re-kicked so a switch to play-only /
    /// back to bid-and-play resumes bot action immediately.
    pub async fn set_board_mode(&self, mode: crate::rooms::BoardMode) {
        self.board_mode.store(mode as u8, Ordering::SeqCst);
        for room in self.rooms_snapshot().await {
            room.state.lock().await.board_mode = mode;
            if !self.bots_paused() {
                crate::bots::kick(room);
            }
        }
        self.notify_lobby();
    }
    pub fn bots_paused(&self) -> bool {
        self.bots_paused.load(Ordering::SeqCst)
    }
    /// Pause/resume bot play across all tables (Shark "Stop play"). Resume
    /// re-kicks every table so play continues.
    pub async fn set_bots_paused(&self, on: bool) {
        self.bots_paused.store(on, Ordering::SeqCst);
        if !on {
            for room in self.rooms_snapshot().await {
                crate::bots::kick(room);
            }
        }
        self.notify_lobby();
    }

    /// Find the room+seat currently bound to `sub`, if any. Snapshots rooms
    /// first, then locks one at a time (no lock held across another).
    pub async fn find_seated(&self, sub: &str) -> Option<(Arc<Room>, Direction)> {
        for room in self.rooms_snapshot().await {
            let inner = room.state.lock().await;
            if let Some((&seat, _)) = inner.seats.iter().find(|(_, o)| o.sub == sub) {
                return Some((room.clone(), seat));
            }
        }
        None
    }

    /// Every room+seat currently bound to `sub` (a connection may hold
    /// MULTIPLE seats — friends-table multi-seat). Snapshots rooms first,
    /// then locks one at a time.
    pub async fn find_all_seated(&self, sub: &str) -> Vec<(Arc<Room>, Direction)> {
        let mut out = Vec::new();
        for room in self.rooms_snapshot().await {
            let inner = room.state.lock().await;
            for (&seat, occ) in inner.seats.iter() {
                if occ.sub == sub {
                    out.push((room.clone(), seat));
                }
            }
        }
        out
    }

    /// The seats (directions only) `sub` occupies across the whole session.
    pub async fn seats_of(&self, sub: &str) -> Vec<Direction> {
        self.find_all_seated(sub)
            .await
            .into_iter()
            .map(|(_, s)| s)
            .collect()
    }

    // ── Connection registry (opaque tokens) ─────────────────────────────────

    /// Register (or re-register on reconnect) a live connection under its
    /// opaque `token`. Any prior entry for the same `sub` is dropped first so
    /// a participant is one roster entry — a reconnect mints a fresh token and
    /// supersedes the stale (disconnected) one.
    pub async fn register_conn(&self, token: &str, sub: &str, name: &str) {
        let mut c = self.connections.lock().await;
        c.retain(|t, m| t == token || m.sub != sub);
        c.insert(
            token.to_string(),
            ConnMeta {
                sub: sub.to_string(),
                name: name.to_string(),
                connected: true,
            },
        );
    }

    /// Mark a connection's socket dropped (kept in the registry so a zombie
    /// seat still shows the greyed occupant in the roster).
    pub async fn mark_conn_disconnected(&self, token: &str) {
        if let Some(m) = self.connections.lock().await.get_mut(token) {
            m.connected = false;
        }
    }

    /// Forget a connection entirely (a waiter/kibitzer who left).
    pub async fn remove_conn(&self, token: &str) {
        self.connections.lock().await.remove(token);
    }

    /// Resolve an opaque token to its (sub, name), for `assign_seat`
    /// place/Sit-by-token.
    pub async fn conn_meta(&self, token: &str) -> Option<(String, String)> {
        self.connections
            .lock()
            .await
            .get(token)
            .map(|m| (m.sub.clone(), m.name.clone()))
    }

    /// The shared roster: one entry per live human connection — seated,
    /// waiting, or the unseated host — keyed by opaque token. Empty `seats`
    /// = a waiter/kibitzer. This is the arrangement the host UI drags and
    /// that rides in every welcome/snapshot plus the `roster_update` event.
    /// Locks rooms sequentially then the connections leaf — never call while
    /// holding a room lock.
    ///
    /// Entries carry `account_id` (null for guests) so the client can offer
    /// "Add friend" — see [`account_id_of`].
    pub async fn roster_json(&self) -> Value {
        let rooms = self.rooms_snapshot().await;
        let mut seats_by_sub: HashMap<String, Vec<String>> = HashMap::new();
        for room in &rooms {
            let inner = room.state.lock().await;
            for seat in Direction::ALL {
                if let Some(occ) = inner.seats.get(&seat) {
                    seats_by_sub
                        .entry(occ.sub.clone())
                        .or_default()
                        .push(seat.to_char().to_string());
                }
            }
        }
        let conns = self.connections.lock().await;
        let mut entries: Vec<(String, String, Value)> = conns
            .iter()
            .filter_map(|(token, m)| {
                let mut seats = seats_by_sub.get(&m.sub).cloned().unwrap_or_default();
                // A disconnected connection that holds NO seat has genuinely
                // left (a departed waiter/kibitzer, or the stale old session of
                // someone who rejoined under a fresh guest sub). Drop it so it
                // doesn't linger as a duplicate ghost in the kibitz list. A
                // disconnected connection that still HOLDS a seat is a live
                // zombie seat and stays (greyed) so the host can see it.
                if !m.connected && seats.is_empty() {
                    return None;
                }
                seats.sort();
                Some((
                    m.name.clone(),
                    token.clone(),
                    json!({
                        "token": token,
                        "name": m.name,
                        "connected": m.connected,
                        "seats": seats,
                        "account_id": account_id_of(&m.sub),
                    }),
                ))
            })
            .collect();
        // Stable order for the host UI (name, then token as tiebreak).
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        Value::Array(entries.into_iter().map(|(_, _, v)| v).collect())
    }

    /// The primary seat to use in a brand-new (empty) table for a shape.
    fn first_seat_of(shape: &SeatPolicy) -> Direction {
        match shape {
            SeatPolicy::OnePerSeat(seats) => seats[0],
            SeatPolicy::Pairs(sides) => sides[0].seats()[0],
            _ => Direction::South, // FirstFree / Manual-bulk
        }
    }

    /// The shape used for bulk/auto seating. Manual has no auto shape → treat
    /// it as FirstFree when the teacher explicitly seats the lobby.
    fn bulk_shape(&self) -> SeatPolicy {
        match self.seat_policy() {
            SeatPolicy::Manual => SeatPolicy::FirstFree,
            other => other,
        }
    }

    /// Seat `sub` at the first free `shape` seat in an existing table; if all
    /// are full, create a table and seat there. `shape` is a concrete auto
    /// policy (never Manual), so this always seats.
    async fn seat_one(&self, shape: &SeatPolicy, sub: &str, name: &str) -> (Arc<Room>, Direction) {
        let rooms = self.rooms_snapshot().await;
        if let Some(placed) = self.try_seat_existing(&rooms, shape, sub, name).await {
            return placed;
        }
        let room = self.add_room().await;
        let seat = Self::first_seat_of(shape);
        room.state.lock().await.try_seat(seat, sub, name);
        (room, seat)
    }

    /// Try to seat `sub` in `rooms` per `shape`; None if every table is full.
    async fn try_seat_existing(
        &self,
        rooms: &[Arc<Room>],
        shape: &SeatPolicy,
        sub: &str,
        name: &str,
    ) -> Option<(Arc<Room>, Direction)> {
        match shape {
            SeatPolicy::Manual => None,
            SeatPolicy::OnePerSeat(seats) => {
                let start = self.next_arrival.fetch_add(1, Ordering::SeqCst);
                self.seat_round_robin(rooms, start, seats, sub, name).await
            }
            SeatPolicy::Pairs(sides) => {
                // Fill a table's whole side before the next, so partners land
                // together.
                for room in rooms {
                    for side in sides {
                        for seat in side.seats() {
                            let mut inner = room.state.lock().await;
                            if inner.try_seat(seat, sub, name) {
                                return Some((room.clone(), seat));
                            }
                        }
                    }
                }
                None
            }
            SeatPolicy::FirstFree => {
                // South first — the traditional student seat.
                const FILL_ORDER: [Direction; 4] = [
                    Direction::South,
                    Direction::West,
                    Direction::North,
                    Direction::East,
                ];
                let start = self.next_arrival.fetch_add(1, Ordering::SeqCst);
                self.seat_round_robin(rooms, start, &FILL_ORDER, sub, name)
                    .await
            }
        }
    }

    /// Try `rooms` from `start % n`, taking the first free seat from `seats`.
    async fn seat_round_robin(
        &self,
        rooms: &[Arc<Room>],
        start: usize,
        seats: &[Direction],
        sub: &str,
        name: &str,
    ) -> Option<(Arc<Room>, Direction)> {
        let n = rooms.len();
        if n == 0 {
            return None;
        }
        for offset in 0..n {
            let idx = (start + offset) % n;
            let mut inner = rooms[idx].state.lock().await;
            for &seat in seats {
                if inner.try_seat(seat, sub, name) {
                    return Some((rooms[idx].clone(), seat));
                }
            }
        }
        None
    }

    /// Place an arriving participant. Reconnects rebind their existing seat; a
    /// returning lobby member keeps their table + `parked` flag (a reconnect
    /// never re-grabs a seat — sticky lobby). New arrivals auto-seat per policy
    /// (creating a table if needed) unless `wait_to_seat` / Manual holds them
    /// in the waiting lobby.
    pub async fn place(&self, sub: &str, name: &str) -> Placement {
        self.place_seat(sub, name, None).await
    }

    /// `place` with an optional requested seat (backs the `?seat=N` invite
    /// link, carried in `hello.seat`). Under Manual policy a free requested
    /// seat is taken directly instead of waiting; otherwise the request is
    /// ignored and normal placement applies.
    pub async fn place_seat(
        &self,
        sub: &str,
        name: &str,
        requested: Option<Direction>,
    ) -> Placement {
        // Serialize placement: simultaneous arrivals (e.g. spawned test tabs)
        // otherwise race to create tables / grab seats, scattering the fill.
        let _seat_guard = self.seating.lock().await;
        let rooms = self.rooms_snapshot().await;
        // 0. Friends-table resume: when the OWNER (re)opens an adhoc table, any
        //    seat still held by a DISCONNECTED occupant is a ghost from a prior
        //    session. Left alone it keeps blocking `first_free` — so fresh
        //    arrivals overflow to kibitz — and, if it's the owner's own old
        //    seat, the reconnect scan below would rebind the owner away from
        //    South. Free every disconnected seat (→ empty → bot) and sit the
        //    owner at South. Gated to Adhoc: classroom (TeacherSet) reconnects
        //    are untouched, since students there legitimately drop and return
        //    to their own seat.
        if self.kind == SessionKind::Adhoc && sub == self.owner_sub {
            for room in &rooms {
                let mut inner = room.state.lock().await;
                let ghosts: Vec<Direction> = inner
                    .seats
                    .iter()
                    .filter(|(_, o)| !o.connected)
                    .map(|(&s, _)| s)
                    .collect();
                for seat in ghosts {
                    inner.vacate(seat);
                }
            }
            let room = self.watch_table(&rooms).await;
            if room
                .state
                .lock()
                .await
                .try_seat(Direction::South, sub, name)
            {
                return Placement {
                    room,
                    seat: Some(Direction::South),
                };
            }
            // South is held by a live player — fall through to normal placement
            // (don't evict a connected human to seat the owner).
        }
        // 1. Reconnect: an existing seat always wins.
        for room in &rooms {
            let mut inner = room.state.lock().await;
            if let Some((&seat, _)) = inner.seats.iter().find(|(_, o)| o.sub == sub) {
                let occ = inner.seats.get_mut(&seat).unwrap();
                occ.connected = true;
                occ.disconnected_at = None;
                occ.name = name.to_string();
                return Placement {
                    room: room.clone(),
                    seat: Some(seat),
                };
            }
        }
        // 2. Returning lobby member keeps their table + parked state (never
        //    re-grabs a seat — sticky lobby).
        let returning = {
            let mut kib = self.kibitzers.lock().await;
            kib.get_mut(sub).map(|k| {
                k.name = name.to_string();
                k.room_id.clone()
            })
        };
        if let Some(room_id) = returning {
            let room = match room_id.and_then(|id| rooms.iter().find(|r| r.id == id).cloned()) {
                Some(r) => r,
                None => self.watch_table(&rooms).await,
            };
            return Placement { room, seat: None };
        }
        // 2.5. Name reclaim: a guest who lost their tab reconnects under a fresh
        //   sub (guest subs are random per mint). If a DISCONNECTED seat holds
        //   their display name, take it over instead of creating a duplicate —
        //   otherwise the same student shows up at two tables.
        for room in &rooms {
            let mut inner = room.state.lock().await;
            let stale = inner
                .seats
                .iter()
                .find(|(_, o)| !o.connected && o.name == name)
                .map(|(&s, _)| s);
            if let Some(seat) = stale {
                let occ = inner.seats.get_mut(&seat).unwrap();
                occ.sub = sub.to_string();
                occ.connected = true;
                occ.disconnected_at = None;
                return Placement {
                    room: room.clone(),
                    seat: Some(seat),
                };
            }
        }
        // 2.75. `?seat=` invite: under Manual policy, a free requested seat is
        //   taken directly (this backs the invite link). If it's occupied,
        //   fall through to the normal waiting-lobby path.
        let policy = self.seat_policy();
        if let (Some(req), SeatPolicy::Manual) = (requested, &policy) {
            if let Some((room, seat)) = self.try_seat_specific(req, sub, name).await {
                return Placement {
                    room,
                    seat: Some(seat),
                };
            }
        }
        // 3. New arrival: auto-seat unless waiting-room / manual.
        let auto =
            !self.wait_to_seat.load(Ordering::SeqCst) && !matches!(policy, SeatPolicy::Manual);
        if auto {
            let (room, seat) = self.seat_one(&policy, sub, name).await;
            return Placement {
                room,
                seat: Some(seat),
            };
        }
        // Waiting lobby: watch a table (create one if none yet). Not parked →
        // eligible for auto-fill / seat_students.
        let room = self.watch_table(&rooms).await;
        self.add_lobby(sub, name, Some(room.id.clone()), false)
            .await;
        Placement { room, seat: None }
    }

    /// A table for a lobby member to watch: the first existing table, or a
    /// freshly created one (so a waiter always has something to see).
    async fn watch_table(&self, rooms: &[Arc<Room>]) -> Arc<Room> {
        match rooms.first() {
            Some(r) => r.clone(),
            None => self.add_room().await,
        }
    }

    /// Seat `sub` at a specific `seat` in the first table (creating one if
    /// none exist). None if that seat is already taken. Backs the `?seat=`
    /// invite under Manual policy.
    async fn try_seat_specific(
        &self,
        seat: Direction,
        sub: &str,
        name: &str,
    ) -> Option<(Arc<Room>, Direction)> {
        let rooms = self.rooms_snapshot().await;
        let room = self.watch_table(&rooms).await;
        if room.state.lock().await.try_seat(seat, sub, name) {
            Some((room, seat))
        } else {
            None
        }
    }

    /// A lobby member disconnected: forget them (a rejoin re-places them).
    pub async fn remove_kibitzer(&self, sub: &str) {
        self.kibitzers.lock().await.remove(sub);
    }

    /// Register `sub` in the lobby. `parked` = benched (boot / drag-to-lobby →
    /// never auto-seated); not parked = waiting (auto-fill eligible).
    pub async fn add_lobby(&self, sub: &str, name: &str, room_id: Option<String>, parked: bool) {
        self.kibitzers.lock().await.insert(
            sub.to_string(),
            Kibitzer {
                name: name.to_string(),
                room_id,
                parked,
            },
        );
    }

    /// Seat every WAITING (non-parked) lobby member per the bulk shape,
    /// creating tables as needed. Returns the rooms whose seats changed.
    pub async fn seat_students(&self) -> Vec<Arc<Room>> {
        let _seat_guard = self.seating.lock().await;
        let waiting: Vec<(String, String)> = {
            let kib = self.kibitzers.lock().await;
            kib.iter()
                .filter(|(_, k)| !k.parked)
                .map(|(s, k)| (s.clone(), k.name.clone()))
                .collect()
        };
        let shape = self.bulk_shape();
        let mut changed: Vec<Arc<Room>> = Vec::new();
        for (sub, name) in waiting {
            let (room, _) = self.seat_one(&shape, &sub, &name).await;
            self.kibitzers.lock().await.remove(&sub);
            if !changed.iter().any(|r| r.id == room.id) {
                changed.push(room);
            }
        }
        changed
    }

    /// Host seat control (seat/token-addressed — the friends-table model).
    /// `table_id` selects the table (defaults to the first table when None,
    /// for the common single-table case). Dispatch by which fields are set:
    ///
    /// - **vacate**: `from = Some(seat)`, `target = None` — the occupant of
    ///   `from` becomes waiting (parked kibitzer, kept CONNECTED, not
    ///   disconnected), unless they still hold other seats.
    /// - **move / swap**: `from = Some(seat)`, `target = Some(seat)` — move
    ///   `from`'s occupant into `target`; if `target` is held by a different
    ///   connection the two SWAP.
    /// - **place / Sit / seat-a-waiter**: `token = Some(..)`,
    ///   `target = Some(seat)` — seat that token's connection at `target`
    ///   (how the host seats a waiter, or Sits itself with its own token).
    ///   A different occupant already at `target` is displaced to waiting.
    ///   The connection MAY thereby hold multiple seats.
    ///
    /// Returns the rooms whose seats changed (for seat_update); the caller
    /// broadcasts roster_update + RECHECK + lobby refresh. A displaced live
    /// human is **parked** (never auto-reseated).
    pub async fn assign_seat(
        &self,
        table_id: Option<&str>,
        target: Option<Direction>,
        from: Option<Direction>,
        token: Option<&str>,
        seat_bot: bool,
    ) -> Result<Vec<Arc<Room>>, String> {
        let rooms = self.rooms_snapshot().await;
        let room = match table_id {
            Some(id) => rooms
                .iter()
                .find(|r| r.id == id)
                .cloned()
                .ok_or_else(|| format!("unknown table {id}"))?,
            None => rooms.first().cloned().ok_or("no tables in session")?,
        };

        match (from, target, token) {
            // ── vacate → empty (open, no bot) OR bot ────────────────────────
            // `seat_bot=false` (Remove) leaves the seat EMPTY — the table pauses
            // on its turn until someone sits; `seat_bot=true` (Seat a bot) drops
            // a bot in. Any human occupant is benched to the kibitz list.
            (Some(f), None, _) => {
                let (removed, changed) = {
                    let mut inner = room.state.lock().await;
                    let was_empty = inner.no_bot.contains(&f);
                    let removed = inner.vacate(f);
                    if seat_bot {
                        inner.set_bot(f);
                    } else {
                        inner.set_empty(f);
                    }
                    // A change if we removed a human OR toggled bot↔empty.
                    let changed = removed.is_some() || was_empty != inner.no_bot.contains(&f);
                    (removed, changed)
                };
                if let Some(occ) = removed {
                    // Still seated elsewhere (multi-seat)? Then not a waiter.
                    if occ.connected && self.seats_of(&occ.sub).await.is_empty() {
                        self.add_lobby(&occ.sub, &occ.name, Some(room.id.clone()), true)
                            .await;
                    }
                }
                Ok(if changed { vec![room] } else { vec![] })
            }
            // ── move / swap ─────────────────────────────────────────────────
            (Some(f), Some(t), _) => {
                if f == t {
                    return Ok(vec![]);
                }
                let mut inner = room.state.lock().await;
                let Some(from_occ) = inner.seats.get(&f).cloned() else {
                    return Err(format!("seat {f:?} is empty"));
                };
                match inner.seats.get(&t).cloned() {
                    Some(target_occ) => {
                        // Swap the two occupants.
                        inner.vacate(f);
                        inner.vacate(t);
                        inner.try_place(t, from_occ);
                        inner.try_place(f, target_occ);
                    }
                    None => {
                        inner.vacate(f);
                        inner.try_place(t, from_occ);
                    }
                }
                drop(inner);
                Ok(vec![room])
            }
            // ── place / Sit / seat-a-waiter (by token) ──────────────────────
            (None, Some(t), Some(tok)) => {
                let Some((sub, name)) = self.conn_meta(tok).await else {
                    return Err(format!("unknown connection token {tok}"));
                };
                // Already this connection at the target? No-op.
                if room
                    .state
                    .lock()
                    .await
                    .seats
                    .get(&t)
                    .is_some_and(|o| o.sub == sub)
                {
                    return Ok(vec![]);
                }
                // Being seated → no longer a waiter.
                self.kibitzers.lock().await.remove(&sub);
                // Displace any different occupant of the target to waiting.
                let displaced = {
                    let mut inner = room.state.lock().await;
                    let d = if inner.seats.contains_key(&t) {
                        inner.vacate(t)
                    } else {
                        None
                    };
                    inner.try_place(
                        t,
                        crate::rooms::Occupant {
                            sub: sub.clone(),
                            name: name.clone(),
                            connected: true,
                            disconnected_at: None,
                        },
                    );
                    d
                };
                if let Some(d) = displaced {
                    if d.connected && self.seats_of(&d.sub).await.is_empty() {
                        self.add_lobby(&d.sub, &d.name, Some(room.id.clone()), true)
                            .await;
                    }
                }
                Ok(vec![room])
            }
            _ => Err("assign_seat needs `from`, or `token`+`seat`".into()),
        }
    }

    /// Uninvite/kick: remove a person from the table ENTIRELY (distinct from
    /// Remove, which only benches them to the kibitz list). Vacate every seat
    /// they hold (→ bot, so the game continues), drop them from the kibitzers
    /// and connection registries, and signal their socket(s) to close. Returns
    /// the rooms whose seats changed. Addressed by connection `token`.
    pub async fn kick(&self, token: &str) -> Result<Vec<Arc<Room>>, String> {
        let Some((sub, _name)) = self.conn_meta(token).await else {
            return Err(format!("unknown connection token {token}"));
        };
        let mut changed = vec![];
        for room in self.rooms_snapshot().await {
            let held: Vec<Direction> = {
                let inner = room.state.lock().await;
                inner
                    .seats
                    .iter()
                    .filter(|(_, o)| o.sub == sub)
                    .map(|(&s, _)| s)
                    .collect()
            };
            if held.is_empty() {
                continue;
            }
            let mut inner = room.state.lock().await;
            for seat in held {
                inner.vacate(seat);
                inner.set_bot(seat); // kicked seat continues as a bot
            }
            drop(inner);
            changed.push(room);
        }
        // Off the table entirely: not a waiter, not a live connection.
        self.kibitzers.lock().await.remove(&sub);
        self.connections.lock().await.retain(|_, m| m.sub != sub);
        // Tell the kicked socket(s) to hang up (see the KICK: handler in ws.rs).
        let _ = self.notify.send(format!("KICK:{sub}"));
        Ok(changed)
    }

    /// PassBot: set which sides' BOT seats always pass in the auction (empty =
    /// none). Applies to every table in the session. Cardplay is unaffected.
    pub async fn set_pass_sides(&self, sides: std::collections::HashSet<Side>) {
        for room in self.rooms_snapshot().await {
            room.state.lock().await.pass_sides = sides.clone();
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
        mode: Option<crate::rooms::BoardMode>,
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
        // Optional board mode carried in with the set (the client's "play the
        // hand after bidding" choice). Absent → leave the current mode as-is, so
        // reloading a set never silently flips a mode the teacher already chose.
        if let Some(mode) = mode {
            self.set_board_mode(mode).await;
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
        for room in self.rooms_snapshot().await {
            let room = &room;
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
        let rooms = self.rooms_snapshot().await;
        let mut tables = Vec::with_capacity(rooms.len());
        for room in &rooms {
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
                        "table_id": k.room_id,
                        "parked": k.parked,
                    })
                })
                .collect()
        };
        // The shared, token-keyed roster (also on every welcome/snapshot and
        // the roster_update event) so the host console can drag seats.
        let roster = self.roster_json().await;
        json!({
            "t": "lobby",
            "session_id": self.id,
            // Reconnect status: a teacher rejoining renders
            // "‹set_label› — board {index} of {total}" straight from here.
            "set_label": set_label,
            "loaded": total > 0,
            "wait_to_seat": self.wait_to_seat.load(Ordering::SeqCst),
            "seat_policy": self.seat_policy().key(),
            "bot_mode": self.bot_mode().as_str(),
            "board_mode": self.board_mode().as_str(),
            "bots_paused": self.bots_paused(),
            "boards": {
                "total": total,
                "open": self.open_boards.load(Ordering::SeqCst),
                "index": cur_board,
            },
            "tables": tables,
            "kibitzers": kibitzers,
            "roster": roster,
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

    /// 0-based table index from a placement's room id ("s1-t3" → 2).
    fn tidx(p: &Placement) -> usize {
        p.room
            .id
            .rsplit("-t")
            .next()
            .unwrap()
            .parse::<usize>()
            .unwrap()
            - 1
    }
    async fn room_at(s: &Session, i: usize) -> Arc<Room> {
        s.rooms.read().await[i].clone()
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
        assert_eq!((tidx(&p1), p1.seat), (0, Some(South)));
        assert_eq!((tidx(&p2), p2.seat), (1, Some(South)));
        // Policy seats full across both tables: a third table is auto-created.
        let p3 = s.place("g3", "Cyd").await;
        assert_eq!((tidx(&p3), p3.seat), (2, Some(South)));
        assert_eq!(s.table_count().await, 3);
        // Reconnect rebinds the same seat.
        let p1b = s.place("g1", "Ann").await;
        assert_eq!((tidx(&p1b), p1b.seat), (0, Some(South)));
    }

    #[tokio::test]
    async fn pairs_fills_a_side_per_table() {
        let s = session(SessionKind::Adhoc, SeatPolicy::Pairs(vec![Side::NS]), 2);
        let p1 = s.place("g1", "Ann").await;
        let p2 = s.place("g2", "Ben").await;
        let p3 = s.place("g3", "Cyd").await;
        let p4 = s.place("g4", "Dee").await;
        assert_eq!((tidx(&p1), p1.seat), (0, Some(South)));
        assert_eq!((tidx(&p2), p2.seat), (0, Some(North)));
        assert_eq!((tidx(&p3), p3.seat), (1, Some(South)));
        assert_eq!((tidx(&p4), p4.seat), (1, Some(North)));
        // Both NS sides full → a third table is created for the 5th arrival.
        let p5 = s.place("g5", "Eve").await;
        assert_eq!((tidx(&p5), p5.seat), (2, Some(South)));
    }

    #[tokio::test]
    async fn manual_policy_kibitzes_until_assigned() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::Manual, 2);
        let p1 = s.place("g1", "Ann").await;
        assert_eq!(p1.seat, None);
        assert!(s.kibitzers.lock().await.contains_key("g1"));
        // Simulate teacher assignment, then a reconnect rebinds the seat.
        {
            let r = room_at(&s, 1).await;
            let mut inner = r.state.lock().await;
            assert!(inner.try_seat(South, "g1", "Ann"));
        }
        s.remove_kibitzer("g1").await;
        let p1b = s.place("g1", "Ann").await;
        assert_eq!((tidx(&p1b), p1b.seat), (1, Some(South)));
    }

    #[tokio::test]
    async fn first_free_round_robins_tables() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 2);
        let p1 = s.place("g1", "Ann").await;
        let p2 = s.place("g2", "Ben").await;
        let p3 = s.place("g3", "Cyd").await;
        assert_eq!((tidx(&p1), p1.seat), (0, Some(South)));
        assert_eq!((tidx(&p2), p2.seat), (1, Some(South)));
        assert_eq!((tidx(&p3), p3.seat), (0, Some(West)));
    }

    #[tokio::test]
    async fn adhoc_owner_resume_frees_ghosts_and_takes_south() {
        // Prior session left three DISCONNECTED occupants holding S/W/N (the
        // owner's own stale seat among them). On the owner reopening the table,
        // all ghosts free (→ bot) and the owner lands at South.
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        {
            let r = room_at(&s, 0).await;
            let mut inner = r.state.lock().await;
            assert!(inner.try_seat(South, "owner-1", "Host")); // owner's old seat
            assert!(inner.try_seat(West, "g2", "Ben"));
            assert!(inner.try_seat(North, "g3", "Cyd"));
            inner.mark_disconnected("owner-1");
            inner.mark_disconnected("g2");
            inner.mark_disconnected("g3");
        }
        let p = s.place("owner-1", "Host").await;
        assert_eq!((tidx(&p), p.seat), (0, Some(South)));
        let r = room_at(&s, 0).await;
        let inner = r.state.lock().await;
        // Only the owner remains seated; the ghosts are gone (bot-driven).
        assert_eq!(inner.seats.len(), 1);
        assert!(inner.seats.get(&South).is_some_and(|o| o.sub == "owner-1"));
        // A fresh arrival now seats (previously blocked by ghosts).
        drop(inner);
        let a = s.place("g9", "New").await;
        assert_eq!((tidx(&a), a.seat), (0, Some(West)));
    }

    #[tokio::test]
    async fn teacher_set_reconnect_keeps_own_seat_not_forced_south() {
        // The Adhoc owner-resume sweep must NOT touch classroom sessions: a
        // student who disconnected from North reconnects to North, not South,
        // and other disconnected students keep their seats.
        let s = session(SessionKind::TeacherSet, SeatPolicy::Manual, 1);
        {
            let r = room_at(&s, 0).await;
            let mut inner = r.state.lock().await;
            assert!(inner.try_seat(North, "owner-1", "Teach"));
            assert!(inner.try_seat(East, "g2", "Ben"));
            inner.mark_disconnected("owner-1");
            inner.mark_disconnected("g2");
        }
        let p = s.place("owner-1", "Teach").await;
        assert_eq!((tidx(&p), p.seat), (0, Some(North)));
        let r = room_at(&s, 0).await;
        let inner = r.state.lock().await;
        assert!(inner.seats.get(&East).is_some_and(|o| o.sub == "g2"));
    }

    #[tokio::test]
    async fn teacher_set_is_lockstep_not_self_advancing() {
        let s = session(
            SessionKind::TeacherSet,
            SeatPolicy::OnePerSeat(vec![South]),
            2,
        );
        s.place("g1", "Ann").await; // t0 South
        let room = room_at(&s, 0).await;

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
        for room in s.rooms_snapshot().await {
            let inner = room.state.lock().await;
            assert_eq!(inner.board_index, 1);
            assert_eq!(inner.table.board.number, 2);
            assert!(inner.ready.is_empty());
            assert_eq!(inner.table.seq(), 0, "fresh action log on the new board");
        }

        // And can back everyone up (Shark redo).
        assert_eq!(s.prev_board().await, 1);
        assert_eq!(room_at(&s, 0).await.state.lock().await.board_index, 0);
        assert_eq!(room_at(&s, 1).await.state.lock().await.board_index, 0);

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
                Some("Scenarios - Transfers".into()),
                None
            )
            .await
            .unwrap(),
            2
        );
        assert!(s.loaded().await);
        let (label, cur, total) = s.deck_status().await;
        assert_eq!(label.as_deref(), Some("Scenarios - Transfers"));
        assert_eq!((cur, total), (1, 2));
        for room in s.rooms_snapshot().await {
            assert_eq!(room.state.lock().await.table.board.number, 1);
        }

        // Move forward, then load a fresh set → pointer resets to board 1.
        s.next_board().await;
        assert_eq!(s.deck_status().await.1, 2);
        s.load_boards(parse_boards(TWO_BOARDS).unwrap(), Some("Mix".into()), None)
            .await
            .unwrap();
        assert_eq!(s.deck_status().await.1, 1, "load resets the pointer");

        // An empty load is rejected.
        assert!(s.load_boards(vec![], None, None).await.is_err());
    }

    #[tokio::test]
    async fn goto_clamps_and_moves_every_table() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::FirstFree, 2);
        assert_eq!(s.goto_board(99).await, 2, "clamped to the last board");
        for room in s.rooms_snapshot().await {
            assert_eq!(room.state.lock().await.board_index, 1);
        }
        assert_eq!(s.goto_board(1).await, 1);
        for room in s.rooms_snapshot().await {
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
            room_at(&s, 0)
                .await
                .state
                .lock()
                .await
                .table
                .board
                .deal
                .total_cards(),
            52
        );
        // No set → nothing advances, even forced.
        assert!(!s.try_advance(&room_at(&s, 0).await, true).await);
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
        let room = room_at(&s, 0).await;
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
        let room = room_at(&s, 0).await;
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
        let room = room_at(&s, 0).await;
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
        let room = room_at(&s, 0).await;
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

    fn ids(rooms: &[Arc<Room>]) -> Vec<String> {
        rooms.iter().map(|r| r.id.clone()).collect()
    }
    async fn seated_at(s: &Session, sub: &str) -> Option<(String, Direction)> {
        s.find_seated(sub)
            .await
            .map(|(r, seat)| (r.id.clone(), seat))
    }

    #[tokio::test]
    async fn assign_seat_places_by_token_moves_and_swaps() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::Manual, 1);
        s.register_conn("tok1", "g1", "Ann").await;
        s.place("g1", "Ann").await; // waiting (manual)

        // Place-by-token: seat the waiter's connection at South.
        let changed = s
            .assign_seat(Some("s1-t1"), Some(South), None, Some("tok1"), false)
            .await
            .unwrap();
        assert_eq!(ids(&changed), vec!["s1-t1"]);
        assert_eq!(seated_at(&s, "g1").await, Some(("s1-t1".into(), South)));
        assert!(
            !s.kibitzers.lock().await.contains_key("g1"),
            "no longer waiting"
        );

        // Move South → North (target empty).
        s.assign_seat(Some("s1-t1"), Some(North), Some(South), None, false)
            .await
            .unwrap();
        assert_eq!(seated_at(&s, "g1").await, Some(("s1-t1".into(), North)));

        // A second connection seated at South, then move North→South SWAPS.
        s.register_conn("tok2", "g2", "Ben").await;
        s.assign_seat(Some("s1-t1"), Some(South), None, Some("tok2"), false)
            .await
            .unwrap();
        s.assign_seat(Some("s1-t1"), Some(South), Some(North), None, false)
            .await
            .unwrap();
        assert_eq!(seated_at(&s, "g1").await, Some(("s1-t1".into(), South)));
        assert_eq!(seated_at(&s, "g2").await, Some(("s1-t1".into(), North)));

        // Unknown token rejected; missing from+token+target rejected.
        assert!(s
            .assign_seat(Some("s1-t1"), Some(East), None, Some("nope"), false)
            .await
            .is_err());
        assert!(s
            .assign_seat(Some("s1-t1"), None, None, None, false)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn roster_drops_disconnected_seatless_connections() {
        // A disconnected connection that still holds a seat stays (greyed
        // zombie seat); a disconnected connection with no seat is dropped so a
        // same-name rejoin doesn't pile up duplicate ghost kibitzers.
        let s = session(SessionKind::Adhoc, SeatPolicy::Manual, 1);
        s.register_conn("seated", "g1", "Doc").await;
        s.register_conn("ghost", "g2", "Doc").await; // stale, seatless
        s.register_conn("live", "g3", "Snow").await; // connected waiter
        s.assign_seat(Some("s1-t1"), Some(North), None, Some("seated"), false)
            .await
            .unwrap();
        // 'seated' loses its socket but keeps seat N; 'ghost' loses its socket
        // with no seat.
        s.mark_conn_disconnected("seated").await;
        s.mark_conn_disconnected("ghost").await;

        let roster = s.roster_json().await;
        let tokens: Vec<&str> = roster
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["token"].as_str().unwrap())
            .collect();
        assert!(tokens.contains(&"seated"), "seated ghost stays (holds N)");
        assert!(tokens.contains(&"live"), "connected waiter stays");
        assert!(
            !tokens.contains(&"ghost"),
            "disconnected seatless conn is dropped"
        );
    }

    #[tokio::test]
    async fn adhoc_uses_fast_takeover_grace_teacher_set_default() {
        use crate::rooms::TakeoverGrace;
        let adhoc = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        let g = room_at(&adhoc, 0).await.state.lock().await.takeover;
        assert_eq!(g.on_turn, TakeoverGrace::FAST.on_turn);
        assert_eq!(g.off_turn, TakeoverGrace::FAST.off_turn);
        // add_room path inherits it too.
        let r2 = adhoc.add_room().await;
        assert_eq!(
            r2.state.lock().await.takeover.on_turn,
            TakeoverGrace::FAST.on_turn
        );

        let cls = session(SessionKind::TeacherSet, SeatPolicy::Manual, 1);
        let gc = room_at(&cls, 0).await.state.lock().await.takeover;
        assert_eq!(gc.on_turn, TakeoverGrace::DEFAULT.on_turn);
        assert_eq!(gc.off_turn, TakeoverGrace::DEFAULT.off_turn);
    }

    #[tokio::test]
    async fn remove_leaves_seat_empty_seat_a_bot_toggles() {
        use std::time::Instant;
        let s = session(SessionKind::Adhoc, SeatPolicy::Manual, 1);
        s.register_conn("tok1", "g1", "Ann").await;
        s.assign_seat(Some("s1-t1"), Some(South), None, Some("tok1"), false)
            .await
            .unwrap();
        // Remove (seat_bot=false) → seat is EMPTY (open, no bot): nobody drives
        // it, so the table pauses on its turn.
        s.assign_seat(Some("s1-t1"), None, Some(South), None, false)
            .await
            .unwrap();
        {
            let r = room_at(&s, 0).await;
            let inner = r.state.lock().await;
            assert!(inner.no_bot.contains(&South));
            assert!(!inner.seat_bot_controlled(South, true, Instant::now()));
            let seats = inner.seats_json();
            assert_eq!(seats["S"]["kind"], "empty");
        }
        // Seat a bot (seat_bot=true) → seat is a bot again.
        s.assign_seat(Some("s1-t1"), None, Some(South), None, true)
            .await
            .unwrap();
        {
            let r = room_at(&s, 0).await;
            let inner = r.state.lock().await;
            assert!(!inner.no_bot.contains(&South));
            assert!(inner.seat_bot_controlled(South, true, Instant::now()));
            assert_eq!(inner.seats_json()["S"]["kind"], "bot");
        }
    }

    #[tokio::test]
    async fn kick_removes_person_entirely_seat_becomes_bot() {
        let s = session(SessionKind::Adhoc, SeatPolicy::Manual, 1);
        s.register_conn("tok1", "g1", "Ann").await;
        s.assign_seat(Some("s1-t1"), Some(South), None, Some("tok1"), false)
            .await
            .unwrap();
        let changed = s.kick("tok1").await.unwrap();
        assert_eq!(ids(&changed), vec!["s1-t1"]);
        // Seat freed → bot (game continues); person gone from conns + kibitzers.
        let r = room_at(&s, 0).await;
        let inner = r.state.lock().await;
        assert!(!inner.seats.contains_key(&South));
        assert!(!inner.no_bot.contains(&South));
        assert_eq!(inner.seats_json()["S"]["kind"], "bot");
        drop(inner);
        assert!(s.conn_meta("tok1").await.is_none());
        assert!(!s.kibitzers.lock().await.contains_key("g1"));
    }

    #[tokio::test]
    async fn set_pass_sides_stores_per_room() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        let mut sides = std::collections::HashSet::new();
        sides.insert(Side::EW);
        s.set_pass_sides(sides).await;
        let r = room_at(&s, 0).await;
        let inner = r.state.lock().await;
        assert!(inner.pass_sides.contains(&Side::EW));
        assert!(!inner.pass_sides.contains(&Side::NS));
        // Side::of maps seats to sides for the bidding gate.
        assert_eq!(Side::of(East), Side::EW);
        assert_eq!(Side::of(North), Side::NS);
        assert_eq!(inner.pass_sides_json(), serde_json::json!(["EW"]));
    }

    #[tokio::test]
    async fn one_connection_can_hold_multiple_seats() {
        let s = session(SessionKind::Adhoc, SeatPolicy::Manual, 1);
        s.register_conn("tok1", "g1", "Ann").await;
        s.assign_seat(Some("s1-t1"), Some(South), None, Some("tok1"), false)
            .await
            .unwrap();
        s.assign_seat(Some("s1-t1"), Some(North), None, Some("tok1"), false)
            .await
            .unwrap();
        let mut seats = s.seats_of("g1").await;
        seats.sort_by_key(|d| d.to_char());
        assert_eq!(seats, vec![North, South]);
        // The roster shows both seats under the single token.
        let roster = s.roster_json().await;
        let entry = roster
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["token"] == "tok1")
            .unwrap();
        assert_eq!(entry["seats"].as_array().unwrap().len(), 2);
        assert_eq!(entry["name"], "Ann");
        assert_eq!(entry["connected"], true);
    }

    #[tokio::test]
    async fn vacate_parks_the_human_as_waiter() {
        let s = session(
            SessionKind::TeacherSet,
            SeatPolicy::OnePerSeat(vec![South]),
            1,
        );
        s.place("g1", "Ann").await;
        {
            // A ready flag must not survive the vacate.
            room_at(&s, 0).await.state.lock().await.ready.insert(South);
        }
        // Vacate = from set, target None.
        let changed = s
            .assign_seat(Some("s1-t1"), None, Some(South), None, false)
            .await
            .unwrap();
        assert_eq!(ids(&changed), vec!["s1-t1"]);
        assert_eq!(s.find_seated("g1").await.map(|(_, seat)| seat), None);
        // Vacated → parked in the lobby (never auto-reseated).
        assert_eq!(
            s.kibitzers.lock().await.get("g1").map(|k| k.parked),
            Some(true)
        );
        let r = room_at(&s, 0).await;
        let inner = r.state.lock().await;
        assert!(inner.seats.is_empty());
        assert!(inner.ready.is_empty());
        drop(inner);
        // Vacating an empty seat is a no-op.
        assert!(s
            .assign_seat(Some("s1-t1"), None, Some(South), None, false)
            .await
            .unwrap()
            .is_empty());
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
        assert_eq!(kib[0]["parked"], false);
    }

    // ── Dynamic tables + lobby (Phase 3.2) ──────────────────────────────────

    #[tokio::test]
    async fn zero_table_session_auto_creates_on_arrival() {
        let s = Session::new(
            "s1".into(),
            SessionKind::Adhoc,
            "o".into(),
            SeatPolicy::FirstFree,
            parse_boards(TWO_BOARDS).unwrap(),
            0,
        );
        assert_eq!(s.table_count().await, 0);
        let p = s.place("g1", "Ann").await;
        assert_eq!((tidx(&p), p.seat), (0, Some(South)));
        assert_eq!(p.room.id, "s1-t1");
        assert_eq!(s.table_count().await, 1);
    }

    #[tokio::test]
    async fn source_settable_on_zero_tables_new_table_adopts_board() {
        // Idle 0-table session: set the source with no tables, then a student
        // arrives and their auto-created table is on the loaded board.
        let s = Session::new(
            "s1".into(),
            SessionKind::Adhoc,
            "o".into(),
            SeatPolicy::FirstFree,
            vec![],
            0,
        );
        s.load_boards(parse_boards(TWO_BOARDS).unwrap(), Some("Set".into()), None)
            .await
            .unwrap();
        assert_eq!(s.table_count().await, 0, "loading a source makes no tables");
        let p = s.place("g1", "Ann").await;
        assert_eq!(s.table_count().await, 1);
        assert_eq!(p.room.state.lock().await.table.board.number, 1);
    }

    #[tokio::test]
    async fn wait_to_seat_holds_then_seat_students() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        s.set_wait_to_seat(true);
        // Arrivals wait in the lobby (not seated, not parked).
        let p = s.place("g1", "Ann").await;
        assert_eq!(p.seat, None);
        assert_eq!(
            s.kibitzers.lock().await.get("g1").map(|k| k.parked),
            Some(false)
        );
        s.place("g2", "Ben").await;
        assert!(s.find_seated("g1").await.is_none());
        // Teacher seats the waiting lobby.
        let changed = s.seat_students().await;
        assert!(!changed.is_empty());
        assert!(s.find_seated("g1").await.is_some());
        assert!(s.find_seated("g2").await.is_some());
        assert!(s.kibitzers.lock().await.is_empty(), "lobby drained");
    }

    #[tokio::test]
    async fn parked_member_is_not_auto_seated() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        s.place("g1", "Ann").await; // South
        s.assign_seat(Some("s1-t1"), None, Some(South), None, false)
            .await
            .unwrap(); // vacate → parked
        assert_eq!(
            s.kibitzers.lock().await.get("g1").map(|k| k.parked),
            Some(true)
        );
        // seat_students must NOT re-seat a parked member.
        let changed = s.seat_students().await;
        assert!(changed.is_empty());
        assert!(s.find_seated("g1").await.is_none());
        assert!(
            s.kibitzers.lock().await.contains_key("g1"),
            "still parked in the lobby"
        );
    }

    #[tokio::test]
    async fn add_tables_grows_the_session() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        assert_eq!(s.add_tables(3).await, 4);
        assert_eq!(
            ids(&s.rooms_snapshot().await),
            vec!["s1-t1", "s1-t2", "s1-t3", "s1-t4"]
        );
    }

    #[tokio::test]
    async fn concurrent_arrivals_fill_deterministically() {
        // 4 students connecting at once (spawned tabs) must fill ONE table
        // S/W/N/E, not race into scattered tables (the seating lock).
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 0);
        let (p0, p1, p2, p3) = tokio::join!(
            s.place("g0", "A"),
            s.place("g1", "B"),
            s.place("g2", "C"),
            s.place("g3", "D"),
        );
        assert_eq!(s.table_count().await, 1, "one table, not a scatter");
        let mut seats: Vec<char> = [&p0, &p1, &p2, &p3]
            .iter()
            .map(|p| p.seat.unwrap().to_char())
            .collect();
        seats.sort_unstable();
        assert_eq!(seats, vec!['E', 'N', 'S', 'W'], "all four distinct seats");
    }

    #[tokio::test]
    async fn set_seat_policy_changes_auto_seat_live() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        // Switch to one-per-table (South) live.
        s.set_seat_policy(SeatPolicy::from_key("one_per_south").unwrap());
        assert_eq!(s.seat_policy().key(), "one_per_south");
        let p1 = s.place("g1", "A").await;
        let p2 = s.place("g2", "B").await;
        assert_eq!((tidx(&p1), p1.seat), (0, Some(South)));
        // Second student → a new table (one per table).
        assert_eq!((tidx(&p2), p2.seat), (1, Some(South)));
        assert!(SeatPolicy::from_key("nope").is_none());
    }

    #[tokio::test]
    async fn pause_and_resume_bots() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        assert!(!s.bots_paused());
        s.set_bots_paused(true).await;
        assert!(s.bots_paused());
        s.set_bots_paused(false).await;
        assert!(!s.bots_paused());
    }

    #[tokio::test]
    async fn disconnected_guest_reclaims_seat_by_name() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        let p = s.place("g1", "Bob").await; // South
        assert_eq!(p.seat, Some(South));
        // Bob's tab drops → seat goes disconnected.
        room_at(&s, 0)
            .await
            .state
            .lock()
            .await
            .mark_disconnected("g1");
        // Bob reopens with a fresh guest sub + same name → reclaims his seat,
        // no duplicate table/seat.
        let p2 = s.place("g2-new", "Bob").await;
        assert_eq!((tidx(&p2), p2.seat), (0, Some(South)));
        assert_eq!(s.table_count().await, 1, "no duplicate table");
        assert_eq!(seated_at(&s, "g2-new").await, Some(("s1-t1".into(), South)));
        assert_eq!(seated_at(&s, "g1").await, None, "old sub replaced");
    }

    #[test]
    fn account_id_is_none_for_guests_and_the_sub_otherwise() {
        // Guests cannot be friended, so they must not surface an account id.
        assert_eq!(
            account_id_of("guest-2f1c9e10-0000-4000-8000-000000000001"),
            None
        );
        // A registered subject IS the classroom users.id.
        assert_eq!(
            account_id_of("2f1c9e10-0000-4000-8000-000000000002"),
            Some("2f1c9e10-0000-4000-8000-000000000002")
        );
    }

    #[tokio::test]
    async fn roster_exposes_account_id_for_members_and_null_for_guests() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        s.register_conn("tok-member", "u-member", "Ann Alpha").await;
        s.register_conn("tok-guest", "guest-abc", "Bob S.").await;

        let roster = s.roster_json().await;
        let entries = roster.as_array().expect("roster is an array");
        let find = |name: &str| {
            entries
                .iter()
                .find(|e| e["name"] == name)
                .unwrap_or_else(|| panic!("{name} missing from roster"))
                .clone()
        };

        assert_eq!(find("Ann Alpha")["account_id"], "u-member");
        assert!(
            find("Bob S.")["account_id"].is_null(),
            "a guest must not expose an account id"
        );
    }

    #[tokio::test]
    async fn set_bot_mode_applies_and_surfaces() {
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        s.set_bot_mode(crate::rooms::BotMode::Random).await;
        assert_eq!(s.bot_mode().as_str(), "random");
        assert_eq!(
            room_at(&s, 0).await.bot_mode().as_str(),
            "random",
            "existing table updated"
        );
        // New tables inherit it.
        let r = s.add_room().await;
        assert_eq!(r.bot_mode().as_str(), "random");
    }

    #[tokio::test]
    async fn load_boards_carries_board_mode_and_new_tables_inherit() {
        use crate::rooms::BoardMode;
        let s = session(SessionKind::Adhoc, SeatPolicy::FirstFree, 1);
        assert_eq!(
            room_at(&s, 0).await.state.lock().await.board_mode,
            BoardMode::BidAndPlay,
            "default is bid-and-play"
        );
        // Loading a set WITH a mode (the client's "play the hand" choice)
        // applies it to every existing table…
        s.load_boards(
            parse_boards(TWO_BOARDS).unwrap(),
            Some("Set".into()),
            Some(BoardMode::BidOnly),
        )
        .await
        .unwrap();
        assert_eq!(s.board_mode(), BoardMode::BidOnly);
        assert_eq!(
            room_at(&s, 0).await.state.lock().await.board_mode,
            BoardMode::BidOnly,
            "existing table updated"
        );
        // …and new tables inherit it.
        let r = s.add_room().await;
        assert_eq!(r.state.lock().await.board_mode, BoardMode::BidOnly);
        // A later load WITHOUT a mode leaves the chosen mode untouched.
        s.load_boards(
            parse_boards(TWO_BOARDS).unwrap(),
            Some("Again".into()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            s.board_mode(),
            BoardMode::BidOnly,
            "absent mode keeps the current mode"
        );
    }
}
