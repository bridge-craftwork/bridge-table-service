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
        Arc::new_cyclic(|weak: &Weak<Session>| {
            let rooms: Vec<Arc<Room>> = (1..=table_count)
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
                rooms: tokio::sync::RwLock::new(rooms),
                weak_self: weak.clone(),
                next_table_no: AtomicUsize::new(table_count),
                wait_to_seat: std::sync::atomic::AtomicBool::new(false),
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
        let room = Room::new(
            format!("{}-t{}", self.id, n),
            board,
            Some(self.weak_self.clone()),
        );
        self.rooms.write().await.push(room.clone());
        crate::bots::ensure_keepalive(room.clone());
        crate::bots::kick(room.clone());
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
        match &self.seat_policy {
            SeatPolicy::Manual => SeatPolicy::FirstFree,
            other => other.clone(),
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
        let rooms = self.rooms_snapshot().await;
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
        // 3. New arrival: auto-seat unless waiting-room / manual.
        let auto = !self.wait_to_seat.load(Ordering::SeqCst)
            && !matches!(self.seat_policy, SeatPolicy::Manual);
        if auto {
            let (room, seat) = self.seat_one(&self.seat_policy, sub, name).await;
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

    /// Teacher seat control. `sub = None` vacates the seat (boot); a booted
    /// connected human becomes a kibitzer of the same table. `sub = Some`
    /// moves a known participant (seated anywhere in the session, or
    /// kibitzing) into the seat, preserving their connected state.
    /// Returns the rooms whose seats changed (for seat_update broadcasts); the
    /// caller sends RECHECK + lobby refresh. A booted/displaced live human is
    /// **parked** in the lobby (never auto-reseated).
    pub async fn assign_seat(
        &self,
        table_id: &str,
        seat: Direction,
        sub: Option<&str>,
    ) -> Result<Vec<Arc<Room>>, String> {
        let rooms = self.rooms_snapshot().await;
        let Some(target) = rooms.iter().find(|r| r.id == table_id).cloned() else {
            return Err(format!("unknown table {table_id}"));
        };

        let Some(sub) = sub else {
            // Vacate (boot). The seat becomes a bot; a booted live human is
            // parked in the lobby watching this table.
            let removed = target.state.lock().await.vacate(seat);
            let Some(occ) = removed else {
                return Ok(vec![]); // already empty — nothing changed
            };
            if occ.connected {
                self.add_lobby(&occ.sub, &occ.name, Some(target.id.clone()), true)
                    .await;
            }
            return Ok(vec![target.clone()]);
        };

        // Fast pre-check so we don't yank someone out of their seat only to
        // find the target taken (the re-check at insert handles races).
        if target.state.lock().await.seats.contains_key(&seat) {
            return Err("seat is occupied — boot it first".into());
        }

        // Pull the participant out of wherever they are.
        let (occ, mut changed) = if let Some((old_room, old_seat)) = self.find_seated(sub).await {
            match old_room.state.lock().await.vacate(old_seat) {
                Some(occ) => (occ, vec![old_room.clone()]),
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
            changed.push(target.clone());
            Ok(changed)
        } else {
            // Lost the race for the seat: park them in the lobby so they aren't
            // dropped from the session.
            drop(inner);
            self.add_lobby(&occ.sub, &occ.name, Some(target.id.clone()), true)
                .await;
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
        json!({
            "t": "lobby",
            "session_id": self.id,
            // Reconnect status: a teacher rejoining renders
            // "‹set_label› — board {index} of {total}" straight from here.
            "set_label": set_label,
            "loaded": total > 0,
            "wait_to_seat": self.wait_to_seat.load(Ordering::SeqCst),
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
        for room in s.rooms_snapshot().await {
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
    async fn assign_seat_moves_kibitzers_and_seated_players() {
        let s = session(SessionKind::TeacherSet, SeatPolicy::Manual, 2);
        s.place("g1", "Ann").await; // kibitzer

        // Kibitzer → seat.
        let changed = s.assign_seat("s1-t2", South, Some("g1")).await.unwrap();
        assert_eq!(ids(&changed), vec!["s1-t2"]);
        assert_eq!(seated_at(&s, "g1").await, Some(("s1-t2".into(), South)));
        assert!(!s.kibitzers.lock().await.contains_key("g1"));

        // Seated → different table (both rooms report a change).
        let changed = s.assign_seat("s1-t1", North, Some("g1")).await.unwrap();
        assert_eq!(ids(&changed), vec!["s1-t2", "s1-t1"]);
        assert_eq!(seated_at(&s, "g1").await, Some(("s1-t1".into(), North)));

        // Occupied target rejected without unseating anyone.
        s.place("g2", "Ben").await; // kibitzer
        let err = s
            .assign_seat("s1-t1", North, Some("g2"))
            .await
            .err()
            .unwrap();
        assert!(err.contains("occupied"), "{err}");
        assert_eq!(seated_at(&s, "g1").await, Some(("s1-t1".into(), North)));

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
            room_at(&s, 0).await.state.lock().await.ready.insert(South);
        }
        let changed = s.assign_seat("s1-t1", South, None).await.unwrap();
        assert_eq!(ids(&changed), vec!["s1-t1"]);
        assert_eq!(s.find_seated("g1").await.map(|(_, seat)| seat), None);
        // Booted → parked in the lobby (never auto-reseated).
        assert_eq!(
            s.kibitzers.lock().await.get("g1").map(|k| k.parked),
            Some(true)
        );
        let r = room_at(&s, 0).await;
        let inner = r.state.lock().await;
        assert!(inner.seats.is_empty());
        assert!(inner.ready.is_empty());
        drop(inner);
        // Booting an empty seat is a no-op.
        assert!(s
            .assign_seat("s1-t1", South, None)
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
        s.load_boards(parse_boards(TWO_BOARDS).unwrap(), Some("Set".into()))
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
        s.assign_seat("s1-t1", South, None).await.unwrap(); // boot → parked
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
}
