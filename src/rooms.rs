//! In-memory table registry.
//!
//! Phase 0 shape: one process-wide registry of rooms; each room owns a
//! `TableState` behind a mutex plus a broadcast channel that fans server
//! events out to every connected socket. When bots and pacing land
//! (Phase 1), each room's mutation moves into a dedicated tokio task
//! (actor) and the mutex disappears; the registry and broadcast surface
//! stay as they are.
//!
//! State is deliberately in-memory only — v1 table sessions are ephemeral
//! (no persistence), per the project plan.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bridge_types::{Call, Direction};
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::sessions::Session;
use crate::table::{BoardSetup, TableState};

/// Zombie-seat policy: how long a disconnected occupant keeps their seat
/// human-driven before the bot driver takes over. The seat binding itself
/// is never released — the human reclaims control simply by reconnecting
/// (takeover is a mode check, not a re-seat).
pub const TAKEOVER_AFTER: Duration = Duration::from_secs(60);
/// Shorter grace when the table is blocked waiting on the disconnected
/// seat's turn.
pub const TAKEOVER_AFTER_ON_TURN: Duration = Duration::from_secs(20);

/// Internal broadcast marker telling every connection to send its own
/// per-viewer redacted snapshot (a broadcast can't carry a snapshot because
/// snapshots are per-viewer). Never forwarded to clients. Used after undo
/// (a rewind can re-hide info) and after the opening lead (dummy goes face
/// up for everyone).
pub const RESYNC: &str = "__resync__";

/// Which backend drives the room's bot seats. Real is the default; the
/// others are set via `"bot":"random"` / `"bot":"rules"` in a hello frame
/// and are sticky for the room's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum BotMode {
    /// BBA bidding + BEN cardplay, with bridge-rulebot as the cardplay
    /// fallback (Pass for bidding).
    #[default]
    Real = 0,
    /// Instant deterministic RandomLegal cardplay, Pass bidding. Testing.
    Random = 1,
    /// Instant rule-based cardplay (bridge-rulebot), Pass bidding — watch
    /// the rulebot play without BBA/BEN in the loop.
    Rules = 2,
}

impl BotMode {
    pub fn parse(s: &str) -> Option<BotMode> {
        match s {
            "random" => Some(BotMode::Random),
            "rules" => Some(BotMode::Rules),
            _ => None,
        }
    }

    pub fn from_u8(v: u8) -> BotMode {
        match v {
            1 => BotMode::Random,
            2 => BotMode::Rules,
            _ => BotMode::Real,
        }
    }

    /// Wire name, echoed in the welcome frame's `bot_mode`.
    pub fn as_str(&self) -> &'static str {
        match self {
            BotMode::Real => "real",
            BotMode::Random => "random",
            BotMode::Rules => "rules",
        }
    }
}

/// A seated (or recently seated) occupant.
#[derive(Debug, Clone)]
pub struct Occupant {
    /// Ticket subject (user or guest id).
    pub sub: String,
    pub name: String,
    pub connected: bool,
    /// When the occupant's socket dropped (None while connected). Drives
    /// the bot-takeover grace periods above.
    pub disconnected_at: Option<Instant>,
}

pub struct Room {
    pub id: String,
    pub state: Mutex<RoomInner>,
    /// Serialized ServerMsg JSON, fanned out to every socket in the room.
    pub events: broadcast::Sender<String>,
    /// True while a bot-driver task is live for this room (see bots::kick).
    pub bot_running: std::sync::atomic::AtomicBool,
    /// True once the per-room keepalive watchdog is spawned (see
    /// bots::ensure_keepalive) — it periodically re-kicks the driver so a
    /// disconnected seat's bot takeover happens without another human action.
    pub keepalive_running: std::sync::atomic::AtomicBool,
    /// Bot backend for this room, stored as `BotMode as u8` (see `BotMode`;
    /// read via `Room::bot_mode()`).
    pub bot_mode_raw: std::sync::atomic::AtomicU8,
    /// BBA's predicted auction for the current board (complete, from call
    /// 1). Bot calls are served from it while the actual auction remains a
    /// prefix; divergence or undo re-requests (see bots/bba.rs). Separate
    /// from `state` so an in-flight BBA request never blocks humans.
    pub bba_cache: Mutex<Option<Vec<Call>>>,
    /// Back-reference to the owning session (None for the demo room). Weak
    /// so removing a session from the registry actually frees it; used to
    /// push lobby refreshes to the teacher after any table change.
    pub session: Option<Weak<Session>>,
}

pub struct RoomInner {
    pub table: TableState,
    pub seats: HashMap<Direction, Occupant>,
    /// Index into the owning session's board list (always 0 for the demo
    /// room). The session's `try_advance` moves it forward.
    pub board_index: usize,
    /// Seats that sent `ready_next_board` for the current board. Cleared on
    /// advance and when a seat's occupant changes.
    pub ready: HashSet<Direction>,
}

impl Room {
    pub fn new(id: String, board: BoardSetup, session: Option<Weak<Session>>) -> Arc<Self> {
        let (events, _) = broadcast::channel(256);
        Arc::new(Self {
            id,
            state: Mutex::new(RoomInner {
                table: TableState::new(board),
                seats: HashMap::new(),
                board_index: 0,
                ready: HashSet::new(),
            }),
            events,
            bot_running: std::sync::atomic::AtomicBool::new(false),
            keepalive_running: std::sync::atomic::AtomicBool::new(false),
            bot_mode_raw: std::sync::atomic::AtomicU8::new(BotMode::Real as u8),
            bba_cache: Mutex::new(None),
            session,
        })
    }

    /// The owning session, if this room belongs to one (and it's alive).
    pub fn session(&self) -> Option<Arc<Session>> {
        self.session.as_ref().and_then(|w| w.upgrade())
    }

    /// The room's current bot backend.
    pub fn bot_mode(&self) -> BotMode {
        BotMode::from_u8(self.bot_mode_raw.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Switch the room's bot backend (sticky; see `BotMode`).
    pub fn set_bot_mode(&self, mode: BotMode) {
        self.bot_mode_raw
            .store(mode as u8, std::sync::atomic::Ordering::Relaxed);
    }

    /// Push a lobby refresh to the session's teacher connections (no-op for
    /// the demo room). Cheap; called after any state change worth showing
    /// on the teacher grid.
    pub fn notify_session_lobby(&self) {
        if let Some(s) = self.session() {
            s.notify_lobby();
        }
    }

    /// Broadcast a serialized event to everyone in the room. Errors (no
    /// receivers) are fine — an empty room still accepts actions.
    pub fn broadcast(&self, msg: String) {
        let _ = self.events.send(msg);
    }

    /// Ask every connection to rebuild and send its own redacted snapshot.
    pub fn broadcast_resync(&self) {
        self.broadcast(RESYNC.to_string());
    }

    /// A room on the demo board, for unit tests in other modules.
    #[cfg(test)]
    pub fn new_for_test(id: &str) -> Arc<Self> {
        Self::new(id.to_string(), demo_board(), None)
    }
}

impl RoomInner {
    /// Seat `sub` at the first free seat (S, W, N, E — South first so the
    /// solo learner lands in the traditional student seat). If the sub was
    /// already seated (reconnect), rebind that seat instead.
    pub fn seat_or_rebind(&mut self, sub: &str, name: &str) -> Option<Direction> {
        use Direction::*;
        if let Some((&seat, _)) = self.seats.iter().find(|(_, o)| o.sub == sub) {
            let occ = self.seats.get_mut(&seat).unwrap();
            occ.connected = true;
            occ.disconnected_at = None;
            occ.name = name.to_string();
            return Some(seat);
        }
        for seat in [South, West, North, East] {
            if let std::collections::hash_map::Entry::Vacant(e) = self.seats.entry(seat) {
                e.insert(Occupant {
                    sub: sub.to_string(),
                    name: name.to_string(),
                    connected: true,
                    disconnected_at: None,
                });
                return Some(seat);
            }
        }
        None // table full → kibitzer
    }

    /// Seat `sub` at `seat` iff it's vacant. Used by the seat policies and
    /// the teacher's assign_seat (which vacates first).
    pub fn try_seat(&mut self, seat: Direction, sub: &str, name: &str) -> bool {
        self.try_place(
            seat,
            Occupant {
                sub: sub.to_string(),
                name: name.to_string(),
                connected: true,
                disconnected_at: None,
            },
        )
    }

    /// Place a pre-built occupant (preserves connected/disconnected state
    /// when the teacher moves someone between seats) iff the seat is
    /// vacant. A newly placed occupant is never "ready".
    pub fn try_place(&mut self, seat: Direction, occ: Occupant) -> bool {
        if let std::collections::hash_map::Entry::Vacant(e) = self.seats.entry(seat) {
            e.insert(occ);
            self.ready.remove(&seat);
            true
        } else {
            false
        }
    }

    /// Vacate a seat (teacher boot / assign away). Returns the removed
    /// occupant. Also clears the seat's ready flag.
    pub fn vacate(&mut self, seat: Direction) -> Option<Occupant> {
        self.ready.remove(&seat);
        self.seats.remove(&seat)
    }

    pub fn mark_disconnected(&mut self, sub: &str) {
        for occ in self.seats.values_mut() {
            if occ.sub == sub {
                occ.connected = false;
                occ.disconnected_at = Some(Instant::now());
            }
        }
    }

    /// Zombie-seat policy: should the bot driver act for `seat` right now?
    /// True for an empty seat, and for a seat whose occupant has been
    /// disconnected past the grace period (`TAKEOVER_AFTER_ON_TURN` when the
    /// table is waiting on this seat, `TAKEOVER_AFTER` otherwise). The seat
    /// binding is untouched — a reconnect (seat_or_rebind) instantly returns
    /// control to the human. `now` is injected for testability.
    pub fn seat_bot_controlled(&self, seat: Direction, on_turn: bool, now: Instant) -> bool {
        let Some(occ) = self.seats.get(&seat) else {
            return true; // empty seat: always a bot
        };
        if occ.connected {
            return false;
        }
        let grace = if on_turn {
            TAKEOVER_AFTER_ON_TURN
        } else {
            TAKEOVER_AFTER
        };
        occ.disconnected_at
            .is_some_and(|t| now.duration_since(t) >= grace)
    }

    pub fn seats_json(&self) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for seat in Direction::ALL {
            let v = match self.seats.get(&seat) {
                Some(o) => serde_json::json!({
                    "kind": "human",
                    "name": o.name,
                    "connected": o.connected,
                }),
                None => serde_json::json!({ "kind": "empty" }),
            };
            m.insert(seat.to_char().to_string(), v);
        }
        serde_json::Value::Object(m)
    }
}

pub struct Registry {
    rooms: RwLock<HashMap<String, Arc<Room>>>,
    /// Live sessions by id (hello tickets route on `session_id`). Session
    /// rooms live inside their session, not in `rooms` (which now only
    /// holds the dev demo room).
    sessions: RwLock<HashMap<String, Arc<Session>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            rooms: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, id: &str) -> Option<Arc<Room>> {
        self.rooms.read().await.get(id).cloned()
    }

    pub async fn get_session(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions.read().await.get(id).cloned()
    }

    /// Register a session. If the id is already live, the existing session
    /// is kept and returned (idempotent create: a retried admin POST must
    /// not nuke a session with people at its tables).
    pub async fn insert_session(&self, session: Arc<Session>) -> (Arc<Session>, bool) {
        let mut sessions = self.sessions.write().await;
        if let Some(existing) = sessions.get(&session.id) {
            return (existing.clone(), false);
        }
        sessions.insert(session.id.clone(), session.clone());
        (session, true)
    }

    pub async fn remove_session(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions.write().await.remove(id)
    }

    /// Get the demo room, creating it on first use. Kept for dev: a ticket
    /// with `session_id == "demo"` lands here without any admin setup.
    pub async fn demo_room(&self) -> Arc<Room> {
        let mut rooms = self.rooms.write().await;
        if let Some(r) = rooms.get("demo") {
            return r.clone();
        }
        let board = demo_board();
        let room = Room::new("demo".to_string(), board, None);
        rooms.insert("demo".to_string(), room.clone());
        room
    }
}

/// Board 1 of the PBN spec's example deal — a fixed, valid deal so the demo
/// table is deterministic.
fn demo_board() -> BoardSetup {
    let pbn = "[Board \"1\"]\n[Dealer \"N\"]\n[Vulnerable \"None\"]\n[Deal \"N:K843.T542.J6.863 AQJ7.K.Q75.AT942 962.AJ7.KT82.J75 T5.Q9863.A943.KQ\"]\n";
    let boards = bridge_encodings::pbn::read_pbn(pbn).expect("demo PBN parses");
    let b = &boards[0];
    BoardSetup {
        number: b.number.unwrap_or(1),
        dealer: b.dealer.unwrap_or(Direction::North),
        vulnerable: b.vulnerable,
        deal: b.deal.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_board_parses_and_is_full() {
        let b = demo_board();
        assert_eq!(b.deal.total_cards(), 52);
        assert_eq!(b.dealer, Direction::North);
    }

    #[test]
    fn seating_fills_south_first_and_rebinds() {
        let board = demo_board();
        let mut inner = RoomInner {
            table: TableState::new(board),
            seats: HashMap::new(),
            board_index: 0,
            ready: HashSet::new(),
        };
        assert_eq!(inner.seat_or_rebind("u1", "Alice"), Some(Direction::South));
        assert_eq!(inner.seat_or_rebind("u2", "Bob"), Some(Direction::West));
        // Reconnect keeps the seat.
        assert_eq!(inner.seat_or_rebind("u1", "Alice"), Some(Direction::South));
        assert_eq!(inner.seat_or_rebind("u3", "Cam"), Some(Direction::North));
        assert_eq!(inner.seat_or_rebind("u4", "Dee"), Some(Direction::East));
        // Fifth person kibitzes.
        assert_eq!(inner.seat_or_rebind("u5", "Eve"), None);
    }

    fn inner_with(subs: &[&str]) -> RoomInner {
        let mut inner = RoomInner {
            table: TableState::new(demo_board()),
            seats: HashMap::new(),
            board_index: 0,
            ready: HashSet::new(),
        };
        for sub in subs {
            inner.seat_or_rebind(sub, sub);
        }
        inner
    }

    #[test]
    fn empty_seat_is_always_bot_controlled() {
        let inner = inner_with(&[]);
        let now = Instant::now();
        assert!(inner.seat_bot_controlled(Direction::South, true, now));
        assert!(inner.seat_bot_controlled(Direction::South, false, now));
    }

    #[test]
    fn connected_occupant_is_never_bot_controlled() {
        let inner = inner_with(&["u1"]); // u1 → South
        let now = Instant::now();
        assert!(!inner.seat_bot_controlled(Direction::South, true, now));
        assert!(!inner.seat_bot_controlled(Direction::South, false, now));
    }

    #[test]
    fn takeover_waits_out_the_grace_period() {
        let mut inner = inner_with(&["u1"]);
        inner.mark_disconnected("u1");
        let dropped = inner.seats[&Direction::South].disconnected_at.unwrap();

        // Within both grace periods: still human-driven.
        let t = dropped + Duration::from_secs(19);
        assert!(!inner.seat_bot_controlled(Direction::South, true, t));
        assert!(!inner.seat_bot_controlled(Direction::South, false, t));

        // Past the on-turn grace (20s) but not the idle one (60s).
        let t = dropped + Duration::from_secs(21);
        assert!(inner.seat_bot_controlled(Direction::South, true, t));
        assert!(!inner.seat_bot_controlled(Direction::South, false, t));

        // Past both.
        let t = dropped + Duration::from_secs(61);
        assert!(inner.seat_bot_controlled(Direction::South, true, t));
        assert!(inner.seat_bot_controlled(Direction::South, false, t));
    }

    #[test]
    fn reconnect_reclaims_the_seat_immediately() {
        let mut inner = inner_with(&["u1"]);
        inner.mark_disconnected("u1");
        let dropped = inner.seats[&Direction::South].disconnected_at.unwrap();
        let long_gone = dropped + Duration::from_secs(300);
        assert!(inner.seat_bot_controlled(Direction::South, true, long_gone));

        // Same sub reconnecting rebinds the SAME seat and ends the takeover
        // (mode check, not a re-seat — the binding never moved).
        assert_eq!(inner.seat_or_rebind("u1", "u1"), Some(Direction::South));
        assert!(!inner.seat_bot_controlled(Direction::South, true, long_gone));
    }
}
