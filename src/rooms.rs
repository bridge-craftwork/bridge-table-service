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

use std::collections::HashMap;
use std::sync::Arc;

use bridge_types::{Call, Direction};
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::table::{BoardSetup, TableState};

/// A seated (or recently seated) occupant.
#[derive(Debug, Clone)]
pub struct Occupant {
    /// Ticket subject (user or guest id).
    pub sub: String,
    pub name: String,
    pub connected: bool,
}

pub struct Room {
    pub id: String,
    pub state: Mutex<RoomInner>,
    /// Serialized ServerMsg JSON, fanned out to every socket in the room.
    pub events: broadcast::Sender<String>,
    /// True while a bot-driver task is live for this room (see bots::kick).
    pub bot_running: std::sync::atomic::AtomicBool,
    /// BBA's predicted auction for the current board (complete, from call
    /// 1). Bot calls are served from it while the actual auction remains a
    /// prefix; divergence or undo re-requests (see bots/bba.rs). Separate
    /// from `state` so an in-flight BBA request never blocks humans.
    pub bba_cache: Mutex<Option<Vec<Call>>>,
}

pub struct RoomInner {
    pub table: TableState,
    pub seats: HashMap<Direction, Occupant>,
}

impl Room {
    pub fn new(id: String, board: BoardSetup) -> Arc<Self> {
        let (events, _) = broadcast::channel(256);
        Arc::new(Self {
            id,
            state: Mutex::new(RoomInner {
                table: TableState::new(board),
                seats: HashMap::new(),
            }),
            events,
            bot_running: std::sync::atomic::AtomicBool::new(false),
            bba_cache: Mutex::new(None),
        })
    }

    /// Broadcast a serialized event to everyone in the room. Errors (no
    /// receivers) are fine — an empty room still accepts actions.
    pub fn broadcast(&self, msg: String) {
        let _ = self.events.send(msg);
    }

    /// A room on the demo board, for unit tests in other modules.
    #[cfg(test)]
    pub fn new_for_test(id: &str) -> Arc<Self> {
        Self::new(id.to_string(), demo_board())
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
            occ.name = name.to_string();
            return Some(seat);
        }
        for seat in [South, West, North, East] {
            if let std::collections::hash_map::Entry::Vacant(e) = self.seats.entry(seat) {
                e.insert(Occupant {
                    sub: sub.to_string(),
                    name: name.to_string(),
                    connected: true,
                });
                return Some(seat);
            }
        }
        None // table full → kibitzer
    }

    pub fn mark_disconnected(&mut self, sub: &str) {
        for occ in self.seats.values_mut() {
            if occ.sub == sub {
                occ.connected = false;
            }
        }
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
}

impl Registry {
    pub fn new() -> Self {
        Self {
            rooms: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, id: &str) -> Option<Arc<Room>> {
        self.rooms.read().await.get(id).cloned()
    }

    /// Get the demo room, creating it on first use. Until session creation
    /// arrives (Phase 1+), every connection lands here — it exists so the
    /// end-to-end WS flow is exercisable the moment the service boots.
    pub async fn demo_room(&self) -> Arc<Room> {
        let mut rooms = self.rooms.write().await;
        if let Some(r) = rooms.get("demo") {
            return r.clone();
        }
        let board = demo_board();
        let room = Room::new("demo".to_string(), board);
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
}
