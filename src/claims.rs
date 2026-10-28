//! Claims: a seat (or the teacher, as a ruling) ends the play by stating how
//! many of the remaining tricks its side takes.
//!
//! The claimer's client checks the claim first, with bridge-solver's claim
//! checker compiled to WASM, so the work normally happens on the user's
//! computer rather than here (claims design, 2026-10-27). The claim then
//! arrives with `plays_seen`, the number of cards played when the client
//! checked it. A claim whose count no longer matches the table is refused as
//! stale, so client and server can never judge different positions.
//!
//! The server may check it again with the same crate at the same version
//! (`VERIFY_CLAIMS`, on by default). A claim is rare, at most once a board,
//! and a late-hand check usually takes milliseconds, so the load is small.
//! It runs on a blocking thread with the room lock released, then the claim
//! is applied only if nothing was played meanwhile.
//!
//! A teacher's claim (`ruling`) is a director's ruling and is never checked.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bridge_types::Direction;
use serde_json::{json, Value};

use crate::rooms::{BoardMode, Room};
use crate::table::{claim_json, Action, Phase};

static VERIFY: AtomicBool = AtomicBool::new(true);

/// Turn the server-side re-check of players' claims on or off (from
/// `Config::verify_claims` at startup).
pub fn set_verify(on: bool) {
    VERIFY.store(on, Ordering::Relaxed);
}

fn verify_enabled() -> bool {
    VERIFY.load(Ordering::Relaxed)
}

/// Why a claim was not applied, as an error frame's `code` and message.
pub struct ClaimRefused {
    pub code: &'static str,
    pub msg: String,
}

fn refused(code: &'static str, msg: impl Into<String>) -> ClaimRefused {
    ClaimRefused {
        code,
        msg: msg.into(),
    }
}

/// One claim to make on `room`'s table.
pub struct ClaimRequest {
    /// The claiming seat; its side takes `tricks`.
    pub seat: Direction,
    /// Tricks the claimer's side takes, of those still to play.
    pub tricks: u8,
    /// Cards played when the claim was checked; must match the table.
    pub plays_seen: usize,
    /// Teacher-directed: applied without a check.
    pub ruling: bool,
}

/// Check (unless a ruling) and apply a claim, then announce it to the room:
/// a `claim` event, the `board_complete` result and a fresh snapshot.
pub async fn make_claim(room: &Arc<Room>, req: ClaimRequest) -> Result<(), ClaimRefused> {
    let solver_request = {
        let inner = room.state.lock().await;
        if inner.board_mode == BoardMode::BidOnly {
            return Err(refused(
                "bid_only",
                "this board ends at the contract — no cardplay",
            ));
        }
        let f = inner.table.fold();
        if f.phase != Phase::Play {
            return Err(refused("rejected", "claims are only made during the play"));
        }
        if req.plays_seen != f.played.len() {
            return Err(refused(
                "claim_stale",
                format!(
                    "the claim was checked at card {}, but {} have been played",
                    req.plays_seen,
                    f.played.len()
                ),
            ));
        }
        if req.tricks > f.tricks_left() {
            return Err(refused(
                "rejected",
                format!(
                    "claimed {} tricks but only {} remain",
                    req.tricks,
                    f.tricks_left()
                ),
            ));
        }
        if req.ruling || !verify_enabled() {
            None
        } else {
            let Some(c) = f.contract.as_ref() else {
                return Err(refused("rejected", "no contract"));
            };
            let won = if matches!(req.seat, Direction::North | Direction::South) {
                f.tricks_ns
            } else {
                f.tricks_ew
            };
            Some(json!({
                "dealstr": inner.table.board.deal.to_pbn(Direction::North),
                "trump": c.strain.to_pbn(),
                "declarer": c.declarer.to_char().to_string(),
                "leader": c.opening_leader().to_char().to_string(),
                "plays": f.played.iter()
                    .map(|(_, card)| format!("{}{}", card.suit.to_char(), card.rank.to_char()))
                    .collect::<Vec<_>>(),
                "claimer": req.seat.to_char().to_string(),
                "total_tricks": won + req.tricks,
            }))
        }
    };

    if let Some(request) = solver_request {
        let verdict = check_claim(request.to_string()).await?;
        if verdict != "valid" {
            tracing::info!(event = "claim_rejected", room = %room.id, verdict = %verdict, "");
            return Err(refused("claim_rejected", rejection_message(&verdict)));
        }
    }

    let mut inner = room.state.lock().await;
    // The lock was released for the check: refuse if the play moved on.
    if inner.table.fold().played.len() != req.plays_seen {
        return Err(refused(
            "claim_stale",
            "the play moved on while the claim was checked",
        ));
    }
    let seq = inner
        .table
        .apply(Action::Claim {
            seat: req.seat,
            tricks: req.tricks,
            ruling: req.ruling,
        })
        .map_err(|e| refused("rejected", e))?;
    let f = inner.table.fold();
    let ev = json!({
        "t": "event",
        "table_id": room.id,
        "seq": seq,
        "kind": "claim",
        "claim": f.claim.as_ref().map(claim_json),
        "tricks": { "ns": f.tricks_ns, "ew": f.tricks_ew },
    })
    .to_string();
    let complete = board_complete(room, &inner, seq);
    drop(inner);
    tracing::info!(
        event = "claim_made",
        room = %room.id,
        seat = %req.seat.to_char(),
        tricks = req.tricks,
        ruling = req.ruling,
        ""
    );
    room.broadcast(ev);
    if let Some(complete) = complete {
        room.broadcast(complete);
    }
    room.broadcast_resync();
    room.notify_session_lobby();
    Ok(())
}

fn board_complete(room: &Room, inner: &crate::rooms::RoomInner, seq: usize) -> Option<String> {
    let result = inner.result_json()?;
    let mut ev = json!({
        "t": "event",
        "table_id": room.id,
        "seq": seq,
        "kind": "board_complete",
    });
    if let (Some(ev), Some(result)) = (ev.as_object_mut(), result.as_object()) {
        ev.extend(result.clone());
    }
    Some(ev.to_string())
}

/// Run bridge-solver's claim checker on a blocking thread. Resolves the
/// verdict (`valid` / `invalid` / `undecided`).
async fn check_claim(request: String) -> Result<String, ClaimRefused> {
    let out = tokio::task::spawn_blocking(move || {
        std::panic::catch_unwind(|| bridge_solver::json_api::check_claim(&request))
    })
    .await;
    let response = match out {
        Ok(Ok(Ok(json))) => json,
        Ok(Ok(Err(e))) => return Err(refused("rejected", format!("claim check: {e}"))),
        Ok(Err(_)) | Err(_) => {
            tracing::error!(event = "claim_check_failed", "");
            return Err(refused(
                "claim_rejected",
                "the claim checker failed, so the claim can't be accepted",
            ));
        }
    };
    let v: Value = serde_json::from_str(&response)
        .map_err(|e| refused("rejected", format!("claim check: {e}")))?;
    Ok(v["verdict"].as_str().unwrap_or("undecided").to_string())
}

/// The same wording the client shows (solverClient.js `claimRejectionMessage`).
fn rejection_message(verdict: &str) -> &'static str {
    match verdict {
        "invalid" => "Not guaranteed: some way the unseen cards could lie holds you below the claim. Play on.",
        _ => "Too complex to prove in time, so the claim can't be accepted. Play on.",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // bridge-solver's README deal at trick one: 10 tricks for North in spades
    // is double-dummy but not a sure claim; 13 for the side holding every
    // spade in a one-suit-each deal is.
    #[tokio::test]
    async fn verdicts_come_from_bridge_solver() {
        let invalid = json!({
            "dealstr": "N:AKQT3.J6.KJ42.95 652.AK42.AQ87.T4 J74.QT95.T.AK863 98.873.9653.QJ72",
            "trump": "S", "declarer": "N", "leader": "E", "plays": [],
            "claimer": "N", "total_tricks": 10,
        });
        assert_eq!(
            check_claim(invalid.to_string()).await.ok().as_deref(),
            Some("invalid")
        );
        let valid = json!({
            "dealstr": "N:AKQJT98765432... .AKQJT98765432.. ..AKQJT98765432. ...AKQJT98765432",
            "trump": "NT", "declarer": "N", "leader": "E", "plays": [],
            "claimer": "E", "total_tricks": 13,
        });
        assert_eq!(
            check_claim(valid.to_string()).await.ok().as_deref(),
            Some("valid")
        );
    }

    #[tokio::test]
    async fn a_bad_request_is_refused_not_a_panic() {
        assert!(check_claim("{".into()).await.is_err());
    }

    // bridge-solver's README deal: North 4S, all pass, East to lead.
    fn room_in_play() -> Arc<Room> {
        use bridge_types::{Call, Deal, Vulnerability};
        use Direction::*;
        let board = crate::table::BoardSetup {
            number: 1,
            dealer: North,
            vulnerable: Vulnerability::None,
            deal: Deal::from_pbn(
                "N:AKQT3.J6.KJ42.95 652.AK42.AQ87.T4 J74.QT95.T.AK863 98.873.9653.QJ72",
            )
            .unwrap(),
            play_line: None,
        };
        let room = Room::new(
            "t".into(),
            board,
            None,
            crate::rooms::TakeoverGrace::DEFAULT,
        );
        {
            let mut inner = room.state.try_lock().unwrap();
            for (seat, call) in [
                (North, Call::from_pbn("4S").unwrap()),
                (East, Call::Pass),
                (South, Call::Pass),
                (West, Call::Pass),
            ] {
                inner.table.apply(Action::Call { seat, call }).unwrap();
            }
        }
        room
    }

    fn claim(seat: Direction, tricks: u8, plays_seen: usize, ruling: bool) -> ClaimRequest {
        ClaimRequest {
            seat,
            tricks,
            plays_seen,
            ruling,
        }
    }

    #[tokio::test]
    async fn a_stale_claim_is_refused() {
        let room = room_in_play();
        let err = make_claim(&room, claim(Direction::North, 10, 3, true))
            .await
            .unwrap_err();
        assert_eq!(err.code, "claim_stale");
    }

    #[tokio::test]
    async fn the_server_check_refuses_a_claim_that_is_not_sure() {
        let room = room_in_play();
        let err = make_claim(&room, claim(Direction::North, 10, 0, false))
            .await
            .unwrap_err();
        assert_eq!(err.code, "claim_rejected");
        assert_eq!(
            room.state.lock().await.table.fold().phase,
            Phase::Play,
            "a refused claim leaves the play going"
        );
    }

    #[tokio::test]
    async fn a_ruling_is_applied_unchecked_and_announced() {
        let room = room_in_play();
        let mut events = room.events.subscribe();
        make_claim(&room, claim(Direction::North, 10, 0, true))
            .await
            .unwrap_or_else(|e| panic!("{}: {}", e.code, e.msg));
        let f = room.state.lock().await.table.fold();
        assert_eq!(f.phase, Phase::Complete);
        assert_eq!((f.tricks_ns, f.tricks_ew), (10, 3));
        assert!(f.claim.unwrap().ruling);

        let first: Value = serde_json::from_str(&events.recv().await.unwrap()).unwrap();
        assert_eq!(first["kind"], "claim");
        assert_eq!(first["claim"]["tricks"], 10);
        let second: Value = serde_json::from_str(&events.recv().await.unwrap()).unwrap();
        assert_eq!(second["kind"], "board_complete");
        assert_eq!(second["contract"]["declarer_tricks"], 10);
        assert_eq!(second["claim"]["ruling"], true);
    }
}
