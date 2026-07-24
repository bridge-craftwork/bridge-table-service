//! Event-sourced table state.
//!
//! A table's authoritative state is its board plus an append-only `Vec<Action>`.
//! Every view (auction, current trick, remaining hands, whose turn) is computed
//! by folding the action log; **undo is truncate-and-refold** — no incremental
//! state can drift, and bot context always derives from the folded state.
//! Sequence numbers are simply indices into the log.

use bridge_types::{Call, Card, Deal, Direction, PlaySequence, Suit, Vulnerability};
use serde_json::{json, Value};

use crate::engine::{auction, play};

/// One action in a table's log. Seats are recorded so a fold never has to
/// re-derive who acted, and so the log is self-describing for later replay.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Call { seat: Direction, call: Call },
    Play { seat: Direction, card: Card },
}

/// A deal source's recorded `[Play]` line, flattened into per-seat queues (the
/// cards each seat is scripted to play, in order). This is the server twin of
/// the frontend's `playLine.bySeat` (see `cardplayBots.js`): the cardplay-first
/// composite bot follows it while the actual play still matches, then hands off
/// to the room's real bot on divergence/absence. Seats index by
/// `Direction::to_index()`.
#[derive(Debug, Clone, Default)]
pub struct PlayLine {
    by_seat: [Vec<Card>; 4],
}

impl PlayLine {
    /// Flatten a parsed `[Play]` section into per-seat queues, or `None` when
    /// the board carries no line (the common case — random/BBA deals).
    pub fn from_sequence(play: Option<&PlaySequence>) -> Option<Self> {
        let play = play?;
        let mut by_seat: [Vec<Card>; 4] = Default::default();
        for trick in &play.tricks {
            for pos in 0..4 {
                if let Some(card) = trick.cards[pos] {
                    by_seat[trick.player_at(pos).to_index()].push(card);
                }
            }
        }
        if by_seat.iter().all(Vec::is_empty) {
            return None;
        }
        Some(Self { by_seat })
    }

    /// The card `seat` is scripted to play on its `i`-th turn, if the line
    /// records one.
    pub fn card_for(&self, seat: Direction, i: usize) -> Option<Card> {
        self.by_seat[seat.to_index()].get(i).copied()
    }

    /// Whether the actual play has departed from the recorded line — a seat
    /// played a card the line didn't script at that position (a deviation), or
    /// played past the end of its recorded queue (the line ran out). Recomputed
    /// from the full history each call, so it is undo-safe: a rewind past the
    /// divergence puts play back on-book. `played` is the chronological
    /// all-seats history.
    pub fn diverged(&self, played: &[(Direction, Card)]) -> bool {
        let mut counts = [0usize; 4];
        for (seat, card) in played {
            let idx = seat.to_index();
            match self.by_seat[idx].get(counts[idx]) {
                Some(expected) if expected == card => counts[idx] += 1,
                _ => return true,
            }
        }
        false
    }
}

/// The board being played (deal + context), fixed for the lifetime of the log.
#[derive(Debug, Clone)]
pub struct BoardSetup {
    pub number: u32,
    pub dealer: Direction,
    pub vulnerable: Vulnerability,
    pub deal: Deal,
    /// The recorded `[Play]` line, when the deal source shipped one (declarer-
    /// play coaching boards). Drives the cardplay-first composite bot; `None`
    /// for random/BBA deals and after a seat rotation (the line's seats no
    /// longer match).
    pub play_line: Option<PlayLine>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Bidding,
    Play,
    Complete,
}

/// Everything derived from folding the log. Cheap to recompute (≤ ~90 actions).
pub struct Folded {
    pub phase: Phase,
    pub calls: Vec<Call>,
    pub contract: Option<auction::ContractOutcome>,
    /// Chronological plays across all tricks.
    pub played: Vec<(Direction, Card)>,
    /// Completed tricks: (leader, four cards, winner).
    pub completed_tricks: Vec<(Direction, [Card; 4], Direction)>,
    /// Current (incomplete) trick: leader + plays so far.
    pub current_trick: Option<(Direction, Vec<(Direction, Card)>)>,
    pub tricks_ns: u8,
    pub tricks_ew: u8,
    /// Seat to act next (bid or play). None once complete / passed out.
    pub next_to_act: Option<Direction>,
    /// True once the opening lead has been made (dummy goes face up).
    pub opening_lead_made: bool,
}

pub struct TableState {
    pub board: BoardSetup,
    pub actions: Vec<Action>,
}

impl TableState {
    pub fn new(board: BoardSetup) -> Self {
        Self {
            board,
            actions: Vec::new(),
        }
    }

    /// Current sequence number = number of applied actions.
    pub fn seq(&self) -> usize {
        self.actions.len()
    }

    /// Validate and append an action. Returns the new seq on success.
    pub fn apply(&mut self, action: Action) -> Result<usize, String> {
        let f = self.fold();
        match &action {
            Action::Call { seat, call } => {
                if f.phase != Phase::Bidding {
                    return Err("auction is over".into());
                }
                if f.next_to_act != Some(*seat) {
                    return Err(format!("not {seat:?}'s turn to call"));
                }
                if !auction::is_legal(self.board.dealer, &f.calls, call) {
                    return Err(format!("illegal call {}", call.to_pbn()));
                }
            }
            Action::Play { seat, card } => {
                if f.phase != Phase::Play {
                    return Err("not in the play phase".into());
                }
                if f.next_to_act != Some(*seat) {
                    return Err(format!("not {seat:?}'s turn to play"));
                }
                let remaining = self.remaining(*seat, &f);
                let lead_suit = f
                    .current_trick
                    .as_ref()
                    .and_then(|(_, plays)| plays.first().map(|(_, c)| c.suit));
                if !play::is_legal_play(*card, &remaining, lead_suit) {
                    return Err(format!(
                        "illegal card {}{}",
                        card.suit.to_char(),
                        card.rank.to_char()
                    ));
                }
            }
        }
        self.actions.push(action);
        Ok(self.actions.len())
    }

    /// Unlimited undo (Shark-style): truncate the log back to `to_seq`.
    pub fn undo_to(&mut self, to_seq: usize) -> Result<usize, String> {
        if to_seq > self.actions.len() {
            return Err("cannot undo forward".into());
        }
        self.actions.truncate(to_seq);
        Ok(self.actions.len())
    }

    /// Cards `seat` still holds after the plays in `f`.
    pub fn remaining(&self, seat: Direction, f: &Folded) -> Vec<Card> {
        let mut cards: Vec<Card> = self.board.deal.hand(seat).cards().to_vec();
        for (s, c) in &f.played {
            if *s == seat {
                cards.retain(|x| x != c);
            }
        }
        cards
    }

    /// Fold the action log into the current derived state.
    pub fn fold(&self) -> Folded {
        let mut calls: Vec<Call> = Vec::new();
        let mut played: Vec<(Direction, Card)> = Vec::new();
        for a in &self.actions {
            match a {
                Action::Call { call, .. } => calls.push(call.clone()),
                Action::Play { seat, card } => played.push((*seat, *card)),
            }
        }

        let auction_done = auction::is_complete(&calls);
        let contract = auction::contract(self.board.dealer, &calls);

        if !auction_done {
            return Folded {
                phase: Phase::Bidding,
                next_to_act: Some(auction::next_caller(self.board.dealer, &calls)),
                calls,
                contract: None,
                played,
                completed_tricks: Vec::new(),
                current_trick: None,
                tricks_ns: 0,
                tricks_ew: 0,
                opening_lead_made: false,
            };
        }

        let Some(contract) = contract else {
            // Passed out: complete with no play.
            return Folded {
                phase: Phase::Complete,
                next_to_act: None,
                calls,
                contract: None,
                played,
                completed_tricks: Vec::new(),
                current_trick: None,
                tricks_ns: 0,
                tricks_ew: 0,
                opening_lead_made: false,
            };
        };

        let trump = play::trump_of(contract.strain);
        let mut completed: Vec<(Direction, [Card; 4], Direction)> = Vec::new();
        let mut leader = contract.opening_leader();
        let mut trick: Vec<(Direction, Card)> = Vec::new();
        let (mut tricks_ns, mut tricks_ew) = (0u8, 0u8);

        for (seat, card) in &played {
            trick.push((*seat, *card));
            if trick.len() == 4 {
                let cards = [trick[0].1, trick[1].1, trick[2].1, trick[3].1];
                let winner = play::trick_winner(leader, &cards, trump);
                if matches!(winner, Direction::North | Direction::South) {
                    tricks_ns += 1;
                } else {
                    tricks_ew += 1;
                }
                completed.push((leader, cards, winner));
                leader = winner;
                trick.clear();
            }
        }

        let play_done = completed.len() == 13;
        let next_to_act = if play_done {
            None
        } else if let Some((last_seat, _)) = trick.last() {
            Some(last_seat.next())
        } else {
            Some(leader)
        };

        Folded {
            phase: if play_done {
                Phase::Complete
            } else {
                Phase::Play
            },
            next_to_act,
            opening_lead_made: !played.is_empty(),
            current_trick: if trick.is_empty() && play_done {
                None
            } else {
                Some((leader, trick))
            },
            calls,
            contract: Some(contract),
            played,
            completed_tricks: completed,
            tricks_ns,
            tricks_ew,
        }
    }

    /// The board-result payload, once the board is complete (played out or
    /// passed out). None while in progress. Broadcast at board completion
    /// as the `board_complete` event body — results are NOT persisted in
    /// v1, this live broadcast is the whole delivery.
    pub fn board_result_json(&self) -> Option<Value> {
        let f = self.fold();
        if f.phase != Phase::Complete {
            return None;
        }
        let contract = f.contract.as_ref().map(|c| {
            let declarer_tricks = if matches!(c.declarer, Direction::North | Direction::South) {
                f.tricks_ns
            } else {
                f.tricks_ew
            };
            let made = declarer_tricks as i16 - (c.level as i16 + 6);
            json!({
                "text": c.to_pbn(),
                "declarer": c.declarer.to_char().to_string(),
                "declarer_tricks": declarer_tricks,
                // Relative result: 0 = made exactly, +N overtricks, -N down.
                "made": made,
            })
        });
        Some(json!({
            "board_no": self.board.number,
            "passed_out": f.contract.is_none(),
            "contract": contract,
            "tricks": { "ns": f.tricks_ns, "ew": f.tricks_ew },
        }))
    }

    /// Full (un-redacted) JSON snapshot of the table. Every viewer receives
    /// ALL four hands as `{visible:true, cards:[...]}` — the client redacts
    /// locally now (friends-table model). The snapshot is therefore
    /// viewer-independent and safe to broadcast to every connection.
    ///
    /// A COMPLETE board reports each seat's ORIGINAL hand (post-mortem
    /// review — the remaining cards would all be empty otherwise). While
    /// play is in progress, each seat's `cards` are its REMAINING cards.
    /// `bid_only` treats a contract-reached board as complete (bid-only
    /// boards end at the contract, all hands shown for discussion).
    ///
    /// `legal` carries the legal actions for whichever seat is on turn
    /// (also viewer-independent); the client shows them only for seats the
    /// connection occupies.
    pub fn snapshot(&self, bid_only: bool) -> Value {
        let f = self.fold();
        let complete = f.phase == Phase::Complete || (bid_only && f.phase == Phase::Play);

        let mut hands = serde_json::Map::new();
        for seat in Direction::ALL {
            let cards = if complete {
                self.board.deal.hand(seat).cards().to_vec()
            } else {
                self.remaining(seat, &f)
            };
            hands.insert(
                seat.to_char().to_string(),
                json!({
                    "visible": true,
                    "cards": cards.iter()
                        .map(|c| format!("{}{}", c.suit.to_char(), c.rank.to_char()))
                        .collect::<Vec<_>>(),
                }),
            );
        }

        // Legal actions for the seat on turn, regardless of viewer.
        let legal_actions = match (f.phase, f.next_to_act) {
            (Phase::Bidding, Some(seat)) => {
                json!({ "seat": seat.to_char().to_string(), "kind": "call" })
            }
            (Phase::Play, Some(seat)) if !complete => {
                let lead_suit = current_lead_suit(&f);
                let legal = play::legal_cards(&self.remaining(seat, &f), lead_suit)
                    .iter()
                    .map(|c| format!("{}{}", c.suit.to_char(), c.rank.to_char()))
                    .collect::<Vec<_>>();
                json!({ "seat": seat.to_char().to_string(), "kind": "card", "cards": legal })
            }
            _ => Value::Null,
        };

        json!({
            "seq": self.seq(),
            "board": {
                "number": self.board.number,
                "dealer": self.board.dealer.to_char().to_string(),
                "vulnerable": self.board.vulnerable.to_pbn(),
            },
            "phase": if complete {
                "complete"
            } else {
                match f.phase {
                    Phase::Bidding => "bidding",
                    Phase::Play => "play",
                    Phase::Complete => "complete",
                }
            },
            "auction": f.calls.iter().map(|c| c.to_pbn()).collect::<Vec<_>>(),
            "contract": f.contract.as_ref().map(|c| json!({
                "text": c.to_pbn(),
                "declarer": c.declarer.to_char().to_string(),
            })),
            "next_to_act": f.next_to_act.map(|d| d.to_char().to_string()),
            "current_trick": f.current_trick.as_ref().map(|(leader, plays)| json!({
                "leader": leader.to_char().to_string(),
                "plays": plays.iter().map(|(s, c)| json!({
                    "seat": s.to_char().to_string(),
                    "card": format!("{}{}", c.suit.to_char(), c.rank.to_char()),
                })).collect::<Vec<_>>(),
            })),
            "tricks": { "ns": f.tricks_ns, "ew": f.tricks_ew },
            "hands": hands,
            "legal": legal_actions,
        })
    }
}

fn current_lead_suit(f: &Folded) -> Option<Suit> {
    f.current_trick
        .as_ref()
        .and_then(|(_, plays)| plays.first().map(|(_, c)| c.suit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_types::Direction::*;

    // A real deal (from the PBN docs example).
    const DEAL: &str = "N:K843.T542.J6.863 AQJ7.K.Q75.AT942 962.AJ7.KT82.J75 T5.Q9863.A943.KQ";

    fn setup() -> TableState {
        TableState::new(BoardSetup {
            number: 1,
            dealer: North,
            vulnerable: Vulnerability::None,
            deal: Deal::from_pbn(DEAL).unwrap(),
            play_line: None,
        })
    }

    fn call(s: &str) -> Call {
        Call::from_pbn(s).unwrap()
    }

    fn apply_calls(t: &mut TableState, seq: &[(Direction, &str)]) {
        for (seat, c) in seq {
            t.apply(Action::Call {
                seat: *seat,
                call: call(c),
            })
            .unwrap_or_else(|e| panic!("{seat:?} {c}: {e}"));
        }
    }

    #[test]
    fn full_auction_reaches_play_phase() {
        let mut t = setup();
        apply_calls(
            &mut t,
            &[
                (North, "Pass"),
                (East, "1NT"),
                (South, "Pass"),
                (West, "3NT"),
                (North, "Pass"),
                (East, "Pass"),
                (South, "Pass"),
            ],
        );
        let f = t.fold();
        assert_eq!(f.phase, Phase::Play);
        let c = f.contract.unwrap();
        assert_eq!(c.declarer, East); // E named NT first
        assert_eq!(f.next_to_act, Some(South)); // opening leader = declarer.next()
    }

    #[test]
    fn wrong_seat_rejected() {
        let mut t = setup();
        let err = t
            .apply(Action::Call {
                seat: East,
                call: call("1S"),
            })
            .unwrap_err();
        assert!(err.contains("turn"), "{err}");
    }

    #[test]
    fn illegal_call_rejected() {
        let mut t = setup();
        apply_calls(&mut t, &[(North, "1S")]);
        let err = t
            .apply(Action::Call {
                seat: East,
                call: call("1H"),
            })
            .unwrap_err();
        assert!(err.contains("illegal"), "{err}");
    }

    #[test]
    fn play_enforces_follow_suit_and_turn() {
        let mut t = setup();
        apply_calls(
            &mut t,
            &[
                (North, "Pass"),
                (East, "1NT"),
                (South, "Pass"),
                (West, "3NT"),
                (North, "Pass"),
                (East, "Pass"),
                (South, "Pass"),
            ],
        );
        // South leads (declarer East). South holds 962.AJ7.KT82.J75.
        let s2 = Card::new(Suit::Spades, bridge_types::Rank::Two);
        t.apply(Action::Play {
            seat: South,
            card: s2,
        })
        .unwrap();
        let f = t.fold();
        assert!(f.opening_lead_made);
        assert_eq!(f.next_to_act, Some(West));

        // West holds T5.Q9863.A943.KQ — has spades, so a heart is illegal.
        let hq = Card::new(Suit::Hearts, bridge_types::Rank::Queen);
        let err = t
            .apply(Action::Play {
                seat: West,
                card: hq,
            })
            .unwrap_err();
        assert!(err.contains("illegal"), "{err}");

        let st = Card::new(Suit::Spades, bridge_types::Rank::Ten);
        t.apply(Action::Play {
            seat: West,
            card: st,
        })
        .unwrap();
    }

    #[test]
    fn card_not_held_rejected() {
        let mut t = setup();
        apply_calls(
            &mut t,
            &[
                (North, "Pass"),
                (East, "1NT"),
                (South, "Pass"),
                (West, "3NT"),
                (North, "Pass"),
                (East, "Pass"),
                (South, "Pass"),
            ],
        );
        // South does not hold the SA.
        let sa = Card::new(Suit::Spades, bridge_types::Rank::Ace);
        assert!(t
            .apply(Action::Play {
                seat: South,
                card: sa
            })
            .is_err());
    }

    #[test]
    fn undo_rewinds_and_refolds() {
        let mut t = setup();
        apply_calls(&mut t, &[(North, "1S"), (East, "Pass"), (South, "2S")]);
        assert_eq!(t.seq(), 3);

        t.undo_to(1).unwrap();
        assert_eq!(t.seq(), 1);
        let f = t.fold();
        assert_eq!(f.calls, vec![call("1S")]);
        assert_eq!(f.next_to_act, Some(East));

        // The rewound line can now diverge.
        t.apply(Action::Call {
            seat: East,
            call: call("2H"),
        })
        .unwrap();
        assert_eq!(t.fold().calls, vec![call("1S"), call("2H")]);

        assert!(t.undo_to(5).is_err());
    }

    #[test]
    fn passed_out_board_is_complete() {
        let mut t = setup();
        apply_calls(
            &mut t,
            &[
                (North, "Pass"),
                (East, "Pass"),
                (South, "Pass"),
                (West, "Pass"),
            ],
        );
        let f = t.fold();
        assert_eq!(f.phase, Phase::Complete);
        assert_eq!(f.next_to_act, None);
    }

    #[test]
    fn snapshot_shows_every_hand_to_everyone() {
        // Un-redacted friends-table model: every seat is fully visible to
        // any viewer (the client redacts locally now).
        let mut t = setup();
        apply_calls(&mut t, &[(North, "1S")]);

        let snap = t.snapshot(false);
        for seat in ["N", "E", "S", "W"] {
            assert_eq!(snap["hands"][seat]["visible"], true, "seat {seat}");
            assert_eq!(snap["hands"][seat]["cards"].as_array().unwrap().len(), 13);
        }
    }

    #[test]
    fn snapshot_shows_original_deal_when_complete() {
        // A passed-out board reaches Complete with all 52 cards unplayed —
        // the review view shows every ORIGINAL hand.
        let mut t = setup();
        apply_calls(
            &mut t,
            &[
                (North, "Pass"),
                (East, "Pass"),
                (South, "Pass"),
                (West, "Pass"),
            ],
        );

        let snap = t.snapshot(false);
        assert_eq!(snap["phase"], "complete");
        for seat in ["N", "E", "S", "W"] {
            assert_eq!(snap["hands"][seat]["visible"], true, "seat {seat}");
            assert_eq!(snap["hands"][seat]["cards"].as_array().unwrap().len(), 13);
        }
    }

    #[test]
    fn snapshot_shows_dummy_before_and_after_opening_lead() {
        let mut t = setup();
        apply_calls(
            &mut t,
            &[
                (North, "Pass"),
                (East, "1NT"),
                (South, "Pass"),
                (West, "3NT"),
                (North, "Pass"),
                (East, "Pass"),
                (South, "Pass"),
            ],
        );
        // Dummy is West (declarer East) — always visible now.
        let snap = t.snapshot(false);
        assert_eq!(snap["hands"]["W"]["visible"], true);

        let s2 = Card::new(Suit::Spades, bridge_types::Rank::Two);
        t.apply(Action::Play {
            seat: South,
            card: s2,
        })
        .unwrap();
        let snap = t.snapshot(false);
        assert_eq!(snap["hands"]["W"]["visible"], true);
    }

    #[test]
    fn legal_cards_are_for_the_seat_on_turn() {
        let mut t = setup();
        apply_calls(
            &mut t,
            &[
                (North, "Pass"),
                (East, "1NT"),
                (South, "Pass"),
                (West, "3NT"),
                (North, "Pass"),
                (East, "Pass"),
                (South, "Pass"),
            ],
        );
        // South is on opening lead (declarer East). The snapshot is
        // viewer-independent: legal names the seat on turn.
        let snap = t.snapshot(false);
        assert_eq!(snap["legal"]["kind"], "card");
        assert_eq!(snap["legal"]["seat"], "S");
        assert_eq!(snap["legal"]["cards"].as_array().unwrap().len(), 13);
    }
}
