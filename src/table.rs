//! Event-sourced table state.
//!
//! A table's authoritative state is its board plus an append-only `Vec<Action>`.
//! Every view (auction, current trick, remaining hands, whose turn) is computed
//! by folding the action log; **undo is truncate-and-refold** — no incremental
//! state can drift, and bot context always derives from the folded state.
//! Sequence numbers are simply indices into the log.

use bridge_types::{Call, Card, Deal, Direction, Suit, Vulnerability};
use serde_json::{json, Value};

use crate::engine::{auction, play};

/// One action in a table's log. Seats are recorded so a fold never has to
/// re-derive who acted, and so the log is self-describing for later replay.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Call { seat: Direction, call: Call },
    Play { seat: Direction, card: Card },
}

/// The board being played (deal + context), fixed for the lifetime of the log.
#[derive(Debug, Clone)]
pub struct BoardSetup {
    pub number: u32,
    pub dealer: Direction,
    pub vulnerable: Vulnerability,
    pub deal: Deal,
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

    /// Redacted JSON snapshot for one viewer. `viewer_seat` is the seat the
    /// viewer occupies (None for kibitzers). `see_all` is teacher/demo mode.
    ///
    /// Redaction rules: you see your own hand; everyone sees dummy once the
    /// opening lead is made; `see_all` sees everything. Hidden hands are
    /// reported as card *counts* so the UI can render card backs.
    pub fn snapshot(&self, viewer_seat: Option<Direction>, see_all: bool) -> Value {
        let f = self.fold();
        let dummy = f.contract.as_ref().map(|c| c.dummy());

        let mut hands = serde_json::Map::new();
        for seat in Direction::ALL {
            let remaining = self.remaining(seat, &f);
            let visible = see_all
                || viewer_seat == Some(seat)
                || (f.opening_lead_made && dummy == Some(seat));
            let entry = if visible {
                json!({
                    "visible": true,
                    "cards": remaining.iter()
                        .map(|c| format!("{}{}", c.suit.to_char(), c.rank.to_char()))
                        .collect::<Vec<_>>(),
                })
            } else {
                json!({ "visible": false, "count": remaining.len() })
            };
            hands.insert(seat.to_char().to_string(), entry);
        }

        let legal_actions = match (f.phase, f.next_to_act) {
            (Phase::Bidding, Some(seat)) if see_all || viewer_seat == Some(seat) => {
                json!({ "seat": seat.to_char().to_string(), "kind": "call" })
            }
            (Phase::Play, Some(seat)) if see_all || viewer_seat == Some(seat) => {
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
            "phase": match f.phase {
                Phase::Bidding => "bidding",
                Phase::Play => "play",
                Phase::Complete => "complete",
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
            "your_seat": viewer_seat.map(|d| d.to_char().to_string()),
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
    fn snapshot_redacts_hidden_hands() {
        let mut t = setup();
        apply_calls(&mut t, &[(North, "1S")]);

        let snap = t.snapshot(Some(South), false);
        assert_eq!(snap["hands"]["S"]["visible"], true);
        assert_eq!(snap["hands"]["N"]["visible"], false);
        assert_eq!(snap["hands"]["N"]["count"], 13);
        assert!(snap["hands"]["N"]["cards"].is_null());

        // Teacher sees everything.
        let snap = t.snapshot(None, true);
        assert_eq!(snap["hands"]["N"]["visible"], true);
        assert_eq!(snap["hands"]["W"]["visible"], true);
    }

    #[test]
    fn snapshot_reveals_dummy_after_opening_lead() {
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
        // Dummy is West (declarer East). Before the lead, West is hidden to N.
        let snap = t.snapshot(Some(North), false);
        assert_eq!(snap["hands"]["W"]["visible"], false);

        let s2 = Card::new(Suit::Spades, bridge_types::Rank::Two);
        t.apply(Action::Play {
            seat: South,
            card: s2,
        })
        .unwrap();
        let snap = t.snapshot(Some(North), false);
        assert_eq!(snap["hands"]["W"]["visible"], true);
    }

    #[test]
    fn legal_cards_only_shown_to_seat_on_turn() {
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
        let snap = t.snapshot(Some(South), false);
        assert_eq!(snap["legal"]["kind"], "card");
        assert_eq!(snap["legal"]["cards"].as_array().unwrap().len(), 13);

        let snap = t.snapshot(Some(North), false);
        assert!(snap["legal"].is_null());
    }
}
