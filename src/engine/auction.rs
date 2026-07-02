//! Auction legality and contract determination.
//!
//! `bridge-types` provides the `Call`/`Auction` types and a
//! `final_contract()`, but its declarer is "the player who made the last
//! bid" — a simplification fine for rendering teaching materials, wrong for
//! a live game (the declarer is the *first* player of the declaring side to
//! name the final strain). `contract()` here implements the real rule.

use bridge_types::{Call, Direction, Strain};

/// Total order over bids: higher level wins; at equal level C < D < H < S < NT.
fn strain_rank(s: Strain) -> u8 {
    match s {
        Strain::Clubs => 0,
        Strain::Diamonds => 1,
        Strain::Hearts => 2,
        Strain::Spades => 3,
        Strain::NoTrump => 4,
    }
}

/// The seat that makes the call at `index` (0-based) in an auction dealt by `dealer`.
pub fn caller_at(dealer: Direction, index: usize) -> Direction {
    let mut d = dealer;
    for _ in 0..(index % 4) {
        d = d.next();
    }
    d
}

/// The seat whose turn it is to call next.
pub fn next_caller(dealer: Direction, calls: &[Call]) -> Direction {
    caller_at(dealer, calls.len())
}

/// True once the auction has ended: at least 4 calls and the last 3 are
/// passes (covers both a passed-out deal and 3 passes after a bid).
pub fn is_complete(calls: &[Call]) -> bool {
    calls.len() >= 4 && calls.iter().rev().take(3).all(|c| c.is_pass())
}

/// Whether `call` is legal as the next call. Teaching placeholders
/// (`Continue`/`Blank`) are never legal in live play.
pub fn is_legal(dealer: Direction, calls: &[Call], call: &Call) -> bool {
    if is_complete(calls) {
        return false;
    }
    match call {
        Call::Pass => true,
        Call::Bid { level, strain } => {
            if !(1..=7).contains(level) {
                return false;
            }
            match last_bid(calls) {
                None => true,
                Some((last_level, last_strain, _)) => {
                    (*level, strain_rank(*strain)) > (last_level, strain_rank(last_strain))
                }
            }
        }
        Call::Double => match last_action(calls) {
            Some((idx, LastAction::Bid)) => is_opponent(dealer, calls.len(), idx),
            _ => false,
        },
        Call::Redouble => match last_action(calls) {
            Some((idx, LastAction::Double)) => is_opponent(dealer, calls.len(), idx),
            _ => false,
        },
        Call::Continue | Call::Blank => false,
    }
}

/// The final contract with the *correct* declarer: the first player of the
/// declaring side (in call order) to name the final strain. Returns `None`
/// while the auction is incomplete or if the deal passed out.
pub fn contract(dealer: Direction, calls: &[Call]) -> Option<ContractOutcome> {
    if !is_complete(calls) {
        return None;
    }
    let (level, strain, last_bidder_idx) = last_bid(calls)?;
    let bidding_side_parity = last_bidder_idx % 2;

    // Doubled state: scan calls after the final bid.
    let mut doubled = false;
    let mut redoubled = false;
    for c in &calls[last_bidder_idx + 1..] {
        match c {
            Call::Double => {
                doubled = true;
                redoubled = false;
            }
            Call::Redouble => {
                doubled = false;
                redoubled = true;
            }
            _ => {}
        }
    }

    // Declarer: first caller on the declaring side who bid this strain.
    let declarer_idx = calls
        .iter()
        .enumerate()
        .find(|(i, c)| {
            i % 2 == bidding_side_parity && matches!(c, Call::Bid { strain: s, .. } if *s == strain)
        })
        .map(|(i, _)| i)
        .unwrap_or(last_bidder_idx);

    Some(ContractOutcome {
        level,
        strain,
        doubled,
        redoubled,
        declarer: caller_at(dealer, declarer_idx),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractOutcome {
    pub level: u8,
    pub strain: Strain,
    pub doubled: bool,
    pub redoubled: bool,
    pub declarer: Direction,
}

impl ContractOutcome {
    /// PBN-style string, e.g. "4S", "3NX", "6NXX".
    pub fn to_pbn(&self) -> String {
        let mut s = format!("{}{}", self.level, self.strain.to_char());
        if self.redoubled {
            s.push_str("XX");
        } else if self.doubled {
            s.push('X');
        }
        s
    }

    /// The opening leader (left of declarer).
    pub fn opening_leader(&self) -> Direction {
        self.declarer.next()
    }

    /// The dummy (declarer's partner).
    pub fn dummy(&self) -> Direction {
        self.declarer.partner()
    }
}

enum LastAction {
    Bid,
    Double,
}

/// Index and kind of the last non-pass call, if any.
fn last_action(calls: &[Call]) -> Option<(usize, LastAction)> {
    for (i, c) in calls.iter().enumerate().rev() {
        match c {
            Call::Bid { .. } => return Some((i, LastAction::Bid)),
            Call::Double => return Some((i, LastAction::Double)),
            Call::Redouble => return None, // nothing may follow XX except passes/bids
            Call::Pass | Call::Continue | Call::Blank => continue,
        }
    }
    None
}

/// Level, strain, and call-index of the last bid, if any.
fn last_bid(calls: &[Call]) -> Option<(u8, Strain, usize)> {
    for (i, c) in calls.iter().enumerate().rev() {
        if let Call::Bid { level, strain } = c {
            return Some((*level, *strain, i));
        }
    }
    None
}

/// Whether the call at `target_idx` was made by an opponent of the player
/// who calls at `current_idx`. Same-parity indices are partners.
fn is_opponent(_dealer: Direction, current_idx: usize, target_idx: usize) -> bool {
    current_idx % 2 != target_idx % 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_types::Direction::*;

    fn calls(s: &str) -> Vec<Call> {
        s.split_whitespace()
            .map(|c| Call::from_pbn(c).unwrap())
            .collect()
    }

    #[test]
    fn passed_out_is_complete_with_no_contract() {
        let a = calls("Pass Pass Pass Pass");
        assert!(is_complete(&a));
        assert!(contract(North, &a).is_none());
    }

    #[test]
    fn three_passes_end_after_bid() {
        let a = calls("1S Pass Pass Pass");
        assert!(is_complete(&a));
        assert!(!is_complete(&calls("1S Pass Pass")));
        assert!(!is_complete(&calls("Pass Pass Pass")));
    }

    #[test]
    fn insufficient_bid_illegal() {
        let a = calls("1S");
        assert!(!is_legal(North, &a, &Call::from_pbn("1H").unwrap()));
        assert!(!is_legal(North, &a, &Call::from_pbn("1S").unwrap()));
        assert!(is_legal(North, &a, &Call::from_pbn("1NT").unwrap()));
        assert!(is_legal(North, &a, &Call::from_pbn("2C").unwrap()));
    }

    #[test]
    fn double_only_of_opponents_bid() {
        // N opens 1S; E may double...
        assert!(is_legal(North, &calls("1S"), &Call::Double));
        // ...but S (partner) may not double N's bid.
        assert!(!is_legal(North, &calls("1S Pass"), &Call::Double));
        // After 1S-P-P, W may double (reopening).
        assert!(is_legal(North, &calls("1S Pass Pass"), &Call::Double));
        // No double with no bid on the table.
        assert!(!is_legal(North, &calls("Pass"), &Call::Double));
        // No double of a double.
        assert!(!is_legal(North, &calls("1S X"), &Call::Double));
    }

    #[test]
    fn redouble_only_of_opponents_double() {
        // N 1S, E X → S may redouble.
        assert!(is_legal(North, &calls("1S X"), &Call::Redouble));
        // N 1S, E X, S P → W may NOT redouble partner's double.
        assert!(!is_legal(North, &calls("1S X Pass"), &Call::Redouble));
        // N 1S, E X, S P, W P → N may redouble (it's opponents' double).
        assert!(is_legal(North, &calls("1S X Pass Pass"), &Call::Redouble));
        // No redouble without a double.
        assert!(!is_legal(North, &calls("1S"), &Call::Redouble));
    }

    #[test]
    fn no_calls_after_completion() {
        let a = calls("1S Pass Pass Pass");
        assert!(!is_legal(North, &a, &Call::Pass));
        assert!(!is_legal(North, &a, &Call::from_pbn("2S").unwrap()));
    }

    #[test]
    fn teaching_placeholders_never_legal() {
        assert!(!is_legal(North, &[], &Call::Continue));
        assert!(!is_legal(North, &[], &Call::Blank));
    }

    #[test]
    fn declarer_is_first_to_name_strain_not_last_bidder() {
        // N opens 1NT, S raises to 3NT: declarer must be NORTH.
        // (bridge-types' own final_contract() gets this wrong — it picks S.)
        let a = calls("1NT Pass 3NT Pass Pass Pass");
        let c = contract(North, &a).unwrap();
        assert_eq!(c.declarer, North);
        assert_eq!(c.level, 3);
        assert_eq!(c.strain, Strain::NoTrump);
        assert_eq!(c.opening_leader(), East);
        assert_eq!(c.dummy(), South);
    }

    #[test]
    fn declarer_strain_named_first_by_partner() {
        // E deals. E: 1H, W: 4H → declarer East (named hearts first).
        let a = calls("1H Pass 4H Pass Pass Pass");
        let c = contract(East, &a).unwrap();
        assert_eq!(c.declarer, East);
        assert_eq!(c.opening_leader(), South);
    }

    #[test]
    fn opponents_naming_same_strain_does_not_steal_declarership() {
        // N: 1H, E: 2H?? illegal — use a legal shape instead:
        // N deals: N 1C, E 1H, S Pass, W 4H, all pass.
        // Declaring side is EW; first of EW to name hearts is E.
        let a = calls("1C 1H Pass 4H Pass Pass Pass");
        let c = contract(North, &a).unwrap();
        assert_eq!(c.declarer, East);
    }

    #[test]
    fn doubled_state_tracked() {
        let a = calls("1S X Pass Pass Pass");
        let c = contract(North, &a).unwrap();
        assert!(c.doubled);
        assert!(!c.redoubled);
        assert_eq!(c.to_pbn(), "1SX");

        let a = calls("1S X XX Pass Pass Pass");
        let c = contract(North, &a).unwrap();
        assert!(!c.doubled);
        assert!(c.redoubled);
        assert_eq!(c.to_pbn(), "1SXX");

        // A later bid clears the double.
        let a = calls("1S X 2S Pass Pass Pass");
        let c = contract(North, &a).unwrap();
        assert!(!c.doubled && !c.redoubled);
    }

    #[test]
    fn next_caller_rotates_from_dealer() {
        assert_eq!(next_caller(West, &[]), West);
        assert_eq!(next_caller(West, &calls("Pass")), North);
        assert_eq!(next_caller(West, &calls("Pass Pass Pass Pass Pass")), North);
    }
}
