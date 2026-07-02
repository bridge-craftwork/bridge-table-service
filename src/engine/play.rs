//! Cardplay legality and trick resolution.
//!
//! Semantics mirror the frontend's `cardplayRules.js` (getLegalCards /
//! isLegalPlay / trickWinner / nextSeatToPlay); the tests below port its
//! vectors so the two implementations can't drift.

use bridge_types::{Card, Direction, Strain, Suit};

/// Trump suit of a contract strain (None for notrump).
pub fn trump_of(strain: Strain) -> Option<Suit> {
    match strain {
        Strain::Clubs => Some(Suit::Clubs),
        Strain::Diamonds => Some(Suit::Diamonds),
        Strain::Hearts => Some(Suit::Hearts),
        Strain::Spades => Some(Suit::Spades),
        Strain::NoTrump => None,
    }
}

/// Legal cards from `remaining` given the suit led to the current trick
/// (`None` when leading): must follow suit if able, else anything.
pub fn legal_cards(remaining: &[Card], lead_suit: Option<Suit>) -> Vec<Card> {
    match lead_suit {
        None => remaining.to_vec(),
        Some(lead) => {
            let of_suit: Vec<Card> = remaining
                .iter()
                .copied()
                .filter(|c| c.suit == lead)
                .collect();
            if of_suit.is_empty() {
                remaining.to_vec()
            } else {
                of_suit
            }
        }
    }
}

/// Whether `card` is a legal play from `remaining` given the led suit.
pub fn is_legal_play(card: Card, remaining: &[Card], lead_suit: Option<Suit>) -> bool {
    legal_cards(remaining, lead_suit).contains(&card)
}

/// Winner of a completed trick. `plays` is chronological (index 0 led),
/// `leader` is the seat that played `plays[0]`.
pub fn trick_winner(leader: Direction, plays: &[Card; 4], trump: Option<Suit>) -> Direction {
    let lead_suit = plays[0].suit;
    let mut winner = 0usize;
    for (i, card) in plays.iter().enumerate().skip(1) {
        if beats(*card, plays[winner], lead_suit, trump) {
            winner = i;
        }
    }
    let mut seat = leader;
    for _ in 0..winner {
        seat = seat.next();
    }
    seat
}

/// True if `a` beats `b` in the same trick. Trump beats non-trump; otherwise
/// higher rank in the lead suit wins; off-suit non-trump can never win.
fn beats(a: Card, b: Card, lead_suit: Suit, trump: Option<Suit>) -> bool {
    let a_trump = trump == Some(a.suit);
    let b_trump = trump == Some(b.suit);
    if a_trump && !b_trump {
        return true;
    }
    if !a_trump && b_trump {
        return false;
    }
    if a_trump && b_trump {
        return a.rank > b.rank;
    }
    if a.suit == lead_suit && b.suit != lead_suit {
        return true;
    }
    if a.suit != lead_suit && b.suit == lead_suit {
        return false;
    }
    if a.suit == lead_suit && b.suit == lead_suit {
        return a.rank > b.rank;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_types::Direction::*;
    use bridge_types::{Rank, Suit};

    fn card(s: &str) -> Card {
        let mut ch = s.chars();
        Card::new(
            Suit::from_char(ch.next().unwrap()).unwrap(),
            Rank::from_char(ch.next().unwrap()).unwrap(),
        )
    }

    fn cards(s: &str) -> Vec<Card> {
        s.split_whitespace().map(card).collect()
    }

    #[test]
    fn leading_any_card_is_legal() {
        let rem = cards("SA H2 D7");
        assert_eq!(legal_cards(&rem, None), rem);
    }

    #[test]
    fn must_follow_suit_when_able() {
        let rem = cards("SA S2 H9");
        let legal = legal_cards(&rem, Some(Suit::Spades));
        assert_eq!(legal, cards("SA S2"));
        assert!(is_legal_play(card("S2"), &rem, Some(Suit::Spades)));
        assert!(!is_legal_play(card("H9"), &rem, Some(Suit::Spades)));
    }

    #[test]
    fn void_in_lead_suit_frees_all_cards() {
        let rem = cards("H9 D3 C8");
        assert_eq!(legal_cards(&rem, Some(Suit::Spades)), rem);
    }

    // trickWinner vectors mirroring cardplayRules.test.js semantics:

    #[test]
    fn highest_of_lead_suit_wins_in_nt() {
        // N leads S5, E plays SK, S plays SA, W discards H2.
        let plays = [card("S5"), card("SK"), card("SA"), card("H2")];
        assert_eq!(trick_winner(North, &plays, None), South);
    }

    #[test]
    fn offsuit_high_card_cannot_win() {
        // E leads D2; S's off-suit HA is worthless in NT; W's D3 wins.
        let plays = [card("D2"), card("HA"), card("D3"), card("H2")];
        assert_eq!(trick_winner(East, &plays, None), West);
    }

    #[test]
    fn trump_beats_lead_suit() {
        // W leads SA; E (3rd to play) ruffs with H2 and wins.
        let plays = [card("SA"), card("S3"), card("H2"), card("S9")];
        assert_eq!(trick_winner(West, &plays, Some(Suit::Hearts)), East);
    }

    #[test]
    fn overruff_wins() {
        // N leads D4; E ruffs H2; S overruffs H9 and wins.
        let plays = [card("D4"), card("H2"), card("H9"), card("D8")];
        assert_eq!(trick_winner(North, &plays, Some(Suit::Hearts)), South);
    }

    #[test]
    fn trump_lead_highest_trump_wins() {
        // S leads HQ; E (4th to play) takes the HA.
        let plays = [card("HQ"), card("HK"), card("H2"), card("HA")];
        assert_eq!(trick_winner(South, &plays, Some(Suit::Hearts)), East);
    }

    #[test]
    fn trump_of_strains() {
        assert_eq!(trump_of(Strain::NoTrump), None);
        assert_eq!(trump_of(Strain::Spades), Some(Suit::Spades));
    }
}
