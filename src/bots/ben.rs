//! BEN cardplay client (opening leads + trick play).
//!
//! Encodings are a careful port of the frontend's `benClient.js` — the two
//! implementations must not drift (BEN's API is picky and its errors are
//! opaque). BEN is stateless: every call carries the full game state.
//!
//! BEN's conventions (learned empirically, mirrored from benClient.js):
//!   - `hand` is the *decision-maker's* original 13-card hand, dot format
//!     ("AK97543.K.T3.AK7"). For declarer-side plays (declarer's hand OR
//!     dummy's hand) the decision-maker is the declarer; never pass the
//!     dummy's hand as `hand`.
//!   - `seat` is the decision-maker's seat, not the seat physically playing.
//!   - `ctx` is the auction as a concatenated string: Pass → "--",
//!     X → "Db", XX → "Rd", bids as level+strain with NT as "N" ("1N").
//!   - `played` is every card played so far, chronologically, as
//!     suit-then-rank pairs ("DJDKD3D2").
//!   - `vul` is seat-relative: "" none, "@v" we are vul, "@V" they are,
//!     "@v@V" both.
//!   - `/play` responses carry a `player` field relative to declarer
//!     (0 = LHO, 1 = dummy, 2 = RHO, 3 = declarer) naming whose card the
//!     returned `card` is.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bridge_types::{Call, Card, Direction, Rank, Suit, Vulnerability};

/// Per-call budget. BEN answers in ~300ms–1s once warm; the ~20s cold start
/// is absorbed by `warm()` at service startup. A slow call falls back to
/// RandomLegal at the call site so a table never freezes.
const TIMEOUT: Duration = Duration::from_secs(8);

// ── Encoders (test vectors ported from benClient.js) ────────────────────

/// Auction → BEN ctx string: `["Pass","1NT","X","XX","2H"]` → `"--1NDbRd2H"`.
pub fn calls_to_ctx(calls: &[Call]) -> String {
    calls
        .iter()
        .map(|c| match c {
            Call::Pass => "--".to_string(),
            Call::Double => "Db".to_string(),
            Call::Redouble => "Rd".to_string(),
            // Strain::to_char is 'N' for notrump, exactly BEN's "1N" form.
            Call::Bid { level, strain } => format!("{}{}", level, strain.to_char()),
            Call::Continue | Call::Blank => String::new(),
        })
        .collect()
}

/// Chronological plays → "DJDKD3D2" (suit then rank, T for 10).
pub fn played_to_string(played: &[(Direction, Card)]) -> String {
    played
        .iter()
        .map(|(_, c)| format!("{}{}", c.suit.to_char(), c.rank.to_char()))
        .collect()
}

/// PBN vulnerability → BEN's seat-relative encoding.
pub fn vul_to_ben(vul: Vulnerability, seat: Direction) -> &'static str {
    match vul {
        Vulnerability::None => "",
        Vulnerability::Both => "@v@V",
        _ => {
            if vul.is_vulnerable(seat) {
                "@v"
            } else {
                "@V"
            }
        }
    }
}

/// Map BEN's `player` field (relative to declarer) back to an actual seat:
/// 0 = LHO (clockwise after declarer), 1 = dummy, 2 = RHO, 3 = declarer.
pub fn player_to_seat(player: u64, declarer: Direction) -> Direction {
    let mut seat = declarer;
    for _ in 0..((player + 1) % 4) {
        seat = seat.next();
    }
    seat
}

/// "S7" / "HT" → Card.
pub fn card_from_code(code: &str) -> Option<Card> {
    let mut ch = code.chars();
    let suit = Suit::from_char(ch.next()?)?;
    let rank = Rank::from_char(ch.next()?)?;
    if ch.next().is_some() {
        return None;
    }
    Some(Card::new(suit, rank))
}

// ── HTTP client ──────────────────────────────────────────────────────────

pub struct BenClient {
    base: String,
    http: reqwest::Client,
}

impl BenClient {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(TIMEOUT)
                .build()
                .expect("reqwest client"),
        }
    }

    /// Fire-and-forget warm-up. The first call after a long idle costs
    /// ~10–20s while the TensorFlow models load; subsequent calls drop to
    /// ~300–1000ms. Called once at service startup so the first real table
    /// doesn't eat the cold start. Errors are logged, never surfaced.
    pub fn warm(&self) {
        let url = format!(
            "{}/bid?hand=AKQ.KQJ.AT98.KQ2&seat=N&dealer=N&vul=&ctx=",
            self.base
        );
        let http = self.http.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            // Generous timeout: this call *is* the cold start.
            match http.get(&url).timeout(Duration::from_secs(60)).send().await {
                Ok(resp) => tracing::info!(
                    event = "ben_warmup",
                    status = %resp.status(),
                    ms = started.elapsed().as_millis() as u64,
                    ""
                ),
                Err(e) => tracing::warn!(event = "ben_warmup_failed", error = %e, ""),
            }
        });
    }

    /// GET /lead — opening lead for `seat` (always a defender).
    pub async fn lead(
        &self,
        hand_pbn: &str,
        seat: Direction,
        dealer: Direction,
        vul: &str,
        ctx: &str,
    ) -> Result<Card> {
        let json = self
            .get(
                "/lead",
                &[
                    ("hand", hand_pbn),
                    ("seat", &seat.to_char().to_string()),
                    ("dealer", &dealer.to_char().to_string()),
                    ("vul", vul),
                    ("ctx", ctx),
                ],
            )
            .await?;
        card_from_code(json["card"].as_str().unwrap_or_default())
            .ok_or_else(|| anyhow!("BEN /lead returned unparseable card: {}", json["card"]))
    }

    /// GET /play — next card. Returns the card plus BEN's `player` field
    /// (declarer-relative; see `player_to_seat`), when present.
    #[allow(clippy::too_many_arguments)] // mirrors BEN's flat query-string API
    pub async fn play(
        &self,
        hand_pbn: &str,
        dummy_pbn: &str,
        seat: Direction,
        dealer: Direction,
        vul: &str,
        ctx: &str,
        played: &str,
    ) -> Result<(Card, Option<u64>)> {
        let json = self
            .get(
                "/play",
                &[
                    ("hand", hand_pbn),
                    ("dummy", dummy_pbn),
                    ("seat", &seat.to_char().to_string()),
                    ("dealer", &dealer.to_char().to_string()),
                    ("vul", vul),
                    ("ctx", ctx),
                    ("played", played),
                ],
            )
            .await?;
        let card = card_from_code(json["card"].as_str().unwrap_or_default())
            .ok_or_else(|| anyhow!("BEN /play returned unparseable card: {}", json["card"]))?;
        Ok((card, json["player"].as_u64()))
    }

    async fn get(&self, path: &str, params: &[(&str, &str)]) -> Result<serde_json::Value> {
        let started = Instant::now();
        let resp = self
            .http
            .get(format!("{}{}", self.base, path))
            .query(params)
            .send()
            .await
            .with_context(|| format!("BEN {path} request failed"))?;
        let status = resp.status();
        let json: serde_json::Value = resp
            .json()
            .await
            .with_context(|| format!("BEN {path} returned non-JSON (HTTP {status})"))?;
        tracing::info!(
            event = "ben_call",
            path = path,
            status = %status,
            ms = started.elapsed().as_millis() as u64,
            ""
        );
        if !status.is_success() {
            bail!(
                "BEN {path}: {}",
                json["error"].as_str().unwrap_or(status.as_str())
            );
        }
        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_types::{Direction::*, Hand, Strain};

    fn call(s: &str) -> Call {
        Call::from_pbn(s).unwrap()
    }

    #[test]
    fn ctx_encoding_matches_ben_client_js() {
        // benClient.js bidsToCtx: Pass → "--", X → "Db", XX → "Rd",
        // "1NT" → "1N", suit bids as-is; all concatenated.
        let calls = [call("Pass"), call("1NT"), call("X"), call("XX"), call("2H")];
        assert_eq!(calls_to_ctx(&calls), "--1NDbRd2H");
        assert_eq!(calls_to_ctx(&[]), "");
        assert_eq!(
            calls_to_ctx(&[call("1C"), call("Pass"), call("3NT")]),
            "1C--3N"
        );
        assert_eq!(
            calls_to_ctx(&[Call::bid(7, Strain::NoTrump), Call::Double]),
            "7NDb"
        );
    }

    #[test]
    fn played_string_is_chronological_suit_rank() {
        // benClient.js playedToString vector: DJ DK D3 D2 → "DJDKD3D2".
        let played = [
            (North, card_from_code("DJ").unwrap()),
            (East, card_from_code("DK").unwrap()),
            (South, card_from_code("D3").unwrap()),
            (West, card_from_code("D2").unwrap()),
        ];
        assert_eq!(played_to_string(&played), "DJDKD3D2");
        // Ten encodes as T.
        assert_eq!(
            played_to_string(&[(North, card_from_code("ST").unwrap())]),
            "ST"
        );
        assert_eq!(played_to_string(&[]), "");
    }

    #[test]
    fn vul_encoding_is_seat_relative() {
        // benClient.js vulToBen: None → "", All/Both → "@v@V" regardless of
        // seat, otherwise "@v" if our side is vul else "@V".
        assert_eq!(vul_to_ben(Vulnerability::None, North), "");
        assert_eq!(vul_to_ben(Vulnerability::Both, East), "@v@V");
        assert_eq!(vul_to_ben(Vulnerability::NorthSouth, North), "@v");
        assert_eq!(vul_to_ben(Vulnerability::NorthSouth, South), "@v");
        assert_eq!(vul_to_ben(Vulnerability::NorthSouth, East), "@V");
        assert_eq!(vul_to_ben(Vulnerability::EastWest, West), "@v");
        assert_eq!(vul_to_ben(Vulnerability::EastWest, North), "@V");
    }

    #[test]
    fn player_maps_relative_to_declarer() {
        // benClient.js playerToSeat: SEAT_ORDER[(declarerIdx + 1 + player) % 4].
        // Declarer South: 0=LHO→W, 1=dummy→N, 2=RHO→E, 3=declarer→S.
        assert_eq!(player_to_seat(0, South), West);
        assert_eq!(player_to_seat(1, South), North);
        assert_eq!(player_to_seat(2, South), East);
        assert_eq!(player_to_seat(3, South), South);
        assert_eq!(player_to_seat(0, North), East);
        assert_eq!(player_to_seat(3, West), West);
    }

    #[test]
    fn hand_pbn_round_trips_dot_format() {
        // The dot format BEN expects ("AK97543.K.T3.AK7") is exactly
        // bridge-types' Hand::to_pbn (suits S.H.D.C, ranks descending).
        let s = "AK97543.K.T3.AK7";
        assert_eq!(Hand::from_pbn(s).unwrap().to_pbn(), s);
    }

    #[test]
    fn card_codes_parse() {
        assert_eq!(
            card_from_code("S7"),
            Some(Card::new(Suit::Spades, Rank::Seven))
        );
        assert_eq!(
            card_from_code("HT"),
            Some(Card::new(Suit::Hearts, Rank::Ten))
        );
        assert_eq!(card_from_code(""), None);
        assert_eq!(card_from_code("S"), None);
        assert_eq!(card_from_code("S77"), None);
        assert_eq!(card_from_code("X7"), None);
    }
}
