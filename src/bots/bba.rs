//! BBA bidding client with the prefix-cache flow.
//!
//! Call shape ported from the frontend's `BiddingPracticeView.vue`
//! (`generateAuction`): POST `{BBA_URL}/api/auction/generate` with the full
//! deal ("N:"-first PBN), dealer, vulnerability ("Both", not PBN's "All"),
//! MP scoring, and explicit convention cards; optionally `auctionPrefix`
//! (the calls made so far, in BBA's own token format — "Pass"/"X"/"XX"/
//! "1NT"). The response is the *complete* predicted auction from call 1,
//! including any prefix.
//!
//! Prefix-cache flow (per the project plan): request the whole predicted
//! auction at the first bot call of a board and serve subsequent bot calls
//! from it while the actual calls remain a prefix of the prediction. Any
//! divergence — a human bidding something else, or an undo rewinding onto a
//! different line — fails the prefix check and triggers a re-request with
//! `auctionPrefix` = the actual calls. The cache is derived-safe: it is
//! validated against the folded auction on every use and never trusted
//! blindly, so undo needs no special-casing.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use bridge_types::{Call, Direction, Strain, Vulnerability};
use serde_json::{json, Value};

use crate::table::BoardSetup;

/// Convention card for both pairs until per-table conventions arrive.
const CONVENTION_CARD: &str = "21GF-DEFAULT";

/// Per-call budget; a slow BBA falls back to Pass at the call site.
const TIMEOUT: Duration = Duration::from_secs(8);

/// A call in BBA's token format: "Pass", "X", "XX", "1NT" (not PBN's "1N").
pub fn call_to_bba(c: &Call) -> String {
    match c {
        Call::Pass => "Pass".to_string(),
        Call::Double => "X".to_string(),
        Call::Redouble => "XX".to_string(),
        Call::Bid { level, strain } => match strain {
            Strain::NoTrump => format!("{level}NT"),
            _ => format!("{}{}", level, strain.to_char()),
        },
        Call::Continue | Call::Blank => String::new(),
    }
}

/// BBA vulnerability token: PBN's "All" is "Both" to BBA (mirrors the
/// frontend's `deal.vulnerable === 'All' ? 'Both' : ...`).
fn vul_to_bba(v: Vulnerability) -> &'static str {
    match v {
        Vulnerability::None => "None",
        Vulnerability::NorthSouth => "NS",
        Vulnerability::EastWest => "EW",
        Vulnerability::Both => "Both",
    }
}

/// Request body for /api/auction/generate.
fn build_body(board: &BoardSetup, prefix: &[Call]) -> Value {
    let mut body = json!({
        "deal": {
            "pbn": board.deal.to_pbn(Direction::North),
            "dealer": board.dealer.to_char().to_string(),
            "vulnerability": vul_to_bba(board.vulnerable),
            "scoring": "MP",
        },
        "conventions": { "ns": CONVENTION_CARD, "ew": CONVENTION_CARD },
    });
    if !prefix.is_empty() {
        body["auctionPrefix"] = json!(prefix.iter().map(call_to_bba).collect::<Vec<_>>());
    }
    body
}

/// Serve the next call from a cached predicted auction, iff the actual
/// `calls` so far are a strict prefix of the prediction. Returns None on
/// divergence (or an exhausted/absent cache) — caller re-requests.
pub fn continuation(cache: &Option<Vec<Call>>, calls: &[Call]) -> Option<Call> {
    let predicted = cache.as_ref()?;
    if predicted.len() > calls.len() && predicted[..calls.len()] == *calls {
        Some(predicted[calls.len()].clone())
    } else {
        None
    }
}

pub struct BbaClient {
    base: String,
    http: reqwest::Client,
}

impl BbaClient {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(TIMEOUT)
                .build()
                .expect("reqwest client"),
        }
    }

    /// Predict the full auction for `board`, continuing from `prefix`.
    /// The returned Vec always starts with `prefix` (verified — a
    /// prediction that contradicts its own prefix would make the cache
    /// instantly stale and hammer BBA with a re-request per bot call).
    pub async fn generate(&self, board: &BoardSetup, prefix: &[Call]) -> Result<Vec<Call>> {
        let started = Instant::now();
        let resp = self
            .http
            .post(format!("{}/api/auction/generate", self.base))
            .json(&build_body(board, prefix))
            .send()
            .await
            .context("BBA request failed")?;
        let status = resp.status();
        let json: Value = resp
            .json()
            .await
            .with_context(|| format!("BBA returned non-JSON (HTTP {status})"))?;
        tracing::info!(
            event = "bba_call",
            status = %status,
            prefix_len = prefix.len(),
            ms = started.elapsed().as_millis() as u64,
            ""
        );
        if !status.is_success() {
            bail!("BBA: {}", json["error"].as_str().unwrap_or(status.as_str()));
        }
        if json["success"] != true {
            bail!("BBA: {}", json["error"].as_str().unwrap_or("success=false"));
        }
        let Some(auction) = json["auction"].as_array() else {
            bail!("BBA: response has no auction array");
        };
        let predicted: Vec<Call> = auction
            .iter()
            .map(|v| v.as_str().and_then(Call::from_pbn))
            .collect::<Option<_>>()
            .context("BBA: unparseable call in auction")?;
        if predicted.len() < prefix.len() || predicted[..prefix.len()] != *prefix {
            bail!("BBA: predicted auction does not extend the requested prefix");
        }
        Ok(predicted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_types::Deal;

    fn call(s: &str) -> Call {
        Call::from_pbn(s).unwrap()
    }

    fn calls(s: &str) -> Vec<Call> {
        s.split_whitespace().map(call).collect()
    }

    fn board(vul: Vulnerability) -> BoardSetup {
        BoardSetup {
            number: 1,
            dealer: Direction::East,
            vulnerable: vul,
            deal: Deal::from_pbn(
                "N:K843.T542.J6.863 AQJ7.K.Q75.AT942 962.AJ7.KT82.J75 T5.Q9863.A943.KQ",
            )
            .unwrap(),
            play_line: None,
        }
    }

    #[test]
    fn bba_tokens_use_nt_not_n() {
        assert_eq!(call_to_bba(&call("Pass")), "Pass");
        assert_eq!(call_to_bba(&call("X")), "X");
        assert_eq!(call_to_bba(&call("XX")), "XX");
        assert_eq!(call_to_bba(&call("1NT")), "1NT");
        assert_eq!(call_to_bba(&call("2H")), "2H");
        assert_eq!(call_to_bba(&call("7C")), "7C");
    }

    #[test]
    fn body_shape_matches_frontend_call() {
        let b = build_body(&board(Vulnerability::Both), &[]);
        assert!(b["deal"]["pbn"].as_str().unwrap().starts_with("N:"));
        assert_eq!(b["deal"]["dealer"], "E");
        // PBN "All" must go out as "Both".
        assert_eq!(b["deal"]["vulnerability"], "Both");
        assert_eq!(b["deal"]["scoring"], "MP");
        assert_eq!(b["conventions"]["ns"], "21GF-DEFAULT");
        assert_eq!(b["conventions"]["ew"], "21GF-DEFAULT");
        // No auctionPrefix key when there is no prefix (frontend omits it).
        assert!(b.get("auctionPrefix").is_none());

        let b = build_body(&board(Vulnerability::None), &calls("1NT Pass"));
        assert_eq!(b["deal"]["vulnerability"], "None");
        assert_eq!(b["auctionPrefix"], json!(["1NT", "Pass"]));
    }

    #[test]
    fn continuation_serves_only_while_prefix_matches() {
        let cache = Some(calls("1C Pass 1H Pass 3NT Pass Pass Pass"));

        // From the top: first predicted call.
        assert_eq!(continuation(&cache, &[]), Some(call("1C")));
        // Mid-auction, on the predicted line.
        assert_eq!(continuation(&cache, &calls("1C Pass")), Some(call("1H")));
        // Divergence: actual calls left the predicted line.
        assert_eq!(continuation(&cache, &calls("1C 1S")), None);
        // Undo case: a rewind that stays on the line still serves.
        assert_eq!(continuation(&cache, &calls("1C")), Some(call("Pass")));
        // Exhausted: actual auction as long as (or longer than) prediction.
        assert_eq!(
            continuation(&cache, &calls("1C Pass 1H Pass 3NT Pass Pass Pass")),
            None
        );
        // No cache yet.
        assert_eq!(continuation(&None, &[]), None);
    }
}
