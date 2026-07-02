//! Script-generated deals via bridge-dealer-service (the dealer3 wrapper).
//!
//! The dealer API needs a Bearer token — a secret the browser can't hold —
//! so this service proxies: the client sends the raw `.dlr` script text
//! (fetched from the public Practice-Bidding-Scenarios repo) and gets a
//! board back through the normal `deal` flow. Process-wide singleton like
//! `bots::CLIENTS`; absent config (no `DEALER_TOKEN`) reports the source
//! as unavailable rather than failing weirdly.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use once_cell::sync::OnceCell;

static CLIENT: OnceCell<DealerClient> = OnceCell::new();

struct DealerClient {
    url: String,
    token: String,
    http: reqwest::Client,
}

/// Cap on client-supplied script text (the PBS `.dlr` files are a few KB).
const MAX_SCRIPT_BYTES: usize = 32 * 1024;

pub fn init(url: &str, token: Option<String>) {
    let Some(token) = token else {
        tracing::info!(event = "dealer_not_configured", "script deals disabled");
        return;
    };
    let client = DealerClient {
        url: url.trim_end_matches('/').to_string(),
        token,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("reqwest client"),
    };
    if CLIENT.set(client).is_err() {
        tracing::warn!(event = "dealer_already_initialized", "");
    } else {
        tracing::info!(event = "dealer_initialized", url, "");
    }
}

pub fn available() -> bool {
    CLIENT.get().is_some()
}

/// Run a dealer script and return the generated board as PBN tag lines.
///
/// The PBS scripts end in `action printoneline, <stats…>` — we swap in
/// `printpbn` (keeping the script's stats actions harmless) and force
/// `produce 1`, then keep only the `[Tag "…"]` lines of the output so the
/// PBN parser never sees the trailing frequency tables.
pub async fn generate_board_pbn(script: &str) -> Result<String> {
    let client = CLIENT
        .get()
        .ok_or_else(|| anyhow!("dealer service is not configured"))?;
    if script.len() > MAX_SCRIPT_BYTES {
        return Err(anyhow!("script too large"));
    }

    let prepared = format!(
        "produce 1\n{}",
        script.replace("action printoneline", "action printpbn")
    );

    let resp = client
        .http
        .post(format!("{}/deal", client.url))
        .bearer_auth(&client.token)
        .json(&serde_json::json!({ "script": prepared }))
        .send()
        .await
        .context("dealer request")?;
    if !resp.status().is_success() {
        return Err(anyhow!("dealer service returned {}", resp.status()));
    }
    let body: serde_json::Value = resp.json().await.context("dealer response")?;
    let output = body["output"].as_str().unwrap_or("");
    let pbn: String = output
        .lines()
        .filter(|l| l.trim_start().starts_with('['))
        .collect::<Vec<_>>()
        .join("\n");
    if !pbn.contains("[Deal ") {
        return Err(anyhow!(
            "dealer produced no deal (script may filter everything out)"
        ));
    }
    Ok(pbn)
}
