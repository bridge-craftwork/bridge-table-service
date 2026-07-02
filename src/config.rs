use anyhow::{Context, Result};

pub struct Config {
    pub port: u16,
    pub log_level: String,
    pub log_format: String,
    pub database_path: String,
    pub dashboard_secret: String,
    pub ticket_secret: String,
    /// BEN cardplay engine. On the droplet set `BEN_URL=http://ben:8085`
    /// (same bridge-net docker network) instead of the public hostname.
    pub ben_url: String,
    /// BBA bidding engine (Windows-VM-hosted, public URL only).
    pub bba_url: String,
    /// Per-call budget for BEN cardplay requests, in milliseconds. Leads
    /// and early plays routinely need >8s even on a warm BEN; the
    /// RandomLegal fallback still catches anything slower than this.
    pub bot_timeout_ms: u64,
    /// bridge-dealer-service (dealer3 wrapper) for script-generated deals.
    /// On the droplet: internal `http://bridge-dealer-service:8001`.
    pub dealer_url: String,
    /// Bearer token for the dealer service. Absent → the "script" deal
    /// source politely reports itself unavailable.
    pub dealer_token: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            port: env_or("PORT", "8004").parse().context("PORT")?,
            log_level: env_or("LOG_LEVEL", "info"),
            log_format: env_or("LOG_FORMAT", "json"),
            database_path: env_or("DATABASE_PATH", "./data/bridge-table-service.db"),
            dashboard_secret: std::env::var("DASHBOARD_SECRET")
                .context("DASHBOARD_SECRET is required")?,
            ticket_secret: std::env::var("TICKET_SECRET").context("TICKET_SECRET is required")?,
            ben_url: env_or("BEN_URL", "https://ben.bridge-craftwork.com"),
            bba_url: env_or("BBA_URL", "https://bba.harmonicsystems.com"),
            bot_timeout_ms: env_or("BOT_TIMEOUT_MS", "20000")
                .parse()
                .context("BOT_TIMEOUT_MS")?,
            dealer_url: env_or("DEALER_URL", "http://bridge-dealer-service:8001"),
            dealer_token: std::env::var("DEALER_TOKEN").ok().filter(|s| !s.is_empty()),
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
