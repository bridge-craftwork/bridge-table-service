use anyhow::{Context, Result};

pub struct Config {
    pub port: u16,
    pub log_level: String,
    pub log_format: String,
    pub database_path: String,
    pub dashboard_secret: String,
    pub ticket_secret: String,
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
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
