//! Domain-event recording. Double-writes to the SQLite `events` table and to
//! stdout as a JSON log line, so the dashboard and ops tooling see the same
//! events.

use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use sqlx::SqlitePool;

/// Record a domain-significant event.
///
/// `name` is a snake_case identifier (e.g. `"table_joined"`). `fields` is
/// any JSON object — its keys are merged into the log line and stored verbatim
/// in the events table.
pub async fn record_event(db: &SqlitePool, name: &str, fields: Value) -> Result<()> {
    let ts = Utc::now().to_rfc3339();
    let fields_json = serde_json::to_string(&fields)?;

    sqlx::query("INSERT INTO events (ts, event, fields) VALUES (?, ?, ?)")
        .bind(&ts)
        .bind(name)
        .bind(&fields_json)
        .execute(db)
        .await?;

    tracing::info!(event = %name, fields = %fields_json, "");
    Ok(())
}
