use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::{middleware, Router};
use tower_http::trace::TraceLayer;

mod auth;
mod config;
mod db;
mod engine;
mod observability;
mod rooms;
mod routes;
mod table;

pub struct AppState {
    pub started_at: Instant,
    pub version: &'static str,
    pub dashboard_secret: String,
    pub ticket_secret: String,
    pub db: sqlx::SqlitePool,
    pub rooms: rooms::Registry,
}

pub type SharedState = Arc<AppState>;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = config::Config::from_env()?;

    observability::logging::init(&cfg.log_level, &cfg.log_format);
    observability::metrics::init();

    let db = db::open(&cfg.database_path).await?;

    let state: SharedState = Arc::new(AppState {
        started_at: Instant::now(),
        version: env!("CARGO_PKG_VERSION"),
        dashboard_secret: cfg.dashboard_secret.clone(),
        ticket_secret: cfg.ticket_secret.clone(),
        db,
        rooms: rooms::Registry::new(),
    });

    let app = Router::new()
        .merge(routes::health::router())
        .merge(routes::metrics::router())
        .merge(routes::dashboard::router())
        .merge(routes::ws::router())
        .layer(middleware::from_fn(observability::metrics::track))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(event = "listening", address = %addr, "service started");

    axum::serve(listener, app).await?;
    Ok(())
}
