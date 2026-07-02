use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;

use crate::SharedState;

#[derive(Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
    uptime_seconds: u64,
}

pub fn router() -> Router<SharedState> {
    Router::new().route("/healthz", get(healthz))
}

async fn healthz(State(state): State<SharedState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        version: state.version,
        uptime_seconds: state.started_at.elapsed().as_secs(),
    })
}
