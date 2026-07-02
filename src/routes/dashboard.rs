use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use serde::Deserialize;

use crate::SharedState;

#[derive(Deserialize)]
pub struct DashboardQuery {
    key: Option<String>,
}

pub fn router() -> Router<SharedState> {
    Router::new().route("/dashboard", get(dashboard))
}

async fn dashboard(State(state): State<SharedState>, Query(q): Query<DashboardQuery>) -> Response {
    if q.key.as_deref() != Some(state.dashboard_secret.as_str()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    // Live-edit fallback: read from disk if present (dev), embedded copy otherwise.
    let html = std::fs::read_to_string("wwwroot/dashboard.html")
        .unwrap_or_else(|_| include_str!("../../wwwroot/dashboard.html").to_string());

    let uptime = state.started_at.elapsed().as_secs();
    let html = html
        .replace("{{VERSION}}", state.version)
        .replace("{{UPTIME_SECONDS}}", &uptime.to_string());

    Html(html).into_response()
}
