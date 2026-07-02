use axum::{routing::get, Router};

use crate::observability::metrics;
use crate::SharedState;

pub fn router() -> Router<SharedState> {
    Router::new().route("/metrics", get(handler))
}

async fn handler() -> String {
    metrics::render()
}
