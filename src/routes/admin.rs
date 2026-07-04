//! Service-to-service admin endpoints (session create/close).
//!
//! Called by the bridge-classroom API only — auth is the shared
//! `X-Service-Secret` header, which must equal `TICKET_SECRET` (the same
//! secret that signs join tickets; one shared secret keeps the Mac↔droplet
//! seam to a single credential).
//!
//! `POST /admin/sessions` body:
//! ```json
//! {
//!   "id": "…uuid…",
//!   "kind": "teacher_set" | "adhoc",
//!   "boards_pbn": "[Board \"1\"]…",     // raw PBN, parsed here
//!   "table_count": 2,
//!   "seat_policy": { "mode": "auto", "pattern": "one_per_seat", "seats": ["S"] },
//!   "owner_sub": "…user id…"
//! }
//! ```
//! Creates the session with rooms `"<id>-t1"…"<id>-tN"`, each starting on
//! the first board. Idempotent: re-POSTing a live id returns the existing
//! session untouched (a retried create must not evict seated players).
//!
//! `DELETE /admin/sessions/:id` unregisters the session and tells every
//! connection at its tables (`session_closed` event) to hang up.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::sessions::{parse_boards, SeatPolicy, Session, SessionKind};
use crate::SharedState;

pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/admin/sessions", post(create_session))
        .route("/admin/sessions/:id", delete(delete_session))
}

fn authorized(headers: &HeaderMap, state: &SharedState) -> bool {
    headers
        .get("x-service-secret")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == state.ticket_secret)
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(json!({ "ok": false, "error": msg.into() }))).into_response()
}

const MAX_TABLES: usize = 20;

#[derive(Deserialize)]
struct CreateSessionRequest {
    id: String,
    #[serde(default = "default_kind")]
    kind: String,
    boards_pbn: String,
    #[serde(default = "default_table_count")]
    table_count: usize,
    #[serde(default)]
    seat_policy: Value,
    owner_sub: String,
}

fn default_kind() -> String {
    "adhoc".to_string()
}

fn default_table_count() -> usize {
    1
}

async fn create_session(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(req): Json<CreateSessionRequest>,
) -> Response {
    if !authorized(&headers, &state) {
        return err(StatusCode::UNAUTHORIZED, "bad service secret");
    }
    if req.id.trim().is_empty() || req.owner_sub.trim().is_empty() {
        return err(StatusCode::BAD_REQUEST, "id and owner_sub are required");
    }
    if req.id == "demo" {
        return err(StatusCode::BAD_REQUEST, "'demo' is reserved");
    }
    // 0 is allowed: a session can start with no tables (student-fill /
    // add_tables create them later — roadmap §Phase 3.2).
    if req.table_count > MAX_TABLES {
        return err(
            StatusCode::BAD_REQUEST,
            format!("table_count must be 0..={MAX_TABLES}"),
        );
    }
    let kind = match SessionKind::parse(&req.kind) {
        Ok(k) => k,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    let seat_policy = match SeatPolicy::from_value(&req.seat_policy) {
        Ok(p) => p,
        Err(e) => return err(StatusCode::BAD_REQUEST, format!("seat_policy: {e}")),
    };
    // An empty board set is allowed: the session starts idle (no deal
    // loaded) and the teacher sends a source later via `load_boards`
    // (roadmap §Phase 3.1). A non-empty PBN is parsed + validated here.
    let boards = if req.boards_pbn.trim().is_empty() {
        Vec::new()
    } else {
        match parse_boards(&req.boards_pbn) {
            Ok(b) => b,
            Err(e) => return err(StatusCode::BAD_REQUEST, e),
        }
    };
    let board_count = boards.len();

    let session = Session::new(
        req.id.clone(),
        kind,
        req.owner_sub.clone(),
        seat_policy,
        boards,
        req.table_count,
    );
    let (session, created) = state.rooms.insert_session(session).await;

    if created {
        let _ = crate::observability::events::record_event(
            &state.db,
            "session_created",
            json!({
                "id": session.id,
                "kind": req.kind,
                "owner_sub": req.owner_sub,
                "tables": req.table_count,
                "boards": board_count,
            }),
        )
        .await;
        tracing::info!(
            event = "session_created",
            id = %session.id,
            kind = %req.kind,
            tables = req.table_count,
            boards = board_count,
            ""
        );
    }

    (
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(json!({
            "ok": true,
            "id": session.id,
            "created": created,
            "tables": session.rooms_snapshot().await.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
            "boards": session.deck_status().await.2,
        })),
    )
        .into_response()
}

async fn delete_session(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !authorized(&headers, &state) {
        return err(StatusCode::UNAUTHORIZED, "bad service secret");
    }
    let Some(session) = state.rooms.remove_session(&id).await else {
        // Idempotent: closing an unknown/already-closed session succeeds.
        return (
            StatusCode::OK,
            Json(json!({ "ok": true, "removed": false })),
        )
            .into_response();
    };
    // Tell everyone at the tables; connections hang up on this event.
    for room in session.rooms_snapshot().await {
        room.broadcast(
            json!({
                "t": "event",
                "table_id": room.id,
                "kind": "session_closed",
            })
            .to_string(),
        );
    }
    // Unblock teacher connections waiting on the notify channel too.
    session.notify_lobby();
    let _ = crate::observability::events::record_event(
        &state.db,
        "session_closed",
        json!({ "id": id }),
    )
    .await;
    tracing::info!(event = "session_closed", id = %id, "");
    (StatusCode::OK, Json(json!({ "ok": true, "removed": true }))).into_response()
}
