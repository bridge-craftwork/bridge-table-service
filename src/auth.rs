//! Join-ticket verification.
//!
//! The bridge-classroom API mints short-lived tickets (HMAC-SHA256 over a
//! JSON payload, keyed by the `TICKET_SECRET` shared between the two
//! services). This service verifies them offline — no callback to the API.
//!
//! Wire format: `base64url(payload_json) + "." + base64url(hmac_sha256(payload_b64))`
//! The MAC is computed over the base64url payload *string* so both sides
//! sign exactly the bytes that travel.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ticket {
    /// User id (bridge-classroom user UUID) or guest id.
    pub sub: String,
    /// Display name shown at the table.
    pub name: String,
    /// The table session this ticket admits to.
    pub session_id: String,
    /// "student" | "teacher" | "guest"
    pub role: String,
    /// Unix seconds expiry.
    pub exp: i64,
}

#[derive(Debug, PartialEq)]
pub enum TicketError {
    Malformed,
    BadSignature,
    Expired,
}

impl std::fmt::Display for TicketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TicketError::Malformed => write!(f, "malformed ticket"),
            TicketError::BadSignature => write!(f, "bad ticket signature"),
            TicketError::Expired => write!(f, "ticket expired"),
        }
    }
}

pub fn verify_ticket(token: &str, secret: &str) -> Result<Ticket, TicketError> {
    verify_ticket_at(token, secret, chrono::Utc::now().timestamp())
}

/// Verification with an injected clock, for tests.
pub fn verify_ticket_at(token: &str, secret: &str, now: i64) -> Result<Ticket, TicketError> {
    let (payload_b64, sig_b64) = token.split_once('.').ok_or(TicketError::Malformed)?;

    let sig = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| TicketError::Malformed)?;

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| TicketError::Malformed)?;
    mac.update(payload_b64.as_bytes());
    mac.verify_slice(&sig)
        .map_err(|_| TicketError::BadSignature)?;

    let payload = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| TicketError::Malformed)?;
    let ticket: Ticket = serde_json::from_slice(&payload).map_err(|_| TicketError::Malformed)?;

    if ticket.exp < now {
        return Err(TicketError::Expired);
    }
    Ok(ticket)
}

/// Mint a ticket. Used by tests here; the production minter is the
/// bridge-classroom API (same format — this function is the reference
/// implementation for it).
#[allow(dead_code)] // test + reference use; production minting lives in the Mac API
pub fn mint_ticket(ticket: &Ticket, secret: &str) -> String {
    let payload_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(ticket).unwrap());
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(payload_b64.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{payload_b64}.{sig_b64}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticket() -> Ticket {
        Ticket {
            sub: "user-123".into(),
            name: "Alice".into(),
            session_id: "sess-1".into(),
            role: "student".into(),
            exp: 2_000_000_000,
        }
    }

    #[test]
    fn round_trip() {
        let t = ticket();
        let token = mint_ticket(&t, "secret");
        let got = verify_ticket_at(&token, "secret", 1_900_000_000).unwrap();
        assert_eq!(got, t);
    }

    #[test]
    fn wrong_secret_rejected() {
        let token = mint_ticket(&ticket(), "secret");
        assert_eq!(
            verify_ticket_at(&token, "other", 1_900_000_000),
            Err(TicketError::BadSignature)
        );
    }

    #[test]
    fn expired_rejected() {
        let token = mint_ticket(&ticket(), "secret");
        assert_eq!(
            verify_ticket_at(&token, "secret", 2_000_000_001),
            Err(TicketError::Expired)
        );
    }

    #[test]
    fn tampered_payload_rejected() {
        let t = ticket();
        let token = mint_ticket(&t, "secret");
        let (_, sig) = token.split_once('.').unwrap();
        let mut evil = t.clone();
        evil.role = "teacher".into();
        let evil_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&evil).unwrap());
        let forged = format!("{evil_b64}.{sig}");
        assert_eq!(
            verify_ticket_at(&forged, "secret", 1_900_000_000),
            Err(TicketError::BadSignature)
        );
    }

    #[test]
    fn garbage_rejected() {
        assert_eq!(
            verify_ticket_at("not-a-ticket", "secret", 0),
            Err(TicketError::Malformed)
        );
    }
}
