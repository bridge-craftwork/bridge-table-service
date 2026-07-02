//! Server-authoritative game rules.
//!
//! Bridge *types* come from the sister `bridge-types` crate; this module adds
//! the *legality* logic a live game referee needs: auction call legality,
//! correct declarer determination, and follow-suit cardplay legality. The
//! semantics mirror the Bridge-Classroom frontend's `cardplayRules.js` (whose
//! test vectors are ported into `play::tests`) so client hints and the server
//! referee can't drift.

pub mod auction;
pub mod play;
