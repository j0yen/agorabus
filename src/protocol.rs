//! Line-protocol message types for the agorabus bus.
//!
//! Wire framing: one JSON object per line (newline-delimited JSON).
//!
//! All messages from a client carry an `op` discriminator. Server replies are
//! either a one-shot `Reply` (for non-streaming ops) or a sequence of
//! `ServerEvent` lines (for `subscribe`).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A peer's announce record. Captured at `announce` time and updated by
/// subsequent `update`/`heartbeat` ops.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerRecord {
    /// Session identifier (opaque; client-chosen).
    pub session_id: String,
    /// OS process id of the announcing session.
    pub pid: u32,
    /// Current working directory of the announcing session.
    pub cwd: String,
    /// Free-form current intent string (e.g. "work on PRD-X").
    #[serde(default)]
    pub intent: String,
    /// Last tool the session used, if reported (heartbeat carries this).
    #[serde(default)]
    pub last_tool: String,
    /// Skill the session is currently inside, if reported via a structured
    /// heartbeat or `intent set`. Sticky: set once, persists until cleared
    /// by an empty value.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub skill: String,
    /// PRD slug the session is currently building, if reported via a
    /// structured heartbeat or `intent set`. Sticky.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prd_slug: String,
    /// Working paths the session is touching. Sticky; bounded to
    /// [`MAX_WORKING_PATHS`] entries by the daemon.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub working_paths: Vec<String>,
    /// UNIX timestamp (seconds) of the most recent message from this peer.
    pub last_heartbeat_unix_secs: u64,
    /// Free-form additional metadata.
    #[serde(default)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// Maximum number of entries allowed in [`PeerRecord::working_paths`].
///
/// Heartbeats carrying more than this are rejected with
/// `{"ok":false,"error":"too_many_paths"}`. The cap keeps the per-peer
/// memory footprint bounded and discourages serializing an entire
/// project tree into the bus.
pub const MAX_WORKING_PATHS: usize = 8;

/// Incoming message from a client.
///
/// `op` is the discriminator. Unknown ops are rejected with
/// `Reply::error("unknown_op")`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ClientMessage {
    /// First-message-only: register the connection's identity. Required before
    /// any other op on the connection.
    Announce {
        /// Session identifier (opaque, client-chosen).
        session_id: String,
        /// Process id.
        pid: u32,
        /// Current working directory.
        cwd: String,
        /// Optional intent string.
        #[serde(default)]
        intent: String,
    },
    /// Update one or more fields of this connection's announce record.
    Update {
        /// New cwd, if changed.
        #[serde(default)]
        cwd: Option<String>,
        /// New intent string, if changed.
        #[serde(default)]
        intent: Option<String>,
    },
    /// Heartbeat: refreshes `last_heartbeat_unix_secs` and (optionally)
    /// `last_tool` plus the structured intent fields.
    ///
    /// `skill`, `prd_slug`, and `working_paths` follow sticky semantics:
    /// a heartbeat that omits a field leaves the prior value in place;
    /// an explicit empty string (or empty vector) clears it. This keeps
    /// the wire small for the common heartbeat-with-just-tool case while
    /// letting an `intent set` invocation update structured fields in
    /// place.
    Heartbeat {
        /// Name of the most recent tool invocation (optional).
        #[serde(default)]
        tool: String,
        /// Skill currently active in this session, if any. `Some("")`
        /// explicitly clears the prior skill; `None` (field omitted)
        /// leaves it sticky.
        #[serde(default)]
        skill: Option<String>,
        /// PRD slug currently being built, if any. Same sticky semantics
        /// as `skill`.
        #[serde(default)]
        prd_slug: Option<String>,
        /// Working paths the session is touching, if any. `Some(vec![])`
        /// explicitly clears; `None` leaves sticky.
        #[serde(default)]
        working_paths: Option<Vec<String>>,
    },
    /// Publish an event on a topic. All matching subscribers see it.
    Publish {
        /// Dotted topic string (e.g. `shared.lock-hint`).
        topic: String,
        /// Free-form JSON payload.
        data: serde_json::Value,
    },
    /// Subscribe to all topics whose dotted name begins with `prefix`.
    /// The connection enters streaming mode; further client messages on this
    /// connection are still accepted (e.g. `heartbeat`).
    Subscribe {
        /// Dotted prefix (empty string matches everything).
        prefix: String,
    },
    /// One-shot snapshot of all currently-live peers.
    Peers {},
    /// Acquire an advisory claim on `path`. Path is expected to be already
    /// canonicalized by the client. Refuses (returns `claim_conflict`) when
    /// an active claim from a different session is held on the same path,
    /// unless `force` is true. Same-session re-acquire is a renewal (TTL
    /// bumped, no error). On success the daemon broadcasts on topic
    /// `claim.acquire` with payload
    /// `{path, session_id, ttl_unix_secs, reason}`.
    ClaimAcquire {
        /// Canonicalized absolute path the claim covers.
        path: String,
        /// UNIX-seconds wall time at which the claim expires.
        ttl_unix_secs: u64,
        /// Free-form rationale shown to peers in `claim list`.
        #[serde(default)]
        reason: String,
        /// If true, evict any existing claim from a different session.
        #[serde(default)]
        force: bool,
    },
    /// Release the claim this session holds on `path`. Idempotent: releasing
    /// an unknown path returns `ok` with `{released: false}`. On a real
    /// release the daemon broadcasts on topic `claim.release` with payload
    /// `{path, session_id}`.
    ClaimRelease {
        /// Canonicalized absolute path the claim covers.
        path: String,
    },
    /// Snapshot of all currently-active claims. Expired claims are pruned
    /// silently before returning. Reply payload is `Vec<ClaimRecord>`.
    ClaimList {},
}

/// Daemon-side record of an active advisory claim. Returned in the
/// `ClaimList` payload and embedded in `claim_conflict` error details.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimRecord {
    /// Canonicalized absolute path the claim covers.
    pub path: String,
    /// Session that holds the claim.
    pub session_id: String,
    /// Wall-clock UNIX seconds at which the claim expires.
    pub ttl_unix_secs: u64,
    /// UNIX seconds at which the claim was acquired (or last renewed).
    pub acquired_unix_secs: u64,
    /// Free-form rationale (may be empty).
    #[serde(default)]
    pub reason: String,
}

/// Server reply (one-shot, line-framed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    /// True if the op succeeded.
    pub ok: bool,
    /// Optional error tag (`snake_case`) when `ok == false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Optional payload (e.g. peer list).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl Reply {
    /// Construct a successful empty reply.
    #[must_use]
    pub const fn ok() -> Self {
        Self {
            ok: true,
            error: None,
            data: None,
        }
    }

    /// Construct a successful reply with an attached JSON payload.
    #[must_use]
    pub const fn ok_with(data: serde_json::Value) -> Self {
        Self {
            ok: true,
            error: None,
            data: Some(data),
        }
    }

    /// Construct an error reply.
    pub fn error(tag: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(tag.into()),
            data: None,
        }
    }
}

/// Streaming event sent to a subscriber.
///
/// Subscribers receive one of these per line per matching publish.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerEvent {
    /// Topic the event was published on.
    pub topic: String,
    /// Payload as-published.
    pub data: serde_json::Value,
    /// `session_id` of the publishing peer.
    pub from: String,
}
