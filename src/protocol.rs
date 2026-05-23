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
    /// UNIX timestamp (seconds) of the most recent message from this peer.
    pub last_heartbeat_unix_secs: u64,
    /// Free-form additional metadata.
    #[serde(default)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

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
    /// `last_tool`.
    Heartbeat {
        /// Name of the most recent tool invocation (optional).
        #[serde(default)]
        tool: String,
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
