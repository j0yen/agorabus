//! agorabus — single-host advisory pub/sub bus for concurrent Claude sessions.
//!
//! See `PRD-cross-session-bus.md` for motivation. This library exposes the
//! daemon (`run_daemon`), the client (`Client`), and the line-protocol message
//! types. The binary in `src/main.rs` is a thin clap wrapper.

#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod claim_guard;
pub mod client;
pub mod daemon;
pub mod doctor;
pub mod fleet;
pub mod persist;
pub mod protocol;
pub mod reconnect;
pub mod reload;

pub use claim_guard::ClaimGuard;
pub use client::Client;
pub use daemon::{DaemonConfig, run_daemon};
pub use fleet::{FleetStore, merge_peers, peer_age_secs};
pub use persist::{DurableState, StickyIntent, default_state_path, load as load_state, save as save_state};
pub use protocol::{ClaimRecord, ClientMessage, DrainNotice, FleetPresenceEvent, PeerRecord, Reply, ServerEvent};
pub use reconnect::{ReconnectConfig, backoff_delay, reconnect_subscribe};

use std::path::PathBuf;

/// Default UDS path for the bus (`~/.cache/agorabus/sock`).
///
/// Falls back to `/tmp/agorabus-<uid>/sock` if `$HOME` is unset.
#[must_use]
pub fn default_socket_path() -> PathBuf {
    std::env::var("HOME").map_or_else(
        |_| {
            let uid = std::env::var("UID").unwrap_or_else(|_| "0".into());
            PathBuf::from(format!("/tmp/agorabus-{uid}/sock"))
        },
        |home| PathBuf::from(home).join(".cache/agorabus/sock"),
    )
}

/// Default heartbeat-timeout in seconds (PRD §4.3).
pub const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 60;

/// Default drain grace period in milliseconds (PRD-agorabus-drain-notice).
///
/// After the drain notice is broadcast, the daemon waits at most this long
/// for subscriber writes to flush before aborting all connections and exiting.
pub const DEFAULT_DRAIN_GRACE_MS: u64 = 200;

/// Default resume hint in milliseconds embedded in the drain notice
/// (PRD-agorabus-drain-notice). Subscribers are advised to wait at least
/// this long before reconnecting to avoid a thundering-herd on rebind.
pub const DEFAULT_DRAIN_RESUME_HINT_MS: u64 = 3_000;

/// Default debounce window in milliseconds for state-flush writes
/// (PRD-agorabus-state-persist). A burst of mutations within this window
/// coalesces into a single write.
pub const DEFAULT_STATE_FLUSH_MS: u64 = 250;
