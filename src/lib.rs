//! agorabus — single-host advisory pub/sub bus for concurrent Claude sessions.
//!
//! See `PRD-cross-session-bus.md` for motivation. This library exposes the
//! daemon (`run_daemon`), the client (`Client`), and the line-protocol message
//! types. The binary in `src/main.rs` is a thin clap wrapper.

#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod client;
pub mod daemon;
pub mod protocol;

pub use client::Client;
pub use daemon::{DaemonConfig, run_daemon};
pub use protocol::{ClientMessage, PeerRecord, Reply, ServerEvent};

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
