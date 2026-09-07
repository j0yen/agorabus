//! Optional NATS uplink: relays local agorabus pub/sub topics to the fleet
//! transport (PRD-agorabus-nats-uplink).
//!
//! Design summary (see the PRD for the full rationale):
//! - Connects to the **local** NATS leaf only (default
//!   `nats://127.0.0.1:4222`) — never hub directly. The leaf owns hub auth,
//!   so this daemon carries no fleet credentials.
//! - Local topic `T` mirrors to NATS subject `wm.bus.T`; the uplink
//!   subscribes to the wildcard `wm.bus.>` for inbound fleet traffic.
//! - Every relayed message is wrapped in an [`Envelope`] carrying
//!   `origin_node` (defaults to the lowercased local hostname). Loop
//!   prevention is two-layered: (1) a message received from NATS whose
//!   `origin_node` is this node's own is dropped outright (self-echo — NATS
//!   delivers our own publishes back to us since we are also a `wm.bus.>`
//!   subscriber); (2) a message that arrived *via* the uplink is tagged
//!   `from_uplink` on the internal broadcast bus and is never re-published
//!   outward by the relay-out half (see [`crate::daemon::BroadcastMsg`]).
//! - No `uplink.toml`, or `enabled = false`: [`UplinkConfig::enabled`] is
//!   false and the daemon never touches the network for this feature —
//!   behavior is byte-identical to v0.12.0 (AC1).

#![allow(
    clippy::future_not_send,
    clippy::missing_errors_doc,
    // Same reasoning as reconnect.rs: a single-entry reconnect/relay loop
    // reads clearer un-split, and the connect-failure branches log a
    // structured line to stderr by design (this is a CLI daemon, not a
    // library consumers embed silently).
    clippy::too_many_lines,
    clippy::print_stderr,
)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::broadcast;

use crate::daemon::BroadcastMsg;

/// NATS subject prefix every local topic is mirrored under.
pub const UPLINK_SUBJECT_PREFIX: &str = "wm.bus.";
/// Wildcard subject the uplink subscribes to for inbound fleet traffic.
pub const UPLINK_SUBSCRIBE_SUBJECT: &str = "wm.bus.>";
/// Default NATS leaf URL — local leaf only, never hub directly.
pub const DEFAULT_UPLINK_URL: &str = "nats://127.0.0.1:4222";
/// Default interval between periodic local-peer presence re-broadcasts.
pub const DEFAULT_FLEET_PRESENCE_INTERVAL_MS: u64 = 10_000;
/// Base delay for the uplink's reconnect backoff (reuses
/// [`crate::reconnect::backoff_delay`]'s formula).
const RECONNECT_BASE_MS: u64 = 200;
/// Cap for the uplink's reconnect backoff.
const RECONNECT_CAP_MS: u64 = 10_000;

/// Raw shape of `~/.config/agorabus/uplink.toml`. All fields optional; a
/// missing field takes the default documented on [`UplinkConfig`].
#[derive(Debug, Clone, Default, Deserialize)]
struct UplinkFileConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    node: Option<String>,
}

/// Resolved uplink configuration (defaults already applied).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UplinkConfig {
    /// Whether the daemon should connect to the uplink at all. Default:
    /// `false` (absent `uplink.toml` means uplink off — v0.12.0 behavior).
    pub enabled: bool,
    /// NATS leaf URL. Default: `nats://127.0.0.1:4222` (local leaf, never
    /// hub directly — the leaf owns hub auth).
    pub url: String,
    /// This node's identity, carried as `origin_node` on every relayed
    /// envelope. Default: the lowercased local hostname.
    pub node: String,
    /// Interval between periodic re-broadcasts of local peer presence on
    /// `wm.fleet.presence.announce` (only relevant while `enabled`).
    pub fleet_presence_interval_ms: u64,
}

impl Default for UplinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: DEFAULT_UPLINK_URL.to_string(),
            node: default_node_name(),
            fleet_presence_interval_ms: DEFAULT_FLEET_PRESENCE_INTERVAL_MS,
        }
    }
}

/// Lowercased local hostname, or `"unknown-node"` if it cannot be determined.
#[must_use]
pub fn default_node_name() -> String {
    let raw = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown-node".to_string());
    raw.to_lowercase()
}

/// Default path for the uplink config file: `~/.config/agorabus/uplink.toml`.
#[must_use]
pub fn default_uplink_config_path() -> PathBuf {
    std::env::var("HOME").map_or_else(
        |_| PathBuf::from("/tmp/agorabus-uplink.toml"),
        |home| PathBuf::from(home).join(".config/agorabus/uplink.toml"),
    )
}

/// Load and resolve the uplink config at `path`.
///
/// An absent (or unreadable) file resolves to [`UplinkConfig::default`]
/// (uplink off) — this is the common case and must never be treated as an
/// error (AC1). A file that exists but fails to parse as TOML logs one
/// error line to stderr and *also* falls back to uplink-off rather than
/// crashing the daemon (AC2).
#[must_use]
pub fn load_uplink_config(path: &Path) -> UplinkConfig {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return UplinkConfig::default();
    };
    match toml::from_str::<UplinkFileConfig>(&raw) {
        Ok(file_cfg) => UplinkConfig {
            enabled: file_cfg.enabled,
            url: file_cfg.url.unwrap_or_else(|| DEFAULT_UPLINK_URL.to_string()),
            node: file_cfg
                .node
                .unwrap_or_else(default_node_name)
                .to_lowercase(),
            fleet_presence_interval_ms: DEFAULT_FLEET_PRESENCE_INTERVAL_MS,
        },
        Err(e) => {
            eprintln!(
                "agorabus: malformed uplink config at {}: {e}; uplink disabled",
                path.display()
            );
            UplinkConfig::default()
        }
    }
}

/// Report-only uplink connection state, surfaced by `agorabus doctor`.
/// Never affects doctor's exit code (AC11) — uplink lines are informational.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UplinkStatus {
    /// No uplink configured, or `enabled = false`.
    Disabled,
    /// Connected to the leaf at `url`.
    Connected {
        /// The NATS leaf URL currently connected to.
        url: String,
    },
    /// Not currently connected; background reconnect is in progress.
    Reconnecting {
        /// Most recent connection event or error, human-readable.
        error: String,
    },
}

impl UplinkStatus {
    /// Render as the one-line text `doctor` prints (`disabled` /
    /// `connected <url>` / `reconnecting <err>`).
    #[must_use]
    pub fn as_text(&self) -> String {
        match self {
            Self::Disabled => "disabled".to_string(),
            Self::Connected { url } => format!("connected {url}"),
            Self::Reconnecting { error } => format!("reconnecting {error}"),
        }
    }
}

/// Envelope wrapping every message relayed over the NATS uplink, in either
/// direction. Carries the originating node so receivers can drop self-echo
/// and so remote peers can be tagged in `agorabus peers --fleet`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Envelope {
    origin_node: String,
    data: serde_json::Value,
}

/// Run the uplink manager until aborted (the daemon holds this task's
/// `JoinHandle` and aborts it on shutdown — see `run_daemon`).
///
/// Connects to `cfg.url`, subscribes to [`UPLINK_SUBSCRIBE_SUBJECT`], and
/// bridges bidirectionally with the local broadcast bus:
/// - local publish (`!from_uplink`) → NATS subject `wm.bus.<topic>`.
/// - NATS `wm.bus.<topic>` → local broadcast, tagged `from_uplink: true`
///   (so it is never relayed back out) — except messages whose envelope
///   `origin_node` is this node's own, which are dropped (self-echo: NATS
///   delivers our own publishes back to every `wm.bus.>` subscriber,
///   including us).
///
/// A dead/unreachable leaf never blocks or panics: `retry_on_initial_connect`
/// plus the crate's own background reconnect loop keep retrying with
/// exponential backoff (reusing [`crate::reconnect::backoff_delay`]'s
/// formula) while `status` reports `Reconnecting`. The local bus is
/// entirely unaffected — this task never touches `state`, only `bcast`.
///
/// `pub(crate)`: only `run_daemon` spawns this; it is not part of the
/// public API surface (it takes `BroadcastMsg`, which is itself
/// `pub(crate)`).
pub(crate) async fn run_uplink(
    cfg: UplinkConfig,
    bcast: broadcast::Sender<BroadcastMsg>,
    status: Arc<Mutex<UplinkStatus>>,
) {
    use futures::StreamExt as _;

    *status.lock().await = UplinkStatus::Reconnecting {
        error: "connecting".to_string(),
    };

    let status_cb = Arc::clone(&status);
    let url_cb = cfg.url.clone();
    let opts = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .event_callback(move |event| {
            let status = Arc::clone(&status_cb);
            let url = url_cb.clone();
            async move {
                let mut st = status.lock().await;
                *st = match event {
                    async_nats::Event::Connected => UplinkStatus::Connected { url },
                    other => UplinkStatus::Reconnecting {
                        error: other.to_string(),
                    },
                };
            }
        })
        .reconnect_delay_callback(|attempts| {
            crate::reconnect::backoff_delay(
                RECONNECT_BASE_MS,
                RECONNECT_CAP_MS,
                u32::try_from(attempts).unwrap_or(u32::MAX),
            )
        });

    let client = match opts.connect(cfg.url.clone()).await {
        Ok(c) => c,
        Err(e) => {
            // Reachable only for connect-time errors independent of leaf
            // reachability (e.g. an unparsable URL) — `retry_on_initial_connect`
            // means an unreachable-but-well-formed leaf never lands here;
            // the client is still returned and retries in the background.
            *status.lock().await = UplinkStatus::Reconnecting {
                error: e.to_string(),
            };
            return;
        }
    };

    let mut inbound = match client
        .subscribe(UPLINK_SUBSCRIBE_SUBJECT.to_string())
        .await
    {
        Ok(s) => s,
        Err(e) => {
            *status.lock().await = UplinkStatus::Reconnecting {
                error: e.to_string(),
            };
            return;
        }
    };

    let mut local_rx = bcast.subscribe();

    loop {
        tokio::select! {
            local_msg = local_rx.recv() => {
                match local_msg {
                    Ok(msg) if !msg.from_uplink => {
                        let subject = format!("{UPLINK_SUBJECT_PREFIX}{}", msg.topic);
                        let env = Envelope { origin_node: cfg.node.clone(), data: msg.data };
                        if let Ok(bytes) = serde_json::to_vec(&env) {
                            let _ = client.publish(subject, bytes.into()).await;
                        }
                    }
                    // from_uplink: never re-relay (loop prevention). Merged
                    // with the Lagged arm below — both are "drop and keep
                    // going", only Closed ends the loop.
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = inbound.next() => {
                let Some(msg) = msg else { break };
                let subject = msg.subject.as_str();
                let Some(topic) = subject.strip_prefix(UPLINK_SUBJECT_PREFIX) else { continue };
                let Ok(env) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
                if env.origin_node.eq_ignore_ascii_case(&cfg.node) {
                    continue; // self-echo (AC8): NATS loops our own publish back to us.
                }
                let _ = bcast.send(BroadcastMsg {
                    topic: topic.to_string(),
                    data: env.data,
                    from: format!("uplink:{}", env.origin_node),
                    from_uplink: true,
                });
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    unsafe_code,
    clippy::undocumented_unsafe_blocks,
    clippy::multiple_unsafe_ops_per_block,
)]
mod tests {
    use super::*;

    // AC1: absent file → uplink off, all defaults.
    #[test]
    fn absent_file_resolves_to_disabled_defaults() {
        let cfg = load_uplink_config(Path::new("/nonexistent/agorabus-uplink-test.toml"));
        assert!(!cfg.enabled);
        assert_eq!(cfg.url, DEFAULT_UPLINK_URL);
        assert_eq!(cfg.node, default_node_name());
    }

    // AC2: any subset of fields present → honored; missing take defaults.
    #[test]
    fn partial_file_honors_present_fields() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "enabled = true\n").unwrap();
        let cfg = load_uplink_config(tmp.path());
        assert!(cfg.enabled);
        assert_eq!(cfg.url, DEFAULT_UPLINK_URL, "url should take default");
        assert_eq!(cfg.node, default_node_name(), "node should take default");
    }

    #[test]
    fn full_file_honors_all_fields() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "enabled = true\nurl = \"nats://10.0.0.1:4222\"\nnode = \"CARBON\"\n",
        )
        .unwrap();
        let cfg = load_uplink_config(tmp.path());
        assert!(cfg.enabled);
        assert_eq!(cfg.url, "nats://10.0.0.1:4222");
        assert_eq!(cfg.node, "carbon", "node must be lowercased");
    }

    // AC2: malformed file → one error logged, falls back to uplink-off
    // rather than panicking or propagating an error.
    #[test]
    fn malformed_file_falls_back_to_disabled() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "this is not valid toml {{{").unwrap();
        let cfg = load_uplink_config(tmp.path());
        assert!(!cfg.enabled, "malformed config must disable uplink, not panic");
    }

    #[test]
    fn status_as_text_matches_doctor_contract() {
        assert_eq!(UplinkStatus::Disabled.as_text(), "disabled");
        assert_eq!(
            UplinkStatus::Connected { url: "nats://127.0.0.1:4222".to_string() }.as_text(),
            "connected nats://127.0.0.1:4222"
        );
        assert_eq!(
            UplinkStatus::Reconnecting { error: "connection refused".to_string() }.as_text(),
            "reconnecting connection refused"
        );
    }

    // AC6: envelope round-trips through JSON carrying origin_node + data.
    #[test]
    fn envelope_json_roundtrip() {
        let env = Envelope {
            origin_node: "testa".to_string(),
            data: serde_json::json!({"hello": "world"}),
        };
        let bytes = serde_json::to_vec(&env).unwrap();
        let back: Envelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.origin_node, "testa");
        assert_eq!(back.data, serde_json::json!({"hello": "world"}));
    }
}
