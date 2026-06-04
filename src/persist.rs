//! Durable-state journal for `BusState` (PRD-agorabus-state-persist).
//!
//! Persists the serializable slice of [`BusState`]: the `claims` map and the
//! per-session sticky intents. Live socket connections and ephemeral peer-ids
//! are **not** persisted — they are meaningless across a restart.
//!
//! ## Atomic write strategy
//!
//! Writes go to `<state-file>.tmp.<pid>` and are renamed into place, so the
//! on-disk state file is never partially written. Mode is set to 0600 before
//! the rename so the final file never has permissive permissions.
//!
//! ## Corruption tolerance
//!
//! A missing or unparseable state file is treated as empty state. The daemon
//! logs a warning and starts clean — it must never refuse to boot on a bad
//! journal.

use crate::protocol::ClaimRecord;
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// The serializable slice of `BusState` that survives a daemon bounce.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DurableState {
    /// `canonical_path` → active claim record.
    #[serde(default)]
    pub claims: HashMap<String, ClaimRecord>,
    /// `session_id` → intent string. Only sessions that set a non-empty intent
    /// via a structured heartbeat are stored here.
    #[serde(default)]
    pub intents: HashMap<String, StickyIntent>,
}

/// Sticky structured intent stored per session_id.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StickyIntent {
    /// Skill currently active (e.g. `/build`). Empty string means unset.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub skill: String,
    /// PRD slug currently being built. Empty string means unset.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prd_slug: String,
    /// Working paths the session is touching (bounded externally).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub working_paths: Vec<String>,
}

impl StickyIntent {
    /// Returns `true` if all fields are empty (i.e. the intent was cleared).
    pub fn is_empty(&self) -> bool {
        self.skill.is_empty() && self.prd_slug.is_empty() && self.working_paths.is_empty()
    }
}

/// Load the durable state from `path`. Returns `Ok(DurableState::default())`
/// (empty state) if the file does not exist or cannot be parsed, logging a
/// warning to stderr in the latter case.
///
/// # Errors
///
/// Only returns `Err` on unexpected I/O that is not a "not found" condition.
pub fn load(path: &Path) -> Result<DurableState> {
    match std::fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<DurableState>(&bytes) {
            Ok(state) => Ok(state),
            Err(e) => {
                eprintln!(
                    "agorabus: state-file {} is unparseable ({e}); starting with empty state",
                    path.display()
                );
                Ok(DurableState::default())
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(DurableState::default()),
        Err(e) => Err(anyhow::Error::from(e))
            .with_context(|| format!("reading state file {}", path.display())),
    }
}

/// Atomically write `state` to `path` (mode 0600, write-and-rename).
///
/// The temporary file is placed in the same directory as `path` to ensure
/// the `rename` is atomic (same filesystem).
///
/// # Errors
///
/// Returns an error if the parent directory cannot be created, the temp file
/// cannot be written or chmod'd, or the rename fails.
pub fn save(path: &Path, state: &DurableState) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating state dir {}", parent.display()))?;

    let pid = std::process::id();
    let tmp_path: PathBuf = {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("state.json");
        parent.join(format!("{name}.tmp.{pid}"))
    };

    let bytes = serde_json::to_vec(state).context("serializing durable state")?;
    std::fs::write(&tmp_path, &bytes)
        .with_context(|| format!("writing temp state file {}", tmp_path.display()))?;

    // chmod before rename so the final file never has permissive mode.
    set_mode(&tmp_path, 0o600)?;

    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "renaming {} → {}",
            tmp_path.display(),
            path.display()
        )
    })?;

    Ok(())
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let perm = std::fs::Permissions::from_mode(mode);
    std::fs::set_permissions(path, perm)
        .with_context(|| format!("chmod {mode:o} {}", path.display()))?;
    Ok(())
}

/// Default state-file path: `~/.cache/agorabus/state.json`.
#[must_use]
pub fn default_state_path() -> PathBuf {
    std::env::var("HOME").map_or_else(
        |_| {
            let uid = std::env::var("UID").unwrap_or_else(|_| "0".into());
            PathBuf::from(format!("/tmp/agorabus-{uid}/state.json"))
        },
        |home| PathBuf::from(home).join(".cache/agorabus/state.json"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;
    use tempfile::tempdir;

    fn make_claim(path: &str, session_id: &str, ttl_unix_secs: u64) -> ClaimRecord {
        ClaimRecord {
            path: path.to_string(),
            session_id: session_id.to_string(),
            ttl_unix_secs,
            acquired_unix_secs: 1_000_000,
            reason: String::new(),
        }
    }

    #[test]
    fn round_trip_claims_and_intents() {
        let tmp = tempdir().unwrap();
        let state_path = tmp.path().join("state.json");

        let mut state = DurableState::default();
        state
            .claims
            .insert("/tmp/foo".into(), make_claim("/tmp/foo", "sid-A", 9_999_999));
        state.intents.insert(
            "sid-A".into(),
            StickyIntent {
                skill: "build".into(),
                prd_slug: "my-prd".into(),
                working_paths: vec!["/tmp/foo".into()],
            },
        );

        save(&state_path, &state).expect("save ok");
        let loaded = load(&state_path).expect("load ok");

        assert_eq!(loaded.claims.len(), 1);
        let c = loaded.claims.get("/tmp/foo").expect("claim present");
        assert_eq!(c.session_id, "sid-A");
        assert_eq!(c.ttl_unix_secs, 9_999_999);

        assert_eq!(loaded.intents.len(), 1);
        let i = loaded.intents.get("sid-A").expect("intent present");
        assert_eq!(i.skill, "build");
        assert_eq!(i.prd_slug, "my-prd");
        assert_eq!(i.working_paths, vec!["/tmp/foo"]);
    }

    #[test]
    fn missing_file_returns_empty_state() {
        let tmp = tempdir().unwrap();
        let state_path = tmp.path().join("nonexistent.json");
        let state = load(&state_path).expect("missing file → empty ok");
        assert!(state.claims.is_empty());
        assert!(state.intents.is_empty());
    }

    #[test]
    fn corrupt_file_returns_empty_state() {
        let tmp = tempdir().unwrap();
        let state_path = tmp.path().join("state.json");
        std::fs::write(&state_path, b"not valid json{{{").unwrap();
        let state = load(&state_path).expect("corrupt file → empty ok, no panic");
        assert!(state.claims.is_empty());
    }

    #[test]
    fn atomic_write_mode_0600_no_tmp_residue() {
        let tmp = tempdir().unwrap();
        let state_path = tmp.path().join("state.json");

        let state = DurableState::default();
        save(&state_path, &state).expect("save ok");

        // Mode must be 0600.
        let meta = std::fs::metadata(&state_path).unwrap();
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected mode 0600, got {mode:o}");

        // No .tmp file should remain.
        let tmp_residue: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .contains(".tmp.")
            })
            .collect();
        assert!(tmp_residue.is_empty(), "tmp files remain: {tmp_residue:?}");
    }

    #[test]
    fn expired_claims_pruned_on_rehydrate() {
        let tmp = tempdir().unwrap();
        let state_path = tmp.path().join("state.json");

        // One expired claim (ttl in the past) and one live claim.
        let mut state = DurableState::default();
        state
            .claims
            .insert("/tmp/expired".into(), make_claim("/tmp/expired", "sid-X", 1)); // unix 1 = ancient
        state.claims.insert(
            "/tmp/live".into(),
            make_claim("/tmp/live", "sid-Y", 9_999_999_999),
        );

        save(&state_path, &state).expect("save ok");
        let mut loaded = load(&state_path).expect("load ok");

        // Simulate prune_expired_claims(now) with a recent timestamp.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        loaded.claims.retain(|_, c| c.ttl_unix_secs > now);

        assert!(
            !loaded.claims.contains_key("/tmp/expired"),
            "expired claim should be pruned"
        );
        assert!(
            loaded.claims.contains_key("/tmp/live"),
            "live claim should survive"
        );
    }

    #[test]
    fn state_file_override_path_is_respected() {
        let tmp1 = tempdir().unwrap();
        let tmp2 = tempdir().unwrap();
        let path1 = tmp1.path().join("state.json");
        let path2 = tmp2.path().join("state.json");

        let mut state = DurableState::default();
        state
            .claims
            .insert("/tmp/x".into(), make_claim("/tmp/x", "sid-Z", 9_999_999));

        save(&path1, &state).expect("save to path1");

        // path2 is empty; loading it should yield empty state.
        let loaded2 = load(&path2).expect("load from absent path2 ok");
        assert!(
            loaded2.claims.is_empty(),
            "path2 is independent from path1"
        );

        // path1 has the claim.
        let loaded1 = load(&path1).expect("load from path1 ok");
        assert_eq!(loaded1.claims.len(), 1);
    }
}
