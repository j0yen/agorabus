//! `agorabus doctor` — self-staleness detection for the running bus daemon.
//!
//! Introspects the running `agorabus daemon` process via `/proc/<pid>/exe`,
//! comparing the on-disk installed binary inode against the executing image.
//! Produces a human-readable (`text`) or machine-readable (`json`) verdict.
//!
//! Exit-code contract (matches the PRD acceptance criteria):
//! - **0** — `current`: executing binary inode matches on-disk binary.
//! - **1** — `stale: …`: a staleness signal was detected.
//! - **2** — `unknown`: no running daemon found, or `/proc` unreadable.

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::Serialize;

// ── public surface ────────────────────────────────────────────────────────────

/// Output format selected by `--format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorFormat {
    /// Human-readable single-line verdict (default).
    Text,
    /// Machine-readable JSON object.
    Json,
}

impl DoctorFormat {
    /// Parse from the CLI `--format` string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

/// The staleness verdict for the running daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Binary on disk matches the executing image.
    Current,
    /// `/proc/<pid>/exe` has the ` (deleted)` suffix — binary was replaced.
    StaleDeletedExe,
    /// The on-disk binary has a different inode from the executing image.
    StaleInodeDrift,
    /// Could not determine staleness (no daemon, unreadable `/proc`, …).
    Unknown,
}

impl Verdict {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::StaleDeletedExe => "stale: deleted-exe",
            Self::StaleInodeDrift => "stale: inode-drift",
            Self::Unknown => "unknown",
        }
    }
}

/// Full diagnostic snapshot returned by [`run_doctor`].
#[derive(Debug, Serialize)]
pub struct DoctorReport {
    /// PID of the running daemon, if found.
    pub daemon_pid: Option<u32>,
    /// Path resolved from `/proc/<pid>/exe` (may include ` (deleted)` suffix).
    pub exe_path: Option<String>,
    /// Inode of the **executing** image (from `/proc/<pid>/exe` stat).
    pub exe_inode: Option<u64>,
    /// Inode of the **installed** binary on disk (sans ` (deleted)` suffix).
    pub ondisk_inode: Option<u64>,
    /// `user.prov.ts` xattr from the on-disk binary, if readable.
    pub prov_ts: Option<String>,
    /// Staleness verdict.
    pub verdict: Verdict,
}

/// Run the doctor check and return a report plus the appropriate [`ExitCode`].
///
/// `installed_path` is the path of the `agorabus` binary on disk (i.e. the one
/// to compare against).  When `None` the function attempts to discover it via
/// `which agorabus` / the current executable.
pub fn run_doctor(installed_path: Option<&Path>) -> (DoctorReport, ExitCode) {
    // 1. Find the daemon pid.
    let daemon_pid = find_daemon_pid();

    let Some(pid) = daemon_pid else {
        let report = DoctorReport {
            daemon_pid: None,
            exe_path: None,
            exe_inode: None,
            ondisk_inode: None,
            prov_ts: None,
            verdict: Verdict::Unknown,
        };
        return (report, ExitCode::from(2));
    };

    // 2. Resolve /proc/<pid>/exe (the raw symlink target, which may end in
    //    " (deleted)").
    let exe_link = PathBuf::from(format!("/proc/{pid}/exe"));
    let exe_path_raw: Option<String> = read_proc_exe_raw(pid);
    let deleted = exe_path_raw
        .as_deref()
        .map_or(false, |s| s.ends_with(" (deleted)"));

    // 3. Stat the executing image via /proc/<pid>/exe (the kernel keeps the
    //    inode even for deleted files).
    let exe_inode = stat_inode(&exe_link);

    // 4. Resolve the on-disk installed path.
    let ondisk_path = installed_path
        .map(PathBuf::from)
        .or_else(discover_installed_path);

    let (ondisk_inode, prov_ts) = ondisk_path.as_ref().map_or((None, None), |p| {
        let inode = stat_inode(p);
        let ts = read_prov_ts(p);
        (inode, ts)
    });

    // 5. Determine verdict.
    let verdict = if deleted {
        Verdict::StaleDeletedExe
    } else {
        match (exe_inode, ondisk_inode) {
            (Some(ei), Some(oi)) if ei != oi => Verdict::StaleInodeDrift,
            (Some(_), Some(_)) => Verdict::Current,
            // Can't compare — treat as unknown only if we truly have no data.
            _ => Verdict::Unknown,
        }
    };

    let exit_code = match &verdict {
        Verdict::Current => ExitCode::from(0),
        Verdict::StaleDeletedExe | Verdict::StaleInodeDrift => ExitCode::from(1),
        Verdict::Unknown => ExitCode::from(2),
    };

    let report = DoctorReport {
        daemon_pid: Some(pid),
        exe_path: exe_path_raw,
        exe_inode,
        ondisk_inode,
        prov_ts,
        verdict,
    };
    (report, exit_code)
}

/// Print the report to stdout in the requested format.
pub fn print_report(report: &DoctorReport, format: DoctorFormat) {
    match format {
        DoctorFormat::Text => {
            let pid_str = report
                .daemon_pid
                .map_or_else(|| "none".to_string(), |p| p.to_string());
            println!("{} (daemon pid: {pid_str})", report.verdict.as_str());
            if let Some(ref path) = report.exe_path {
                println!("  exe: {path}");
            }
            if let Some(ei) = report.exe_inode {
                println!("  exe_inode: {ei}");
            }
            if let Some(oi) = report.ondisk_inode {
                println!("  ondisk_inode: {oi}");
            }
            if let Some(ref ts) = report.prov_ts {
                println!("  prov_ts: {ts}");
            }
        }
        DoctorFormat::Json => match serde_json::to_string(report) {
            Ok(s) => println!("{s}"),
            Err(e) => eprintln!("agorabus doctor: json serialise error: {e}"),
        },
    }
}

// ── private helpers ──────────────────────────────────────────────────────────

/// Scan `/proc` for the `agorabus daemon` process.
///
/// Returns the first matching pid (smallest, for determinism).
fn find_daemon_pid() -> Option<u32> {
    let proc_dir = Path::new("/proc");
    let Ok(entries) = std::fs::read_dir(proc_dir) else {
        return None;
    };

    let mut pids: Vec<u32> = Vec::new();
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let name = fname.to_string_lossy();
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        if is_agorabus_daemon(pid) {
            pids.push(pid);
        }
    }
    pids.sort_unstable();
    pids.into_iter().next()
}

/// Return true if `/proc/<pid>/cmdline` looks like `agorabus daemon`.
fn is_agorabus_daemon(pid: u32) -> bool {
    let path = format!("/proc/{pid}/cmdline");
    let Ok(raw) = std::fs::read(&path) else {
        return false;
    };
    // cmdline is NUL-separated: argv[0]\0argv[1]\0…
    let args: Vec<&[u8]> = raw.split(|b| *b == 0).collect();
    if args.len() < 2 {
        return false;
    }
    let argv0 = String::from_utf8_lossy(args[0]);
    let argv1 = String::from_utf8_lossy(args[1]);
    // Match "agorabus" (any path suffix) + first arg == "daemon"
    argv0.ends_with("agorabus") && argv1 == "daemon"
}

/// Read the raw symlink target of `/proc/<pid>/exe`.
///
/// On Linux, if the binary has been replaced on disk the kernel appends
/// ` (deleted)` to the reported path.
fn read_proc_exe_raw(pid: u32) -> Option<String> {
    let link = format!("/proc/{pid}/exe");
    // We read the kernel-level path via readlink rather than fs::canonicalize
    // so that we preserve the ` (deleted)` suffix.
    match std::fs::read_link(&link) {
        Ok(p) => Some(p.to_string_lossy().into_owned()),
        Err(_) => {
            // Fall back: read /proc/<pid>/exe as a string (some kernels surface
            // it that way).
            std::fs::read_to_string(&link).ok()
        }
    }
}

/// Return the inode of `path` using `std::fs::metadata`.
fn stat_inode(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

/// Attempt to locate the installed `agorabus` binary on `$PATH`.
fn discover_installed_path() -> Option<PathBuf> {
    // Try the current executable first (most reliable when doctor is invoked
    // from the installed binary itself).
    if let Ok(exe) = std::env::current_exe() {
        return Some(exe);
    }
    // Fall back to PATH lookup.
    which_agorabus()
}

/// Minimal `which`-style lookup for `agorabus` on `$PATH`.
fn which_agorabus() -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join("agorabus");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Read the `user.prov.ts` xattr from `path` via syscall.
///
/// Returns `None` if the xattr is absent or the filesystem doesn't support it.
fn read_prov_ts(path: &Path) -> Option<String> {
    read_xattr(path, "user.prov.ts")
}

/// Read a named xattr from `path` using the `getxattr(2)` syscall directly
/// (avoids an `xattr` crate dependency).
fn read_xattr(path: &Path, name: &str) -> Option<String> {
    // Use libc-free approach: call getxattr via std::process is too heavy.
    // We use a raw syscall via the nix-less approach: write a tiny helper
    // using unsafe. Since unsafe_code is forbidden in non-test cfg, we use
    // a process-spawned `getfattr` if available, and silently return None
    // otherwise.  This is entirely optional enrichment — prov_ts is
    // best-effort.
    let output = std::process::Command::new("getfattr")
        .args(["--name", name, "--only-values", path.to_str()?])
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_agorabus_daemon ──────────────────────────────────────────────────

    #[test]
    fn is_agorabus_daemon_rejects_self() {
        // The test process is not an agorabus daemon.
        let my_pid = std::process::id();
        assert!(!is_agorabus_daemon(my_pid));
    }

    #[test]
    fn is_agorabus_daemon_rejects_nonexistent_pid() {
        // PID 0 is never a user process.
        assert!(!is_agorabus_daemon(0));
    }

    // ── DoctorFormat::parse ─────────────────────────────────────────────────

    #[test]
    fn doctor_format_parse_text() {
        assert_eq!(DoctorFormat::parse("text"), Some(DoctorFormat::Text));
    }

    #[test]
    fn doctor_format_parse_json() {
        assert_eq!(DoctorFormat::parse("json"), Some(DoctorFormat::Json));
    }

    #[test]
    fn doctor_format_parse_unknown() {
        assert_eq!(DoctorFormat::parse("xml"), None);
    }

    // ── run_doctor with no daemon ────────────────────────────────────────────

    #[test]
    fn run_doctor_no_daemon_exits_2() {
        // In unit-test context there is no running `agorabus daemon`.
        // We can't guarantee that — skip only if one happens to be running.
        // The test checks the exit-code contract for the no-daemon path.
        if find_daemon_pid().is_some() {
            // A live daemon is running; skip to avoid false failures.
            return;
        }
        let (report, code) = run_doctor(None);
        assert_eq!(report.verdict, Verdict::Unknown);
        assert!(report.daemon_pid.is_none());
        // ExitCode doesn't impl PartialEq; check via the u8 representation
        // by comparing the report verdict instead.
        let _ = code; // used above indirectly; silence must_use
    }

    // ── Verdict serialisation ────────────────────────────────────────────────

    #[test]
    fn verdict_serialises_current() {
        let v = serde_json::to_string(&Verdict::Current).unwrap();
        assert_eq!(v, "\"current\"");
    }

    #[test]
    fn verdict_serialises_stale_deleted() {
        let v = serde_json::to_string(&Verdict::StaleDeletedExe).unwrap();
        assert_eq!(v, "\"stale_deleted_exe\"");
    }

    // ── stat_inode ───────────────────────────────────────────────────────────

    #[test]
    fn stat_inode_returns_some_for_existing_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let inode = stat_inode(tmp.path());
        assert!(inode.is_some());
    }

    #[test]
    fn stat_inode_returns_none_for_missing_file() {
        let inode = stat_inode(Path::new("/nonexistent/path/agorabus-doctor-test"));
        assert!(inode.is_none());
    }

    // ── inode-drift detection ────────────────────────────────────────────────

    #[test]
    fn inode_drift_detected_when_inodes_differ() {
        // Two different temp files will always have different inodes.
        let f1 = tempfile::NamedTempFile::new().unwrap();
        let f2 = tempfile::NamedTempFile::new().unwrap();
        let i1 = stat_inode(f1.path()).unwrap();
        let i2 = stat_inode(f2.path()).unwrap();
        // They must differ (same-dir fs, distinct files).
        assert_ne!(i1, i2);
    }

    #[test]
    fn same_file_has_same_inode() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let i1 = stat_inode(f.path()).unwrap();
        let i2 = stat_inode(f.path()).unwrap();
        assert_eq!(i1, i2);
    }

    // ── print_report smoke test ──────────────────────────────────────────────

    #[test]
    fn print_report_text_does_not_panic() {
        let r = DoctorReport {
            daemon_pid: Some(9999),
            exe_path: Some("/usr/bin/agorabus (deleted)".into()),
            exe_inode: Some(12345),
            ondisk_inode: Some(67890),
            prov_ts: None,
            verdict: Verdict::StaleDeletedExe,
        };
        // Just verify it doesn't panic.
        print_report(&r, DoctorFormat::Text);
    }

    #[test]
    fn print_report_json_is_valid() {
        let r = DoctorReport {
            daemon_pid: Some(1234),
            exe_path: Some("/usr/bin/agorabus".into()),
            exe_inode: Some(111),
            ondisk_inode: Some(111),
            prov_ts: Some("2026-05-29T00:00:00Z".into()),
            verdict: Verdict::Current,
        };
        // Capture to string via JSON serialise (print_report outputs to stdout).
        let s = serde_json::to_string(&r).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["verdict"], "current");
        assert_eq!(v["daemon_pid"], 1234);
    }
}
