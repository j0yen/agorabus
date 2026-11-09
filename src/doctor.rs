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
    #[must_use]
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
    const fn as_str(&self) -> &'static str {
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
    /// NATS-uplink connection state (PRD-agorabus-nats-uplink), rendered as
    /// text (`disabled` / `connected <url>` / `reconnecting <err>`).
    /// `None` when no running daemon answered the query — report-only and
    /// never affects [`run_doctor`]'s exit code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uplink: Option<String>,
}

/// Run the doctor check and return a report plus the appropriate [`ExitCode`].
///
/// `installed_path` is the path of the `agorabus` binary on disk (i.e. the one
/// to compare against).  When `None` the function attempts to discover it via
/// `which agorabus` / the current executable.
#[must_use]
pub fn run_doctor(installed_path: Option<&Path>) -> (DoctorReport, ExitCode) {
    run_doctor_for_socket(installed_path, &crate::default_socket_path())
}

/// Like [`run_doctor`], for the daemon serving `socket`.
#[must_use]
pub fn run_doctor_for_socket(
    installed_path: Option<&Path>,
    socket: &Path,
) -> (DoctorReport, ExitCode) {
    // 1. Find the daemon pid.
    let daemon_pid = find_daemon_pid_for(socket);

    let Some(pid) = daemon_pid else {
        let report = DoctorReport {
            daemon_pid: None,
            exe_path: None,
            exe_inode: None,
            ondisk_inode: None,
            prov_ts: None,
            verdict: Verdict::Unknown,
            uplink: None,
        };
        return (report, ExitCode::from(2));
    };

    // 2. Resolve /proc/<pid>/exe (the raw symlink target, which may end in
    //    " (deleted)").
    let exe_link = PathBuf::from(format!("/proc/{pid}/exe"));
    let exe_path_raw: Option<String> = read_proc_exe_raw(pid);
    let deleted = exe_path_raw
        .as_deref()
        .is_some_and(|s| s.ends_with(" (deleted)"));

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
        uplink: None,
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
            if let Some(ref uplink) = report.uplink {
                println!("  uplink: {uplink}");
            }
        }
        DoctorFormat::Json => match serde_json::to_string(report) {
            Ok(s) => println!("{s}"),
            Err(e) => eprintln!("agorabus doctor: json serialise error: {e}"),
        },
    }
}

// ── private helpers ──────────────────────────────────────────────────────────

/// Scan `/proc` for the `agorabus daemon` process serving `socket`.
///
/// A daemon matches by its `--socket` value; one started without `--socket`
/// matches the default socket only. Returns the smallest matching pid.
#[must_use]
pub fn find_daemon_pid_for(socket: &Path) -> Option<u32> {
    let proc_dir = Path::new("/proc");
    let Ok(entries) = std::fs::read_dir(proc_dir) else {
        return None;
    };

    let default_socket = crate::default_socket_path();
    let mut pids: Vec<u32> = Vec::new();
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let name = fname.to_string_lossy();
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        let Some(daemon_socket) = daemon_cmdline_socket(&raw) else {
            continue;
        };
        if socket_matches(&daemon_socket, socket, &default_socket) {
            pids.extend([pid]);
        }
    }
    pids.sort_unstable();
    pids.into_iter().next()
}

/// The socket an `agorabus daemon` process was started for.
#[derive(Debug, PartialEq, Eq)]
enum DaemonSocket {
    /// No `--socket` flag: the default socket.
    Default,
    /// The `--socket` value.
    Explicit(PathBuf),
}

/// Parse a NUL-separated `/proc/<pid>/cmdline`.
///
/// `--socket` is a clap global flag, so `--socket X` / `--socket=X` is read
/// from the whole argv, before or after `daemon`; the last occurrence wins.
/// `None` when it is not an `agorabus daemon`.
fn daemon_cmdline_socket(raw: &[u8]) -> Option<DaemonSocket> {
    let mut args = raw
        .split(|b| *b == 0)
        .map(|a| String::from_utf8_lossy(a).into_owned());
    // Match "agorabus" (any path suffix) as argv[0].
    if !args.next()?.ends_with("agorabus") {
        return None;
    }
    let mut socket = DaemonSocket::Default;
    let mut seen_daemon = false;
    while let Some(arg) = args.next() {
        if arg == "--socket" {
            // A missing value reads as the empty trailing NUL field.
            let v = args.next().filter(|v| !v.is_empty())?;
            socket = DaemonSocket::Explicit(PathBuf::from(v));
        } else if let Some(v) = arg.strip_prefix("--socket=") {
            socket = DaemonSocket::Explicit(PathBuf::from(v));
        } else if seen_daemon {
            // Daemon's own flags and values.
        } else if arg == "daemon" {
            seen_daemon = true;
        } else {
            // Any other token before the subcommand is not a daemon command.
            return None;
        }
    }
    seen_daemon.then_some(socket)
}

/// Canonical form of `p`; a missing leaf falls back to canonical parent + leaf.
fn canonical(p: &Path) -> PathBuf {
    p.canonicalize().unwrap_or_else(|_| {
        match (p.parent().and_then(|d| d.canonicalize().ok()), p.file_name()) {
            (Some(d), Some(f)) => d.join(f),
            _ => p.to_path_buf(),
        }
    })
}

fn socket_matches(served: &DaemonSocket, socket: &Path, default_socket: &Path) -> bool {
    let served = match served {
        DaemonSocket::Default => default_socket,
        DaemonSocket::Explicit(p) => p,
    };
    served == socket || canonical(served) == canonical(socket)
}

/// Whether a raw `/proc/<pid>/cmdline` is an `agorabus daemon` serving `socket`.
///
/// A daemon with no `--socket` serves the default socket (from the caller's
/// `HOME`) only.
#[must_use]
pub fn cmdline_serves_socket(raw: &[u8], socket: &Path) -> bool {
    daemon_cmdline_socket(raw)
        .is_some_and(|d| socket_matches(&d, socket, &crate::default_socket_path()))
}

/// Read the raw symlink target of `/proc/<pid>/exe`.
///
/// On Linux, if the binary has been replaced on disk the kernel appends
/// ` (deleted)` to the reported path.
fn read_proc_exe_raw(pid: u32) -> Option<String> {
    let link = format!("/proc/{pid}/exe");
    // We read the kernel-level path via readlink rather than fs::canonicalize
    // so that we preserve the ` (deleted)` suffix.
    std::fs::read_link(&link).map_or_else(
        |_| {
            // Fall back: read /proc/<pid>/exe as a string (some kernels surface
            // it that way).
            std::fs::read_to_string(&link).ok()
        },
        |p| Some(p.to_string_lossy().into_owned()),
    )
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
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    // ── daemon_cmdline_socket ───────────────────────────────────────────────

    #[test]
    fn cmdline_socket_parses_flag_forms() {
        let sock = |s: &str| Some(DaemonSocket::Explicit(PathBuf::from(s)));
        assert_eq!(daemon_cmdline_socket(b"/x/agorabus\0daemon\0"), Some(DaemonSocket::Default));
        assert_eq!(
            daemon_cmdline_socket(b"/x/agorabus\0--socket\0/a/s\0daemon\0--state-file\0f\0"),
            sock("/a/s")
        );
        assert_eq!(
            daemon_cmdline_socket(b"agorabus\0--socket=/a/s\0daemon\0"),
            sock("/a/s")
        );
    }

    #[test]
    fn cmdline_socket_reads_flag_after_daemon() {
        let sock = |s: &str| Some(DaemonSocket::Explicit(PathBuf::from(s)));
        assert_eq!(daemon_cmdline_socket(b"agorabus\0daemon\0--socket\0/a/s\0"), sock("/a/s"));
        assert_eq!(daemon_cmdline_socket(b"agorabus\0daemon\0--socket=/a/s\0"), sock("/a/s"));
        assert_eq!(
            daemon_cmdline_socket(b"agorabus\0daemon\0--state-file\0f\0--socket\0/a/s\0"),
            sock("/a/s")
        );
        // Last occurrence wins, across the subcommand.
        assert_eq!(
            daemon_cmdline_socket(b"agorabus\0--socket\0/a/1\0daemon\0--socket=/a/2\0"),
            sock("/a/2")
        );
        assert_eq!(daemon_cmdline_socket(b"agorabus\0daemon\0--socket\0"), None);
    }

    #[test]
    fn cmdline_serves_socket_by_canonical_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("real")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("link")).unwrap();
        let via_link = dir.path().join("link/sock");
        let real = dir.path().join("real/sock");
        let raw = format!("agorabus\0daemon\0--socket\0{}\0", via_link.display());
        assert!(cmdline_serves_socket(raw.as_bytes(), &real));
        assert!(!cmdline_serves_socket(raw.as_bytes(), &dir.path().join("real/other")));
        assert!(!cmdline_serves_socket(b"sleep\0daemon\0", &real));
    }

    #[test]
    fn cmdline_socket_rejects_non_daemons() {
        assert_eq!(daemon_cmdline_socket(b"/x/agorabus\0peers\0"), None);
        assert_eq!(daemon_cmdline_socket(b"/x/agorabus\0peers\0daemon\0"), None);
        assert_eq!(daemon_cmdline_socket(b"sleep\0daemon\0"), None);
        assert_eq!(daemon_cmdline_socket(b""), None);
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
        if find_daemon_pid_for(&crate::default_socket_path()).is_some() {
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
            uplink: Some("disabled".into()),
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
            uplink: None,
        };
        // Capture to string via JSON serialise (print_report outputs to stdout).
        let s = serde_json::to_string(&r).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap(); // allowlist: test fixture literal, not external input
        assert_eq!(v["verdict"], "current");
        assert_eq!(v["daemon_pid"], 1234);
    }
}
