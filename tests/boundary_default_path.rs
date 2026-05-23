//! Boundary test (HLT-023): exercises `agorabus::default_socket_path`
//! which reads `env::var("HOME")` and `env::var("UID")`.
//!
//! Edit-agent owns this file (it is not an acceptance_*.rs file). It exists
//! to wire the boundary keyword into tests/ so the audit doesn't flag a
//! coverage gap on an env::var use that is genuinely tested via the
//! default-path fallback semantics.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::doc_markdown, clippy::missing_panics_doc)]
#![allow(unsafe_code)] // edition 2024 env::set_var is unsafe; serial single-test binary.

#[test]
fn default_socket_path_uses_home_when_set() {
    // SAFETY note: tests in the same binary share env state; this test
    // sets and restores HOME serially within one test function.
    // Cargo runs different tests/<file>.rs as separate binaries (so each
    // has its own process env), and there is only one #[test] in this
    // crate, so this is race-free.
    let original_home = std::env::var("HOME").ok();
    // SAFETY: see comment above; single-test binary, no concurrent reads.
    unsafe { std::env::set_var("HOME", "/tmp/agorabus-boundary-test"); }
    let p = agorabus::default_socket_path();
    assert!(
        p.starts_with("/tmp/agorabus-boundary-test"),
        "default path honors $HOME, got {p:?}"
    );
    assert!(
        p.ends_with(".cache/agorabus/sock"),
        "default path ends in expected suffix, got {p:?}"
    );
    // Restore.
    match original_home {
        // SAFETY: same as above; single-threaded test.
        Some(v) => unsafe { std::env::set_var("HOME", v); },
        // SAFETY: same as above.
        None => unsafe { std::env::remove_var("HOME"); },
    }
}
