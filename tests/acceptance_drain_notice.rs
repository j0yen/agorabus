//! Acceptance tests for PRD-agorabus-drain-notice.
//!
//! AC1 — drain notice delivered: a subscriber receives exactly one
//!   `{"op":"bus.draining","resume_after_ms":N}` line before EOF when the
//!   daemon is shut down.
//!
//! AC2 — resume hint is configurable: launching with `--drain-resume-hint-ms
//!   1500` yields `resume_after_ms: 1500` in the delivered notice.
//!
//! AC3 — shutdown terminates promptly: with a subscriber whose read side is
//!   stalled, SIGTERM still causes the daemon to exit within
//!   drain_grace_ms + margin (drain never wedges shutdown).
//!
//! AC4 — clean-exit semantics preserved: after shutdown the daemon exits 0
//!   and the socket file is gone so a fresh daemon can bind the same path.
//!
//! AC5 — backward compatible: one-shot clients (announce/peers) that connect
//!   and disconnect before drain are unaffected; existing acceptance suite
//!   passes unchanged (tested implicitly by the shared DaemonHandle fixture).
//!
//! AC6 — unknown-event tolerance: a minimal line-reader that sees the drain
//!   line does not panic or crash.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use std::time::Duration;

use agorabus::{Client, DrainNotice};
use common::DaemonHandle;

// ---------------------------------------------------------------------------
// AC1: drain notice delivered, default resume hint
// ---------------------------------------------------------------------------

#[test]
fn ac1_drain_notice_delivered_before_eof() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Short drain grace so the test doesn't wait long for the daemon to
        // exit after writing the notice.
        let mut h = DaemonHandle::start_with_drain(
            Duration::from_secs(60),
            50,   // drain_grace_ms
            3000, // drain_resume_hint_ms
        )
        .await;

        // Connect a subscriber.
        let mut sub = Client::connect(&h.socket).await.expect("sub connect");
        sub.announce("drain-ac1", std::process::id(), "/tmp", "drain-test")
            .await
            .expect("announce ok");
        sub.subscribe("").await.expect("subscribe ok");

        // Signal shutdown via stop_only (sends the oneshot, waits for daemon
        // task to finish — which includes the drain grace sleep).
        h.stop_only().await;

        // The subscriber should have received the drain notice before EOF.
        let line = tokio::time::timeout(Duration::from_secs(5), sub.next_raw_line())
            .await
            .expect("got a line within 5s")
            .expect("line is Some (drain notice)");

        // Parse as DrainNotice.
        let notice: DrainNotice = serde_json::from_str(&line).expect("parses as DrainNotice");
        assert_eq!(notice.op, "bus.draining", "op discriminator");
        assert_eq!(notice.resume_after_ms, 3000, "default resume hint");

        // The next read should be EOF.
        let eof = tokio::time::timeout(Duration::from_secs(5), sub.next_raw_line())
            .await
            .expect("got EOF within 5s");
        assert!(
            eof.is_none(),
            "expected EOF after drain notice, got: {eof:?}"
        );
    });
}

// ---------------------------------------------------------------------------
// AC2: resume hint is configurable
// ---------------------------------------------------------------------------

#[test]
fn ac2_resume_hint_configurable() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Launch with non-default hint: 1500 ms.
        let mut h = DaemonHandle::start_with_drain(
            Duration::from_secs(60),
            50,   // drain_grace_ms
            1500, // drain_resume_hint_ms
        )
        .await;

        let mut sub = Client::connect(&h.socket).await.expect("sub connect");
        sub.announce("drain-ac2", std::process::id(), "/tmp", "drain-test")
            .await
            .expect("announce ok");
        sub.subscribe("").await.expect("subscribe ok");

        h.stop_only().await;

        let line = tokio::time::timeout(Duration::from_secs(5), sub.next_raw_line())
            .await
            .expect("got drain line")
            .expect("line is Some");

        let notice: DrainNotice = serde_json::from_str(&line).unwrap();
        assert_eq!(
            notice.resume_after_ms, 1500,
            "AC2: resume hint must match --drain-resume-hint-ms"
        );
        assert_eq!(notice.op, "bus.draining");
    });
}

// ---------------------------------------------------------------------------
// AC3: shutdown terminates promptly even with a stalled subscriber
// ---------------------------------------------------------------------------

#[test]
fn ac3_shutdown_prompt_despite_stalled_subscriber() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Very short grace period so the bound is tight.
        let grace_ms = 80u64;
        let mut h = DaemonHandle::start_with_drain(Duration::from_secs(60), grace_ms, 3000).await;

        // Connect a subscriber but NEVER read from it — stalling the write side.
        let stream = tokio::net::UnixStream::connect(&h.socket)
            .await
            .expect("raw connect");
        let (_, mut write_half) = stream.into_split();
        // Send announce + subscribe so the daemon registers us as a subscriber.
        let announce_msg =
            r#"{"op":"announce","session_id":"stalled","pid":1,"cwd":"/","intent":""}"#;
        use tokio::io::AsyncWriteExt as _;
        write_half
            .write_all(format!("{announce_msg}\n").as_bytes())
            .await
            .unwrap();
        write_half.flush().await.unwrap();
        // brief pause to let daemon process the announce
        tokio::time::sleep(Duration::from_millis(20)).await;
        let sub_msg = r#"{"op":"subscribe","prefix":""}"#;
        write_half
            .write_all(format!("{sub_msg}\n").as_bytes())
            .await
            .unwrap();
        write_half.flush().await.unwrap();
        // brief pause so subscribe is registered
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Signal shutdown and measure elapsed time.
        let start = std::time::Instant::now();
        h.stop_only().await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "AC3: daemon must exit within 2s even with a stalled subscriber (elapsed {elapsed:?})"
        );
    });
}

// ---------------------------------------------------------------------------
// AC4: clean-exit — socket file is gone after shutdown
// ---------------------------------------------------------------------------

#[test]
fn ac4_socket_removed_on_shutdown() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut h = DaemonHandle::start_with_drain(Duration::from_secs(60), 50, 3000).await;
        let socket_path = h.socket.clone();

        // Verify socket exists while daemon is live.
        assert!(
            socket_path.exists(),
            "socket should exist while daemon is running"
        );

        h.stop_only().await;

        // Socket should be gone.
        assert!(
            !socket_path.exists(),
            "AC4: socket file should be removed after daemon exits"
        );
    });
}

// ---------------------------------------------------------------------------
// AC6: unknown-event tolerance — a raw line-reader does not crash on the notice
// ---------------------------------------------------------------------------

#[test]
fn ac6_raw_line_reader_does_not_crash_on_drain_notice() {
    // The drain notice is valid JSON. A subscriber that reads raw lines and
    // does not pattern-match on it must not panic or error.
    let raw = r#"{"op":"bus.draining","resume_after_ms":3000}"#;

    // Attempt 1: parse as DrainNotice (the intended type).
    let notice: DrainNotice = serde_json::from_str(raw).expect("parses as DrainNotice");
    assert_eq!(notice.op, "bus.draining");
    assert_eq!(notice.resume_after_ms, 3000);

    // Attempt 2: a "legacy" subscriber that tries ServerEvent and falls back
    // gracefully when it doesn't match (no panic, no crash). The parse will
    // fail (missing topic/data/from) — that's expected and acceptable.
    use agorabus::ServerEvent;
    let result: Result<ServerEvent, _> = serde_json::from_str(raw);
    // Either Ok or Err is fine; just no panic/crash.
    let _ = result;
}
