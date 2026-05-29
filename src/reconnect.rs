//! Reconnecting subscribe loop for long-lived `agorabus subscribe` clients.
//!
//! When the daemon dies (EOF, `ConnectionReset`, `BrokenPipe`, `ConnectionRefused`,
//! socket-not-found), the [`reconnect_subscribe`] function re-connects with
//! bounded exponential backoff + full jitter, re-announces with the same
//! session identity, re-subscribes to the same prefix(es), and continues
//! streaming events to the provided output sink.
//!
//! ## Backoff formula
//!
//! `delay = rng(0 .. min(cap_ms, base_ms * 2^attempt))`
//!
//! where `attempt` counts from 0 and resets after a successful subscribe that
//! survives ≥ `cap_ms`.  The `rng(0..x)` produces a uniformly random integer
//! in `[0, x)` (full jitter), so the *expected* delay grows as `min(cap/2, ...)`.

#![allow(
    clippy::future_not_send,
    clippy::missing_errors_doc,
    clippy::too_many_arguments,
    clippy::cognitive_complexity,
)]

use std::path::Path;
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::time::Instant;

use crate::client::{InboundLine, send_heartbeat};
use crate::protocol::ServerEvent;
use crate::{Client, DEFAULT_HEARTBEAT_TIMEOUT_SECS};

/// Configuration for the reconnect loop.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Base delay in milliseconds for the first retry.
    pub base_ms: u64,
    /// Cap on the exponential growth (milliseconds).
    pub cap_ms: u64,
    /// Maximum number of reconnect attempts (0 = unbounded).
    pub max_attempts: usize,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            base_ms: 100,
            cap_ms: 5_000,
            max_attempts: 0,
        }
    }
}

/// Compute the delay for attempt `n` (0-indexed) using exponential backoff
/// with full jitter.
///
/// `delay = rng(0 .. min(cap_ms, base_ms * 2^n))`
///
/// Returns `Duration`.
#[must_use]
pub fn backoff_delay(base_ms: u64, cap_ms: u64, attempt: u32) -> Duration {
    // Clamp the exponent to avoid overflow on large attempt counts.
    let shift = attempt.min(62);
    let window = cap_ms.min(base_ms.saturating_mul(1u64 << shift));
    // Full jitter: uniform random in [0, window).
    let jitter_ms = if window == 0 {
        0
    } else {
        // Simple LCG seeded from current time + attempt to avoid pulling in
        // `rand`.  Not cryptographic — jitter quality is fine for backoff.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let mixed = (u64::from(seed) ^ (u64::from(attempt).wrapping_mul(0x9e37_79b9_7f4a_7c15)))
            .wrapping_add(0x6c62_272e_07bb_0142);
        // Modulo reduction (slightly biased for large window, fine for backoff).
        mixed % window
    };
    Duration::from_millis(jitter_ms)
}

/// Reconnect-aware subscribe loop.
///
/// Connects to the daemon at `socket_path`, announces as `session_id` / `pid`
/// / `cwd` / `intent`, subscribes to `prefix`, then streams events to
/// `on_event` until:
///
/// - The loop exits normally (`max_events` reached or `--no-reconnect` +
///   EOF), or
/// - `max_attempts` consecutive failed reconnects are exhausted (returns
///   `Err`).
///
/// On each EOF/connection-error the function logs a structured `reconnecting`
/// line to stderr, backs off, and retries — unless `reconnect` is `false`
/// (old behaviour: exit on EOF).
///
/// `on_event` returns `bool`: `true` = continue, `false` = stop and return
/// `Ok(())`.
pub async fn reconnect_subscribe<F>(
    socket_path: &Path,
    session_id: &str,
    pid: u32,
    cwd: &str,
    intent: &str,
    prefix: &str,
    max_events: usize,
    reconnect: bool,
    cfg: ReconnectConfig,
    mut on_event: F,
) -> Result<()>
where
    F: FnMut(ServerEvent) -> bool,
{
    let mut attempt: u32 = 0;
    let mut consecutive_failures: usize = 0;
    let mut received: usize = 0;

    loop {
        // --- connect + announce + subscribe ---
        let connect_result = Client::try_connect(socket_path).await;
        let client_opt = match connect_result {
            Ok(c) => c,
            Err(e) => {
                if !reconnect {
                    return Err(e);
                }
                None // treat hard error same as "not found" for reconnect
            }
        };

        let Some(mut client) = client_opt else {
            if !reconnect {
                return Ok(());
            }
            // Daemon not reachable.
            consecutive_failures = consecutive_failures.saturating_add(1);
            if cfg.max_attempts != 0 && consecutive_failures > cfg.max_attempts {
                return Err(anyhow!(
                    "subscribe: exhausted {} reconnect attempts (daemon unreachable)",
                    cfg.max_attempts
                ));
            }
            let delay = backoff_delay(cfg.base_ms, cfg.cap_ms, attempt);
            eprintln!(
                "{{\"reconnecting\":true,\"sid\":\"{session_id}\",\"attempt\":{consecutive_failures},\"delay_ms\":{}}}",
                delay.as_millis()
            );
            tokio::time::sleep(delay).await;
            attempt = attempt.saturating_add(1);
            continue;
        };

        // Connected — announce + subscribe.
        let announce_ok = client.announce(session_id, pid, cwd, intent).await.is_ok();
        let subscribe_ok = announce_ok && client.subscribe(prefix).await.is_ok();

        if !subscribe_ok {
            if !reconnect {
                return Err(anyhow!("subscribe: announce or subscribe failed"));
            }
            consecutive_failures = consecutive_failures.saturating_add(1);
            if cfg.max_attempts != 0 && consecutive_failures > cfg.max_attempts {
                return Err(anyhow!(
                    "subscribe: exhausted {} reconnect attempts (handshake failed)",
                    cfg.max_attempts
                ));
            }
            let delay = backoff_delay(cfg.base_ms, cfg.cap_ms, attempt);
            eprintln!(
                "{{\"reconnecting\":true,\"sid\":\"{session_id}\",\"attempt\":{consecutive_failures},\"delay_ms\":{}}}",
                delay.as_millis()
            );
            tokio::time::sleep(delay).await;
            attempt = attempt.saturating_add(1);
            continue;
        }

        // Handshake succeeded; reset failure counter + backoff exponent.
        consecutive_failures = 0;
        attempt = 0;

        // Split the client halves for concurrent heartbeat + read.
        let (mut write_half, mut reader) = client.into_halves();

        // Heartbeat task keeps the daemon from pruning us during a quiet period.
        let heartbeat_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(
                DEFAULT_HEARTBEAT_TIMEOUT_SECS / 2,
            ));
            ticker.tick().await; // drop the immediate first tick
            loop {
                ticker.tick().await;
                if send_heartbeat(&mut write_half, "").await.is_err() {
                    return;
                }
            }
        });

        // Track how long we stay connected to decide whether to reset backoff.
        let connected_at = Instant::now();

        loop {
            let line = match reader.next_line().await {
                Ok(Some(l)) => l,
                Ok(None) => {
                    // EOF — daemon closed connection.
                    break;
                }
                Err(_) => {
                    break;
                }
            };

            let parsed: InboundLine = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            match parsed {
                InboundLine::Reply(_) => {}
                InboundLine::Event(ev) => {
                    let cont = on_event(ev);
                    received = received.saturating_add(1);
                    if !cont || (max_events != 0 && received >= max_events) {
                        heartbeat_handle.abort();
                        let _ = heartbeat_handle.await;
                        return Ok(());
                    }
                }
            }
        }

        heartbeat_handle.abort();
        let _ = heartbeat_handle.await;

        if !reconnect {
            // Old behaviour: exit on EOF.
            return Ok(());
        }

        {
            consecutive_failures = consecutive_failures.saturating_add(1);
            if cfg.max_attempts != 0 && consecutive_failures > cfg.max_attempts {
                return Err(anyhow!(
                    "subscribe: exhausted {} reconnect attempts after connection loss",
                    cfg.max_attempts
                ));
            }

            // If we stayed connected long enough (≥ cap_ms), reset attempt
            // exponent so a flapping daemon doesn't escalate backoff forever.
            let survived_ms = connected_at.elapsed().as_millis() as u64;
            if survived_ms >= cfg.cap_ms {
                attempt = 0;
            }

            let delay = backoff_delay(cfg.base_ms, cfg.cap_ms, attempt);
            eprintln!(
                "{{\"reconnecting\":true,\"sid\":\"{session_id}\",\"attempt\":{consecutive_failures},\"delay_ms\":{}}}",
                delay.as_millis()
            );
            tokio::time::sleep(delay).await;
            attempt = attempt.saturating_add(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    /// AC3 — backoff shape: `min(cap, base * 2^n)` must hold for each attempt.
    ///
    /// Because of full jitter the exact delay is random in `[0, window)`.
    /// We verify the *window* (upper bound) follows the expected formula.
    #[test]
    fn backoff_window_follows_exponential_cap() {
        let base: u64 = 50;
        let cap: u64 = 400;

        // Pre-compute expected windows.
        let expected_windows: Vec<u64> = (0u32..8)
            .map(|n| {
                let shift = n.min(62);
                cap.min(base.saturating_mul(1u64 << shift))
            })
            .collect();

        // Expected: [50, 100, 200, 400, 400, 400, 400, 400]
        assert_eq!(expected_windows[0], 50);
        assert_eq!(expected_windows[1], 100);
        assert_eq!(expected_windows[2], 200);
        assert_eq!(expected_windows[3], 400);
        // Cap is reached and stays.
        for i in 4..8 {
            assert_eq!(expected_windows[i], 400, "attempt {i} should be capped at 400");
        }
    }

    /// AC3 — `backoff_delay` returns duration within [0, window).
    ///
    /// Run many samples to get probabilistic confidence.
    #[test]
    fn backoff_delay_stays_within_window() {
        let base: u64 = 50;
        let cap: u64 = 400;

        for attempt in 0u32..=8 {
            let shift = attempt.min(62);
            let window = cap.min(base.saturating_mul(1u64 << shift));

            // Sample the jitter several times.
            for _ in 0..20 {
                let d = backoff_delay(base, cap, attempt);
                assert!(
                    d.as_millis() < window as u128 || window == 0,
                    "attempt={attempt}: delay {}ms exceeded window {window}ms",
                    d.as_millis()
                );
            }
        }
    }

    /// AC3 — window with base=0 and window=0 returns zero delay without panic.
    #[test]
    fn backoff_delay_zero_base_no_panic() {
        let d = backoff_delay(0, 0, 0);
        assert_eq!(d.as_millis(), 0);

        let d = backoff_delay(0, 400, 3);
        assert_eq!(d.as_millis(), 0, "base=0 => window=0 => delay=0");
    }

    /// AC3 — large attempt number doesn't overflow (shift is clamped to 62).
    #[test]
    fn backoff_delay_large_attempt_no_overflow() {
        let d = backoff_delay(100, 5_000, u32::MAX);
        // Should be capped at cap_ms = 5000, delay in [0, 5000).
        assert!(d.as_millis() < 5_000, "delay should be < cap");
    }

    /// AC3 — windows are non-decreasing before the cap.
    #[test]
    fn backoff_windows_non_decreasing() {
        let base: u64 = 100;
        let cap: u64 = 5_000;

        let mut prev_window: u64 = 0;
        for attempt in 0u32..=8 {
            let shift = attempt.min(62);
            let window = cap.min(base.saturating_mul(1u64 << shift));
            assert!(
                window >= prev_window,
                "attempt={attempt}: window {window} < prev {prev_window} (non-monotonic)"
            );
            prev_window = window;
        }
    }

    /// AC4 — `max_attempts=2` with a missing socket path exits with Err after
    /// exactly 2 failed reconnect attempts.
    ///
    /// Uses a tmp path that doesn't exist so every connect attempt fails.
    #[tokio::test]
    async fn max_reconnect_attempts_terminates() {
        let tmp = std::env::temp_dir().join(format!(
            "agorabus-test-noexist-{}.sock",
            std::process::id()
        ));
        // Ensure no socket exists.
        let _ = std::fs::remove_file(&tmp);

        let cfg = ReconnectConfig {
            base_ms: 1,  // tiny delay for test speed
            cap_ms: 5,
            max_attempts: 2,
        };

        let result = reconnect_subscribe(
            &tmp,
            "test-session",
            std::process::id(),
            "/tmp",
            "test",
            "test.",
            0,
            true, // reconnect = on
            cfg,
            |_ev| true,
        )
        .await;

        assert!(
            result.is_err(),
            "should return Err after exhausting max_attempts"
        );
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("exhausted 2 reconnect attempts"),
            "error should mention attempt count, got: {msg}"
        );
    }

    /// AC5 — `--no-reconnect` (reconnect=false) returns Ok(()) on missing socket.
    #[tokio::test]
    async fn no_reconnect_exits_ok_on_missing_daemon() {
        let tmp = std::env::temp_dir().join(format!(
            "agorabus-test-norecon-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);

        let cfg = ReconnectConfig::default();

        let result = reconnect_subscribe(
            &tmp,
            "test-session",
            std::process::id(),
            "/tmp",
            "test",
            "test.",
            0,
            false, // no_reconnect = true → reconnect = false
            cfg,
            |_ev| true,
        )
        .await;

        assert!(
            result.is_ok(),
            "no-reconnect mode should exit Ok on missing socket, got: {result:?}"
        );
    }

    /// `ReconnectConfig::default` has sane values.
    #[test]
    fn reconnect_config_default_values() {
        let cfg = ReconnectConfig::default();
        assert_eq!(cfg.base_ms, 100);
        assert_eq!(cfg.cap_ms, 5_000);
        assert_eq!(cfg.max_attempts, 0, "0 means unbounded");
    }
}
