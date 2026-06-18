# Changelog

## v0.11.0 — 2026-06-18

Added fleet-wide peer presence to agorabus (tether-presence PRD).

Changes:
- PeerRecord gains optional `node` field (backward-compatible; absent for local peers)
- New FleetPresenceEvent type + wm.fleet.presence.* subjects + TTL constant
- FleetStore: in-memory store for remote peers with announce/remove/live_peers
- merge_peers() / peer_age_secs() pure utilities
- Client.announce_with_node() for tagging peers with a node name
- Daemon captures node from Announce op into PeerRecord
- `agorabus peers --fleet` merges remote peers from wm.fleet.presence.announce
- 7 new integration tests (fleet_presence_acs.rs); all 50+ existing tests green

## v0.10.0 — 2026-06-13

Add `ClaimGuard` — a lifetime-bound handle that acquires an agorabus advisory
claim, auto-renews it before TTL expiry, and releases it on drop or explicit
`ClaimGuard::release()` (PRD-changeover-claim-guard).

**New surface:**
- `ClaimGuard::hold(client, socket_path, session_id, path, ttl)` — acquire
  and start auto-renew. The client is consumed by the guard.
- `Client::hold_claim(path, ttl, socket_path, session_id)` — convenience
  wrapper on `Client` that calls `ClaimGuard::hold`.
- `ClaimGuard::release(self) -> Result<()>` — explicit awaitable release for
  SIGTERM handlers.
- `impl Drop for ClaimGuard` — best-effort fire-and-forget release.

Renew failures (bus unreachable) are logged and the loop retries via a fresh
connection; the claim is never permanently abandoned on a transient bus blip.

**Example:**
```rust
let guard = client.hold_claim(path, Duration::from_secs(30), &socket, &session_id).await?;
// ... work ...
guard.release().await?;
```

## v0.9.0 — 2026-06-04

The agorabus daemon keeps its claims table and sticky intents in memory
only. A restart — exactly the operation vigil wants to make routine —
silently drops every active chord-claim lock and every intent string. A
file-lock another session is holding through agorabus vanishes the moment
the bus is rolled. This PRD journals the durable slice of bus state
(claims + sticky intents) to `~/.cache/agorabus/state.json` on mutation
and on drain, and rehydrates it on start, so a reload does not quietly
revoke locks.

## v0.8.0 — 2026-05-29

Add `agorabus reload` subcommand (PRD-agorabus-reload): one non-destructive
command to roll the running bus daemon. Resolves the daemon pid via `/proc`
scan, checks binary freshness via the shared `doctor` logic, snapshots the
pre-bounce peer set, SIGTERMs the old daemon, relaunches the fresh binary via
`nohup`, then polls until all pre-bounce session_ids have re-registered or
the reconnect timeout elapses. Emits a structured verdict:
`{old_pid, new_pid, binary_before, binary_after, peers_before, peers_after,
peers_recovered, peers_missing, elapsed_ms, status}`. Status is `reloaded`,
`reloaded-degraded` (some sessions did not reconnect), or `failed`.

Default posture is `--dry-run true` (prints the plan without mutating); use
`--no-dry-run` / pass `--apply` to perform the actual bounce. Guards:
`--require-fresh` (default on) refuses to bounce an already-current daemon;
`--start-if-absent` allows launching a daemon when none is running. New
`src/reload.rs` module exposed via `agorabus::reload`; five acceptance tests
in `tests/acceptance_reload_dryrun.rs` cover AC1/AC3/AC4/AC5/AC6.

## v0.7.0 — 2026-05-29

Add graceful drain notice on shutdown (PRD-agorabus-drain-notice). When
the daemon receives SIGTERM/SIGINT it now broadcasts a
`{"op":"bus.draining","resume_after_ms":N}` notice to every subscriber
before closing connections. Subscribers see the advisory hint and can
pace reconnect attempts (PRD-agorabus-client-reconnect) to avoid a
thundering-herd on rebind.

New `agorabus daemon` flags: `--drain-grace-ms` (default 200) and
`--drain-resume-hint-ms` (default 3000). New public API: `DrainNotice`
struct, `Client::next_raw_line()`, `DEFAULT_DRAIN_GRACE_MS`,
`DEFAULT_DRAIN_RESUME_HINT_MS`, and two new `DaemonConfig` fields.
Shutdown remains prompt: drain is best-effort; connections are aborted
unconditionally after the grace window (stuck subscribers cannot wedge a
roll). All existing tests pass unchanged (44 total).

## v0.6.0 — 2026-05-29

Add subscriber reconnect: the long-lived `agorabus subscribe` loop now
survives daemon restarts without the client process exiting. On EOF,
`ConnectionReset`, `BrokenPipe`, `ConnectionRefused`, or missing-socket,
the subscriber re-opens the socket with bounded exponential backoff + full
jitter, re-announces with the same session_id/pid/cwd/intent, re-subscribes
to the same prefix, and continues streaming to the same output sink.

New flags: `--reconnect-base-ms` (default 100), `--reconnect-cap-ms`
(default 5000), `--max-reconnect-attempts` (default 0 = unbounded),
`--no-reconnect` (restores old exit-on-EOF for one-shot callers). Reconnect
is on by default; the attempt counter resets after surviving ≥ cap_ms so a
clean reconnect starts fresh. New public API: `reconnect_subscribe` async fn
+ `ReconnectConfig` struct in `src/reconnect.rs`.

## v0.5.0 — 2026-05-29

Add `agorabus doctor` subcommand: self-staleness detection for the running
agorabus daemon. The subcommand introspects `/proc/<daemon-pid>/exe`, detects
the ` (deleted)` suffix (binary replaced underneath the running process),
compares running vs on-disk inodes, and reads the optional `user.prov.ts`
xattr. Verdict output: `current` (exit 0), `stale: deleted-exe` or
`stale: inode-drift` (exit 1), `unknown` / no daemon (exit 2).
Supports `--format text` (default, human-readable) and `--format json`
(`{daemon_pid, exe_path, exe_inode, ondisk_inode, prov_ts, verdict}`).
Daemon pid discovery is self-contained (proc scan; no binstale dependency).
All existing subcommands unaffected. New `src/doctor.rs` module exposed
via `agorabus::doctor`.
## v0.4.0 — 2026-05-29

Fix multi-prefix subscribe: the daemon's per-connection state held a single
`subscribed_prefix: Option<String>` slot, so each `Subscribe` op overwrote the
prior prefix and only the last `subscribe()` call took effect. Changed to a
`Vec<String>` that appends on each `Subscribe`, with `topic_matches` now
"any prefix matches". Single-subscribe clients are unaffected; multi-prefix
clients (wm-audio `["wm.tts.","wm.dialog.","wm.audio.reload"]`, wm-dialog
`["wm.audio.","wm.stt.","wm.brain."]`) now receive events on all their
prefixes instead of just the last one. Semver minor — behaviour change.

## v0.3.0 — 2026-05-28

Extend the heartbeat envelope with three optional, sticky structured-intent
fields — `skill`, `prd_slug`, `working_paths` (max 8) — and add two CLI
subcommands so a session can publish what it is doing and any peer can read
it in one call.

New surface:
- CLI: `agorabus intent set --session-id <sid> [--skill S] [--prd P] [--paths a,b]`
  writes a single heartbeat populating the structured fields (sticky: omit a
  flag to leave it untouched, pass an empty value to clear). `agorabus intent
  list` returns only peers with intent set, projected to `session_id` plus the
  set fields. Both fail-open (no daemon ⇒ exit 0, `[]`/silent).
- Protocol/daemon: `Heartbeat` carries optional `skill`/`prd_slug`/`working_paths`;
  daemon stores them sticky on the peer record; `working_paths` > 8 rejected with
  `too_many_paths`. Existing `{"op":"heartbeat","tool":"Bash"}` clients unchanged.
- `PeerRecord` gains the three fields (omitted from JSON when empty).

All 6 PRD ACs verified: AC1/AC2/AC5 daemon-level (tests/chord_intent_acs.rs),
AC3/AC4/AC6 CLI-level (tests/intent_cli_acs.rs). Cargo.toml v0.2.0 → v0.3.0
(AC7 literal "0.1.0→0.2.0" overtaken — chord-claim already took 0.2.0).
REPOS.md untouched.

## v0.2.0 — 2026-05-28

Add an **advisory soft-lock** primitive to agorabus so a Claude
session can announce "I'm about to touch this path for the next N
seconds" and peer sessions can see the claim before they start their
own write. No kernel locks. No enforcement. Pure cooperation: each
session decides whether to honor, override, or coordinate.

New surface:
- CLI: `agorabus claim {acquire,release,list}` with `--force`, `--wait`,
  `--ttl`, `--reason`, path filters, text/json output.
- Daemon: `ClaimAcquire`/`ClaimRelease`/`ClaimList` envelopes with
  same-session renewal, conflict detection, force-evict + publish
  `claim.release` for the evicted holder, TTL-based prune.
- Client: `claim_acquire`/`claim_release`/`claim_list` methods.

All 10 PRD ACs verified: 9 daemon-level + CLI fail-open + `--wait`
both branches. Cargo.toml v0.1.0 → v0.2.0; REPOS.md untouched.
