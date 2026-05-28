# Changelog

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
