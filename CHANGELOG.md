# Changelog

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
