#!/usr/bin/env bash
# VENDORED COPY — canonical source: rustbuild skill/rules/audit-checks.sh @ 1f7ad40
# (rustbuild is where that file lives; this repo-root copy is only for CI
# boxes with no ~/.claude — same convention every other onboarded repo uses.)
# audit-checks.sh — mechanizable BAD_RUST / HLT-* detectors
#
# Runs grep-based and clippy-restriction-based audits against a Rust project.
# Emits a JSON findings array to stdout. Non-zero exit if any BLOCKING finding.
#
# Usage:
#   audit-checks.sh <project_root>
#
# Output schema (per finding):
#   {
#     "rule_id": "HLT-029-RUST-BAD-BEHAVIOR",
#     "detector": "grep_unwrap_on_external_input",
#     "severity": "blocking" | "advisory",
#     "path": "src/main.rs",
#     "line": 42,
#     "snippet": "...",
#     "explanation": "..."
#   }
#
# The wrapping object is { "findings": [...], "blocking_count": N, "advisory_count": M }.
# Exit code 0 means no BLOCKING findings (advisory findings are fine for exit code).

# `errexit` is intentionally OFF: most detectors are `grep | while` pipelines
# that return non-zero when there are no matches (the common case), and
# `pipefail` would otherwise kill the script before the aggregation block
# runs. We rely on the aggregation block to always emit a valid envelope,
# even when individual detectors fail.
set -uo pipefail

PROJECT_ROOT="${1:-.}"
cd "$PROJECT_ROOT"

# Collect findings into a temporary file so we can count at the end.
FINDINGS_FILE="$(mktemp)"
AGGREGATED=0
emit_aggregate() {
  if [ "$AGGREGATED" = "1" ]; then
    return
  fi
  AGGREGATED=1
  local blocking advisory head
  blocking=$(jq -s '[.[] | select(.severity == "blocking")] | length' "$FINDINGS_FILE" 2>/dev/null || echo 0)
  advisory=$(jq -s '[.[] | select(.severity == "advisory")] | length' "$FINDINGS_FILE" 2>/dev/null || echo 0)
  head=$(git rev-parse HEAD 2>/dev/null || echo unknown)
  jq -s --arg schema "autobuilder.bad_rust_audit.v1" --arg head "$head" \
        --argjson b "${blocking:-0}" --argjson a "${advisory:-0}" \
    '{schema: $schema, head_sha: $head, findings: ., blocking_count: $b, advisory_count: $a}' \
    "$FINDINGS_FILE" 2>/dev/null || \
    echo '{"schema": "autobuilder.bad_rust_audit.v1", "findings": [], "blocking_count": 0, "advisory_count": 0}'
}
trap 'emit_aggregate; rm -f "$FINDINGS_FILE"' EXIT
: > "$FINDINGS_FILE"

emit_finding() {
  # emit_finding <rule_id> <detector> <severity> <path> <line> <snippet> <explanation>
  jq -n \
    --arg rule_id "$1" \
    --arg detector "$2" \
    --arg severity "$3" \
    --arg path "$4" \
    --arg line "$5" \
    --arg snippet "$6" \
    --arg explanation "$7" \
    '{rule_id: $rule_id, detector: $detector, severity: $severity, path: $path, line: ($line | tonumber? // 0), snippet: $snippet, explanation: $explanation}' \
    >> "$FINDINGS_FILE"
}

# ---- Source enumeration ----
rust_files() {
  find src -type f -name '*.rs' 2>/dev/null || true
}

# Skip a finding if the line carries a `// allowlist:` comment.
is_allowlisted() {
  local file="$1" line="$2"
  awk -v n="$line" 'NR == n' "$file" | grep -q '// allowlist:'
}

# ---- HLT-029: BAD_RUST grep detectors ----

grep_unsafe_without_safety_comment() {
  while IFS= read -r -d '' file; do
    awk '
      /unsafe[[:space:]]*\{/ {
        if (NR > 1 && prev !~ /SAFETY:/) {
          print FILENAME ":" NR ":" $0
        }
      }
      { prev = $0 }
    ' "$file"
  done < <(rust_files | tr '\n' '\0') | while IFS= read -r match; do
    file=$(echo "$match" | cut -d: -f1)
    line=$(echo "$match" | cut -d: -f2)
    snippet=$(echo "$match" | cut -d: -f3-)
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-029-RUST-BAD-BEHAVIOR" "grep_unsafe_without_safety_comment" "blocking" "$file" "$line" "$snippet" \
      "unsafe block without a SAFETY: comment on the preceding line"
  done
}

grep_unwrap_on_external_input() {
  # We can't statically know whether a value is "external," so we approximate:
  # flag .unwrap() / .expect() on results of common boundary functions.
  local pattern='(env::var|fs::read|fs::read_to_string|fs::write|std::io|stdin|stdout|stderr|args|TcpStream|TcpListener|UdpSocket|reqwest|hyper|serde_json::from_str|serde_json::from_slice|toml::from_str|process::Command|child::wait|Receiver::recv|Sender::send)[^;]{0,200}\.(unwrap|expect)\('
  while IFS= read -r -d '' file; do
    grep -nHE "$pattern" "$file" 2>/dev/null || true
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-029-RUST-BAD-BEHAVIOR" "grep_unwrap_on_external_input" "blocking" "$file" "$line" "$snippet" \
      "unwrap/expect on a boundary-input result; use ? and propagate"
  done
}

grep_box_leak() {
  while IFS= read -r -d '' file; do
    grep -nHE 'Box::leak' "$file" 2>/dev/null || true
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-029-RUST-BAD-BEHAVIOR" "grep_box_leak" "blocking" "$file" "$line" "$snippet" \
      "Box::leak as a lifetime fix is a lie; pass references explicitly"
  done
}

grep_static_mut() {
  while IFS= read -r -d '' file; do
    grep -nHE '\bstatic\s+mut\b' "$file" 2>/dev/null || true
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-029-RUST-BAD-BEHAVIOR" "grep_static_mut" "blocking" "$file" "$line" "$snippet" \
      "static mut is UB-prone; use a typed synchronization primitive"
  done
}

grep_mem_transmute() {
  while IFS= read -r -d '' file; do
    grep -nHE 'mem::transmute|std::mem::transmute' "$file" 2>/dev/null || true
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-029-RUST-BAD-BEHAVIOR" "grep_mem_transmute" "blocking" "$file" "$line" "$snippet" \
      "mem::transmute is last-resort; layout/validity/provenance must be proven"
  done
}

grep_unsafe_impl_send_sync() {
  while IFS= read -r -d '' file; do
    grep -nHE 'unsafe\s+impl\s+(Send|Sync)' "$file" 2>/dev/null || true
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-029-RUST-BAD-BEHAVIOR" "grep_unsafe_impl_send_sync" "blocking" "$file" "$line" "$snippet" \
      "unsafe impl Send/Sync requires proof of thread-safety; document or remove"
  done
}

# ---- HLT-010: secret sprawl ----

grep_hardcoded_secrets() {
  local pattern='(api[_-]?key|secret|password|passwd|token|bearer)[[:space:]]*=[[:space:]]*"[A-Za-z0-9_\-]{8,}"'
  while IFS= read -r -d '' file; do
    grep -niHE "$pattern" "$file" 2>/dev/null || true
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-010-SECRET-SPRAWL" "grep_hardcoded_secrets" "blocking" "$file" "$line" "$snippet" \
      "hardcoded secret-like string; load from env or secret manager"
  done
}

grep_secret_in_debug_impl() {
  # Heuristic: types with field names that are EXACTLY one of the
  # secret-like names (followed by `:` for the field-type separator), inside
  # a struct that derives Debug. Prefix-match would catch innocent fields
  # like `token_budget` or `api_key_id` (which itself is fine to log).
  while IFS= read -r -d '' file; do
    awk '
      /#\[derive\([^)]*Debug[^)]*\)\]/ {pending=1; next}
      pending && /^[[:space:]]*(pub(\([^)]*\))? )?struct / {struct=1; next}
      struct && /^[[:space:]]*\}/ {struct=0; pending=0; next}
      struct && /^[[:space:]]*(pub(\([^)]*\))? )?(secret|password|passwd|token|api_key|private_key|bearer)[[:space:]]*:/ {
        print FILENAME ":" NR ":" $0
      }
    ' "$file"
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-010-SECRET-SPRAWL" "grep_secret_in_debug_impl" "blocking" "$file" "$line" "$snippet" \
      "secret-named field on a #[derive(Debug)] struct will leak via {:?}"
  done
}

# ---- HLT-016: supply chain ----

cargo_deny_check() {
  if ! command -v cargo-deny >/dev/null 2>&1; then
    emit_finding "HLT-016-SUPPLY-CHAIN-DRIFT" "cargo_deny_check" "advisory" "deny.toml" "0" "" \
      "cargo-deny not installed; supply-chain check skipped"
    return
  fi
  # Run bans + licenses + sources locally. The advisories check is delegated
  # to CI, which can install a newer cargo-deny that parses CVSS 4.0; an
  # older locally-installed cargo-deny fails to parse the current advisory
  # DB which would otherwise produce false-positive blocking findings.
  emit_finding "HLT-016-SUPPLY-CHAIN-DRIFT" "cargo_deny_check" "advisory" "deny.toml" "0" "" \
    "advisories check delegated to CI; bans/licenses/sources checked locally"
  if ! cargo deny check bans licenses sources 2>&1 | tail -50 > /tmp/deny-out.txt; then
    while IFS= read -r line; do
      emit_finding "HLT-016-SUPPLY-CHAIN-DRIFT" "cargo_deny_check" "blocking" "Cargo.toml" "0" "$line" \
        "cargo-deny reported a policy violation"
    done < /tmp/deny-out.txt
  fi
}

check_cargo_lock_committed() {
  if [ ! -f Cargo.lock ]; then
    emit_finding "HLT-016-SUPPLY-CHAIN-DRIFT" "check_cargo_lock_committed" "blocking" "Cargo.lock" "0" "" \
      "Cargo.lock missing; commit it for reproducible builds"
  fi
}

# ---- HLT-020/034: workflow hygiene ----

check_workflow_pinned_actions() {
  for wf in .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -f "$wf" ] || continue
    grep -nE 'uses:[[:space:]]+[^@]+@(v?[0-9]+|main|master)$' "$wf" 2>/dev/null | while IFS=: read -r line snippet; do
      emit_finding "HLT-020-CI-HARDENING-GAP" "check_workflow_pinned_actions" "blocking" "$wf" "$line" "$snippet" \
        "GitHub Action not pinned to a commit SHA"
    done
  done
}

check_workflow_minimal_permissions() {
  for wf in .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -f "$wf" ] || continue
    if ! grep -qE '^[[:space:]]*permissions:' "$wf"; then
      emit_finding "HLT-020-CI-HARDENING-GAP" "check_workflow_minimal_permissions" "blocking" "$wf" "0" "" \
        "workflow missing top-level permissions: block (defaults to write-all)"
    fi
  done
}

check_workflow_no_nonblocking_security_steps() {
  for wf in .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -f "$wf" ] || continue
    grep -nE 'continue-on-error:[[:space:]]*true' "$wf" 2>/dev/null | while IFS=: read -r line snippet; do
      emit_finding "HLT-034-CI-BAD-BEHAVIOR" "check_workflow_no_nonblocking_security_steps" "blocking" "$wf" "$line" "$snippet" \
        "continue-on-error: true on a workflow step; security/proof steps must block"
    done
  done
}

check_no_force_push_in_workflows() {
  for wf in .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -f "$wf" ] || continue
    grep -nE 'git[[:space:]]+push[[:space:]]+.*(--force|-f[[:space:]])' "$wf" 2>/dev/null | while IFS=: read -r line snippet; do
      emit_finding "HLT-035-GIT-BAD-BEHAVIOR" "check_no_force_push_in_workflows" "blocking" "$wf" "$line" "$snippet" \
        "force-push in a workflow; remove or scope explicitly"
    done
  done
}

check_no_skip_hooks_in_scripts() {
  for f in scripts/*.sh; do
    [ -f "$f" ] || continue
    grep -nE '(--no-verify|--no-gpg-sign|HUSKY=0|SKIP_HOOKS=1)' "$f" 2>/dev/null | while IFS=: read -r line snippet; do
      emit_finding "HLT-035-GIT-BAD-BEHAVIOR" "check_no_skip_hooks_in_scripts" "blocking" "$f" "$line" "$snippet" \
        "skipping git hooks/signing in scripts; fix the underlying issue"
    done
  done
}

# ---- HLT-003/004: owner-map and test-map coverage ----

check_owner_map_coverage() {
  [ -f agent/owner-map.json ] || {
    emit_finding "HLT-003-OWNERLESS-PATH" "check_owner_map_coverage" "blocking" "agent/owner-map.json" "0" "" \
      "owner-map.json missing"
    return
  }
  # Verify every src/ path has at least a glob match in owner-map.
  # Minimal check: file must be valid JSON.
  if ! jq empty agent/owner-map.json 2>/dev/null; then
    emit_finding "HLT-003-OWNERLESS-PATH" "check_owner_map_coverage" "blocking" "agent/owner-map.json" "0" "" \
      "owner-map.json is not valid JSON"
  fi
}

check_test_map_coverage() {
  [ -f agent/test-map.json ] || {
    emit_finding "HLT-004-UNMAPPED-PROOF" "check_test_map_coverage" "blocking" "agent/test-map.json" "0" "" \
      "test-map.json missing"
    return
  }
  if ! jq empty agent/test-map.json 2>/dev/null; then
    emit_finding "HLT-004-UNMAPPED-PROOF" "check_test_map_coverage" "blocking" "agent/test-map.json" "0" "" \
      "test-map.json is not valid JSON"
  fi
}

# ---- HLT-008: false green ----

check_changed_paths_have_targeted_tests() {
  # Compare HEAD against HEAD~1; if a src/ path changed, ensure at least one tests/
  # file references it or was also changed.
  if ! git diff --name-only HEAD~1..HEAD 2>/dev/null > /tmp/changed-paths.txt; then
    return  # First commit; nothing to compare.
  fi
  local src_changed
  src_changed=$(grep '^src/' /tmp/changed-paths.txt || true)
  local tests_changed
  tests_changed=$(grep '^tests/' /tmp/changed-paths.txt || true)
  if [ -n "$src_changed" ] && [ -z "$tests_changed" ]; then
    emit_finding "HLT-008-FALSE-GREEN-RISK" "check_changed_paths_have_targeted_tests" "advisory" "src/" "0" "" \
      "src/ changed but no tests/ paths changed; advance only if proof-lanes routing covers the diff"
  fi
}

# ---- HLT-023: input-boundary tests ----

check_input_boundary_tests() {
  # Look for stdin/argv/env::var/fs::read usage in src/, ensure a test file
  # mentions the same boundary keyword.
  #
  # Severity scoping (added after iter-1 of session-trace-receipt hit a
  # false-positive verdict=crash):
  #
  #   - When the ONLY boundary use is inside a small src/main.rs (<30
  #     lines), this almost always means "the CLI is a stub that just
  #     parses --version, the real surface is in src/lib.rs and tests
  #     call the lib directly." Emit as ADVISORY, not BLOCKING — the
  #     edit-agent will wire the CLI later and the test will follow.
  #   - When the boundary use is in src/lib.rs (or in a non-trivial
  #     main.rs), keep the BLOCKING severity — that's a real test gap.
  local boundaries='(stdin|args\(\)|env::var|fs::read|fs::write|TcpListener|UdpSocket)'
  local src_uses
  src_uses=$(grep -rE "$boundaries" src/ 2>/dev/null | wc -l || true)
  local test_uses
  test_uses=$(grep -rE "$boundaries" tests/ 2>/dev/null | wc -l || true)
  if [ "${src_uses:-0}" -gt 0 ] && [ "${test_uses:-0}" -eq 0 ]; then
    local lib_uses=0
    if [ -f src/lib.rs ]; then
      lib_uses=$(grep -cE "$boundaries" src/lib.rs 2>/dev/null || echo 0)
    fi
    local main_lines=0
    if [ -f src/main.rs ]; then
      main_lines=$(wc -l < src/main.rs 2>/dev/null || echo 0)
    fi
    local severity="blocking"
    local note="src/ uses an input boundary but no tests/ exercises it"
    if [ "${lib_uses:-0}" -eq 0 ] && [ "${main_lines:-0}" -lt 30 ]; then
      severity="advisory"
      note="src/main.rs uses an input boundary but is small (<30 lines, likely a CLI stub); tests call lib directly. Wire a CLI integration test when the binary has a real surface."
    fi
    emit_finding "HLT-023-INPUT-BOUNDARY-GAP" "check_input_boundary_tests" "$severity" "tests/" "0" "" "$note"
  fi
}

# ---- HLT-aux-PROPTEST-DENSITY: property tests per public surface ----
#
# Counts `proptest!` macro invocations under tests/ vs `pub fn` declarations
# under src/. Below threshold = the test suite is example-only and unlikely
# to catch boundary errors the edit-agent didn't think of. Advisory severity
# initially — promote to blocking once the threshold is calibrated.

check_proptest_density() {
  local proptests
  proptests=$(grep -rEo '\bproptest!' tests/ 2>/dev/null | wc -l || true)
  local pub_fns
  pub_fns=$(grep -rE '^\s*pub fn ' src/ 2>/dev/null | wc -l || true)
  # No public surface = nothing to property-test. Skip.
  if [ "${pub_fns:-0}" -eq 0 ]; then
    return
  fi
  local threshold=$(( (pub_fns + 9) / 10 ))  # ceil(pub_fns / 10)
  if [ "${proptests:-0}" -lt "$threshold" ]; then
    emit_finding "HLT-aux-PROPTEST-DENSITY" "check_proptest_density" "advisory" "tests/" "0" "" \
      "proptest density $proptests proptest!() vs $pub_fns pub fn — below 1-per-10 threshold (target: $threshold). Add property tests to catch boundary errors the edit-agent did not anticipate."
  fi
}

# ---- HLT-025/027: receipts present ----

check_seven_receipts_present() {
  local dir=target/autobuilder/receipts
  for r in intake.json vti-plan.json proof-receipt.json risk-gate.json reviewer-agent.json rollback-plan.json ci-checks.json; do
    if [ ! -f "$dir/$r" ]; then
      emit_finding "HLT-025-RELEASE-READINESS-GAP" "check_seven_receipts_present" "blocking" "$dir/$r" "0" "" \
        "missing required receipt: $r"
    fi
  done
}

check_reviewer_agent_receipt_present() {
  if [ ! -f target/autobuilder/receipts/reviewer-agent.json ]; then
    emit_finding "HLT-027-HUMAN-REVIEW-EVIDENCE-GAP" "check_reviewer_agent_receipt_present" "blocking" \
      "target/autobuilder/receipts/reviewer-agent.json" "0" "" \
      "reviewer-agent receipt missing; risk gate cannot pass"
  fi
}

# ---- HLT-041: comment hygiene ----

grep_dangerous_comments() {
  local pattern='(TODO[: ].*later|FIXME|XXX|HACK|TEMP[: ]|AI[- ]generated|copilot[- ]generated|claude[- ]generated|chatgpt[- ]generated|works on my machine|should not happen)'
  while IFS= read -r -d '' file; do
    grep -niHE "$pattern" "$file" 2>/dev/null || true
  done < <(rust_files | tr '\n' '\0') | while IFS=: read -r file line snippet; do
    is_allowlisted "$file" "$line" && continue
    emit_finding "HLT-041-COMMENT-HYGIENE" "grep_dangerous_comments" "advisory" "$file" "$line" "$snippet" \
      "dangerous-comment marker; resolve or document explicitly"
  done
}

# ---- Clippy restriction lints ----

clippy_restriction_lints() {
  if ! command -v cargo >/dev/null 2>&1; then
    return
  fi
  # Run a focused clippy with restriction lints we care about.
  local lints=(
    "-W clippy::undocumented_unsafe_blocks"
    "-W clippy::multiple_unsafe_ops_per_block"
    "-W clippy::unwrap_used"
    "-W clippy::expect_used"
    "-W clippy::panic"
    "-W clippy::todo"
    "-W clippy::unimplemented"
    "-W clippy::unreachable"
    "-W clippy::indexing_slicing"
    "-W clippy::mem_forget"
    "-W clippy::as_conversions"
    "-W clippy::float_arithmetic"
  )
  # Only emit findings if clippy exits with warnings. Parse JSON output if possible.
  if ! cargo clippy --workspace --message-format=json -- "${lints[@]}" 2>/dev/null \
       | jq -r 'select(.reason == "compiler-message") | select(.message.level == "warning") | "\(.target.src_path):\(.message.spans[0].line_start):\(.message.code.code // "unknown"):\(.message.message)"' \
       > /tmp/clippy-out.txt 2>/dev/null; then
    return
  fi
  while IFS=: read -r path line code msg; do
    emit_finding "HLT-029-RUST-BAD-BEHAVIOR" "clippy_restriction_lints" "advisory" "$path" "$line" "$code" "$msg"
  done < /tmp/clippy-out.txt
}

# ---- Driver ----

main() {
  grep_unsafe_without_safety_comment
  grep_unwrap_on_external_input
  grep_box_leak
  grep_static_mut
  grep_mem_transmute
  grep_unsafe_impl_send_sync
  grep_hardcoded_secrets
  grep_secret_in_debug_impl
  cargo_deny_check
  check_cargo_lock_committed
  check_workflow_pinned_actions
  check_workflow_minimal_permissions
  check_workflow_no_nonblocking_security_steps
  check_no_force_push_in_workflows
  check_no_skip_hooks_in_scripts
  check_owner_map_coverage
  check_test_map_coverage
  check_changed_paths_have_targeted_tests
  check_input_boundary_tests
  check_proptest_density
  # NOTE: receipt-presence checks (`check_seven_receipts_present`,
  # `check_reviewer_agent_receipt_present`) intentionally do NOT run here.
  # The risk gate (`autobuilder gate`) owns the receipt-presence check;
  # duplicating it in the audit creates a meta-recursive bootstrap deadlock
  # (the audit's own output is the risk-gate receipt, so it cannot require
  # all 7 to already exist).
  grep_dangerous_comments
  # Clippy is expensive; only run if BAD_RUST_AUDIT_CLIPPY=1
  if [ "${BAD_RUST_AUDIT_CLIPPY:-0}" = "1" ]; then
    clippy_restriction_lints
  fi
}

main "$@"

# Aggregation runs unconditionally via the EXIT trap (emit_aggregate).
# Compute the exit code from the same source the trap will use.
BLOCKING=$(jq -s '[.[] | select(.severity == "blocking")] | length' "$FINDINGS_FILE" 2>/dev/null || echo 0)
if [ "${BLOCKING:-0}" -gt 0 ]; then
  exit 1
fi
exit 0
