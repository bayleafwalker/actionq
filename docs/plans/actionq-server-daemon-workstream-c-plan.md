---
doc_id: actionq-server-daemon-workstream-c
status: active
last_verified: 2026-07-18
---

# actionq server and devbox daemon plan

Workstream C of `/projects/dev/agentops/docs/plans/agentops/agent-ops-substrate-plan.md`. `/projects/dev/actionq` is the Postgres-backed queue and HTTP server. `/projects/dev/actionq-dispatch` is the separate compatibility repository that provides a working `dispatcher-once` implementation for one claimed action.

## Verified implementation checkpoint

Checked against both repositories on 2026-07-18:

- `actionq-server` v1 exists in `actionq` with health, sessions, dispatches,
  and contract-gated dispatch routes.
- `actionq-dispatch` provides only the bounded `dispatcher-once` coordinator;
  its 18-test suite passes.
- No `actionq-daemon` entrypoint, daemon/session registry, audit client,
  takeup client, or multi-harness routing implementation exists in either
  checked-out repository.

The ownership decision for backlog item #968 is therefore: build the new
daemon in `actionq`; retain `actionq-dispatch` unchanged as the supported
one-shot compatibility and rollback surface until daemon parity is proven.
Do not import the compatibility package as a permanent daemon dependency.
Reuse its command/config/ACL concepts through explicit migration or copied,
reviewed modules. Retirement requires a later, separately evidenced decision.

## Goal

Actionq gains a long-running devbox daemon that consumes the existing Postgres-backed actionq queue, supervises agent sessions across configured harnesses, records session liveness in actionq, and mirrors session lifecycle into sprintctl takeup and auditctl.

## Scope

What already exists and stays as the base:

- `actionq`: Postgres-backed queue with `actionctl`, CNPG deployment, append-only events, and lifecycle states `pending`, `claimed`, `completed`, `failed`, `rejected`, `cancelled`.
- CNPG cluster `actionq-cnpg-main` in the `vscode` namespace.
- `actionq-dispatcher`: `dispatcher-once`, config loading, action claiming through `actionctl`, scoped git worktree creation, `scope-iterate` handler, Claude local runner, fake commit runner, ACL validation, gates, and result recording.
- `ops/systemd/actionq-dispatcher.service`: shell loop around `dispatcher-once`.
- `actionctl sweep`: existing recovery path for expired claims.

Current deployment:

- Appservice has the actionq database at `/projects/dev/appservice/clusters/main/kubernetes/apps/actionq-db/`.
- That app creates CNPG cluster `actionq-cnpg-main` in namespace `vscode`.
- There is not yet an `actionq-server` Kubernetes app. C-minimum should continue to use `actionctl` against the existing queue.

What changes in this workstream:

- Add `actionq-daemon`, a real long-running entrypoint that owns the poll loop, signal handling, child process supervision, session registry, heartbeat emission, and graceful shutdown.
- Keep `dispatcher-once` as the manual/debug and cron-compatible execution path.
- Add multi-harness configuration and runner adapters for `claude`, `codex`, `copilot-cli`, and `codestral` through OpenCode.
- Add session lifecycle tracking: daemon session id, child PID, start/exit timestamps, heartbeat age, worktree path, branch, harness, model, and pause state.
- Add new actionq coordinator event types for session lifecycle and liveness.
- Call `sprintctl takeup take` when a remote-mode session starts and `sprintctl takeup release` when it ends.
- Emit auditctl events for dispatch, session start, session heartbeat anomaly if useful, pause/resume, and session exit.
- Preserve fake-runner support so daemon behavior can be tested without invoking real model CLIs.

What does not change in this workstream:

- No new queue database. The existing actionq Postgres schema remains the coordinator store.
- No new actionq-server API in the minimum step.
- No smart LLM scheduler. Routing is deterministic config and action metadata.
- No sprintctl heartbeat or TTL semantics. Sprintctl takeup remains an opaque event pair.
- No automatic merge, push, deploy, or PR workflow beyond whatever a future action handler explicitly owns.

## Architecture clarification

`actionq-server` maps to the existing `actionq` installation:

- Queue state is in the existing actionq Postgres tables.
- Durable lifecycle and coordinator facts are appended through `actionctl emit`.
- Queue inspection, enqueue, claim, complete, fail, reject, cancel, sweep, and events remain the public interface.
- CNPG remains the operational database layer. No sqlite scheduling layer is introduced.

`actionq-daemon` starts from the existing `actionq-dispatcher` behavior but should land as an `actionq`-owned daemon:

- It runs on the devbox because the devbox has the shared `/projects/dev` mount, direnv project environments, model CLIs, and persistent user-local state.
- It claims actions by invoking `actionctl claim` in a loop.
- For each claimed action, it constructs a session record, prepares the same worktree/gates/prompt path used today, starts the selected harness as a child process, emits heartbeats while the child is alive, validates output on exit, and records the action outcome.
- It replaces the current systemd shell loop as the primary service. The existing `dispatcher-once` path remains valid for manual runs and as a cron/systemd backstop.

The minimum topology is:

```
CNPG/actionq in cluster
  ^ actionctl claim/emit/complete/fail/reject
  |
devbox actionq-daemon
  |- harness child process: claude | codex | copilot-cli | opencode
  |- sprintctl takeup take/release, remote mode only
  |- auditctl add, best-effort but visible on failure
  `- worktrees under ~/.local/state/actionq/worktrees
```

## New event types in actionq

Actionq remains the session liveness source. New facts are coordinator events emitted through `actionctl emit`; they do not require direct SQL writes by the daemon.

### `session.dispatch`

Emitted after an action is claimed and routed, before worktree/session start.

Payload:

```json
{
  "session_id": "aqs:<ulid-or-uuid>",
  "runtime_session_id": "aqs:<ulid-or-uuid>",
  "daemon_id": "devbox:<hostname>:<pid>:<boot_uuid>",
  "action_id": 123,
  "action_type": "scope-iterate",
  "project": "sprintctl",
  "target_ref": "42",
  "harness": "claude",
  "model": "claude-sonnet-4-6",
  "routing_source": "action-explicit|project-default|action-kind-default",
  "worktree": "/home/dev/.local/state/actionq/worktrees/sprintctl/123",
  "branch": "agent/scope-iterate/123",
  "ttl_seconds": 120,
  "claim": {
    "claim_id": 456,
    "work_item_id": 42,
    "claim_type": "execute"
  }
}
```

### `session.started`

Emitted after the harness child process starts.

Payload:

```json
{
  "session_id": "aqs:<id>",
  "runtime_session_id": "aqs:<id>",
  "pid": 12345,
  "started_at": "2026-04-26T12:34:56Z",
  "harness": "claude",
  "model": "claude-sonnet-4-6",
  "ttl_seconds": 120,
  "claim": {
    "claim_id": 456,
    "work_item_id": 42,
    "claim_type": "execute"
  },
  "sprint_takeup": {
    "attempted": true,
    "enabled": true,
    "status": "ok|failed|skipped",
    "event_id": 987,
    "error": null
  }
}
```

### `session.heartbeat`

Emitted every `heartbeat_interval_seconds` while a child process is still alive. Heartbeats are action-scoped events so `actionctl show` reconstructs liveness history for a session.

Payload:

```json
{
  "session_id": "aqs:<id>",
  "runtime_session_id": "aqs:<id>",
  "pid": 12345,
  "monotonic_age_seconds": 120,
  "status": "running",
  "worktree": "/path/to/worktree",
  "ttl_seconds": 120,
  "claim": {
    "claim_id": 456,
    "work_item_id": 42,
    "claim_type": "execute"
  }
}
```

### `session.paused`

Emitted when dispatch is intentionally paused or when a usage-limit pause is detected and the daemon records a stop point.

Payload:

```json
{
  "session_id": "aqs:<id>",
  "runtime_session_id": "aqs:<id>",
  "reason": "operator|usage-limit|shutdown|unsupported-harness",
  "mechanism": "checkpoint-and-fail|sigstop|native",
  "handoff_ref": "/path/to/handoff.md",
  "resumable": false,
  "ttl_seconds": 120,
  "claim": {
    "claim_id": 456,
    "work_item_id": 42,
    "claim_type": "execute"
  }
}
```

### `session.resumed`

Emitted when a later daemon session is launched from a prior handoff context.

Payload:

```json
{
  "session_id": "aqs:<new-id>",
  "resumed_from_session_id": "aqs:<old-id>",
  "handoff_ref": "/path/to/handoff.md",
  "mechanism": "redispatch"
}
```

### `session.exited`

Emitted after the child exits and before final action outcome is recorded.

Payload:

```json
{
  "session_id": "aqs:<id>",
  "runtime_session_id": "aqs:<id>",
  "pid": 12345,
  "exit_code": 0,
  "duration_seconds": 1800,
  "outcome": "completed|failed|rejected|cancelled|shutdown",
  "result_ref": "branch=agent/scope-iterate/123",
  "failure_reason": null,
  "audit_status": "ok|failed|skipped",
  "ttl_seconds": 120,
  "claim": {
    "claim_id": 456,
    "work_item_id": 42,
    "claim_type": "execute"
  },
  "sprint_release": {
    "attempted": true,
    "status": "ok|failed|skipped",
    "event_id": 988,
    "error": null
  }
}
```

Keep existing `coordinator_cycle` and `coordinator_paused` events for compatibility with `dispatcher-once` and current observability. The daemon may emit `coordinator_cycle` once per loop iteration as a coarse heartbeat, but session liveness must use `session.heartbeat`.

## New actionq-daemon features

### Config schema additions

Keep the existing `[global]`, `[projects.<name>]`, and `[actions.<type>]` structure. Add daemon, harness, routing, sprintctl takeup, and audit settings.

```toml
[global]
poll_interval_seconds = 30
heartbeat_interval_seconds = 60
graceful_shutdown_seconds = 30
session_state_path = "~/.local/state/actionq/sessions.json"
daemon_id = "devbox-{hostname}"
actionctl_bin = "actionctl"
sprintctl_bin = "sprintctl"
auditctl_bin = "auditctl"

[global.sprintctl_takeup]
enabled = true
remote_only = true
actor_prefix = "actionq"
release_on_sprintctl_error = false

[global.audit]
enabled = true
fail_action_on_emit_error = false

[harnesses.claude]
bin = "claude"
kind = "claude"
default_model = "claude-sonnet-4-6"

[harnesses.codex]
bin = "codex"
kind = "codex"
default_model = "gpt-5.3-codex"

[harnesses.copilot-cli]
bin = "gh"
kind = "copilot-cli"
default_model = "default"

[harnesses.codestral]
bin = "opencode"
kind = "opencode"
default_model = "mistral/codestral-latest"

[projects.sprintctl]
path = "/projects/dev/sprintctl"
base_ref = "HEAD"
default_harness = "claude"
default_model = "claude-sonnet-4-6"
env = { SPRINTCTL_BACKEND = "remote", SPRINTCTL_URL = "postgresql://...", KCTL_DB = "/projects/dev/sprintctl/.kctl/kctl.db" }

[actions.scope-iterate]
default_harness = "claude"
model = "claude-sonnet-4-6"
runner = "local"
prompt_template = "/projects/dev/actionq-dispatch/prompts/scope-iterate.md"
tool_acl = "/projects/dev/actionq-dispatch/acls/scope-iterate.json"
test_command = "pytest"
```

Compatibility rule: existing configs with `runner = "local"` and `claude_bin` continue to mean `harness = "claude"` unless an explicit harness is set.

Compatibility path rule: existing operator configs under `~/.config/actionq-dispatcher/config.toml` remain valid. The new preferred target path is `~/.config/actionq/config.toml`, but the daemon should check the old path before failing when no explicit `--config` is provided.

Action metadata may override routing. The actionq CLI should grow enqueue flags:

```bash
actionctl add --type scope-iterate --project sprintctl --target 42 \
  --harness claude --model claude-sonnet-4-6
```

If actionq does not yet have first-class `harness`/`model` columns or add flags, store these in the action payload/metadata field once one exists. Until then, C-minimum can rely on dispatcher config defaults and treat enqueue-time overrides as the next actionq CLI extension.

### Code modules to add or split

Daemon-owned modules land directly in `/projects/dev/actionq/actionq/`.
`actionq-dispatch/actionq_dispatcher/` remains the one-shot compatibility
surface during migration; it must not become a second daemon authority.

- `actionq/daemon.py`: poll loop, signal handling, session registry, graceful shutdown.
- `actionq/session.py`: `SessionRecord`, session ids, state file read/write, heartbeat payloads.
- `actionq/routing.py`: deterministic harness/model resolution from action metadata, project defaults, and action defaults.
- `actionq/harnesses/base.py`: common adapter interface.
- `actionq/harnesses/claude.py`: Claude invocation ported from the compatibility worker.
- `actionq/harnesses/codex.py`: Codex CLI adapter.
- `actionq/harnesses/copilot.py`: Copilot CLI adapter.
- `actionq/harnesses/opencode.py`: OpenCode/Codestral adapter.
- `actionq/lifecycle.py`: actionq lifecycle event helpers and failure-safe emit wrappers.
- `actionq/audit.py`: auditctl client wrapper.
- `actionq/takeup.py`: sprintctl takeup take/release wrapper.

Concepts to port from the compatibility repository, without importing it as
a runtime dependency:

- `core.py` action-handler and validation flow.
- `clients.py` subprocess boundary and error mapping.
- `worker.py` fake-runner and Claude invocation behaviour.

## Daemon evolution

The daemon is not a shell loop around `dispatcher-once`; it is a Python process that owns child process lifecycle.

Loop architecture:

1. Load config once at startup. Validate action configs, project paths, harness binaries, and pause file path.
2. Generate `daemon_id` and write a small state file with PID, hostname, started_at, and config path.
3. Install signal handlers for `SIGTERM`, `SIGINT`, and `SIGHUP`.
4. On each loop:
   - If pause file exists, emit `coordinator_paused`, sleep `poll_interval_seconds`, and do not claim.
   - Call `actionctl claim --worker <daemon_id> --timeout <claim_timeout>`.
   - If no action, emit or skip coarse `coordinator_cycle` per config, sleep, and loop.
   - Resolve action handler, project, harness, model, worktree, branch, and prompt.
   - Emit `session.dispatch`.
   - Run existing pre-gates.
   - Create worktree and sprintctl item claim where the action handler requires it.
   - Call `sprintctl takeup take` if configured and the project is in remote mode.
   - Start harness child process.
   - Emit `session.started`.
   - While child is running, emit `session.heartbeat` every `heartbeat_interval_seconds`; persist session state after each heartbeat.
   - On child exit, emit `session.exited`, run post-gates, complete/fail/reject the action, release sprintctl takeup, and update audit.
   - Sleep only after the action is fully settled.

Concurrency: C-minimum is one active child session per daemon. That matches the current one-action-at-a-time safety model and keeps worktree, budget, claim, and takeup semantics simple. Parallel sessions can be added later by turning the active session slot into a bounded worker pool, but no implementation step should require that.

Signal handling:

- `SIGTERM`/`SIGINT` sets a shutdown flag.
- If no child is running, the daemon exits 0 after flushing state.
- If a child is running, emit `session.paused` with `reason="shutdown"` and wait up to `graceful_shutdown_seconds`.
- If the child exits during the grace window, settle normally.
- If the child remains alive, terminate the child process group, emit `session.exited` with `outcome="shutdown"`, call `actionctl fail` with a shutdown reason, release sprintctl takeup best-effort, and leave the worktree in place.
- `SIGHUP` reloads config only when no child is active. During an active child, record a reload-pending flag and reload before the next claim.

The current systemd unit should evolve from:

```ini
ExecStart=/bin/sh -c 'while true; do dispatcher-once; sleep 30; done'
```

to:

```ini
ExecStart=%h/.local/bin/actionq-daemon --config %h/.config/actionq/config.toml
Restart=always
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=90
```

Keep `dispatcher-once` installed and documented for manual smoke/debug.

## Wire protocol answer

C-minimum uses poll-based dispatch over the existing `actionctl` CLI:

- The daemon calls `actionctl claim --worker <daemon_id> --timeout <minutes>` every `poll_interval_seconds`.
- It emits all lifecycle, heartbeat, and pause/resume facts through `actionctl emit`.
- It records final state through `actionctl complete`, `actionctl fail`, or `actionctl reject`.
- `actionctl sweep` remains the requeue mechanism for expired claims and should continue to run every five minutes through the existing timer/cron path.

This is sufficient for the current system because:

- Claiming is already atomic in Postgres.
- The system has one primary devbox daemon.
- The default 30-second poll interval is operationally acceptable.
- It preserves `actionctl` as the only queue contract.
- It allows daemon-only-on-devbox to ship before any actionq cluster API or scheduler service exists.

Event-driven dispatch with Postgres `LISTEN/NOTIFY` is deferred. Add it only after poll latency or needless wakeups become real pain. When added, it should be an optimization: the daemon listens for `action_enqueued`, wakes immediately, and still uses the same `actionctl claim` call. LISTEN/NOTIFY must not become a separate scheduling authority.

No separate scheduling API is required for Workstream C. Future scheduler/policy features should first try to fit as actionq fields and coordinator events; only build a new service when there is a second client that cannot reasonably use `actionctl`.

## Pause/resume answer

No supported harness currently provides a proven, uniform graceful pause/resume contract that the daemon can rely on for C-minimum. Treat native pause as unsupported until verified per CLI version and documented with tests.

Minimum viable mechanism:

- Operator pause before claim: pause file prevents new claims, as today.
- Usage-limit or runtime pause during a session: the daemon records `session.paused`, asks the harness to produce or preserve handoff context if possible, terminates or fails the current action, leaves the worktree in place, and relies on a new action or operator re-dispatch with the handoff reference.
- Resume means re-dispatch, not process continuation. A new session starts with the previous handoff/worktree context and emits `session.resumed`.

Harness-specific C-minimum behavior:

- `claude`: do not assume graceful process pause. If the Claude CLI exits with a usage-limit/rate-limit signal, capture stdout/stderr and any session transcript, write `handoff.md` under the worktree or session state directory, emit `session.paused` with `mechanism="checkpoint-and-fail"`, fail the action with a retryable reason, and leave the worktree. If no handoff can be generated, record the reason and fail.
- `codex`: same minimum as Claude. Do not assume a stable pause protocol. Preserve transcript/log output and worktree state; re-dispatch from handoff context.
- `opencode`/`codestral`: same minimum. If OpenCode exposes a resume/session id in the installed version, adapter support can be added behind the harness interface, but C-minimum does not depend on it.
- `copilot-cli`: same minimum. Treat it as non-resumable unless the CLI provides a documented continuation mechanism.

SIGSTOP/SIGCONT is not the C-minimum pause mechanism. It may freeze a process, but it does not solve usage-limit recovery, pod restarts, durable handoff, or operator-visible state. It can be considered later for explicit operator pause on a local devbox, behind `mechanism="sigstop"`, but not as the main architecture.

## Daemon failure answer

If the daemon dies mid-session:

- The action remains `claimed` in actionq until its `claim_deadline`.
- The harness child may die with the daemon or may be orphaned depending on process group handling. The daemon must spawn children in its own process group and clean them on normal shutdown; crash recovery assumes the child is gone or operator-killed.
- The worktree and branch remain on disk under `worktree_root`.
- `actionctl sweep`, run every five minutes, returns timed-out claims to `pending` by clearing claim ownership and writing `claim_timed_out`.
- The sprintctl takeup event remains active because the daemon did not release it.

Restart procedure:

1. Systemd restarts `actionq-daemon`.
2. On startup, daemon reads `session_state_path` and checks for active sessions whose PIDs no longer exist.
3. For each dead session, emit a best-effort `session.exited` with `outcome="daemon-recovered"` or `session.paused` with `reason="daemon-crash"` if no prior exit was recorded.
4. Do not complete/fail/reject a still-claimed action during startup unless the daemon can prove it owns the live child outcome. Let `actionctl sweep` requeue the expired claim.
5. If the worktree contains useful unmerged changes or logs, write or preserve a handoff file at `<worktree>/.actionq/handoff.md` and include that path in the recovery event.
6. After sweep requeues the action, a new daemon claim may re-dispatch it. The handler must reject or adapt if the expected branch/worktree already exists. The preferred behavior is to detect the prior worktree, attach the handoff reference, and create a new branch/worktree suffix such as `agent/scope-iterate/<action-id>-retry-1`.

Sprintctl cleanup:

- Best effort automatic cleanup is attempted on daemon restart only if the stored session state contains enough data: project path, sprint id, actor, instance id, and remote-mode env. The daemon calls `sprintctl takeup release --reason daemon-recovered`.
- If automatic cleanup fails, the daemon emits an audit event and actionq event that names the stale takeup. Operator cleanup is then explicit:

```bash
direnv exec /projects/dev/<repo> sprintctl takeup release \
  --sprint-id <id> --actor actionq:<session_id> --instance-id <session_id> \
  --reason daemon-crash-recovery
```

This is acceptable because sprintctl takeup is an opaque signal, not a lock with TTL semantics. Stale takeups should be visible in cockpit/operator render, not silently hidden.

## Actionq-cluster storage answer

All coordinator and scheduling state for Workstream C uses the existing actionq Postgres tables:

- Current action state remains in `actions`.
- Session lifecycle and liveness are append-only rows in `events` emitted by `actionctl emit`.
- No sqlite scheduler state is added.
- No second daemon-owned database is added.

The daemon may keep a local JSON state file for crash recovery of active child sessions. That file is not authoritative queue state; it is an operational recovery cache. If it is missing, actionq plus the worktree on disk remain the source for what should happen next.

If actionq needs additional first-class fields for routing (`harness`, `model`, metadata), add them to actionq Postgres migrations and expose them through `actionctl add`, `claim`, and `show`. Do not create a separate policy database.

## Harness routing answer

Routing is deterministic with explicit metadata taking precedence. Order:

1. Per-action explicit harness/model set at enqueue time.
2. Per-project default harness/model from `[projects.<name>]`.
3. Per-action-kind default harness/model from `[actions.<type>]`.
4. Global fallback harness/model from `[harnesses.<name>].default_model` only if exactly one default harness is configured.
5. Otherwise reject the action with validator `harness-routing`.

No smart routing in C-minimum. The daemon should not infer provider choice from token cost, queue depth, or file patterns until real dispatch history exists. Those can become deterministic policy rules later, but the first shippable system needs explainable routing.

Recommended actionq CLI extension:

```bash
actionctl add \
  --type scope-iterate \
  --project sprintctl \
  --target 42 \
  --harness claude \
  --model claude-sonnet-4-6 \
  --created-by human:cockpit
```

Claim output should include `harness` and `model` when present. Until that lands, actionq-daemon should support config-only routing and record `routing_source="project-default"` or `routing_source="action-kind-default"` in `session.dispatch`.

## Sprintctl takeup integration

Sprintctl takeup is a side effect of starting and ending a daemon-owned session. It is not used to claim work or determine liveness.

When to call:

- After action pre-gates pass and worktree/session context exists.
- Before starting the harness child process.
- Only when `[global.sprintctl_takeup].enabled = true`.
- Only when the project is remote mode if `remote_only = true`.
- Release after child exit and final action settlement attempt, even if validation fails.

Take command shape:

```bash
direnv exec /projects/dev/<repo> sprintctl takeup take \
  --sprint-id <sprint-id> \
  --actor actionq:<session_id> \
  --actor-kind agent \
  --instance-id <session_id> \
  --runtime-session-id <session_id> \
  --context "actionq action <action_id> <action_type> via <harness>/<model>" \
  --json
```

Release command shape:

```bash
direnv exec /projects/dev/<repo> sprintctl takeup release \
  --sprint-id <sprint-id> \
  --actor actionq:<session_id> \
  --actor-kind agent \
  --instance-id <session_id> \
  --runtime-session-id <session_id> \
  --reason "<completed|failed|rejected|shutdown>" \
  --json
```

Environment:

- Always run through `direnv exec <project.path>` so `.envrc` loads.
- Project config may pass explicit `env` values for `SPRINTCTL_BACKEND=remote`, `SPRINTCTL_URL`, `SPRINTCTL_DB`, or other repo-local settings.
- The daemon should detect remote mode from project env or `direnv exec <repo> sprintctl config/show` once sprintctl exposes a stable command. Until then, `remote_only=true` means require `SPRINTCTL_BACKEND=remote` in project env; if absent, skip with `status="skipped"`.

Error handling:

- If takeup fails before the harness starts, default behavior is to fail the action before doing model work. This prevents sessions that are invisible to cockpit takeup.
- Config may allow `release_on_sprintctl_error = false` only for smoke/local testing.
- If release fails after the session exits, do not change a completed action to failed. Emit `session.exited` with `sprint_release.status="failed"`, emit an audit event, and leave operator cleanup instructions.

Existing item-level sprintctl claim behavior for `scope-iterate` remains separate. The daemon may hold both an item claim and a sprint takeup. The item claim is work-item coordination; takeup is session presence.

## Auditctl integration

Audit events are emitted through the `auditctl` binary, not by direct sqlite or NDJSON writes.

Command shape:

```bash
direnv exec /projects/dev/<repo> auditctl add \
  --type session.start \
  --source actionq-daemon \
  --actor actionq:<session_id> \
  --summary "actionq session started: <action_type> #<action_id>" \
  --ref "wi:<target_ref>" \
  --ref "sprint:<sprint-id>" \
  --metadata '{"session_id":"<session_id>","harness":"<harness>","model":"<model>","action_id":<action_id>}' \
  --detail "<json-or-markdown-detail>"
```

Valid ref prefixes in auditctl are `wi:`, `ka:`, `ad:`, `sha:`, `pr:`, `sprint:` — validated at insert time. Action and session identifiers (`action_id`, `session_id`) belong in `--metadata`, not `--ref`. The `wi:` ref is only emitted when `target_ref` on the action corresponds to a work item; omit it otherwise. Include `sprint:<sprint-id>` when the sprint context is known from the project.

Auditctl event types emitted by the daemon (these differ in naming from actionq coordinator events, which use `session.dispatch`, `session.started`, etc.):

- `dispatch.queued`: after action is claimed and routed, before worktree or harness setup.
- `dispatch.started`: worktree ready and harness selected, before child process starts.
- `session.start`: after harness child PID exists.
- `session.pause`: when pause/re-dispatch handoff is recorded.
- `session.resume`: when a session starts from a prior handoff context.
- `session.exit`: after child exit and validation outcome is known.
- `pr.open`: after session exits completed, when `gh pr view` confirms a PR exists for the session branch (step 6).
- `pr.merge`: when the PR state is confirmed `MERGED` at exit time or on a subsequent observer pass (step 6).

Error handling:

- Audit emission is best-effort by default: `fail_action_on_emit_error = false`.
- Every audit failure is mirrored into actionq event payload fields such as `audit_status="failed"` and `audit_error`.
- For regulated or stricter repos, config may set `fail_action_on_emit_error = true`, in which case failure to emit `dispatch.queued` or `session.start` rejects/fails before model work begins. Do not enable that for the first devbox rollout.

Audit details should be concise and machine-readable enough for cockpit aggregation: include action id, session id, harness, model, worktree, branch, result, and failure reason.

## Tier-1 context-candidates integration (item #1116, implemented)

Depends on sprintctl #1160's `context-candidates --json` contract
(`sprintctl/docs/reference/context-and-handoff.md`) and on
`sprintctl/docs/ops-upgrade-plan.md` Tier 1 / `agentops`'
`docs/plans/agentops/session-mechanization-plan.md` Tier 1: at session start
the daemon requests a small, bounded, deterministically ranked context packet
and only ever auto-claims for a found, `claim_eligible` explicit target.

When to call:

- Before `_start_child` -- this is earlier than sprintctl takeup, which needs
  the child's PID and so can only run after the child starts. A context/claim
  failure here means the child never starts at all.
- Only when `[global.context].enabled = true`.
- Only when the project is remote mode if `remote_only = true` (same
  project-env convention as takeup: require `SPRINTCTL_BACKEND=remote`).

Fetch command shape:

```bash
sprintctl context-candidates --sprint-id <sprint-id> --item-id <target_ref> --limit <limit> --json
```

`--item-id` is only passed when the dispatched action's `target_ref` parses
as an integer; sprintctl reports `explicit_target: null` in the packet when
no `--item-id` was supplied at all, and `{"item_id": N, "found": false}` when
that item does not exist.

Pre-start claim command shape (only for a found, `claim_eligible` explicit
target -- rank 1 only, never an advisory rank 2-5 candidate):

```bash
sprintctl claim start --item-id <item_id> --actor actionq:<session_id> --ttl <ttl_seconds> --json
```

Error handling:

- The context-candidates fetch itself is always best-effort/fail-open: a
  failure (sprintctl down, unreachable, non-zero exit) yields
  `context={"status": "failed", "error": ...}` in the `session.dispatch`
  payload and never blocks or fails the action by itself.
- Claim acquisition fails closed: it is only ever attempted for a
  successfully fetched packet whose explicit target sprintctl itself marked
  `claim_eligible`, and if that attempt raises, the action fails
  (`context claim acquisition failed before session start: <error>`) before
  any child process is started -- there is no kill-already-started-child step
  to run, unlike the takeup pre-start failure path.
- The claim token is never included in any event payload or log; only
  `claim_id` (when available) is recorded, mirroring sprintctl's own
  "claim secrets are never included" bundle rule.
- `[global.context].auto_claim = false` disables the claim step entirely
  while still fetching and injecting the advisory packet -- useful for a
  rollout phase that wants context without automatic claim behavior.

Both the packet (`context`) and the claim outcome (`context_claim`) are
folded into the `session.dispatch` event payload alongside `audit_dispatch`,
so a consumer building the harness prompt has the ranked candidates and
watermark age available before the child starts.

## Test plan

Fake-runner tests that can ship before real harness adapters:

- `actionq-daemon` starts, polls once or continuously, claims a fake action, emits `session.dispatch`, `session.started`, at least one `session.heartbeat` with a short interval, `session.exited`, and completes the action.
- Pause file prevents claims and emits `coordinator_paused`.
- `SIGTERM` with no active child exits cleanly.
- `SIGTERM` with a fake long-running child emits shutdown lifecycle events and fails or settles the action according to the configured grace period.
- Routing picks action explicit over project default over action-kind default.
- Missing harness rejects with validator `harness-routing`.
- Sprintctl takeup commands are invoked when project env says `SPRINTCTL_BACKEND=remote`; skipped when local or disabled.
- Sprintctl takeup failure before child start fails the action and does not invoke the harness.
- Sprintctl release failure after child exit records actionq/audit failure metadata but does not erase the original action outcome.
- Auditctl client emits expected commands and degrades according to `fail_action_on_emit_error`.
- Startup recovery reads a stale session state file, sees missing PID, emits recovery event, and leaves the action for sweep.
- Existing `dispatcher-once` tests continue to pass.

Integration tests with existing fake worker and Postgres:

- Use `ACTIONQ_TEST_URL` or the smoke CNPG schema.
- Enqueue `scope-iterate`, run daemon with `runner = "fake"` and short heartbeat interval.
- Verify `actionctl show` contains completed action and session events.
- Verify worktree/branch exists and validation passes.
- Verify `actionctl sweep` requeues an intentionally expired claim after a simulated daemon crash.

Real harness tests:

- Claude disposable action in a throwaway repo/work item: verifies ACL args, process supervision, transcript capture, and post-gates.
- Codex disposable action: verifies command adapter, model argument mapping, and output capture.
- OpenCode/Codestral disposable read-only action first, write action second.
- Copilot CLI disposable action only after CLI auth and noninteractive behavior are confirmed.
- Usage-limit simulation per harness by forcing a command wrapper to exit with known rate-limit text; assert handoff/fail/re-dispatch behavior.

Manual operational checks:

- `systemctl --user start actionq-daemon.service` launches `actionq-daemon`.
- `systemctl --user stop actionq-daemon.service` releases takeup or emits cleanup instructions.
- `actionctl events --type session.heartbeat --limit 5` shows current liveness.
- `sprintctl takeup list --json` shows active takeup while a real session runs and none after release.
- `auditctl list --type session.start --limit 5` shows session lifecycle artifacts.

## Implementation order

Each step is shippable. The checked-out source does not contain steps 1–5;
all five are pending. The shipped `actionq-server` v1 baseline is independent
of these daemon steps.

1. **Daemon minimum on devbox — pending (#969).** Add the `actionq-daemon` entrypoint, Python poll loop, signal handling (SIGTERM/SIGINT/SIGHUP), session ids, state file, session lifecycle events (dispatch/started/heartbeat/paused/exited), and fake-runner support. Keep one active session at a time and add the long-running systemd unit.

2. **Session events and recovery hardening — pending (#971/#1115).** Add `session.heartbeat`, `session.exited`, stale-state startup recovery, and daemon-crash plus `actionctl sweep` tests. Preserve existing `dispatcher-once` behavior.

3. **Sprintctl takeup side effects — pending (#972).** Add a bounded sprintctl subprocess client, `[global.sprintctl_takeup]` config, remote-mode detection, lifecycle wiring, and fake-client tests.

4. **Auditctl publisher — core lifecycle — pending (#973; auditctl #965).** Add an auditctl subprocess client, `[global.audit]` config, lifecycle call sites, visible degradation fields, and fake-client coverage. PR events remain step 6.

5. **Harness routing config — pending (#970).** Add the adapter/config model and deterministic priority chain after the fake-runner daemon minimum. Preserve `runner="local"` compatibility and reject unresolved routing explicitly.

6. **Actionq-daemon publisher integration — full event set.** Complete the auditctl event coverage specified in the auditctl Workstream D plan. After a session exits with `outcome=completed`, query `gh pr view --json number,state,headRefName` against the session's branch in the project path; emit `pr.open` as an auditctl event if a PR exists for the branch. Emit `pr.merge` if the PR state is `MERGED` at exit time. For sessions without a PR at exit, emit nothing for pr events. This validates the full actionq-daemon publisher set from Workstream D: `dispatch.queued`, `dispatch.started`, `session.start`, `session.pause`, `session.resume`, `session.exit`, `pr.open`, `pr.merge`. Depends on steps 4 and 5.

7. **Claude harness adapter extraction.** Move current Claude invocation from `daemon._start_harness()` behind the common harness adapter interface while keeping behavior equivalent. Verify one real Claude disposable action.

8. **Codex and OpenCode/Codestral adapters.** Add noninteractive command mapping, env handling, transcript capture, timeout handling, and fakeable tests. Real smoke each adapter with a disposable low-risk action.

9. **Copilot CLI adapter.** Add only after confirming the installed CLI has a usable noninteractive mode and auth path in devbox. If it does not, keep the harness configured as unsupported and reject with a clear message.

10. **Actionq routing metadata extension.** Add first-class `--harness` and `--model` support to `actionctl add`, claim output, and action display. Until this step lands, routing is config-only.

11. **Pause/resume C-minimum.** Implement usage-limit detection as checkpoint-and-fail with `session.paused`, handoff file, and re-dispatch context. Do not claim native pause support until a harness-specific implementation is proven.

12. **Optional LISTEN/NOTIFY wakeup.** Add only if poll latency is annoying. It must wake the same claim loop and not replace `actionctl claim`.

At the end of step 4, the daemon emits core session lifecycle events into auditctl. At the end of step 6, the daemon satisfies the full publisher spec from the auditctl Workstream D plan. Steps 7 onward widen harness support and routing without changing the core wire protocol.
