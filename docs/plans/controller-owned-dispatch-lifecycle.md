---
doc_id: controller-owned-dispatch-lifecycle
status: proposed
last_verified: 2026-08-08
tracker_items:
  - actionq#2124
  - actionq#2114
  - actionq#2123
supersedes: null
---

# Controller-owned dispatch lifecycle and verified completion

## Decision summary

ActionQ must stop treating worker process exit as dispatch completion. A
provider-backed run is complete only after the ActionQ controller has bounded
the work phase, forced a synthesis-only finalization phase, validated a
machine-readable completion declaration against controller-observed evidence,
and sealed an immutable terminal result.

The first rollout is the qualified OpenCode scope-iterate pilot. The contracts
and controller machinery are harness-neutral, but Claude and Codex remain on
their existing paths until their adapters qualify equivalent finalization and
result behavior.

This plan responds to the lifecycle, read-only stall, and missing-final-memo
failures recorded by the devbox coordinator in
`docs/DISPATCH_OPERATIONS_FINDINGS.md` at commit `9f41333`. That source is not
present in this derived project worktree, so this document treats the
coordinator-provided findings as evidence without fabricating their contents.

## Current defect

The current daemon classifies child exit code zero as `completed` and, for a
non-published run, may settle the action with a synthetic `session=<id>` result
reference. The runner captures output and enforces process-group termination,
but it has no completion-result channel, controller-owned work/finalization
phase boundary, or consistency check between a worker's claim and the actual
workspace.

That leaves two unsafe assumptions:

1. a running model will eventually decide that it has enough evidence and
   change phase; and
2. a terminated model has produced a usable outcome.

Prompt pressure may improve average behavior but cannot make either assumption
an execution contract.

## Ownership

| Concern | Owner | Rule |
|---|---|---|
| Action, claim, lease, cancellation, phase lifecycle, budgets, settlement | ActionQ | The controller owns progression and authoritative terminal classification. |
| Work, investigation, edits, and attempted synthesis | Harness worker | Worker declarations are untrusted input until checked. |
| Process supervision, evidence capture, immutable result publication | ActionQ runner | The runner does not grant queue or acceptance authority to the worker. |
| Raw stdout, event, and tool evidence | outctl or equivalent instrumentation | Observation only; it never decides success. |
| Terminal evidence observation | Auditctl | Records immutable result identity and reason without becoming lifecycle authority. |
| Generic served composition | Vuoro | Exposes released owner operations and does not interpret completion. |

ActionQ's existing database terminal states remain `completed` and `failed`.
The richer dispatch outcome lives in the immutable result contract and does
not require a new queue state machine.

## `dispatch-result/v1`

The controller seals one canonical result artifact per authoritative attempt.
The finalizer's JSON declaration is input to this artifact, not the artifact
itself.

Required fields:

| Field | Meaning |
|---|---|
| `contract_id` | Literal `dispatch-result/v1`. |
| `action_id`, `attempt_id`, `dispatch_id` | Authoritative correlation identities. |
| `harness_profile`, `task_profile` | Qualified execution and lifecycle profiles. |
| `status` | `completed`, `blocked`, `no_change`, `failed`, or `budget_exhausted`. |
| `stop_reason`, `phase_at_stop` | Controller classification and final observed phase. |
| `summary`, `blockers`, `next_action` | Bounded worker synthesis after validation. |
| `changed_paths` | Controller-observed relative paths, never worker assertions. |
| `validation` | Registered command IDs, outcomes, and immutable evidence refs. |
| `process` | Exit code, signal, timeout, and escalation facts for work and finalizer. |
| `telemetry` | Phase durations, tool categories, read-only streak, and last mutation. |
| `evidence_refs` | Non-secret immutable references; never raw prompts or transcripts. |

Detailed `stop_reason` values include `completed`, `blocked`, `no_change`,
`launcher_failed`, `worker_crashed`, `implementation_not_started`,
`budget_exhausted`, `finalization_timeout`, `missing_completion_result`,
`inconsistent_completion_result`, `validation_failed`, `cancelled`, and
`claim_lost`.

Settlement mapping is fixed:

| Result status | ActionQ state | Rule |
|---|---|---|
| `completed` | `completed` | Valid result, required non-empty allowed diff for implementation, required gates pass. |
| `no_change` | `completed` | Must be declared explicitly and be consistent with the observed unchanged workspace. |
| `blocked` | `failed` | Result retains blockers and next action. |
| `failed` | `failed` | Result retains the precise stop reason. |
| `budget_exhausted` | `failed` | Final synthesis may exist, but exhaustion remains authoritative. |

Both successful and failed terminal transitions retain the immutable result
reference. `actionctl fail` therefore gains a result-reference input for
verified-lifecycle actions and stores it in the existing nullable
`actions.result_ref` column. No migration is required. A verified run never
uses `session=<id>` as a substitute result.

## Controller lifecycle and budgets

The initial state machine is:

```text
working -> finalizing -> terminal
```

The initial implementation profile is intentionally simple:

```yaml
implementation:
  working:
    max_wall: 20m
  finalizing:
    max_wall: 2m
  total:
    max_wall: 25m
```

The ActionQ and Sprintctl claim TTL must remain at least 30 minutes so the
controller has settlement margin without granting the worker more execution
time.

The controller transitions to `finalizing` after normal work exit, an explicit
completion indication, a recoverable worker crash with a known session, or the
work deadline. At the deadline it stops the work subprocess through the
existing TERM/grace/KILL process-group boundary before starting finalization;
work and finalization never execute concurrently. Total deadline expiry kills
all descendants and produces a deterministic terminal reason.

Cancellation and claim loss take precedence. Cancellation follows the fenced
acknowledgement path. Once claim renewal fails, the worker is terminated and
must not settle even a failure result.

## OpenCode finalization

The qualified OpenCode path runs work with `--format json`, captures the stable
session identity and normalized tool events, and then continues that same
session with `--session <id>` under a separately qualified finalizer profile.

The finalizer profile:

- exposes no read, edit, shell, web, Git, queue, Sprintctl, or deployment
  tools;
- does not require a writable workspace;
- instructs the model to use only evidence already present in the session;
- requires exactly one schema-conforming JSON declaration; and
- retains the contained worker identity without granting ingest or settlement
  credentials.

If work fails before a resumable session identity exists, the controller seals
a failed result from observed facts rather than inventing a worker declaration.
If finalization times out, returns malformed JSON, or omits the declaration,
the action fails with the corresponding stop reason.

OpenCode 1.18.4 currently advertises both `--format json` and `--session`.
Those observations are not qualification: the AgentOps harness profile must
add blocking probes for event shape, session extraction, continuation, and the
no-tools finalizer configuration.

## Completion gate

Process exit never passes the gate by itself. Completion requires:

1. a successful finalizer invocation or a controller-authored failure result
   where finalization is impossible;
2. a schema-valid, bounded declaration for worker-declared outcomes;
3. agreement between the declaration and controller-observed Git state;
4. an allowed-path diff for implementation `completed`;
5. explicit `no_change` for an unchanged implementation;
6. required registered validation commands passing; and
7. immutable result publication before ActionQ settlement.

Contradictory status, a claimed completion with no implementation diff,
out-of-scope changes, missing planning deliverables, or mismatched validation
claims produce `inconsistent_completion_result`. Cold deterministic gates and
candidate publication remain authoritative; worker prose is never evidence of
their success.

## Telemetry and later phase tuning

The OpenCode event parser records read, write, shell, and other tool counts;
the current consecutive read-only streak; last tool call; last observed
workspace mutation; and phase durations. Workspace consistency uses trusted
before/after Git evidence rather than tool names.

The first release measures read-only streaks but does not enforce a threshold.
After the pilot produces distributions, a later profile may split `working`
into `orient` and `execute` and use streaks to advance phases. The model may
advance earlier, but it will never own an unbounded phase.

## Failure and verification matrix

The required pathological workers are:

| Worker | Required terminal behavior |
|---|---|
| Reads forever | Work deadline, forced finalization, `budget_exhausted`. |
| Edits forever | Bounded stop, observed diff retained, deterministic failure or exhausted result. |
| Exits without result | Finalization runs; absent declaration becomes `missing_completion_result`. |
| Says complete with no diff | `inconsistent_completion_result`. |
| Produces diff but never summarizes | Forced finalization creates or fails the declaration deterministically. |
| Becomes legitimately blocked | Valid `blocked` result with blockers and next action. |
| Completes normally | Valid result, gates, immutable candidate/result, completed settlement. |

Tests also cover launcher failure, crash before and after session creation,
failed validation, out-of-scope mutation, finalization timeout, cancellation,
claim loss, explicit no-change, and finalization after work-budget exhaustion.

## Rollout and rollback

The behavior is opt-in through `completion_protocol = "verified-v1"`. Enable
it only for the qualified OpenCode scope-iterate pilot first. Wider OpenCode
pilot scaling is blocked until contract, daemon/runner, state-protocol,
pathological-worker, and one contained live smoke matrix pass.

Rollback disables the verified profile for new actions. Published results and
ActionQ events remain immutable; rollback must not reinterpret prior terminal
outcomes. Other harnesses retain legacy behavior until independently
qualified, and no prompt-only workaround is treated as closing this defect.

## Related plans

- `agentops/docs/plans/agentops/opencode-dispatch-lifecycle-hardening.md`
- `agentops/docs/plans/agentops/live-session-completion-alert-architecture.md`
- `agentops/docs/plans/agentops/live-session-completion-alert-plan.md`
- `vuoro/docs/architecture/portable-execution.md`
