---
doc_id: actionq.action-lifecycle
status: draft
supersedes: null
---

# Action lifecycle protocol

## Boundary

An action is the smallest state object. The authoritative mutable projection is one row in `actions`; `events` is the append-only lifecycle history. Both are owned by actionq and written through `actionctl`/`actionq.db` transactions.

## States and transitions

| Operation | Preconditions | Durable success effect | Failure effect |
|---|---|---|---|
| `enqueue` | Valid parent/depth and producer below rate limit | Insert `pending` action and `action_enqueued` event | Transaction rolls back |
| `claim` | At least one `pending` action | Selected action becomes `claimed`; owner/deadline set; `action_claimed` appended | No action changes |
| `renew` | Action `claimed` by exactly the requesting worker, deadline not yet passed | `claim_deadline` extended from now; `claim_renewed` appended | No action changes; a durable `claim_renewal_rejected` event is appended instead (see below) |
| `complete` | Action is `claimed` by matching receipt and its lease is live | Status `completed`, result and timestamp stored, claim metadata cleared, event appended | No action changes |
| `fail` / `reject` | Action is `claimed` by matching receipt and its lease is live | Terminal status and reason stored, claim metadata cleared, event appended | No action changes |
| `cancel` | `pending` or `claimed` | Pending becomes `cancelled`; claimed becomes `cancelling` with receipt/lease revoked and a bounded stop deadline | No action changes |
| `cancel acknowledge` | Matching cancellation request, former runner identity, and revoked receipt proof on `cancelling` | Status `cancelled`, claim metadata cleared, stop acknowledgement recorded, event appended | No action changes |
| `sweep` | Status is `claimed` and deadline is in the past | Status returns to `pending`; claim metadata cleared; timeout event appended | Transaction rolls back |

## Claim/lease authority commands (work item #1117)

`claim`, `renew`, and the timeout/conflict determination `sweep` authors are
authority commands and remote decisions per
`sprintctl/docs/plans/adr-outbox-sync-model.md` and
`agentops/docs/plans/agentops/state-event-command-matrix.md`: no offline or
optimistic local grant exists, and a stale or invalid command must remain
visible as an immutable request plus a rejection decision rather than
silently mutating state or silently succeeding.

- A `renew` command is granted only when the action is `claimed` by
  exactly the requesting worker and its `claim_deadline` has not already
  passed; the grant is the `claim_renewed` event.
- Any other `renew` attempt -- wrong worker, expired lease, wrong status,
  or an unknown action id -- is rejected. The rejection is a durable
  `claim_renewal_rejected` event recording the requester, the requested
  timeout, and the actual state found; the action row is left completely
  unchanged.
- Reduced state remains visible through the existing read surfaces
  (`show`, `ls`, `events`): a claim's validity is `claim_deadline` versus
  `now()`, and its request/decision history is the ordered event stream
  for that action id -- no new read endpoint or separate command-request
  ledger was added (out of scope per work item #1117: a universal event
  store, and delegating actionq leases to sprintctl).

## Linearization and atomicity

- Enqueue, claim, terminal transitions, and each sweep batch take effect at their Postgres transaction commit.
- Claim selection uses `FOR UPDATE SKIP LOCKED` inside the update transaction. Competing completed claim calls should not return the same pending action.
- Action projection and corresponding lifecycle event are intended to commit atomically.
- A lost response after commit is an unknown outcome to the caller. The caller must read the action and event history before retrying a non-idempotent command.

## Served invocation provenance and retry

Vuoro execution mutations pass through `ActionQApplication` with authenticated
actor/environment identity, a request ID, catalog and optional basis
revisions, and a required idempotency key. Actionq serializes the
identity/environment/operation/key tuple with a transaction-scoped advisory
lock. The immutable history links `invocation.requested`, the owner lifecycle
events, and `invocation.decided` through the same provenance record.

A retry with the same key and normalized arguments returns the original durable
decision without repeating the lifecycle mutation. A different argument set
under that key is rejected as an idempotency conflict. This served retry
contract does not retrofit idempotency onto the legacy CLI when no invocation
provenance is supplied. Terminal authority is fenced separately by the live
claim receipt and lease.

## Consistency target

- Claim selection target: linearizable per action within one primary Postgres database, subject to transaction and connection behavior.
- Action/event target: one transaction establishes a serializable-looking lifecycle pair for each successful command; the implementation does not currently claim serializable isolation for arbitrary multi-action histories.
- Queue ordering is priority then creation time among rows visible and unlocked to the claiming statement. `SKIP LOCKED` optimizes safe contention, not global fairness.

## Terminal fencing and cancellation

Terminal transitions require matching worker and claim receipt while the lease is
live; the row update and event commit are the fencing linearization point. An
expired-but-unswept claimant cannot settle. Controller cancellation locks the
row, records a cancellation request, clears the live receipt/lease, and moves
the action to `cancelling`. This commit fences renewal, settlement, and sweep.
The supervisor acknowledges process death or a bounded reaper finalizes without
claiming that a process stopped before that evidence exists. A reaped action is
recorded as `stop-unacknowledged-timeout` with process state `unknown`; its
private recovery spool remains unreconciled and therefore ineligible for garbage
collection until the supervisor or an operator records an independent decision.

## Verified dispatch settlement

The verified dispatch path settles only from the exact `dispatch-result/v1`
contract in `docs/contracts/dispatch-result-v1.md`. The packet binds one
positive `action_id` and ActionQ-minted claim attempt to one immutable `result_ref`/`result_digest`
pair. Before opening the lifecycle mutation transaction, the authority reads
the referent from its configured owner-controlled durable CAS and re-hashes the
bytes against the locator. Missing, corrupt, or rehashed bytes fail closed with
no row, claim, or event change. `completed` and `no_change` map to action status
`completed`; `blocked`, `failed`, and `budget_exhausted` map to action status
`failed`. The packet uses the frozen `stop_reason` vocabulary and stores it as
the bounded failure reason for failed action rows. ActionQ owns the claim-fenced
row update and lifecycle event; it does not inspect action-specific artifact
contents or grant another system
terminal authority. The row update and `action_completed`/`action_failed`
event commit in the same transaction. Exit status, session observation, or
worker prose alone cannot settle a verified dispatch.
The daemon reuses the claim attempt as its session and execution-envelope
identity. A timeout sweep or reclaim mints a different attempt and fences the
earlier packet. Stop reasons are a closed privacy-safe registry, never free-
form failure text.

## Bounded OpenCode lifecycle

The optional `opencode-verified` profile adds controller phases
`working -> finalizing -> terminal` with fixed 20-minute work, 2-minute
finalization, and 25-minute total ceilings. A worker exit only advances the
phase. The daemon requires JSON events with one stable
`properties.sessionID`, then continues that same session with the qualified
`ao-finalizer` agent and an explicitly denied tool/MCP surface. The runner
also rejects any finalizer workspace mutation. The finalizer must emit one
bound `dispatch-finalization/v1` declaration; missing, malformed, changed-
session, tool-bearing, or timed-out output is a deterministic failure. The
controller still creates and verifies the immutable `dispatch-result/v1`
referent before asking ActionQ to settle, and claim loss or cancellation
short-circuits finalization and cannot settle success.

## Safety properties

- One completed `claim` call returns at most one action.
- Two concurrent completed claim calls do not both return the same pending action.
- A successful state-changing command appends its corresponding event in the same transaction.
- Terminal state does not transition again through the normal terminal commands.
- Chain depth never exceeds the configured maximum at enqueue commit.

## Liveness and recovery

- No fairness guarantee is made between contending workers.
- Expired claims progress only when an operator or scheduler invokes `sweep`.
- Recovery from unknown outcome is read-and-reconcile using `show` and event history.
- Replaying enqueue or terminal commands is not generally idempotent without a caller-supplied idempotency key; none is currently part of the contract.

## Verification evidence

Reusable contexts live under `verification/contexts/`. A result must state its depth and evidence class; this document alone is `documented-only` until matching tests or models run.
