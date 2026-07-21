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
| `complete` | Action status is `claimed` | Status `completed`, result and timestamp stored, event appended | No action changes |
| `fail` / `reject` | Action status is `claimed` | Terminal status and reason stored, event appended | No action changes |
| `cancel` | Status is `pending` or `claimed` | Status `cancelled`, reason stored, event appended | No action changes |
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
  unchanged. This is the first authority-command boundary in this repo
  with a real, tested claimant check (contrast with the ownership
  limitation below, which still applies to `complete`/`fail`/`reject`).
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

## Consistency target

- Claim selection target: linearizable per action within one primary Postgres database, subject to transaction and connection behavior.
- Action/event target: one transaction establishes a serializable-looking lifecycle pair for each successful command; the implementation does not currently claim serializable isolation for arbitrary multi-action histories.
- Queue ordering is priority then creation time among rows visible and unlocked to the claiming statement. `SKIP LOCKED` optimizes safe contention, not global fairness.

## Ownership limitation

`claimed_by` is metadata, not proof for the terminal transitions (`complete`/`fail`/`reject`): they check only `status = claimed`, not a claim token, epoch, or matching worker. A worker that timed out may therefore still attempt a terminal transition after the action is reassigned. Until fencing is added for those commands specifically, do not claim exclusive terminal authority or stale-owner rejection for complete/fail/reject.

`renew` is a narrower exception: it does check `claimed_by` against the requesting worker (see "Claim/lease authority commands" above), because that specific comparison is implemented and tested. This does not extend to the terminal transitions -- they remain as described in this section until a separate, explicitly authorized change adds claimant proof there too.

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
