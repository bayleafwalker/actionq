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
| `complete` | Action status is `claimed` | Status `completed`, result and timestamp stored, event appended | No action changes |
| `fail` / `reject` | Action status is `claimed` | Terminal status and reason stored, event appended | No action changes |
| `cancel` | Status is `pending` or `claimed` | Status `cancelled`, reason stored, event appended | No action changes |
| `sweep` | Status is `claimed` and deadline is in the past | Status returns to `pending`; claim metadata cleared; timeout event appended | Transaction rolls back |

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

`claimed_by` is metadata, not proof. Terminal transitions check only `status = claimed`; they do not require a claim token, epoch, or matching worker. A worker that timed out may therefore attempt a terminal transition after the action is reassigned. Until fencing is added, do not claim exclusive terminal authority or stale-owner rejection.

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
