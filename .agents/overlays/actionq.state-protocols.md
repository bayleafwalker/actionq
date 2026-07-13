# actionq state-protocol overlay

## Closed subjects

| Subject | State owner | Default depth | Primary anchors |
|---|---|---:|---|
| Action lifecycle | Postgres `actions` row | 1 | `actionq.db:enqueue`, `_transition_terminal` |
| Claim selection | Postgres transaction and row lock | 2 | `actionq.db:claim` |
| Claim timeout and requeue | Postgres transaction and row lock | 2 | `actionq.db:sweep` |
| Action/event agreement | `actions` plus append-only `events` in one transaction | 2 | `actionq.db:insert_event` and all writers |
| Parent chain and rate limit | Parent row plus enqueue-event history | 1 | `actionq.db:enqueue` |

Escalate claim, timeout, or terminal-transition redesign to Depth 3 when it introduces fencing, leases, heartbeats, unknown outcomes, or multiple persistence backends.

## Required scenarios

- Two independent connections claim one pending action concurrently.
- Competing workers claim multiple actions without duplicate ownership.
- Sweep races with completion, cancellation, and a new claim.
- Crash or lost response after the transaction commits.
- A stale worker attempts a terminal transition after timeout and reassignment.
- Action state and lifecycle event remain transactionally aligned.
- Equal-priority ordering is deterministic within the documented boundary.

## Current limitation to preserve in reports

Terminal transitions currently validate action status but not claimant proof. `claimed_by` is not a fencing token. Do not report stale-owner rejection or exclusive terminal authority as established until the product contract and implementation add proof.

## Verification environment

Use a disposable Postgres schema through `ACTIONQ_TEST_URL`, one connection per actor, deterministic barriers before the target SQL statement, and recorded invocation/completion histories. Never use the production queue.
