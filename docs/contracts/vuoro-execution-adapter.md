# Vuoro execution adapter contract

Actionq owns the application handlers, operation names, JSON Schemas, and
compatibility record for the Vuoro `execution` domain. Vuoro supplies the
authenticated transport envelope and registry; it does not import database
functions or reproduce Actionq lifecycle rules.

The composition entry points are `actionq.vuoro.register_operations` and
`actionq.vuoro.compatibility_record`. Importing either module does not connect
to PostgreSQL, run a migration, or start a service. Every handler opens a
runtime-role connection and fails closed through the same read-only schema and
role compatibility check used by `actionctl` and `actionq-server`.

## Catalog

| Operation | Authority | Semantics | Idempotency |
| --- | --- | --- | --- |
| `execution.action.enqueue` | `execution.enqueue` | enqueue | required |
| `execution.action.list` | `execution.read` | read | forbidden |
| `execution.action.show` | `execution.read` | read | forbidden |
| `execution.action.claim` | `execution.claim` | write | required |
| `execution.action.renew` | `execution.claim` | write | required |
| `execution.action.complete` | `execution.transition` | write | required |
| `execution.action.fail` | `execution.transition` | write | required |
| `execution.action.reject` | `execution.transition` | write | required |
| `execution.action.cancel` | `execution.transition` | write | required |
| `execution.action.sweep` | `execution.sweep` | admin | required |
| `execution.event.list` | `execution.read` | read | forbidden |
| `execution.session.list` | `execution.read` | read | forbidden |
| `execution.session.record` | `execution.session.report` | write | required |
| `execution.dispatch.enqueue` | `execution.dispatch.enqueue` | enqueue | required |
| `execution.dispatch.list` | `execution.read` | read | forbidden |

The transport identity supplies the actor and environment. Claim and renewal
inputs therefore do not accept a caller-provided worker, and dispatch inputs do
not accept `requested_by`. Session recording stores evidence supplied by a
machine-local runner; no catalog operation runs a harness, touches a worktree,
or performs another runner effect in the service process.

## Durable mutation decisions

Every served mutation requires an idempotency key. Actionq serializes the
identity/environment/operation/key tuple with a transaction-scoped PostgreSQL
advisory lock and records:

1. `invocation.requested`, containing the request fingerprint and invocation
   provenance;
2. the existing owner-specific lifecycle or session events, with the same
   provenance attached; and
3. `invocation.decided`, containing an accepted or rejected domain decision,
   result when accepted, and stable `actionq:event:<id>` references.

A same-key retry with the same normalized arguments returns the original
decision and appends `invocation.replayed`; it does not repeat the state
transition. Reusing the key with different arguments creates a rejected
`idempotency-key-conflict` decision. This is domain idempotency, not a claim
token or fencing epoch.

The Vuoro invocation envelope being accepted means that the adapter ran. The
nested Actionq decision remains authoritative and can itself be `rejected`.
Callers must inspect `result.decision.status` and retain its decision reference.

## Preserved lifecycle limitation

Claim renewal verifies the authenticated actor against `claimed_by` and rejects
an expired or reassigned lease. Terminal `complete`, `fail`, and `reject`
operations still inherit Actionq's documented limitation: `claimed_by` is
metadata, not claimant proof, and those transitions are not fenced. The adapter
does not claim stale-terminal-owner rejection. Adding fencing remains a
separately authorized lifecycle and schema decision.

`actionctl` and the legacy HTTP façade delegate to `ActionQApplication` without
invocation provenance, so their existing output and lifecycle event shapes stay
compatible while sharing the same application core.
