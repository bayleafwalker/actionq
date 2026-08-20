# ActionQ action resource owner contract v1 — GO-review candidate

Status: **FROZEN FOR INDEPENDENT REVIEW; NOT APPROVED FOR IMPLEMENTATION**  
Work item: ActionQ #2027  
Contract id: `actionq.action-resource-owner/v1`  
Golden fixture set: `verification/fixtures/action-resource-owner-v1/manifest.json`.
Every normative file is digest-linked there; unregistered files are invalid.

This document resolves the operator decisions required before #2027 may be
claimed. It is an immutable review input, not evidence that the server
implements the contract. Only an independent recorded GO over this document
and the golden fixture set permits a fresh Sprintctl implementation claim.
Existing `/v2/dispatch` prototype code is non-authoritative.

## Authority and resource identity

ActionQ owns one opaque **action-root** resource from enqueue through terminal
retention. An execution session is a projection attached to that root. A
session is never an authority root and cannot independently define lifecycle,
retention, cancellation, recovery, idempotency, claim, or fencing state.
Vuoro may later carry an ActionQ reference and generic envelopes; it gains none
of those authorities.

The external reference grammar is exactly `aqr1_` followed by 43 base64url
characters (`[A-Za-z0-9_-]`), encoding 256 random bits without padding. A
reference is opaque: clients MUST NOT decode, synthesize, order, enumerate, or
derive tenant, action id, session id, time, shard, or outcome from it. References
are generated with a CSPRNG, unique in the owner database, and compared as
case-sensitive bytes. They may be disclosed only by enqueue or an already
authorized owner response.

Authorization is scoped to `(principal, action-root, verb)` and is checked
without exposing existence. Malformed, syntactically valid but absent, foreign,
and unauthorized references return the byte-identical not-found fixture,
including status, selected headers, and body. No database lookup is permitted
for malformed input; authorization must not use an existence-revealing branch.
No list, search, prefix, sequential-id, timing-class, or reference-validation
endpoint is part of v1.

## Enqueue and lost responses

Enqueue binds the caller scope, operation, idempotency key, and canonical
request digest to one action-root in one database transaction. That transaction
creates the root, its initial projection/event, the opaque reference, and a
monotonic per-resource revision. The response uses the **revision returned by
the committing statement/transaction**, never a later read.

An identical retry after any disconnect or commit-before-response loss returns
the original reference and original enqueue revision. It never creates a
second root and never substitutes a current revision. Reuse of the binding with
different canonical request bytes conflicts without disclosing the original
reference or request. Client disconnect never rolls back an already committed
enqueue and never triggers an implicit retry under a new key.

## Snapshot, changes, and atomic handoff

Every resource has uint64 revisions beginning at 1. A snapshot response contains
one redacted projection at revision `R`, an opaque cursor whose position is
exactly `R`, and a non-null `recovery_floor`. Snapshot construction and cursor
position are taken either by one SQL statement or within one PostgreSQL
`REPEATABLE READ` transaction. The proof must force a terminal commit between
the projection read and response construction and demonstrate exactly one of:

1. terminal state is in the snapshot and the cursor is at or after its revision;
2. terminal state is absent and the event is returned exactly once after the
   snapshot cursor.

A mixed snapshot with a cursor beyond an omitted terminal revision is forbidden.
Changes are strictly ordered, greater than the cursor position, and contain no
duplicate revision. Cursors are opaque, integrity protected, bound to the
authorized action-root and principal scope, and reveal no identifiers.

## Retention and cursor expiry

Each action-root has a durable, non-null `recovery_floor` stored independently
of retained event rows. A cursor is a **position after a revision**. The floor
is the smallest cursor position from which changes can be resumed without a
gap. It starts at `0` (before revision 1), is monotonic, and is atomically
advanced whenever pruning commits. It remains
non-null when no events have been pruned, some have been pruned, all change rows
have been pruned, the resource is terminal, and after restart.

Retention is per resource, never inferred from a global minimum. Pruning may
remove only change rows at or below the newly committed floor; it cannot
remove the current snapshot, terminal projection, immutable enqueue binding,
or required terminal envelope. A cursor below the floor returns
`cursor_expired` with the current non-null floor and a recovery instruction to
fetch a fresh snapshot. Recovery does not replay from zero and a fresh snapshot
must preserve byte-identical terminal projection content across pruning and
restart (apart from envelope revision/cursor fields explicitly identified by
the fixture schema).

Arithmetic is normative. With revisions 1–5 retained, floor 0 resumes with
1–5. After pruning revisions 1–2, floor 2 resumes with 3–5 and cursor 1 is
expired. After pruning every change through terminal revision 5, floor 5 is
still durable and non-null; cursor 4 expires and cursor 5 resumes with an empty
set. The terminal projection and its complete redacted terminal envelope are
stored outside the prunable change-row set for the resource retention floor,
survive the all-pruned state and restart, and are returned by a fresh snapshot.

## Projection and redaction boundary

The allowlist is exact: `resource_ref`, `revision`, `state`, `terminal`,
`outcome`, `created_at`, `updated_at`, and `execution_sessions`. A session
projection may contain only `ordinal`, `state`, `started_at`, and `ended_at`.
`outcome` is only `completed`, `failed`, `rejected`, or `cancelled`; it conveys
no reason. Non-terminal resources use `null` outcome. Events contain only
`revision`, `kind`, `state`, `terminal`, `outcome`, and `occurred_at`.

Everything not allowlisted is denied, recursively. In particular no secret-like
data, result/output/attachment references, failure details or messages, claims,
claim receipts, workers, leases, fencing tokens, provenance/actor/principal,
request snapshots or digests, payload/metadata, logs, prompts, branches,
worktrees, environment, authorization facts, or internal numeric ids may occur
in a projection or event. Redaction happens before serialization and before
logging/caching; it is not a best-effort key scrubber. Unknown owner fields fail
closed rather than pass through.

## Bounded wait

`wait` is a changes request with an integer `wait_seconds` from 0 through 30.
Missing means 0; negative, fractional, non-numeric, or greater than 30 is a
400 contract error. The served application boundary returns immediately when a later revision is
available or the action is terminal. Otherwise it returns an empty change set
no later than the requested bound plus a documented 1-second scheduling grace.
Disconnect cancels only the wait, not the action. Wakeups are hints: the
transactional revision query remains authoritative. There is no unbounded wait,
SSE, lifecycle command, cancellation, or recovery mutation in v1.

## Retired standalone HTTP quarantine

The raw-target `/v2/dispatch` quarantine belonged to the deleted standalone
ActionQ HTTP server. It is not a behavior of `ActionQApplication`, `actionctl`,
or the Vuoro adapter and is no longer part of this contract. Its fixture and
result packet were removed with that server so current evidence cannot claim
to exercise an absent wire boundary. Historical evidence remains available in
the Git revision that shipped the HTTP facade.

## Required disposable-PostgreSQL falsifying histories

Implementation approval is separate from completion. Completion requires
immutable result packets from a disposable PostgreSQL instance recording
server version, isolation level, seed, bounds, faults, and minimized failures:

- pruning with none/some/all retained changes and terminal restart, proving the
  durable non-null per-resource floor;
- the forced concurrent-terminal snapshot/cursor race described above;
- malformed, absent, foreign, and unauthorized non-disclosure byte equality;
- recursive redaction canaries in every forbidden class and unknown fields;
- commit-before-enqueue-response loss plus exact retry reference and revision;
- wait at 0 and 30 seconds, early wake, spurious wake, disconnect, and restart;
- stale session/claim settlement attempts proving existing ActionQ fencing is
  neither exposed nor weakened by observation.

Golden fixtures are normative for shapes and bytes. PostgreSQL histories are
required future implementation evidence; fixture-validation tests alone are
not product proof and cannot produce a GO.
