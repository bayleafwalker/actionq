# Tranche 4 federation/storage contract freeze

**Status:** proposed owner oracle; coordinator-supplied acceptance checklist pending
ratification

**Basis:** `main` at `9dccf4e` (PR #30 merged), `HANDOFF.md`,
`2026-08-19-execution-plane-deletion-constraint.md`,
`2026-08-20-execution-plane-deletion-order.md`, and the owner code named below

**Scope:** contract and work splitting only. This plan does not authorize application,
deployment, runtime, schema, catalog, or migration mutation.

## Decision

ActionQ must **not** extract claim/lease as a reusable package. Claim, renew, sweep,
runner-proof settlement, execution groups, and their queue projections are execution-plane
semantics. Packaging them would preserve the boundary PR #30 was intended to remove.

The target authority is a first-class federation resource with monotonically increasing
revision and compare-and-swap writes. It records identity, relations, externally owned
execution references, acceptance/evidence, reconciliation, and append-only changes; it does
not grant an ActionQ worker the right to execute anything. The current
`ActionResourceOwner` is the closest implementation, but it is not yet that authority:
`action_resources.action_id` is a required foreign key to `actions`, enqueue creates a queue
row first, and its projection imports claim state and receipt. It therefore cannot merely be
renamed or moved.

Until a replacement resource, backfill, projection comparison, consumer cutover, and
rollback window all pass, existing claim/lease rows and events remain an **internal,
read-only compatibility archive**. They are neither the future revision authority nor an
independently published package. No new native execution may depend on them after catalog
cutover.

The only eventual extraction candidate is a narrow storage package. It may own connection
and identifier primitives, immutable migration assets and ledger validation, transactional
repositories for federation resources/changes/completion observations, and read-only legacy
archive projections. It must not own application policy, Vuoro operations, CLI presentation,
provider execution, claims, leases, worker proofs, execution groups, or managed dispatch.

## Coordinator-supplied acceptance checklist

No GitHub review record exists for this checklist. These ten gates were supplied by the
coordinating session and are **pending owner ratification**; this document does not invent a
review reference for them.

1. Complete reachability for claim/lease, database, schema, application, CLI, Vuoro,
   migrations, and external consumers.
2. Explicit retained federation semantics and removed execution-plane semantics.
3. Lossless historical-data treatment.
4. Immutable, ordered, checksum-preserving migration treatment.
5. Revision CAS and idempotency invariants.
6. Stale-writer fencing invariants.
7. Deterministic projection and rebuild invariants.
8. Version, compatibility, and served-catalog invariants.
9. Tested rollback boundaries.
10. A package boundary and staged work packets with exact falsifying tests.

The remainder of this plan is the proposed oracle for those gates.

## Reachability inventory

“Archive” means preserved for historical reads during transition, not retained as a public
write contract. “Remove” means absent from the final federation surface, not immediate
deletion.

| Surface | Current authoritative reach | Current callers/consumers | Target disposition and cutover proof |
|---|---|---|---|
| Claim/lease | `db.claim`, `_claim_schema3`, `renew`, `_transition_terminal`, `complete`, `fail`, `reject`, `settle_dispatch_result`, `cancel`, `acknowledge_cancellation`, `reap_cancellations`, `sweep`; receipt digest and runner-request consumption | `application_claim.py`, `application_completion.py`, `cli.py`, `vuoro.py`; claim-authority, runner-auth, settlement, group, CLI and adapter tests | Remove public writes. Preserve rows/events/receipts read-only until W6. Never extract. Revision CAS replaces authority; stale revisions must fail without a change row. |
| Queue and groups | `db.enqueue`, listing/dispatch summaries, group realize/stop/projection, managed-dispatch envelopes | enqueue/dispatch/group services; CLI; Vuoro catalog | Remove enqueue/dispatch/group mutation from final catalog. Historical queue/list/show may be served only as explicitly versioned archive reads during the window. |
| Federation resource | `action_resource.py`; migration 009 tables; revision/change cursor/recovery floor; encrypted owner cursor | `ActionQCore`, Vuoro resource operations, owner tests | Retain semantics, not physical coupling. Introduce an action-independent resource root before it becomes authority; expected revision is mandatory on every update. |
| Completion observations | `completion_log.py`; migration 011 event/consumer/delivery/recovery-floor tables | ActionQ core/Vuoro completion ingest, list and replay | Retain as externally produced evidence. Preserve event identity, digest conflict handling, ordered cursor and recovery floors. It must not imply ActionQ executed the work. |
| Immutable candidate/publication records | `db.create_immutable_action`, `register_publication`; migrations 006–008 | application dispatch/completion, contracts and publication tests | Retain evidence and acceptance semantics only where they can reference external executions. Remove runner launch/claim preconditions from the new version; preserve old records as archive. |
| CAS | `cas._DaemonCAS`; settlement referent verification | application core, `db.settle_dispatch_result`, tests | Retain content-addressed evidence verification, rename away from daemon only after import compatibility is tested. CAS presence is evidence integrity, never execution ownership. |
| DB primitives | `ActionQError`, schema/qname/connect, JSON/time helpers in `db.py` | nearly every module; `schema.py`, action resource and completion log import back into `db.py` | First isolate dependency-free primitives, then repositories. Keep root facades until all callers are moved. No bulk move of `db.py`. |
| Schema compatibility | `schema.py` versions 1–12, shape checks, schema-3 bridge, grants, locks and migration | `db` facades, CLI, Vuoro deployment/preflight scripts, integration tests | Retain. Split mechanics only after exact migration bytes/order/checksums and compatibility records are pinned. Runtime principals never gain schema creation or ledger writes. |
| Application | `ActionQApplication` composed from core plus claim, completion, enqueue, dispatch, group and outbox services | CLI and Vuoro service composition | Split retained federation/reconciliation services from legacy queue services. Legacy services become internal archive adapters, then are deleted after zero-consumer proof. |
| CLI | `actionctl migrate`, compatibility, queue/group/claim/settle/publication/event/session commands | operators, Vuoro migration Job, Vuoro cloud/preflight, gitops/devbox scripts, q-spec | Retain migration/compatibility and federation/evidence/reconciliation reads/writes. Version and deprecate legacy commands before removal. Never silently reinterpret `claim` as resource CAS. |
| Vuoro adapter | `actionq.vuoro`, 26-operation execution catalog and compatibility record | Vuoro composition and released-adapter pin validators; specialized composition verification | Publish a new catalog version containing federation resources, refs, evidence, acceptance and reconciliation only. Claim/renew/settle, enqueue/managed dispatch and execution groups do not cross the new boundary. Old hashes remain pinned for archive compatibility until explicit cutover. |
| Migration assets | `actionq/migrations/001`–`012`; loaded as `actionq` package data; ledger uses version/name/checksum | wheel packaging, migration CLI, deployment migration Job, schema tests | Bytes, names, versions and order are immutable. A storage-package move must prove byte-for-byte identity and continue to recognize the existing ledger before switching the loader. New evolution is additive migration 013+, never edits to 001–012. |
| `actionq-runner` workspace package | separate portable executor/publisher/candidate code, reachable from its own scripts/tests, not from the retained federation root | package-specific consumers and tests | Out of this extraction. It is native execution-plane residue requiring a separate owner/retirement decision; storage must not import it. |
| Vuoro repository | `packages/vuoro-service/.../composition.py`; adapter pins; released catalog validators; pre-migration script; migration Job | deployed and release composition | Blocking consumer. W4 must land coordinated pins/composition before W5 removes legacy catalog operations. Deployment mutation remains separate operator work. |
| q-spec | `actionq-spec.md` and `dispatcher-spec.md` define claim/daemon/CLI queue as contract | architectural readers and dispatcher implementations | Historical/stale external specification. Supersede in q-spec in a separately owned change; do not treat it as the tranche-4 oracle. |
| gitops/devbox | runbooks, Nix module and scripts invoke `actionctl` compatibility, add and sessions | external operator automation | Blocking consumer inventory. Read-only compatibility/session uses can move; enqueue/daemon uses must retire under their owner. No changes from this plan. |
| appservice/runtime | deployed image, roles, migration jobs and catalog consumers | cluster/runtime owners | External cutover dependency only. No mutation is authorized here; package/schema/catalog rollout requires a separate reviewed plan. |

## Normative retained and removed semantics

Retained federation semantics:

- opaque work/resource identity, parent/child and source relations;
- externally owned execution references and provider-neutral status observations;
- monotonically increasing resource revision, mandatory expected-revision CAS, append-only
  change stream and explicit recovery floor;
- immutable evidence/acceptance records, content-addressed referent verification and durable
  completion observations;
- served idempotency keyed by environment, operation and idempotency key, with canonical
  request-digest conflict detection;
- reconciliation and deterministic projection/rebuild from retained facts;
- backend qualification, schema compatibility and migration authority separation;
- a Vuoro-facing catalog that describes only those capabilities.

Removed execution-plane semantics:

- pending-work queues, dequeue ordering, worker claims, lease renewal/expiry/sweep and claim
  receipt as authority;
- runner-request authorization as permission to execute, ActionQ-owned terminal settlement,
  cancellation acknowledgement/reaping and managed-dispatch fan-out;
- execution groups as ActionQ launch/control objects, harness/model/prompt routing and any
  claim-coupled publication path;
- an ActionQ daemon, server or runner as the owner of native runtime execution.

Historical events may retain these names. They must be labeled archive facts and must not be
projected into new authority without an explicit, deterministic mapping.

## Frozen invariants

### Historical data and migration

1. Migrations 001–012 and their recorded name/checksum triples never change. Existing rows,
   event ordering, claim receipts, idempotency decisions, candidate/publication records,
   action-resource changes, and completion-log cursors remain queryable through the declared
   retention window.
2. Migration 013+ is additive. It creates or enables an action-independent federation root;
   no new authoritative row may require `actions.id`, `claim_receipt`, `claimed_by`, or a
   lease deadline.
3. Backfill is restartable and monotonic. A source row maps to a stable destination identity
   and digest; a retry produces no second logical resource or change.
4. The migration principal retains DDL/ledger authority. Runtime/federation principals get
   only the minimum table/sequence capabilities and never schema `CREATE` or ledger writes.
5. Moving migration resources into a package is a later loader cutover: both old and new
   loaders must report identical assets and compatibility against a copied v12 schema first.

### CAS, fencing, projection and rebuild

1. Every federation mutation carries the caller's current revision. The transaction locks
   or conditionally updates exactly that resource, rejects a stale revision, increments by
   one, and appends exactly one change. Rejection changes neither projection nor history.
2. Idempotent replay with the same canonical digest returns the prior decision; reuse of a
   key with another digest conflicts. Advisory locks may serialize this decision but never
   convey execution authority.
3. Legacy claim receipts fence only legacy archive transitions. They are not accepted as a
   federation revision or translated into one. External provider attestations, if retained,
   are evidence with an explicit assurance type, not a worker lease.
4. The live projection at revision N must equal a clean rebuild from retained changes through
   N. Rebuild order is stable, detects gaps/duplicates/digest conflicts, honors recovery floors,
   and never calls an executor or provider.
5. Backfilled archive facts are distinguished from post-cutover native federation changes.
   Given the same schema snapshot, mapping version and source rows, rebuild output is bytewise
   canonical and repeatable.
6. Evidence CAS verification checks bytes against their reference before acceptance. Missing
   or mismatched evidence cannot advance acceptance state; it also cannot mutate a native
   runtime execution.

### Version, catalog and rollback

1. Schema compatibility, package version and Vuoro catalog version are independent explicit
   dimensions. A catalog hash change requires a declared catalog version; schema 13 cannot
   silently alter the 26-operation legacy catalog.
2. The clean federation catalog has no claim, renew, settle, sweep, managed-dispatch enqueue,
   group-control, harness, model or prompt fields. Removed operation names return a versioned
   not-supported result, never an alias to a new operation.
3. Root imports and `actionctl migrate` remain compatibility facades while Vuoro and operator
   consumers cut over. The facade must delegate without importing application or execution
   policy into the storage package.
4. Before authoritative cutover, rollback means disabling new writes and returning reads to
   v12 with no source mutation. During dual-write, rollback is permitted only while a proven
   reverse audit identifies every new fact and no v13-only accepted state would be lost.
5. After legacy writes are disabled, rollback does not re-enable claim/lease. It restores the
   prior federation release or pauses mutations for repair. Deleting archive rows, old catalog
   support or migration-loader compatibility is irreversible and requires a later operator
   decision after the retention window.

## Extraction boundary

The intended dependency direction is:

```text
actionctl / actionq.vuoro
        -> federation application and policy (root actionq distribution)
        -> repository protocols
        -> narrow storage package
             -> connection + SQL identifiers
             -> immutable migration catalog/ledger/compatibility mechanics
             -> federation/change/completion repositories
             -> read-only legacy archive repository
```

The storage package must be usable without importing `actionq.application`, `actionq.cli`,
`actionq.vuoro`, `actionq.runner_auth`, `actionq.managed_dispatch`, or `actionq-runner`.
Conversely, schema/migration code must stop importing the monolithic `db.py`; common errors,
connection and identifier validation move behind the primitive seam first. Public package
naming is deliberately not frozen until W2 proves the import graph. The semantic boundary is
frozen; a premature package name is not.

## Staged work packets and falsifying tests

Each packet is a separately reviewable change. A command passing is necessary, not sufficient;
the listed false condition is what the test must actually reject.

### W0 — freeze the oracle (this change)

Add this plan and a post-merge handoff pointer only. No Python, SQL, catalog, package or
deployment edits.

```bash
uv run --extra dev pytest tests/test_application_structure.py \
  tests/test_release_contract.py tests/test_repository_retirement_contract.py \
  tests/test_verification_contracts.py -q
python /projects/dev/agentops/templates/dispatch/scripts/validate_verification_artifacts.py --root .
git diff --check
```

Falsifier: any existing execution-plane deletion or repository/verification contract regresses.

### W1 — pin reachability, then split internal modules

Add `tests/test_tranche4_reachability_contract.py` before moving code. It must enumerate all
public `db`, application, CLI and Vuoro references classified above and fail on an unclassified
new edge. Isolate primitives, legacy repository, federation repository and migration mechanics
inside the existing distribution; keep import facades.

```bash
uv run --extra dev pytest tests/test_tranche4_reachability_contract.py \
  tests/test_application_structure.py tests/test_schema.py tests/test_unit.py -q
```

Falsifier: storage imports application/CLI/Vuoro/runner code; a current callable disappears;
or `schema` and `db` retain a circular dependency through the new seam.

### W2 — add independent revision authority

Add migration 013+ and `tests/test_federation_revision_authority.py`. Do not alter migrations
001–012. New roots must be creatable without an action row or claim receipt.

```bash
uv run --extra dev pytest tests/test_schema.py \
  tests/test_federation_revision_authority.py tests/test_action_resource_owner.py -q
ACTIONQ_INTEGRATION_URL="$DISPOSABLE_URL" uv run --extra dev pytest \
  tests/test_integration_postgres.py -q
```

Falsifier: migration bytes/checksums drift; a root requires `actions.id`; stale expected
revision writes; failed CAS appends a change; retry duplicates a resource; runtime role can
write the migration ledger; or v12-to-v13 compatibility fails. `$DISPOSABLE_URL` must name a
task-owned disposable database/schema, never a shared deployment.

### W3 — deterministic backfill and rebuild

Add `tests/test_federation_backfill_rebuild.py` with fixtures covering every legacy lifecycle
state, reclaims/renewals, stale receipts, cancellations, candidate publications, action-resource
cursor pruning and completion recovery floors.

```bash
uv run --extra dev pytest tests/test_federation_backfill_rebuild.py \
  tests/test_claim_authority.py tests/test_completion_log_integration.py -q
```

Falsifier: rerun changes identity/digest; revision gaps or duplicates pass; rebuilt projection
differs from live projection; archive facts become execution authority; or recovery-floor loss
is hidden.

### W4 — dual-read application and clean Vuoro catalog

Add a versioned federation application surface and catalog while retaining the legacy catalog
unchanged. Coordinate Vuoro composition/pins in its own repository and PR.

```bash
uv run --extra dev pytest tests/test_vuoro_adapter_integration.py \
  tests/test_federation_catalog_contract.py tests/test_dispatch_v2_contract.py -q
uv run --project /projects/dev/vuoro python \
  /projects/dev/vuoro/scripts/validate_released_execution_adapter.py
```

Falsifier: the new catalog contains claim/lease/managed-dispatch/group/harness fields; old
hashes drift; an external execution ref is treated as an ActionQ launch instruction; or a
served mutation omits expected revision.

### W5 — consumer cutover and legacy-write fence

Require zero known writers from Vuoro, q-spec implementations, gitops/devbox automation and
other deployed clients. Then disable legacy writes at application/catalog/CLI boundaries;
retain archive reads.

```bash
uv run --extra dev pytest tests/test_legacy_write_fence.py \
  tests/test_repository_retirement_contract.py tests/test_release_contract.py -q
```

Falsifier: claim/renew/settle/enqueue/group mutation is reachable from a supported catalog or
CLI; the fence changes historical rows; or rollback can re-enable native execution ownership.

### W6 — extract storage and prove rollback

Only now create the storage distribution, relocate immutable assets with an old/new loader
equivalence test, and retain root compatibility facades. Add wheel and copied-schema tests.

```bash
uv run --extra dev pytest tests/test_storage_package_boundary.py \
  tests/test_migration_asset_equivalence.py tests/test_tranche4_rollback.py \
  tests/test_schema.py tests/test_integration_postgres.py -q
uv build
unzip -l dist/actionq-*.whl
```

Falsifier: migration asset name/order/bytes/checksum differs; existing v12 ledger is rejected;
storage imports policy/execution modules; root facade changes output; wheel omits an asset; or
rollback loses a v13 fact.

### W7 — archive retirement (later operator decision)

Delete legacy mutation code only after the retention window, zero-consumer proof and durable
export/restore rehearsal. Archive row deletion, old catalog removal and migration-loader
compatibility removal are separate irreversible decisions, not implied by W1–W6.

## Dispatch and review graph

```text
W0 oracle freeze -> R0 owner + independent architecture ratification
                         |
                         v
W1 reachability/import seams -> R1 boundary review
                         |
                         v
W2 revision authority + migration -> R2 schema/CAS/fencing review
                         |
                         v
W3 backfill/rebuild/rollback proof -> R3 historical-data review
                         |
                         v
W4 ActionQ catalog -----> coordinated Vuoro consumer/pin PR -> R4 composition review
                         |
                         v
W5 zero-consumer proof + legacy-write fence -> operator cutover decision
                         |
                         v
W6 storage extraction + loader equivalence -> R5 package/release review
                         |
                         v
W7 archive retirement -> separate retention/destruction authorization
```

W1 and W2 are ActionQ owner work. W3 should be implemented independently from its reviewer.
W4 has separate ActionQ and Vuoro owners but cannot merge as a partial catalog cutover. W5
requires current external-consumer evidence. W6 must not start while W0–W5 gates are open.

## Remaining operator decisions

The following are real decisions and are intentionally not guessed here:

1. Ratify or amend the ten coordinator-supplied gates and the revision-authority decision.
2. Choose the legacy read-retention duration and durable export/restore target.
3. Approve the final catalog cutover after Vuoro and external consumer evidence is current.
4. Approve any deployment/schema migration execution; this plan authorizes none.
5. After W2 proves the graph, choose whether the storage seam merits a separately published
   distribution or remains an internal package. The semantic boundary does not depend on that
   packaging choice.
6. Separately decide the fate of `actionq-runner`; it is not smuggled into tranche 4.
