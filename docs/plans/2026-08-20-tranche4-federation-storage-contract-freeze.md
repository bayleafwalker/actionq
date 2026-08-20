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

Until W5, existing queue/claim/lease commands and direct repository methods remain a
**writable compatibility subsystem**. Calling them read-only before credentials and database
privileges are revoked would be false: old binaries and direct SQL can still write them. At
W5 their writer role/credentials are revoked and the database enforces a redacted,
read-only archive-reader role. Only then do their rows and events become an internal
compatibility archive. They are neither the future revision authority nor an independently
published package. No native execution may acquire authority from them.

The only eventual extraction candidate is a narrow storage boundary. It may own connection
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

## Blocked-oracle findings and adjudication

The coordinator blocked head `316f321` while accepting its core ownership direction. There
is no GitHub review thread to resolve. This revision adjudicates every supplied finding:

| Finding | Adjudication frozen here |
|---|---|
| Execution migrations 001–012 lacked a safe successor domain | Choose a separate `federation` migration/schema domain v1. Freeze execution v1–v12 forever; reject execution migration 013 as the selected path. |
| “Read-only archive” overstated the current fence | Call it writable compatibility through W4; W5 revokes database roles/credentials and grants only redacted archive reads. |
| CAS was treated as authority | CAS is concurrency control only. Aggregate state-machine rules plus actor/ACL authorization decide whether a command is allowed. |
| Acceptance/settlement ownership was ambiguous | They are ActionQ-local facts about evidence and federation closure. They cannot control native runtimes or mutate Sprintctl state. |
| Idempotency was coupled to resource changes | Add an independent durable command-decision ledger; rejected decisions persist without resource changes. |
| Create/relation/digest/replay details were open | Creation uses expected absence/revision 0; source owns directed relations; canonical bytes/digests and response-loss replay are frozen below. |
| Pruning made rebuild ambiguous | Choose no pruning before a separately reviewed checkpoint+tail format exists. Retention/export/restore is a prerequisite to W3, not deferred to W7. |
| Catalog transition was abstract | Choose clean-break `federation/v1`, served beside frozen `execution/v1`, with exact global revision and release order below. |
| Test commands were not executable contracts | Use `ACTIONQ_TEST_URL`; give the Vuoro validator exact `MANIFEST`/`WHEEL_DIRECTORY` arguments and pin its source revision per W4. |
| W1 inventory was prose only | Require a checked-in machine-readable reachability manifest and a completeness test. |
| `actionq-runner` was deferred indefinitely | Make its retire/extract/transfer decision and zero-root-reach proof a W6 closure dependency. |
| README/HANDOFF mixed repository and deployment state | Separate repository truth from historical deployment observations; no deployment claim is refreshed by this PR. |

The coordinator's second review blocked head `0561685` on executable consistency. Its four
findings are adjudicated as follows:

| Finding | Adjudication frozen here |
|---|---|
| Actor authority and database roles were conflated; ownership/supersede was incomplete | Separate end-actor ACL from service/migration/archive PG roles; immutable creator ownership, no v1 transfer, and explicit owner-scoped `federation.supersede`. |
| W4 proved only a mutable/current four-domain tree | Preserve an immutable four-domain baseline and require an exact future candidate SHA, released-wheel fetch/digest stage, and new five-domain whole-composition validator. |
| W5 mixed repository intent with deployment truth; W7 implied deletion authority | Require fresh environment-bound auditctl evidence for every writer/credential/grant/denial surface; explicitly leave W7 unauthorized pending a separate destructive plan. |
| Release order admitted ambiguous partial rollout | Freeze ActionQ merge → exact wheel publish → migration → exact Vuoro candidate validation → Vuoro merge/release/deploy → rediscovery → current evidence → W5 fence; dormant source merge is distinct from serving. |

## Reachability inventory

“Archive” means preserved for historical reads during transition, not retained as a public
write contract. “Remove” means absent from the final federation surface, not immediate
deletion.

| Surface | Current authoritative reach | Current callers/consumers | Target disposition and cutover proof |
|---|---|---|---|
| Claim/lease | `db.claim`, `_claim_schema3`, `renew`, `_transition_terminal`, `complete`, `fail`, `reject`, `settle_dispatch_result`, `cancel`, `acknowledge_cancellation`, `reap_cancellations`, `sweep`; receipt digest and runner-request consumption | `application_claim.py`, `application_completion.py`, `cli.py`, `vuoro.py`; claim-authority, runner-auth, settlement, group, CLI and adapter tests | Writable compatibility through W4. W5 removes supported writes **and** revokes database writer credentials/privileges so old binaries/direct SQL fail; only redacted archive reads remain. Never extract. |
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
| Migration assets | execution-domain `actionq/migrations/001`–`012`; loaded as `actionq` package data; ledger uses domain/version/name/checksum | wheel packaging, migration CLI, deployment migration Job, schema tests | Freeze bytes, names, versions, order, `DOMAIN=execution`, `API_VERSION=v1`, and maximum 12. Federation starts at its own v1 migration/schema domain and never adds execution migration 013. |
| `actionq-runner` workspace package | separate portable executor/publisher/candidate code, reachable from its own scripts/tests, not from the retained federation root | package-specific consumers and tests | Out of the storage package, but not indefinitely deferred. A reviewed retire/extract/transfer disposition and proof that federation roots do not reach it are mandatory W6 closure gates. |
| Retired dispatcher | `actionq-dispatcher` tombstone PR #2 merged at `510822a`; release `actionq-dispatcher-v0.2.0` publishes the retirement | stale q-spec, ActionQ historical protocol/README language, root and generated agent guidance | Retired, not a compatibility launcher. No tranche-4 work may dispatch through it. Remove stale normative references under their owning repositories; preserve historical receipts as evidence. |
| Vuoro repository | `packages/vuoro-service/.../composition.py`; adapter pins; released catalog validators; pre-migration script; migration Job | deployed and release composition | Blocking consumer. W4 must land coordinated pins/composition before W5 removes legacy catalog operations. Deployment mutation remains separate operator work. |
| q-spec | `actionq-spec.md` and `dispatcher-spec.md` still define claim/daemon/CLI queue as a normative contract | architectural readers | Stale after dispatcher retirement. Supersede in q-spec in a separately owned change, explicitly citing the tombstone release and native-runtime/ActionQ-federation boundary; do not treat either file as a tranche-4 oracle or implementation contract. |
| Workspace/agentops guidance | root `AGENTS.md` and generated agentops workspace guidance still instruct operators to start/use the legacy daemon/launcher | agent sessions and runbooks generated from the shared template | Separate cross-repository documentation cleanup. Until corrected, the retirement release and this owner oracle take precedence; tranche 4 must not edit the generated guidance locally. |
| gitops/devbox | runbooks, Nix module and scripts invoke `actionctl` compatibility, add and sessions | external operator automation | Blocking consumer inventory. Read-only compatibility/session uses can move; enqueue/daemon uses must retire under their owner. No changes from this plan. |
| appservice/runtime | deployed image, roles, migration jobs and catalog consumers | cluster/runtime owners | External cutover dependency only. No mutation is authorized here; package/schema/catalog rollout requires a separate reviewed plan. |

## Normative retained and removed semantics

Retained federation semantics:

- opaque work/resource identity, parent/child and source relations;
- externally owned execution references and provider-neutral status observations;
- monotonically increasing resource revision, mandatory expected-revision CAS, an unpruned
  append-only v1 change stream, and recovery floor fixed at zero;
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
- runner-request authorization as permission to execute, ActionQ-owned claim-coupled terminal settlement,
  cancellation acknowledgement/reaping and managed-dispatch fan-out;
- execution groups as ActionQ launch/control objects, harness/model/prompt routing and any
  claim-coupled publication path;
- an ActionQ daemon, server or runner as the owner of native runtime execution.

Historical events may retain these names. They must be labeled archive facts and must not be
projected into new authority without an explicit, deterministic mapping.

## Chosen federation contract v1

### Migration and schema domain

The selected design is a separate logical and physical schema domain:

- domain `federation`, API `v1`, compatibility label `federation-schema/v1`;
- configurable `ACTIONQ_FEDERATION_SCHEMA`, default `actionq_federation`, with its own
  migration ledger and assets starting at federation migration 001;
- no foreign keys, views, triggers, grants or write paths from federation tables to the
  execution schema; archive identifiers are opaque external references when recorded;
- execution `DOMAIN=execution`, `API_VERSION=v1`, migrations 001–012 and maximum version 12
  remain frozen and independently checkable.

Adding `actionq/migrations/013_*` in the execution domain is explicitly **not chosen**. It is
a conceivable expand/contract alternative, but it retains the wrong migration authority,
couples rollback to execution tables, and weakens privilege separation. Any future proposal
to choose it must supersede this oracle before code is written.

### Aggregate, state machine and facts

The aggregate is `FederationResource`, identified by an opaque `resource_ref`. Its projection
contains revision, local state, directed relations, external execution references, evidence
references, ActionQ acceptance decisions and ActionQ settlement facts. V1 local states are
`registered`, `evidence-recorded`, `accepted`, `rejected`, and `superseded`.

Allowed transitions are:

| Command | From | To/effect |
|---|---|---|
| `create` | expected absence/revision 0 | `registered`, revision 1 |
| `add-relation` / `record-execution-ref` | any non-superseded state | state unchanged; revision +1 |
| `record-evidence` | `registered` or `evidence-recorded` | `evidence-recorded`; revision +1 |
| `decide-acceptance(accepted)` | `evidence-recorded` | `accepted`; revision +1 |
| `decide-acceptance(rejected)` | `registered` or `evidence-recorded` | `rejected`; revision +1 |
| `record-settlement` | `accepted` or `rejected` | state unchanged; append local settlement fact; revision +1 |
| `supersede` | any non-superseded state | `superseded`; revision +1 |

Acceptance means only “ActionQ accepted/rejected the cited evidence under a named policy.”
Settlement means only “ActionQ recorded the local federation record as reconciled against
cited evidence/external references.” Neither is a native-runtime command or status, and
neither may claim, renew, cancel, complete or otherwise mutate a native execution. ActionQ
also performs no Sprintctl write or cross-tool transaction; Sprintctl remains sovereign over
work state. Legacy `claimed` or `completed` rows may be imported as provenance observations,
but can never infer v1 acceptance or settlement.

### Actor and ACL matrix

Authenticated actor authority and PostgreSQL role privilege are separate layers. Vuoro/CLI
authenticates the end actor; the federation command service evaluates the authority below in
the same transaction that durably records its accepted/rejected decision, before any aggregate
change. End actors receive no database credentials. Expected-revision CAS only prevents
concurrent overwrite and never grants authority.

| Actor class | Allowed v1 commands | Forbidden even with current revision |
|---|---|---|
| federation creator (`federation.create`) | create | acceptance, settlement, schema/archive writes |
| relation writer (`federation.relate`) | add directed relation from a source it owns; record external execution ref | target mutation, execution control, acceptance |
| evidence ingester (`federation.evidence.ingest`) | record verified evidence | acceptance, settlement, relation ownership changes |
| acceptance reviewer (`federation.acceptance.decide`) | accepted/rejected decision under named policy | execution control, Sprintctl mutation, schema/archive writes |
| reconciler (`federation.settlement.record`) | record ActionQ-local settlement against cited facts | native terminal transition, Sprintctl mutation |
| owning superseder (`federation.supersede`) | supersede a resource whose immutable owner is the actor | ownership transfer, target mutation, execution/Sprintctl control |
| federation reader (`federation.read`) | unredacted authorized federation reads | all writes |
| archive reader (`execution.archive.read`) | redacted legacy projection after W5 | raw secrets/receipts/proofs and all writes |
| backfill principal (`federation.backfill`) | deterministic provenance import before cutover | acceptance/settlement inference, normal serving |
| migration principal | DDL and its selected domain's migration ledger only | resource, change, command-decision, acceptance and settlement writes |

Relations are directed (`source_ref`, `relation_type`, `target_ref`). The source aggregate owns
the edge; creation requires `federation.relate` plus equality between the authenticated actor's
stable `principal_id` and the source's immutable `owner_principal_id`. `create` stores the
authenticated creator as owner. V1 has **no ownership transfer**; correction requires creating
a new resource and relating/superseding the old one under both commands' ACLs. The edge does
not mutate or increment the target. V1 types are `parent-of`, `depends-on`, `derived-from`, and
`supersedes`; self-edges are invalid, and `parent-of`/`depends-on` cycle checks run atomically.

PostgreSQL roles are frozen separately:

| Database role | Direct database privilege |
|---|---|
| federation migration principal | federation-schema DDL and federation migration-ledger writes only |
| execution migration principal | frozen execution-schema DDL path and execution migration-ledger writes only; no federation access |
| federation command service | narrowly scoped transactional DML on federation resources, resource-change ledger, command-decision ledger and idempotency binding; no DDL/migration-ledger writes |
| authenticated end actor | none; acts only through the federation service after authority evaluation |
| legacy runtime credential | temporary execution-compatibility DML before W5 only; never federation DML; target privilege is zero direct DB access after W5 |
| archive reader | after W5, SELECT on redacted archive views only; no base-table, sequence or federation write privilege |

Only the federation command service may atomically write a resource projection, its resource-
change ledger row, and its command-decision row. Its database privilege does not bypass actor
authorization: missing/denied actor authority produces a durable rejected command decision and
no resource/change write. The target state gives both end actors and legacy runtime credentials
zero direct database write privilege; the pre-W5 execution-only grant is the explicitly bounded
compatibility exception removed by W5.

### Command-decision ledger

Every mutation attempt first resolves an independent durable decision keyed by
`(environment, principal_id, operation, idempotency_key, request_digest)`. A companion unique
binding on the first four fields records the first request digest, allowing a conflicting
digest attempt to persist its own rejected decision without replacing the binding. Decisions
carry status (`accepted` or `rejected`), stable code/message, response bytes and digest,
resource reference, and before/after revision when applicable. Request and response
digests are `sha256:` over `actionq-contracts.canonical_bytes`: UTF-8 JSON, recursively
normalized values, sorted keys, compact separators, Unicode preserved, floats/NaN rejected.

The decision and any accepted aggregate change commit atomically. A rejected authorization,
validation, stale-revision or digest-conflict decision is durable but appends no resource
change. A retry with the same key and digest returns the byte-identical stored response after
response loss; the same key with another digest returns the stored idempotency conflict and
does not inspect or mutate aggregate state.

## Frozen invariants

### Historical data and migration

1. Migrations 001–012 and their recorded name/checksum triples never change. Existing rows,
   event ordering, claim receipts, idempotency decisions, candidate/publication records,
   action-resource changes, and completion-log cursors remain queryable through the declared
   retention window.
2. Federation migration 001+ evolves only `federation-schema/v1`. There is no execution
   migration 013. No federation row may require `actions.id`, `claim_receipt`, `claimed_by`,
   a lease deadline, or an execution-schema foreign key.
3. Backfill is restartable and monotonic. A source row maps to a stable destination identity
   and digest; a retry produces no second logical resource or change.
4. Each migration principal has DDL plus writes to its own migration ledger only. The
   federation command service alone gets minimum resource/change/decision DML and never schema
   `CREATE` or migration-ledger writes. End actors have no DB role; legacy runtime credentials
   never receive federation access and reach zero direct DB privilege at W5.
5. Execution and federation migration selections are explicit CLI/config inputs; an execution
   v12 compatibility result never claims federation readiness. Moving either loader later
   requires byte-identical assets and compatibility against copied schemas.
6. Before W3 starts, the owner must ratify a retention duration, durable-authoritative export
   target, restore objective and destructive-archive authorization. W3 must exercise export
   and restore; W7 cannot invent these after cutover.

### CAS, fencing, projection and rebuild

1. Creation carries expected revision 0 and succeeds only when the resource is absent. Every
   update carries exact current revision N. The transaction conditionally updates exactly
   that resource, rejects stale/absent expectations, increments by one, and appends exactly
   one change. Rejection changes no resource projection/history but does persist its command
   decision.
2. Idempotent replay and digest-conflict behavior follow the independent ledger above.
   Advisory locks and expected-revision CAS serialize decisions but never convey actor
   authority or make an invalid state transition valid.
3. Legacy claim receipts fence only legacy archive transitions. They are not accepted as a
   federation revision or translated into one. External provider attestations, if retained,
   are evidence with an explicit assurance type, not a worker lease.
4. V1 chooses **no pruning** of federation changes. The live projection at revision N must
   equal a clean rebuild from changes 1..N. Rebuild order is stable, detects gaps, duplicates
   and digest conflicts, and never calls an executor or provider. Pruning is prohibited until
   a separately versioned checkpoint+tail format proves checkpoint digest, covered revision,
   tail continuity, export and restore.
5. Backfilled archive facts are distinguished from post-cutover native federation changes.
   Given the same schema snapshot, mapping version and source rows, rebuild output is bytewise
   canonical and repeatable.
6. Evidence content-address verification checks bytes against their reference before acceptance. Missing
   or mismatched evidence cannot advance acceptance state; it also cannot mutate a native
   runtime execution.

### Version, catalog and rollback

1. Schema compatibility, package version and Vuoro catalog version are independent explicit
   dimensions. The selected new served domain/API is clean-break `federation/v1`, registered
   separately from frozen `execution/v1`; it is never called execution v2 and never silently
   alters the 26-operation legacy catalog.
2. The `federation/v1` catalog has no claim, renew, settle, sweep, managed-dispatch enqueue,
   group-control, harness, model or prompt fields. Removed operation names return a versioned
   not-supported result, never an alias to a new operation.
3. Vuoro's global catalog revision is the digest of the whole composed catalog. Adding
   `federation/v1` changes it. Every `federation/v1` invocation, read or write, sends the exact
   discovered global revision; a missing or mismatched revision is rejected as `stale-catalog` before
   authorization, handler, or decision-ledger execution, with no ActionQ write. Clients must
   rediscover and issue a new deliberate request; Vuoro never transparently retries a mutation.
4. Release/cutover order is fixed:
   1. merge dormant ActionQ federation source;
   2. build and publish the exact ActionQ wheel, recording full source SHA, artifact URL and
      SHA-256;
   3. run the federation migration job using that exact released wheel;
   4. validate the exact-SHA five-domain Vuoro candidate manifest/composition against the
      fetched/digest-verified wheel set;
   5. merge Vuoro, then release and deploy that validated composition;
   6. require clients to rediscover the new global catalog revision;
   7. capture current durable-authoritative zero-writer evidence;
   8. execute the W5 database privilege fence.

   Preparatory source may be merged while its serving path is dormant; what is forbidden is
   partial serving or cutover (serving federation before migration, serving an unvalidated or
   unpinned composition, fencing before deployment/rediscovery/evidence, or silently removing
   `execution/v1`). A later separately validated Vuoro release may remove `execution/v1`,
   causing another global revision change.
5. Root imports and `actionctl migrate` remain compatibility facades while Vuoro and operator
   consumers cut over. The facade must delegate without importing application or execution
   policy into the storage boundary.
6. Before authoritative cutover, rollback disables federation writes and leaves execution v12
   compatibility untouched. During a bridge/backfill, rollback is permitted only while a
   reverse audit identifies every new fact and no federation-only accepted state is lost.
7. After legacy writes are disabled, rollback does not re-enable claim/lease. It restores the
   prior federation release or pauses mutations for repair. Deleting archive rows, old catalog
   support or migration-loader compatibility is irreversible and requires a later operator
   decision after the retention window.

## Extraction boundary

The intended dependency direction is:

```text
actionctl / actionq.vuoro
        -> federation application and policy (root actionq distribution)
        -> repository protocols
        -> internal actionq.storage boundary
             -> connection + SQL identifiers
             -> immutable migration catalog/ledger/compatibility mechanics
             -> federation/change/completion repositories
             -> read-only legacy archive repository
```

W6 keeps this boundary as internal `actionq.storage` modules because no second real consumer
exists. A separately published distribution is prohibited until a second consumer and an
owner-reviewed packaging RFC prove why distribution independence is needed.
`actionq.storage` must be usable without importing `actionq.application`, `actionq.cli`,
`actionq.vuoro`, `actionq.runner_auth`, `actionq.managed_dispatch`, or `actionq-runner`.
Conversely, schema/migration code must stop importing the monolithic `db.py`; common errors,
connection and identifier validation move behind the primitive seam first. The internal module
name and semantic boundary are frozen; a distribution name is not.

## Staged work packets and falsifying tests

Each packet is a separately reviewable change. A command passing is necessary, not sufficient;
the listed false condition is what the test must actually reject.

### W0 — freeze the oracle (this change)

Add this plan plus repository/deployment and supersession notes in maintained docs only. No
Python, SQL, catalog, package or deployment edits.

```bash
uv run --extra dev pytest tests/test_application_structure.py \
  tests/test_release_contract.py tests/test_repository_retirement_contract.py \
  tests/test_verification_contracts.py -q
python /projects/dev/agentops/templates/dispatch/scripts/validate_verification_artifacts.py --root .
git diff --check
```

Falsifier: any existing execution-plane deletion or repository/verification contract regresses.

### W1 — pin reachability, then split internal modules

Check in `docs/contracts/tranche4-reachability-v1.json` before moving code. It must list
every symbol/file/import edge and external consumer in the table above with one disposition,
owner and falsifying test. `tests/test_tranche4_reachability_contract.py` compares the manifest
to AST/import, CLI-command, catalog-operation, migration-asset and repository-wide consumer
scans and fails on an unclassified new or missing edge. Isolate primitives, legacy repository,
federation repository and migration mechanics inside the existing distribution; keep facades.

```bash
uv run --extra dev pytest tests/test_tranche4_reachability_contract.py \
  tests/test_application_structure.py tests/test_schema.py tests/test_unit.py -q
```

Falsifier: storage imports application/CLI/Vuoro/runner code; a current callable disappears;
or `schema` and `db` retain a circular dependency through the new seam.

### W2 — add independent federation domain and revision authority

Add federation-domain migration 001 and `tests/test_federation_revision_authority.py`. Do not
alter execution migrations 001–012 or add execution migration 013. New roots must be creatable
in `ACTIONQ_FEDERATION_SCHEMA` without an action row or claim receipt. Implement the state
machine, ACL matrix, independent command-decision ledger, expected-absence creation, directed
relations, canonical digests and response-loss replay exactly as frozen above.

```bash
uv run --extra dev pytest tests/test_schema.py \
  tests/test_federation_revision_authority.py tests/test_action_resource_owner.py -q
ACTIONQ_TEST_URL="$DISPOSABLE_URL" uv run --extra dev pytest \
  tests/test_integration_postgres.py -q
```

Falsifier: execution bytes/checksums/maximum drift; execution 013 exists; federation root
requires execution schema; expected-absence or stale revision writes; denied/rejected command
changes a resource or is absent from the decision ledger; relation mutates its target; retry
after response loss changes response; owner mismatch writes or ownership can transfer; an end
actor or legacy runtime credential directly writes a federation resource, resource-change
ledger, command-decision ledger, idempotency binding, or federation migration ledger; the
migration principal writes the resource-change or command-decision ledger; the command service
writes either execution/federation migration ledger; or execution v12 and federation v1 cannot
be checked independently. `$DISPOSABLE_URL` names a task-owned disposable database/schema only.

### W3 — deterministic backfill and rebuild

First ratify and check in the retention/export/restore contract required above. Then add
`tests/test_federation_backfill_rebuild.py` with fixtures covering every legacy lifecycle
state, reclaims/renewals, stale receipts, cancellations, candidate publications, action-resource
cursor pruning and completion recovery floors.

```bash
uv run --extra dev pytest tests/test_federation_backfill_rebuild.py \
  tests/test_claim_authority.py tests/test_completion_log_integration.py -q
```

Falsifier: rerun changes identity/digest; revision gaps or duplicates pass; any v1 change was
pruned; rebuilt projection differs from live projection; legacy claimed/completed implies
acceptance/settlement; export/restore changes canonical projection; or a configured retention
objective cannot be restored.

### W4 — dual-read application and clean Vuoro catalog

Add separate `federation/v1` application/catalog while retaining `execution/v1` byte-for-byte.
Coordinate Vuoro composition/pins in its own repository and PR. Validation has two distinct
gates; the immutable four-domain baseline is not evidence for the future five-domain candidate.

```bash
uv run --extra dev pytest tests/test_vuoro_adapter_integration.py \
  tests/test_federation_catalog_contract.py tests/test_dispatch_v2_contract.py -q

# Baseline: immutable Vuoro commit; proves four domains and frozen execution/v1.
VUORO_BASELINE_SHA=9dc7efa8d3f546851218f49da7653f806d5e8ca4
VUORO_BASELINE_TREE=/tmp/vuoro-baseline-9dc7efa8
git -C /projects/dev/vuoro worktree add --detach "$VUORO_BASELINE_TREE" "$VUORO_BASELINE_SHA"
BASELINE_MANIFEST="$VUORO_BASELINE_TREE/packages/vuoro-service/composition/adapter-pins.json"
BASELINE_WHEELS=/tmp/vuoro-baseline-wheels-9dc7efa8
uv run --project "$VUORO_BASELINE_TREE" python \
  "$VUORO_BASELINE_TREE/scripts/fetch_pinned_adapters.py" \
  "$BASELINE_MANIFEST" "$BASELINE_WHEELS"
uv run --project "$VUORO_BASELINE_TREE" python \
  "$VUORO_BASELINE_TREE/scripts/validate_released_execution_adapter.py" \
  "$BASELINE_MANIFEST" "$BASELINE_WHEELS"
uv run --project "$VUORO_BASELINE_TREE" python \
  "$VUORO_BASELINE_TREE/scripts/validate_released_catalog_composition.py"

# Candidate: work-packet coordinator supplies a literal full commit SHA, never HEAD/branch/tag.
test "${VUORO_CANDIDATE_SHA:?exact 40-hex candidate SHA required}" != HEAD
[[ "$VUORO_CANDIDATE_SHA" =~ ^[0-9a-f]{40}$ ]]
VUORO_CANDIDATE_TREE="/tmp/vuoro-candidate-$VUORO_CANDIDATE_SHA"
git -C /projects/dev/vuoro worktree add --detach \
  "$VUORO_CANDIDATE_TREE" "$VUORO_CANDIDATE_SHA"
CANDIDATE_MANIFEST="$VUORO_CANDIDATE_TREE/packages/vuoro-service/composition/adapter-pins.json"
CANDIDATE_WHEELS="/tmp/vuoro-candidate-wheels-$VUORO_CANDIDATE_SHA"
uv run --project "$VUORO_CANDIDATE_TREE" python \
  "$VUORO_CANDIDATE_TREE/scripts/fetch_pinned_adapters.py" \
  "$CANDIDATE_MANIFEST" "$CANDIDATE_WHEELS"
uv run --project "$VUORO_CANDIDATE_TREE" python \
  "$VUORO_CANDIDATE_TREE/scripts/validate_released_federation_composition.py" \
  "$CANDIDATE_MANIFEST" "$CANDIDATE_WHEELS" \
  --source-revision "$VUORO_CANDIDATE_SHA"
```

`fetch_pinned_adapters.py` is the required fetch/stage procedure: it accepts only canonical
GitHub release-wheel URLs from the manifest, downloads each exact artifact into the supplied
task-owned directory, verifies every manifest SHA-256, and rejects an existing mismatched file.
The candidate version must require exactly the five selected domains rather than the baseline
script's four. The new whole-composition validator is a required Vuoro-candidate deliverable. It must prove
exactly five descriptors (`work`, frozen `execution/v1`, `federation/v1`, `knowledge`, `audit`),
legacy execution operation bytes/hashes, a changed global revision, federation actor ACL and
expected-revision schemas, and stale-catalog rejection before any ActionQ write.
It emits a validation record bound to the literal `VUORO_CANDIDATE_SHA` passed above; branch,
tag, working-tree state and symbolic `HEAD` are never source-revision proof.

Falsifier: baseline is not the literal pinned commit or fails its four-domain/global-revision/
execution-v1 assertions; candidate uses a mutable ref or lacks a literal full SHA; a wheel was
not fetched from the manifest and digest-verified; candidate lacks the five-domain validator;
new catalog uses `execution/*` or contains claim/lease/managed-dispatch/group/harness fields;
old execution hashes drift; global revision is unchanged; stale revision reaches ActionQ;
external execution ref is a launch instruction; mutation omits expected revision/ACL; manifest
lacks exact ActionQ/Vuoro SHAs, wheel digest or separate federation descriptor; or release
order differs from below.

### W5 — consumer cutover and legacy-write fence

Require zero known writers from Vuoro, legacy q-spec-derived integrations, gitops/devbox
automation and other deployed clients. Disable application/catalog/CLI writes, revoke/delete
legacy writer credentials, revoke DML/sequence privileges from runtime roles, and grant a
distinct archive-reader role only redacted views that exclude claim receipts, runner proofs,
request payload secrets and credentials. Database grants—not Python routing—are the fence.

W5 requires a fresh, durable-authoritative evidence set. Pre-fence records 1–4 are captured
after Vuoro deployment/client rediscovery and no more than one hour before the fence. Post-fence
records 3–7 are recaptured/completed no more than one hour after it; W5 is incomplete until
they pass. Each auditctl record binds `captured_at`, environment/cluster identifier, database
endpoint fingerprint (no secret), ActionQ/Vuoro release and deployment revisions, actor,
command/tool versions and result digest. The required records are:

1. complete consumer inventory plus a machine diff against the W1 reachability manifest;
2. served catalog and installed CLI writer-surface scans proving no supported legacy writer;
3. database role **and credential** inventory, including owner/membership inheritance;
4. effective table and sequence grants for execution base tables, archive views, federation
   tables, resource-change ledger, command-decision ledger and both migration ledgers;
5. denial receipts using each former runtime credential for direct table DML and sequence use;
6. denial receipts from the actually installed old ActionQ wheel, not a source-tree simulation;
7. archive-reader receipts proving required redaction and denial of base-table/federation writes.

Repository tests below prove code intent only. They are not current deployment evidence and
cannot substitute for the timestamped, environment-bound auditctl records. A host-local bundle
may index the records but is not the durable authority.

```bash
uv run --extra dev pytest tests/test_legacy_write_fence.py \
  tests/test_repository_retirement_contract.py tests/test_release_contract.py -q
```

Falsifier: any required audit record is missing, stale, environment-unbound, lacks a result
digest, or differs from the current releases/deployment; consumer diff is nonempty;
claim/renew/settle/enqueue/group mutation is reachable from a supported surface; an
old installed binary or direct SQL using former runtime credentials can insert/update/delete
legacy rows or use sequences; archive reader sees raw fenced fields or writes base tables; the
fence changes historical rows; a repository test is presented as deployment evidence; or
rollback silently re-grants legacy execution ownership.

### W6 — internal storage boundary and closure dependencies

Create internal `actionq.storage` modules, retain root compatibility facades and prove copied
schema rollback. Do **not** create a distribution or inspect a new storage wheel without a
second real consumer and separately ratified packaging RFC. Resolve `actionq-runner` by a
separate reviewed retire/extract/transfer decision and prove federation roots have no reach to
it; unresolved disposition blocks W7.

```bash
uv run --extra dev pytest tests/test_storage_boundary.py \
  tests/test_migration_domain_independence.py tests/test_tranche4_rollback.py \
  tests/test_schema.py tests/test_integration_postgres.py -q
```

Falsifier: execution asset/ledger changes; domains cannot migrate/check independently; storage
imports policy/execution modules; root facade changes output; rollback loses a federation fact;
federation imports `actionq-runner`; or runner disposition/zero-reach evidence is absent.

### W7 — destructive archive retirement (not authorized)

This oracle does **not** authorize W7, archive row deletion, old catalog removal, execution
migration-loader removal, credential destruction, or retention expiry. W7 requires a separate
ratified destructive-retirement plan after W1–W6, the pre-W3 retention/export/restore contract,
current zero-consumer evidence, restore rehearsal, and final `actionq-runner` disposition. That
future plan must name exact targets, backups, restore tests, evidence freshness, rollback point,
operator approvals and destruction commands. Until it is approved, the archive and its redacted
read path remain.

## Dispatch and review graph

```text
W0 oracle freeze -> R0 owner + independent architecture ratification
                         |
                         v
W1 reachability/import seams -> R1 boundary review
                         |
                         v
W2 federation domain + revision authority -> R2 schema/ACL/CAS review
                         |
                         v
retention/export/restore decision -> W3 backfill/rebuild -> R3 historical-data review
                         |
                         v
W4 ActionQ catalog -----> coordinated Vuoro consumer/pin PR -> R4 composition review
                         |
                         v
W5 zero-consumer proof + legacy-write fence -> operator cutover decision
                         |
                         v
W6 internal storage seam + runner disposition -> R5 boundary/closure review
                         |
                         v
W7 destructive retirement -> NOT AUTHORIZED; separate ratified plan required
```

W1 and W2 are ActionQ owner work. W3 should be implemented independently from its reviewer.
W4 has separate ActionQ and Vuoro owners. Dormant prerequisite source can merge in the frozen
order, but no partial catalog may be served or cut over. The
q-spec supersession and root/agentops generated-guidance cleanup are separate documentation
work packets owned outside this branch; both must cite the dispatcher tombstone rather than
preserve a compatibility-launcher fiction. W5 requires current external-consumer evidence.
W6 must not start while W0–W5 gates are open.

## Remaining operator decisions

The following are real decisions and are intentionally not guessed here:

1. Ratify or amend the ten coordinator-supplied gates and the revision-authority decision.
2. Before W3, choose the legacy read-retention duration, durable-authoritative export target,
   restore objective and destructive-archive approval path.
3. Approve the final catalog cutover after Vuoro and external consumer evidence is current.
4. Approve any deployment/schema migration execution; this plan authorizes none.
5. If a second real consumer appears, decide whether it justifies superseding the chosen
   internal `actionq.storage` boundary with a published distribution.
6. Before W6 closes, ratify the separate retire/extract/transfer disposition of
   `actionq-runner`; unresolved ownership blocks tranche-4 closure.
7. Decide whether to commission and ratify a separate destructive W7 plan. This oracle leaves
   retirement unauthorized; indefinite redacted archive retention is the safe default.
