# W5+ backlog

Status: **ratified** (2026-08-22), with 5.2 and 5.8 decided in §5.2 and §5.8 below and one
factual correction to the draft (see *Baseline*). Generated from what the ratified documents
already defer forward; every item cites where its obligation originates. This is the list the W4 rescope
§8 said a W5 author should start from, expanded to everything W0–W4 handed on. It authorizes
nothing: W5 work is operator-owned, and W6/W7 carry their own gates.

Sources: tranche-4 freeze (`2026-08-20-tranche4-federation-storage-contract-freeze.md`, §W5–W7
and release order items 3–8), W3 retention (`2026-08-21-w3-retention-export-restore.md`, known
gap), W4 rescope (`2026-08-22-w4-authority-plane-rescope.md`, §5, §7, §8), Vuoro composition v4
freeze (§6 proof cases, §7).

Current baseline: actionq `f26e50e` / wheel v0.1.27 (`11bcedf6…`), vuoro `ab00d29`, reference
profile revision `23cfc276…`.

**Correction to the draft.** It stated that the federation schema reports `uninitialized` in
every environment. That is what the code guarantees *until its migration runs*
(`federation_schema.check_compatibility` returns `uninitialized` when the ledger is absent), and
no migration has been run under this plan — but it is not something this document verified. The
appservice cluster was not reachable from where the draft was reviewed, and reading a live
production database to decorate a plan is not a trade worth making. **Confirming the actual
state of each environment is the first action of 5.3**, before the migration job, not an
assumption underneath it.

## W5 — consumer cutover and legacy-write fence

Ordered. Items marked **critical path** block everything below them; the rest are independent
and can run in parallel with their neighbours. Owner is `operator` unless stated.

| # | Item | Origin | Owner | Blocks |
|---|------|--------|-------|--------|
| 5.0 | **Vuoro Cloud mints and persists a per-subject `principal_epoch`.** Until it exists the gateway path refuses every assertion; only the static-registry path resolves. Critical path for any gateway-issued federation caller. | rescope §3, §7 | vuoro-cloud | 5.4, 5.7 |
| 5.1 | Fold ActionQ's `vuoro-adapter-kit` pin from 0.1.0 to 0.1.1 (`pyproject.toml:34`) into the next ActionQ change. **Do not cut a release for it.** | rescope §8.2 | actionq | — |
| 5.2 | **Decided — see §5.2.** Owners assigned for release-order steps 3 and 5–7. | rescope §7 | decided | — |
| 5.3 | **Run the federation migration job using the exact released wheel v0.1.27** (digest-verified fetch, not a source tree). First state change in the cascade. Critical path. | freeze order item 3 | operator | 5.4 |
| 5.4 | **Bind `federation.resource/v1` in the Vuoro profile**: second ActionQ provider record (iterative, separate release unit from frozen `execution/v1`), adapter record, closure, attestation. Must not precede 5.3 — binding against an `uninitialized` schema yields an incompatible domain at startup. v4 validator must still reject one release unit backing both exclusive capabilities. | rescope §4.6, §8.1; v4 freeze §1 | vuoro | 5.5, 5.6 |
| 5.5a | Wire `w4r-reissued-identity-cannot-inherit` to a test. Its rejection half already exists (`tests/test_federation_catalog_contract.py` refuses an unminted principal before any command executes); its ownership half — an id whose epoch differs failing the `owner_principal_id` comparison — needs a database-backed test that does not exist. The gate binds the whole declared scope to one docstring, so this stays a declared gap until both halves are covered rather than being half-claimed. | rescope §6; W4 build | actionq | — |
| 5.5 | Record the remaining federation contracts as settled in the Vuoro support manifest: `federation.principal/v1` owned by Vuoro's identity plane (scope `global`), `federation.grant/v1` at `scope_kind: environment`; `ownership_evidence` on any provider bound to principal/v1; land the `w4r-grant-scope-matches-implementation` falsifier test there (currently `test: null`). | rescope §2, §3, §6 | vuoro | — |
| 5.6 | Validate the five-domain candidate against fetched, digest-verified wheels; merge, release, deploy that exact composition; require clients to rediscover the new global catalog revision. | freeze order items 4–6 | vuoro + operator | 5.7 |
| 5.7 | **Backfill completes before any native principal holds `federation.create` in an environment.** Backfill writes under the pinned `federation-backfill/v1`; a concurrent native create leaves unreconstructable provenance in a ledger with no provenance column. Gate every `federation.create` grant on a completed-backfill receipt for that environment. | rescope §5; freeze W3 | operator | any grant issuance |
| 5.8 | **Decided — see §5.8.** Federation operations are authority-gated, not repository-scoped. | rescope §7 | decided | — |
| 5.9 | Capture pre-fence auditctl records 1–4 after deployment/rediscovery and ≤1 h before the fence: consumer inventory + machine diff vs the W1 reachability manifest; served catalog / CLI writer-surface scan; role **and credential** inventory incl. inheritance; effective table + sequence grants on execution, archive, federation, both ledgers, both migration ledgers. Each record binds `captured_at`, environment, DB endpoint fingerprint, release + deployment revisions, actor, tool versions, result digest. | freeze §W5 | operator | 5.10 |
| 5.10 | **Execute the database privilege fence**: disable application/catalog/CLI writes, revoke/delete legacy writer credentials, revoke DML + sequence privileges from runtime roles, create the distinct archive-reader role with SELECT on redacted views only (no claim receipts, runner proofs, payload secrets, credentials). Grants, not Python routing, are the fence. | freeze §W5, order item 8 | operator | 5.11 |
| 5.11 | Recapture/complete post-fence records 3–7 ≤1 h after the fence: roles/creds, grants, denial receipts per former runtime credential (DML + sequence), denial receipts from the **actually installed old wheel**, archive-reader redaction + write-denial receipts. W5 is incomplete until all pass. | freeze §W5 | operator | W6 |
| 5.12 | Rehearse rollback before authoritative cutover: disable federation writes, leave execution v12 intact, and prove rollback does not silently re-grant legacy execution ownership. | freeze item 6; §W5 falsifier | operator | 5.10 |
| 5.13 | W3 retention made operational: schedule the periodic export to the designated TrueNAS path (no export has been written yet), then redundancy, snapshot policy, offsite copy, digest sidecar, periodic integrity check. Independent of the federation chain; should precede 5.10 so the fence never runs without a restorable export. | W3 known gap | operator | 5.10 (soft) |

W5 exit: records 1–7 current and passing, consumer diff empty, 5.7 receipts for every environment
with a grant, rollback rehearsed. Then the operator cutover decision (freeze dependency graph).

## 5.2 Decided: owners for release-order steps 3 and 5–7

The rule underneath the assignment, because it decides the edge cases rather than just this
list: **the operator owns anything that mutates a live system or needs a credential; the
repository workflow owns anything that is source, validation or release artifact; and no actor
is the sole attester of its own change.**

| Step | Owner | Why this owner, and what it may not do |
|---|---|---|
| 3. run the federation migration with the exact released wheel | **operator, exclusively** | First state change in the cascade, and it needs a migration role. The freeze already separates those roles — migration principals do not serve and runtime principals do not migrate — and an agent session holds neither. Preceded by verifying the wheel digest by fetch rather than by trust, and by confirming each environment's actual schema state (see *Baseline*). |
| 5a. merge and release the validated Vuoro composition | **repository workflow** (agent-prepared, operator-approved at merge) | This is what the whole session just did for #53–#58: CI validates against fetched, digest-verified wheels, and a merge is revertible. Nothing here touches a running system. |
| 5b. deploy that composition | **operator, exclusively** | Deployment mutates the cluster and is the step the freeze names as separate operator work. It is split from 5a deliberately: bundling them would let a green PR imply a deploy. |
| 6. client rediscovery | **operator verifies; nobody performs** | Rediscovery is not a task — the changed global revision forces it, and Vuoro rejects a stale revision `409 stale-catalog` before resolving the operation, never retrying a mutation transparently. So the failure mode is loud, not silent, and what needs an owner is *confirming* no consumer is pinned to the previous revision. |
| 7. capture zero-writer evidence | **operator captures; repository workflow owns the tooling** | The evidence is about whether the operator's own change worked, so the operator captures it under the binding the freeze requires (`captured_at`, environment, endpoint fingerprint, release and deployment revisions, actor, tool versions, result digest). An agent session may keep the capture tooling green and must not author or edit a record — a change and its proof having the same author is the weakest arrangement available, and it is avoidable here at no cost. |

## 5.8 Decided: federation operations are authority-gated, not repository-scoped

**Decision: plain authority-gated.** Vuoro's authorizer gets no repository check for federation
operations, and `_execution_authorizer`'s repo-scoped shape is not copied across.

The reasons are the same class as the `project` → `environment` correction in the rescope §2.3,
and they are structural rather than stylistic:

1. **There is no repository dimension to scope by.** No federation table carries a repo column;
   a resource is an opaque `aqf1_` reference owned by a principal. A repo-scoped check would
   have to invent an association at the serving edge and then persist it nowhere, which is
   precisely the "describes a dimension no implementation has" failure the rescope refused.
2. **Authority-gating is already the implemented model, inside the authority.**
   `FederationAuthority._require_authority` checks a named authority per command
   (`federation.create`, `federation.relate`, `federation.acceptance.decide`, …). A second check
   at the serving edge would duplicate it, and duplicated authorization drifts — the copy that
   is not the one enforcing becomes wrong quietly.
3. **Ownership is the second gate and it is principal-based.** Commands that require it compare
   `owner_principal_id` against the caller. Between "may you do this verb" and "is this yours",
   a repository adds nothing either does not already cover.
4. **Repo-scoping would be actively wrong for what federation is for.** Its subject is external
   execution references and cross-boundary assurance; binding a resource to one repository would
   make a cross-repository reference either unrepresentable or a lie.

**The honest cost, stated rather than buried:** authority-gating means a principal holding
`federation.create` can create resources anywhere in its environment. That is a wider blast
radius than a work operation, whose repo scope is enforced. The boundary that contains it is the
grant, not the authorizer: `federation.grant/v1` is `environment`-scoped (rescope §2.3), and 5.7
gates the first grant in an environment on completed backfill. If finer granularity is ever
needed, it belongs in a v2 grant contract with a real dimension behind it — the same disposition
as project scope — and never as a serving-edge check over data that has no such field.

## W6 — internal storage boundary and closure dependencies

Must not start while any W0–W5 gate is open.

| # | Item | Origin |
|---|------|--------|
| 6.1 | Internal storage seam: root imports and `actionctl migrate` stop being compatibility facades; the storage boundary imports no application or execution policy. | freeze item 5, §W6 |
| 6.2 | `actionq-runner` disposition — retire, extract, or transfer — with a zero-root-reach proof, as a closure dependency rather than "deferred indefinitely". | freeze corrections table |
| 6.3 | R5 boundary/closure review. | freeze dependency graph |

## W7 — destructive retirement

**NOT AUTHORIZED.** Requires a separate ratified plan; nothing in W5/W6 confers deletion authority.
Listed only so no one reads its absence as permission.

## Later / v2 — recorded so they are proposed as what they are

| Item | Origin | Note |
|------|--------|------|
| Project-scoped `federation.grant/v2` | rescope §2.3 | A v2 change with the schema change that makes a project dimension real; not a v1 edit. |
| Remove `execution/v1` from the served catalog | freeze order item 4 note | A separately validated Vuoro release causing another global revision change; never silent. |
| Checkpoint + tail format before any pruning | freeze corrections table | No pruning is permitted until this exists and is separately reviewed. |
| OpenBao `secret.lease/v1` proof case | v4 freeze §6; vuoro #54 | Deliberately absent from the grounded proof cases; needs a real image digest + closure, not a fabricated record. |
| q-spec supersession and root/agentops generated-guidance cleanup | freeze §W-ownership | Separate documentation packets; must cite the dispatcher tombstone. |
| Retire the epoch-less static-registry exception once 5.0 lands | rescope §3 | Static ids are operator-minted today; revisit whether that path should also require issuer epochs. |

## What this document does not do

It assigns no dates, runs nothing, and does not move any frozen text. 5.2 and 5.8 are now
decided above; 5.0 remains the only engineering item that is a prerequisite rather than a
follow-up, and it is owned by Vuoro Cloud rather than by either repository here.

Ratifying this list is not authorization to execute any of it. W5 items remain operator-owned,
W6 stays blocked behind every W0–W5 gate, and W7 stays unauthorized.
