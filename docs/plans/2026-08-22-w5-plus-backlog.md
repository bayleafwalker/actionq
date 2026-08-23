# W5+ backlog

Status: **ratified** (2026-08-22), with 5.2 and 5.8 decided in §5.2 and §5.8 below and one
factual correction to the draft (see *Baseline*). Generated from what the ratified documents
already defer forward; every item cites where its obligation originates. This is the list the W4 rescope
§8 said a W5 author should start from, expanded to everything W0–W4 handed on. It authorizes
nothing: W5 work is operator-owned, and W6/W7 carry their own gates.

**2026-08-23 realignment.** This backlog was realigned to the requirements pathway
(vuoro `docs/plans/2026-08-23-requirements-pathway-v5-v7.md`, draft for owner ratification) under
owner constraints C-1 (breaking changes allowed; correctness rules untouched) and C-2 (hand-off to
a dumb orchestrator + cheap implementer per gated release). Changes: baseline moved to actionq
`fcf8259` / wheel v0.1.28; 5.1 is done; 5.3's CLI prerequisite is done (actionq #46,
`actionctl federation migrate|check-compatibility`) and 5.3 is reframed as an appservice GitOps PR
scoped to `vuoro-dev` only (D-4 default); every 5.x item carries a `Pathway:` back-reference so the
two documents cannot drift unnoticed; `[C-1]` D-7 moves "remove `execution/v1` from the served
catalog" from *Later / v2* into v5 scope pending the owner's yes; W7 stays not authorized (D-10).
Nothing here is newly authorized by the realignment itself.

Sources: tranche-4 freeze (`2026-08-20-tranche4-federation-storage-contract-freeze.md`, §W5–W7
and release order items 3–8), W3 retention (`2026-08-21-w3-retention-export-restore.md`, known
gap), W4 rescope (`2026-08-22-w4-authority-plane-rescope.md`, §5, §7, §8), Vuoro composition v4
freeze (§6 proof cases, §7).

Current baseline: actionq `fcf8259` / wheel v0.1.28 (`b5f1d6fc…`), vuoro `ab00d29`, reference
profile revision `23cfc276…`. (Previous baseline: `f26e50e` / v0.1.27 `11bcedf6…`; 0.1.28 adds the
federation migrate/check-compatibility CLI, actionq #46.)

**Correction to the draft, since verified.** The draft stated that the federation schema reports
`uninitialized` in every environment. That was inference from code
(`federation_schema.check_compatibility` returns `uninitialized` when the ledger is absent), not
an observation, and the document said so.

It has since been checked against the cluster, read-only: **no federation migration job has ever
existed in any namespace of the appservice cluster.** The `vscode` namespace carries
`actionq-schema-migrate-v1` … `-v12` (execution domain; v12 completed 2026-08-15T11:26:45Z, run
from `ghcr.io/bayleafwalker/actionq-server@sha256:2d5121cf…`) and nothing federation-named
anywhere. Since the federation ledger can only be created by that job, the schema is
uninitialized cluster-wide, and 5.3 is the first thing that would change it.

Two facts found in the same pass that 5.3 and 5.6 both need, and which no document recorded:

* **There are three vuoro-service deployments, on three different image digests** — `vuoro-dev`,
  `vuoro-shared`, and `agent-cockpit` in the `vscode` namespace. "Deploy that validated
  composition" (release-order step 5) is therefore not one action, and which of the three is in
  scope is an input 5.3 and 5.6 need before either runs. Pathway D-4 default: **`vuoro-dev` only**
  for v5; `vuoro-shared` and `agent-cockpit` follow in v5.1 after the rollback rehearsal (5.12).
* **Three migration jobs are suspended, not completed** — `actionq-schema-migrate-v4`,
  `-v5`, and `actionq-schema-v8-quiescence` (superseded by `-v2`). They are execution-domain and
  do not block federation, but a consumer inventory (5.9 record 1) that treats every job as
  evidence of a run would misread them.

## W5 — consumer cutover and legacy-write fence

Ordered. Items marked **critical path** block everything below them; the rest are independent
and can run in parallel with their neighbours. Owner is `operator` unless stated.

| # | Item | Origin | Owner | Blocks |
|---|------|--------|-------|--------|
| 5.0 | **Vuoro Cloud mints and persists a per-subject `principal_epoch`.** Until it exists the gateway path refuses every assertion; only the static-registry path resolves. Critical path for any gateway-issued federation caller. Hand-off: vuoro-cloud `docs/plans/2026-08-23-principal-epoch-backlog.md` (E-1..E-8). Pathway: R1.1.1, R1.1.2; D-2, D-3. | rescope §3, §7 | vuoro-cloud | 5.4, 5.7 |
| 5.1 | **Done.** `vuoro-adapter-kit` pin is 0.1.1 (`pyproject.toml:34`), folded into the 0.1.28 change without a dedicated release. Pathway: baseline. | rescope §8.2 | actionq | — |
| 5.2 | **Decided — see §5.2.** Owners assigned for release-order steps 3 and 5–7. Pathway: G1.2, E1.2.1, D-1. | rescope §7 | decided | — |
| 5.3 | **Initialize the federation schema in `vuoro-dev` via an appservice GitOps PR.** CLI prerequisite **done**: actionq #46 shipped `actionctl federation migrate\|check-compatibility` in v0.1.28. Remaining work is an appservice PR in the `vuoro-dev-db` pattern (`clusters/main/kubernetes/apps/vuoro-dev-db/app/`): `vuoro-federation-migration.secret.yaml` + `vuoro-federation-runtime.secret.yaml` (operator-minted, sops-encrypted; federation's own principals, D-1, never execution's), a grants job in the `vuoro-schema-grants-v1.yaml` shape creating the migration/runtime roles and grants, and a migrate Job running `actionctl federation migrate` pinned to the **0.1.28 wheel digest** (`b5f1d6fc…`, fetched and verified, never a source tree), followed by `check-compatibility` = `compatible`. Scoped to **`vuoro-dev` only** (D-4 default); `vuoro-shared`/`agent-cockpit` are v5.1. Flux applies it — no hand-applied cluster change (it would be pruned, pathway O4). Operator merges (5.2 step 3 ownership unchanged). First state change in the cascade; critical path. Pathway: G1.2, E1.2.1, R1.2.2, R1.2.3 (done); D-1, D-4. | freeze order item 3; actionq #46 | appservice PR (agent-prepared) + operator | 5.4 |
| 5.4 | **Bind `federation.resource/v1` in the Vuoro profile**: second ActionQ provider record (iterative, separate release unit from frozen `execution/v1`), adapter record, closure, attestation. Must not precede 5.3 — binding against an `uninitialized` schema yields an incompatible domain at startup. v4 validator must still reject one release unit backing both exclusive capabilities. Requires the v4 rule 8 filename→digest refinement (v4 freeze Proposed Amendment 2, D-6) so two provider records naming one filename with the same digest are not a collision. Pathway: R1.3.1, R1.3.2; D-6. | rescope §4.6, §8.1; v4 freeze §1 | vuoro | 5.5, 5.6 |
| 5.5a | Wire `w4r-reissued-identity-cannot-inherit` to a test. Its rejection half already exists (`tests/test_federation_catalog_contract.py` refuses an unminted principal before any command executes); its ownership half — an id whose epoch differs failing the `owner_principal_id` comparison — needs a database-backed test that does not exist. The gate binds the whole declared scope to one docstring, so this stays a declared gap until both halves are covered rather than being half-claimed. Pathway: R1.1.3. | rescope §6; W4 build | actionq | — |
| 5.5 | Record the remaining federation contracts as settled in the Vuoro support manifest: `federation.principal/v1` owned by Vuoro's identity plane (scope `global`), `federation.grant/v1` at `scope_kind: environment`; `ownership_evidence` on any provider bound to principal/v1 (the vuoro-cloud E-7 conformance artifact); land the `w4r-grant-scope-matches-implementation` falsifier test there (currently `test: null`). Pathway: R1.3.3. | rescope §2, §3, §6 | vuoro | — |
| 5.6 | Validate the five-domain candidate against fetched, digest-verified wheels; merge, release, deploy that exact composition to `vuoro-dev` (D-4); require clients to rediscover the new global catalog revision. `[C-1]` D-7, pending owner yes: the same v5 release train also drops `execution/v1` from the served catalog as a separately validated revision (moved here from *Later / v2*). Pathway: G1.3; D-4, D-7. | freeze order items 4–6 | vuoro + operator | 5.7 |
| 5.7 | **Backfill completes before any native principal holds `federation.create` in an environment.** Backfill writes under the pinned `federation-backfill/v1`; a concurrent native create leaves unreconstructable provenance in a ledger with no provenance column. Gate every `federation.create` grant on a completed-backfill receipt for that environment. Pathway: R1.4.4 (O7). | rescope §5; freeze W3 | operator | any grant issuance |
| 5.8 | **Decided — see §5.8.** Federation operations are authority-gated, not repository-scoped. Pathway: G1.4 (correctness rule; not loosened by C-1). | rescope §7 | decided | — |
| 5.9 | Capture pre-fence auditctl records 1–4 after deployment/rediscovery and ≤1 h before the fence: consumer inventory + machine diff vs the W1 reachability manifest; served catalog / CLI writer-surface scan; role **and credential** inventory incl. inheritance; effective table + sequence grants on execution, archive, federation, both ledgers, both migration ledgers. Each record binds `captured_at`, environment, DB endpoint fingerprint, release + deployment revisions, actor, tool versions, result digest. The inventory reads job *status*, not existence (suspended v4/v5/v8-quiescence jobs are not runs; pathway O11). Pathway: E1.4.1, R1.4.2. | freeze §W5 | operator | 5.10 |
| 5.10 | **Execute the database privilege fence**: disable application/catalog/CLI writes, revoke/delete legacy writer credentials, revoke DML + sequence privileges from runtime roles, create the distinct archive-reader role with SELECT on redacted views only (no claim receipts, runner proofs, payload secrets, credentials). Grants, not Python routing, are the fence. Pathway: E1.4.3. | freeze §W5, order item 8 | operator | 5.11 |
| 5.11 | Recapture/complete post-fence records 3–7 ≤1 h after the fence: roles/creds, grants, denial receipts per former runtime credential (DML + sequence), denial receipts from the **actually installed old wheel**, archive-reader redaction + write-denial receipts. W5 is incomplete until all pass. Pathway: E1.4.1, R1.4.2; v5 proving point. | freeze §W5 | operator | W6 |
| 5.12 | Rehearse rollback before authoritative cutover: disable federation writes, leave execution v12 intact, and prove rollback does not silently re-grant legacy execution ownership. Pathway: E1.4.3; gates v5.1 (D-4). | freeze item 6; §W5 falsifier | operator | 5.10 |
| 5.13 | W3 retention made operational: schedule the periodic export to the designated TrueNAS path (no export has been written yet), then redundancy, snapshot policy, offsite copy, digest sidecar, periodic integrity check. Independent of the federation chain; should precede 5.10 so the fence never runs without a restorable export. Pathway: G1.5. | W3 known gap | operator | 5.10 (soft) |

W5 exit: records 1–7 current and passing, consumer diff empty, 5.7 receipts for every environment
with a grant, rollback rehearsed. Then the operator cutover decision (freeze dependency graph).

## 5.2 Decided: owners for release-order steps 3 and 5–7

The rule underneath the assignment, because it decides the edge cases rather than just this
list: **the operator owns anything that mutates a live system or needs a credential; the
repository workflow owns anything that is source, validation or release artifact; and no actor
is the sole attester of its own change.**

| Step | Owner | Why this owner, and what it may not do |
|---|---|---|
| 3. run the federation migration with the exact released wheel | **operator, exclusively** (merge + credentials; the GitOps manifests may be agent-prepared — see 5.3) | First state change in the cascade, and it needs a migration role. The freeze already separates those roles — migration principals do not serve and runtime principals do not migrate — and an agent session holds neither. Preceded by verifying the wheel digest by fetch rather than by trust, and by confirming each environment's actual schema state (see *Baseline*). |
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
Listed only so no one reads its absence as permission. Pathway D-10 `[C-1]` confirms: C-1 removes
the consumer reason for caution, not the evidence reason; a separate short plan may be
commissioned only after v5's post-fence records (5.11) pass.

## Later / v2 — recorded so they are proposed as what they are

| Item | Origin | Note |
|------|--------|------|
| Project-scoped `federation.grant/v2` | rescope §2.3 | A v2 change with the schema change that makes a project dimension real; not a v1 edit. |
| Remove `execution/v1` from the served catalog | freeze order item 4 note | **Moved into v5 scope (5.6) by pathway D-7 `[C-1]`, pending owner yes.** Still a separately validated Vuoro release causing another global revision change; never silent. Listed here only until the owner answers D-7. |
| Checkpoint + tail format before any pruning | freeze corrections table | No pruning is permitted until this exists and is separately reviewed. |
| OpenBao `secret.lease/v1` proof case | v4 freeze §6; vuoro #54 | Deliberately absent from the grounded proof cases; needs a real image digest + closure, not a fabricated record. |
| q-spec supersession and root/agentops generated-guidance cleanup | freeze §W-ownership | Separate documentation packets; must cite the dispatcher tombstone. |
| Retire the epoch-less static-registry exception once 5.0 lands | rescope §3 | Static ids are operator-minted today. Pathway D-7 `[C-1]` proposes retiring it with 5.0 (vuoro-cloud backlog E-8), pending owner yes. |
| W6 compatibility facades (6.1) dropped outright rather than phased | pathway D-7 `[C-1]` | Pending owner yes; W6 remains blocked behind every W0–W5 gate regardless. |

## What this document does not do

It assigns no dates, runs nothing, and does not move any frozen text. 5.2 and 5.8 are now
decided above; 5.0 remains the only engineering item that is a prerequisite rather than a
follow-up, and it is owned by Vuoro Cloud rather than by either repository here. The 2026-08-23
realignment changes framing and back-references only; the pathway document itself is still a
draft for owner ratification.

Ratifying this list is not authorization to execute any of it. W5 items remain operator-owned,
W6 stays blocked behind every W0–W5 gate, and W7 stays unauthorized.
