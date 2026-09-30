# ActionQ long goal — state and plan, 2026-09-30

**Status:** state note, written by a background agent session. It changes no decision; it
records which decision now governs and what that leaves to do. Checked against the artifacts
named in each row, not against `HANDOFF.md`'s own claims.

**Corrected 2026-09-30 (operator).** The headline below misread D1 against the extended plan
and is withdrawn. The section *Correction* that follows governs; the original text is kept
under it, unedited, for history.

## Correction: the abstraction is relocated per split horizon, not superseded

The operator, on reading this note: *"I explicitly needed that abstraction. Check the extended
plan documentations."* and *"The whole point was to enable \*somewhere\*. Not explicitly or
necessarily actionq. There should be a split horizon model."*

**What this note got wrong.** It read the register's `actionq` row (vuoro
`docs/direction/disposition-register.yaml`, derived 2026-09-17 under delegation: *"no new
execution, federation or placement work"*) as retiring the goal. The row retires ActionQ as the
**host**. D1 itself (`/projects/dev/_artifacts/agentops/session-notes/2026-09-14-owner-decisions.md`) is the
product sentence *"Vuoro owns the semantics of agent-performed work (release, evidence,
decision) inside sprintctl's served authority"* and names neither ActionQ nor federation. The
direction (vuoro `docs/plans/2026-08-22-long-term-direction.md` §10, :686: *"Federation
becomes three authority-plane capability contracts — `federation.principal/v1` (frozen),
`federation.grant/v1` (iterates), `federation.resource/v1` (the W3 ledger) — …"*; §6:
coordination is *"ActionQ today; Restate, Hatchet, DBOS, Temporal-class challengers"*; §7.3,
:556: ActionQ's *"durable long-term contribution may become the invariant suite, migration
evidence, and adapter rather than the execution implementation itself"*), with the extended
plan (vuoro `docs/plans/2026-08-22-extended-sprint-plan.md`, Week 4 items 11–13) as its
ActionQ-side sprint, define the abstraction as
provider-neutral contracts with ActionQ as the first provider, not as ActionQ code. Records
dated after D1 restate the need: vuoro `2026-09-19-agentic-pipeline-first-principles-rebuild.md`
(R3, :44 and :119, and "Coordination: leased claims, effect grants, resource reservations —
Build"); agentops `2026-09-26-cloud-environments-options-memo.md` Open question 3
(recommendation, :344–350: an exclusive owner-backed lease);
agentops `2026-09-26-trusted-service-boundary-design.md` (EffectIntent lifecycle and settlement
acceptance); agentops `docs/architecture/2026-09-27-agentic-ecosystem-and-split-horizon.md`
(#255) and `docs/plans/2026-09-27-backlog-ideation.md` R4 (#253: Decision 1 A, the normative
lease semantics, INV-L1/L2/E1).

**The abstraction, precisely.** The authority plane separated from execution, as `HANDOFF.md` §2
"done when" states it (work identity, relations and revisions; authority; evidence requirements
and acceptance; references to external executions; reconciliation; no daemon, queue or
fan-out), made concrete from two sources. The tranche-4 freeze's *Chosen federation contract v1* is
the reference for the revisioned CAS resource aggregate, the actor/ACL matrix, the
command-decision and idempotency ledger keyed by environment, principal, operation, key and
request digest, and acceptance of cited evidence kept distinct from settlement; the W4 rescope
§3 adds the mint-once principal (`issuer:subject:epoch`) and environment-scoped grants. The
**lease is not part of that contract**: the freeze's *Decision* (:28–31) classes claim, renew
and sweep as execution-plane semantics. The fenced claim/lease with a generation enters the
abstraction from records dated after D1: agentops #253 R4 (the seven normative lease
semantics, INV-L1/L2) and the vuoro 2026-09-19 rebuild R3 (:44, :119). ActionQ's fenced-claim
tests are a cross-check for it, not its reference.

**Where each part lives now** (split horizon, #255 §3.0–§3.2: the coordination plane records
and proposes; the protected horizon owns acceptance, credentials and effect):

| Part | Horizon | Home | State (2026-09-30) |
|---|---|---|---|
| Fenced claim/lease, generation, stale takeover (reference: #253 R4, INV-L1/L2; rebuild R3) | coordination | sprintctl `work.lease.*` (agentops #2520, sprintctl 0.9.0; 0.10.x), served through vuoro-service and vuoro.cloud | shipped; inline in sprintctl's PostgreSQL store, with no provider-neutral interface |
| Command-decision / idempotency ledger | coordination | sprintctl shared idempotency ledger (agentops #2542) | shipped |
| Resource identity, revision CAS, relations, supersession | coordination | sprintctl work items and Release (S3) | shipped as work state; `federation.resource/v1` has no provider (vuoro's adapter shim still names this repository) |
| Effect proposal | coordination | sprintctl `work.effect.propose` (agentops #2541) | shipped |
| Digest-bound acceptance, settlement | protected | `work.effect.accept` / `mark-applied` held only by a trusted-side principal; `credctl accept`; homelab reconciler (M2-2) | partly shipped |
| Principal issuance (mint-once) | protected | Vuoro identity (vuoro #53) and vuoro-cloud's per-subject epoch | issuer half built; epoch minting not confirmed here |
| Grant | protected | cred-broker grant evidence (register `effect-grant` row, S7) | deferred while D2 stands |

**Consequences for this repository.**

- ActionQ may still retire. What must survive it is the contract and its invariants (the
  freeze's *Chosen federation contract v1* and *Frozen invariants*), as the reference the
  providers above conform to for the resource aggregate, the ACL matrix, the command-decision
  ledger and acceptance distinct from settlement. Lease conformance points at #253 R4, not at
  ActionQ's fenced-claim tests. Archive therefore waits on the contract being carried to its new
  home, not only on the Release-test harvest.
- Tranche 4 *inside this repository* stays not-next, and for a better reason than the one
  given below: the freeze's own *Decision* forbids packaging claim/lease out of ActionQ. The
  recommended next unit (open; the operator has not decided it) is a narrow, provider-neutral
  interface on the coordination-horizon provider (sprintctl), outside this repository.
- *Remaining work* item 1 widens from the immutable-action invariants to the whole contract
  (command-decision ledger, ACL matrix, acceptance distinct from settlement; lease semantics
  per #253 R4, with ActionQ's fenced-claim tests only as a cross-check); item 2's
  "or drop" is withdrawn — the ownership projection is a derived query over the resource
  contract, not optional.

## Original headline (withdrawn 2026-09-30): the HANDOFF goal is superseded by D1 (retiring)

`HANDOFF.md` §2 (2026-08-20) set the goal *"reduce actionq from an execution plane to a
federation layer"*, with tranche 4 (lease/claim extraction) and the W1–W7 federation packets as
the path. That goal no longer governs:

- **D1, decided 2026-09-17** (agentops `docs/plans/2026-09-17-target-state.md`, TS-1/TS-2; vuoro
  `docs/direction/disposition-register.yaml`, entry `actionq`): ActionQ is `status: retiring`.
  Intention: *"Until S3 lands, the freeze and grant-conflict invariants are the reference for
  sprintctl's Release; no new execution, federation or placement work."* Nothing supersedes
  actionq; there is no successor federation layer to build here.
- The decided residue step in that entry: *harvest the freeze and conflict-refusal invariants
  into S3 Release tests in sprintctl, make any resource projection (action-resource ownership,
  the 2026-08-27 effect-intent pilot) a derived query, and archive the repo after S3.*
- **S3 is complete and live since 2026-09-19** (agentops handoff
  `docs/dispatch/handoffs/2026-09-19-s3-d14-closed-next-s6.v1.md`: sprintctl 0.6.0, work schema
  16, vuoro-service 0.1.67/0.1.68). The archive precondition "after S3" is therefore met; the
  harvest is not evidenced anywhere (no sprintctl test references these invariants).

Consequence: tranche 4 (extracting lease/claim out of `db.py`/`schema.py` behind an
`actionq.storage` seam) is **not** the next unit. It would be new federation/storage work in a
repository whose disposition is archive. This session started that refactor, confirmed the
supersession, and discarded it uncommitted.

## Step / tranche / wave status

| Item | Status | Evidence |
|---|---|---|
| Step 1 fence experiment | closed | HANDOFF §2, F9 |
| Step 2 harness qualification | closed; follow-ons moot (adapters deleted in tranche 3) | PR #29/#30 |
| Step 3 deletion, tranches 1–3 | done in code | PR #30 |
| §8.1 cluster rollout | done | the HTTP server deployment deleted 2026-09-01 (orphan record under `docs/evidence/`, dated 2026-09-01); `actionq-db`/`actionq-db-proxy` dropped by appservice #1676/#1678 (2026-09-18) |
| §8.2 devbox `actionq-dispatch.service` | done | `ssh devbox-agent systemctl is-enabled actionq-dispatch.service` → `not-found` (2026-09-30); gitops-nixos `scripts/check-actionq-retirement.sh` enforces absence of `modules/system/actionq-dispatch.nix` |
| W0 freeze, W1 reachability pin | done (W1's module split never started) | PR #31, #33 |
| W2 federation authority, W3 backfill/export, W4 serving surface | done in code, **never served** | PR #34, #35, #42; register: no federation migration job ever ran |
| W5 5.0 principal_epoch | done | vuoro-cloud 61649d5, vuoro 58f2575 (2026-08-28) |
| W5 5.3 federation schema init | **moot**: staged under `vuoro-dev-db/app/staged-federation/` (appservice 1e1d23d8), then the whole tree was dropped | appservice 890a8781 (#1678, 2026-09-18) |
| W5 5.4–5.6 Vuoro binding | **moot**: vuoro #71 (0.1.61, 2026-09-16) unbound ActionQ's execution domain; no actionq wheel is pinned in vuoro; only a federation shim exists | vuoro 18039fa, 2e77cb1 |
| W5 5.7–5.13, W6, W7 | **moot** under D1; no consumer, no deployed database | register `actionq` entry |
| Tranche 4 (lease/claim extraction) | ~~**superseded**~~ **relocated** (see *Correction*); not next in this repository | D1 as corrected; freeze *Decision* |
| agentops#2467 "ActionQ durable intent-lifecycle operation" | **not actionq's**: #2467 (E3) is done (vuoro #130/#132, 0.1.76); the intent lifecycle moved to sprintctl `work.effect.*` (agentops#2541, sprintctl 64ab0c9, accepted 2026-09-30) | vuoro `packages/vuoro-mcp-edge/src/vuoro_mcp_edge/effect_tools.py:20-35` still names ActionQ as intent-store owner: stale, a vuoro change |

Vuoro's remaining reach into actionq (all lazy imports, none of lease/claim/renew/settle):
`vuoro_adapter_kit/adapters/execution.py` (`actionq.application`, `actionq.vuoro`),
`adapters/federation.py` (`actionq.vuoro_federation`), `scripts/verify_pre_migration_startup.py`
(the schema module and the root `migrate` facade) and one specialized test. None is exercised by the
served composition, which pins no actionq wheel. These are vuoro-side removals before archive.

## Remaining work, in order

1. **Harvest the invariants into sprintctl Release tests** (sprintctl repo; the decided step).
   Source invariants: `actionq/application_enqueue.py:48-73` (dossier L3) and
   `tests/test_integration_immutable_actions.py`: exact replay returns the one existing record
   (`replayed: True`, no second row); the same logical identity with changed bytes is refused
   and leaves no row; binding (the runtime grant) revalidates authoritative stored bytes and
   fails closed on corrupt bytes without advancing state; exactly one grant per binding. The
   unit maps each to a Release operation and adds a sprintctl test that fails if the
   invariant is lost. Bounded; needs someone to read sprintctl's Release first.
2. **Derived-query disposition of the resource projections** (action-resource ownership,
   `docs/contracts/action-resource-owner-v1.md`; the 2026-08-27 effect-intent pilot). The
   effect-intent half is already superseded by sprintctl `work.effect.*`; the ownership half
   needs a one-line owner answer: derived query in sprintctl, or drop.
3. **Vuoro-side detach** (vuoro repo): delete the `execution`/`federation` adapter shims and the
   actionq lines of `verify_pre_migration_startup.py`; correct the `effect_tools.py` docstring
   that names ActionQ as intent-store owner.
4. **Close the stale actionq sprintctl items** ~~that D1 contradicts, each by a reject/supersede
   decision citing D1~~ *(amended 2026-09-30: the VUORO-CP lease items close as superseded by
   sprintctl `work.lease.*`, agentops #2520, not by citing D1; the execution and
   dispatch-lifecycle items close against D1 as before)*: #2112, #2114, #2123, #2094, #2128, #1445, #2164, #2166, #2198, #2199, #2200,
   #2202 (execution, dispatch-lifecycle and VUORO-CP lease items). #2081 (quick-start/daemon
   plan status reconciliation, active) can close against this note once the banners merge.
5. **Archive** (operator): final tombstone README commit, then archive the GitHub repository.
   *(Amended 2026-09-30: precondition — the contract text and its conformance tests have a home
   outside this repository first; see* Correction.*)*
   The `actionq-pg` LoadBalancer retires separately at S5 (TS-10) and is not part of this.

## Next bounded unit in this repository

None that is code. The only actionq-side change still worth making before archive is making
the docs say what governs: a supersession banner on `HANDOFF.md` §2/§5 and on the W5+ backlog
and tranche-4 freeze, pointing here, so no later session starts tranche 4 again. That banner
ships with this note.

*Corrected 2026-09-30:* the banners now say "relocated per split horizon, not superseded" and
point at *Correction* above. Still no code unit in this repository: the next unit of the
abstraction is, as a recommendation not yet decided by the operator, a provider-neutral
claim/lease and effect-authority interface on the coordination-horizon provider, which would
be sprintctl's to build.
