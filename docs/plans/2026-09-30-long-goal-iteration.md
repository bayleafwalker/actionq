# ActionQ long goal — state and plan, 2026-09-30

**Status:** state note, written by a background agent session. It changes no decision; it
records which decision now governs and what that leaves to do. Checked against the artifacts
named in each row, not against `HANDOFF.md`'s own claims.

## Headline: the HANDOFF goal is superseded by D1 (retiring)

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
| Tranche 4 (lease/claim extraction) | **superseded**, not next | D1 |
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
4. **Close the stale actionq sprintctl items** that D1 contradicts, each by a reject/supersede
   decision citing D1: #2112, #2114, #2123, #2094, #2128, #1445, #2164, #2166, #2198, #2199, #2200,
   #2202 (execution, dispatch-lifecycle and VUORO-CP lease items). #2081 (quick-start/daemon
   plan status reconciliation, active) can close against this note once the banners merge.
5. **Archive** (operator): final tombstone README commit, then archive the GitHub repository.
   The `actionq-pg` LoadBalancer retires separately at S5 (TS-10) and is not part of this.

## Next bounded unit in this repository

None that is code. The only actionq-side change still worth making before archive is making
the docs say what governs: a supersession banner on `HANDOFF.md` §2/§5 and on the W5+ backlog
and tranche-4 freeze, pointing here, so no later session starts tranche 4 again. That banner
ships with this note.
