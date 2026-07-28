# actionq hybrid-worker overlay

Applies when a low-risk mechanical OpenCode worker implements a frozen `agentops-task/v1`
packet against this repository. The governing contract is
`/projects/dev/agentops/templates/dispatch/hybrid/hybrid-dispatch.v1.json`.

This repository is the queue and dispatch authority for the fleet. That makes
its worker-eligible surface unusually narrow, and the narrowness is the point.

## Worker-eligible work

- Mechanical test support under `tests/`, except the authority tests listed
  below, only when the test is not the primary deliverable and a
  coordinator-owned oracle already discriminates the required behaviour.
- Reusable verification intent under `verification/contexts/`.
- Documentation under `docs/` that restates a contract the coordinator already
  decided, and `README.md`.

## Never worker-eligible

- `actionq/**` — the entire package is authority code. `scope_iterate.py` is
  the path/tool ACL and the `diff-scope-respected` invariant; `harnesses/**`
  builds the worker command line and composes the worker's environment;
  `daemon.py` owns claim receipts, sprintctl claim coordination, child launch,
  and settlement; `routing.py` selects harness/provider/model; `usage_limit.py`
  is the spend control; `db.py` and `migrations/**` own claim atomicity and the
  append-only event ledger; `cli.py`/`server.py` are the public write contracts.
  A worker must never be able to edit the code that bounds workers.
- `tests/test_claim_authority.py`, `tests/test_cross_authority_fault_matrix.py`,
  `tests/test_integration_postgres.py`, `tests/test_scope_iterate_kernel.py`,
  `tests/conftest.py` — these pin the authority and containment invariants and
  the hermetic PostgreSQL harness. Weakening them is how a bad candidate makes
  a gate pass.
- `Dockerfile`, `.dockerignore`, `ops/**`, `examples/**`, `pyproject.toml`,
  `uv.lock` — image, unit, sample-configuration, and dependency/release surface.
- `actionq.dispatch.json`, `.agents/**`, `AGENTS.md`, `.claude/**`, `.github/**`
  — the manifest, overlays, and harness/CI configuration that bound this worker.

## Two worker paths, never composed

`actionq` implements its own worker path (`runner = "scope-iterate"`), whose
`ToolACL` allows `git add` and `git commit` because `ScopeIterateKernel.verify`
requires a *committed* diff. agentops hybrid dispatch denies the worker git
entirely and adjudicates scope over the workspace diff.

A hybrid worker running in this repository is a **subject** of the agentops
contract, never an operator of actionq's. It must not run `actionq-daemon`,
`actionctl`, `dispatcher-once`, or any command that enqueues, claims, sweeps,
or settles an action, and no packet may register one. Granting a hybrid worker
actionq's operator path would hand it the git authority the agentops contract
denies it.

## Stop conditions

Return a structured blocker instead of guessing when the packet would require:

- editing anything under `actionq/**`, or any protected path;
- adding a dependency, changing `uv.lock`, or reaching the network;
- a PostgreSQL-backed test — the hermetic harness needs `initdb`/`pg_ctl` and
  is deliberately not a registered command; that verification is the
  coordinator's;
- resolving an ambiguity in lifecycle, claim, fencing, or migration semantics
  rather than implementing a decided one;
- inventing or modifying the test oracle, constructing parity fixtures, or
  proving cross-layer behaviour;
- weakening, skipping, or deleting an existing test to make a gate pass.

A worker never runs `git`, never touches sprintctl or actionq state, and never
points anything at a live queue. Verification runs only the packet's registered
command ids.
