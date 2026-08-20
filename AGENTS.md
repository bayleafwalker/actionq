# AGENTS.md — actionq

> Shared environment guidance lives in `/projects/dev/AGENTS.md`.

## Purpose

`actionq` is the Postgres-backed action and session lifecycle owner. It owns
action lifecycle state and its append-only event ledger. `actionctl` is the
public write contract; consumers must not write queue tables directly, use
direct SQL, or import internal database functions to enqueue, claim, complete,
reject, cancel, sweep, or emit action events.

The behavioral contract is `../q-spec/actionq-spec.md`. Product-native runtimes
own worker execution. The sibling `actionq-dispatcher` package is a historical
compatibility launcher for the retired daemon and must not be used with this
revision. Sprintctl owns sprint and work-item state; appservice owns deployment
and cluster mutation.

## Development workflow

- Use Python 3.11+ and `pytest`.
- Run targeted tests for changed behavior before broader verification.
- Run Postgres integration tests for transaction, locking, claim, timeout, or migration changes.
- Keep action state and its lifecycle event in the same transaction.
- Do not claim stronger ownership, idempotency, or consistency semantics than the implementation and tests establish.

## Working rules

- Load `ACTIONQ_URL` before running the CLI. `ACTIONQ_SCHEMA` must remain a
  simple PostgreSQL identifier.
- Preserve lifecycle semantics: `pending -> claimed -> completed|failed|rejected`,
  with cancellation and timeout sweep following the CLI's declared rules.
- Claims must remain atomic and auditable. Changes to claims, deadlines,
  scheduling, parent-child depth, rate limits, or events require focused tests
  and careful migration review.
- Keep action types opaque in this repository. Their external-runtime behavior
  and gates do not belong in queue schema logic.

## Stateful protocol work

Shared protocol verification uses `verify-state-protocols`; repo-specific boundaries live in `.agents/overlays/actionq.state-protocols.md`.

- Use `survey` for assessment and review; it is read-only.
- Use `verify` to add models, contexts, fixtures, and tests without changing product semantics.
- Route confirmed defects to a separately authorized build action.
- Treat `full` as a sequence of independent actions, not blanket mutation authority.
- Run protocol review whenever `actionq/db.py`, migrations, claim/sweep behavior, or terminal transitions change.

The governing protocol description is `docs/protocols/action-lifecycle.md`. Reusable test intent lives under `verification/contexts/`; generated histories and result packets belong under CI artifacts or `verification/results/` when intentionally committed.

## Safety boundaries

- Use only a disposable `ACTIONQ_TEST_URL` for mutating verification. Never
  point integration tests, migrations under development, or exploratory
  scripts at a production queue.
- Never run concurrency or fault tests against the production queue.
- Record isolation level, backend version, seed, bounds, and exercised faults.
- Preserve minimized counterexamples.
- Do not make Kubernetes, Flux, or infrastructure mutations from this repo.
- Do not implement cross-tool transactions with sprintctl, kctl, or auditctl.
- Do not treat event history as a substitute for the authoritative `actions`
  state table.
- Do not expose database URLs, credentials, session secrets, runtime tokens, or
  queue payload secrets in fixtures, packets, logs, events, or documentation.

## Hybrid dispatch

Only `mechanical_bulk` packets are worker-eligible: low-risk implementation
against frozen interfaces, a coordinator-owned oracle the worker cannot
modify, and explicit registered gates that fail for each relevant incorrect
behaviour. The entire `actionq/` authority package remains protected.

Test-oracle and parity-fixture construction, tests as the primary deliverable,
cross-layer behavioural proof, lifecycle/claim/settlement semantics, and any
worker or dispatcher authority change are coordinator-only. One rejected
attempt returns to the coordinator.

## Verification

```bash
uv run pytest <specific-test-files> -x --tb=short
ACTIONQ_TEST_URL=<disposable-postgres-url> uv run pytest tests/test_integration_postgres.py -q
python /projects/dev/agentops/templates/dispatch/scripts/validate_verification_artifacts.py --root .
```

Run the narrowest affected tests first. Exercise `actionctl migrate` only
against an explicitly selected disposable schema or test database.

<!-- agentops-project-pointer:start -->
See `.agents/project.generated.md` for cross-repo project context (agentops-managed; do not hand-edit).
<!-- agentops-project-pointer:end -->

<!-- agentops-environment-pointer:start -->
See `.agents/environment.generated.md` for the active Vuoro environment's constraints and runbooks (agentops-managed; do not hand-edit).
<!-- agentops-environment-pointer:end -->
