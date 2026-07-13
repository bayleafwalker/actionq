# AGENTS.md — actionq

## Purpose

`actionq` is the Postgres-backed action and session lifecycle owner. `actionctl` is the public write contract; consumers must not write queue tables directly.

## Development workflow

- Use Python 3.11+ and `pytest`.
- Run targeted tests for changed behavior before broader verification.
- Run Postgres integration tests for transaction, locking, claim, timeout, or migration changes.
- Keep action state and its lifecycle event in the same transaction.
- Do not claim stronger ownership, idempotency, or consistency semantics than the implementation and tests establish.

## Stateful protocol work

Shared protocol verification uses `verify-state-protocols`; repo-specific boundaries live in `.agents/overlays/actionq.state-protocols.md`.

- Use `survey` for assessment and review; it is read-only.
- Use `verify` to add models, contexts, fixtures, and tests without changing product semantics.
- Route confirmed defects to a separately authorized build action.
- Treat `full` as a sequence of independent actions, not blanket mutation authority.
- Run protocol review whenever `actionq/db.py`, migrations, claim/sweep behavior, or terminal transitions change.

The governing protocol description is `docs/protocols/action-lifecycle.md`. Reusable test intent lives under `verification/contexts/`; generated histories and result packets belong under CI artifacts or `verification/results/` when intentionally committed.

## Safety boundaries

- Use only a disposable `ACTIONQ_TEST_URL` for mutating verification.
- Never run concurrency or fault tests against the production queue.
- Record isolation level, backend version, seed, bounds, and exercised faults.
- Preserve minimized counterexamples.
- Do not expose database credentials, session secrets, or runtime tokens in packets or logs.

## Verification

```bash
uv run pytest <specific-test-files> -x --tb=short
ACTIONQ_TEST_URL=<disposable-postgres-url> uv run pytest tests/test_integration_postgres.py -q
python /projects/dev/agentops/templates/dispatch/scripts/validate_verification_artifacts.py --root .
```
