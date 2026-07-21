# actionq

`actionq` is a Postgres-backed action queue for deterministic agent and operator dispatch. It gives you a small, explicit CLI contract for enqueuing work, claiming one item at a time, recording lifecycle transitions, and reading the queue event log without exposing direct SQL writes to consumers.

`actionctl` is the public contract. Consumers should not import the package directly or write to the database outside the queue interface.

## What It Does

- Stores actions in a Postgres schema.
- Supports a strict action lifecycle: `pending`, `claimed`, `completed`, `failed`, `rejected`, `cancelled`.
- Records append-only queue events for auditability and coordination.
- Enforces chain-depth limits for child actions.
- Applies per-source enqueue rate limiting for automated producers.
- Lets dispatchers emit coordinator events without broad database access.

## Install

### Use In A Project

```bash
uv sync
```

### Install As A Tool

```bash
uv tool install /path/to/actionq
```

### Editable Development Install

```bash
uv sync --extra dev
```

## Runtime Contract

`actionq` uses environment variables for connection and schema selection.

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `ACTIONQ_URL` | Yes | None | Postgres connection string used by `actionctl`; deployment migration Jobs and runtime processes use separate role-specific values. |
| `ACTIONQ_SCHEMA` | No | `actionq` | Schema name for queue tables and events. |
| `ACTIONQ_RUNTIME_ROLE` | Migration Job | None | Simple PostgreSQL role name that `actionctl migrate` grants queue DML, sequence use, and migration-ledger read access. It never grants schema `CREATE` or ledger writes. |
| `ACTIONQ_MAX_CHAIN_DEPTH` | No | `3` | Maximum allowed parent-child depth for enqueued actions. |
| `ACTIONQ_RATE_LIMIT_PER_HOUR` | No | `20` | Hourly enqueue cap for `agent:` and `script:` producers. |
| `ACTIONQ_TEST_URL` | Test-only | None | Separate Postgres URL used by integration tests. |

`ACTIONQ_SCHEMA` must be a simple Postgres identifier: letters, digits, and underscores, not starting with a digit.

## Quick Start

Initialize the queue schema with a deployment/migration identity:

```bash
export ACTIONQ_URL='postgresql://user:password@localhost:5432/app'
export ACTIONQ_SCHEMA='actionq'
export ACTIONQ_RUNTIME_ROLE='actionq_runtime'

actionctl migrate
```

Check the same schema with the runtime identity before starting service:

```bash
export ACTIONQ_URL='postgresql://actionq_runtime:password@localhost:5432/app'
actionctl check-compatibility
```

Normal commands and `actionq-server` fail closed when this check is not
compatible. They never apply migrations as a startup side effect.

Enqueue one action:

```bash
actionctl add \
	--type scope-iterate \
	--project sprintctl \
	--target 42 \
	--source doc:plan \
	--created-by human:cli
```

Claim the next pending action:

```bash
actionctl claim --worker worker:dispatcher-1
```

Complete the claimed action:

```bash
actionctl complete 1 --result branch=agent/scope-iterate/1
```

Inspect the action and its event history:

```bash
actionctl show 1
```

## Queue Lifecycle

The normal action flow is:

1. `add` inserts a `pending` action and writes `action_enqueued`.
2. `claim` atomically marks the oldest highest-priority pending action as `claimed` and writes `action_claimed`.
3. A worker finishes with one of `complete`, `fail`, or `reject`.
4. Operators may `cancel` a `pending` or `claimed` action.
5. `sweep` requeues expired claims by clearing claim ownership and writing `claim_timed_out`.

Priority is ascending, so smaller numbers are claimed first.

## Command Surface

| Command | Purpose |
| --- | --- |
| `actionctl migrate` | Create or upgrade the queue schema. |
| `actionctl check-compatibility` | Report the read-only execution API/schema compatibility record. |
| `actionctl add` | Enqueue a new action. |
| `actionctl ls` | List actions with optional status, type, and project filters. |
| `actionctl show ACTION_ID` | Show one action plus all recorded events. |
| `actionctl claim --worker NAME` | Claim the next pending action. Exits with code `2` if none are available. |
| `actionctl complete ACTION_ID --result REF` | Mark a claimed action completed. |
| `actionctl fail ACTION_ID --reason TEXT` | Mark a claimed action failed. |
| `actionctl reject ACTION_ID --reason TEXT --validator NAME` | Reject a claimed action after validation. |
| `actionctl cancel ACTION_ID --reason TEXT` | Cancel a pending or claimed action. |
| `actionctl sweep` | Requeue timed-out claims. |
| `actionctl events` | Read the event log, optionally filtered or tailed. |
| `actionctl emit` | Emit coordinator events without direct SQL writes. |

## Daemon minimum

`actionq-daemon` is the actionq-owned, long-running coordinator. It uses only
the public `actionctl` commands to claim work, emit `session.*` events, and
settle actions. The current minimum is deliberately single-session and supports
the fake runner for disposable verification; it does not import
`actionq-dispatch` at runtime.

Start from [examples/actionq-daemon.toml](examples/actionq-daemon.toml), then:

```bash
mkdir -p ~/.config/actionq
cp examples/actionq-daemon.toml ~/.config/actionq/config.toml
actionq-daemon --config ~/.config/actionq/config.toml
```

For user-systemd operation, install
`ops/systemd/actionq-daemon.service`, reload the user manager, and start the
unit. `SIGTERM` and `SIGINT` stop after a bounded child grace period; `SIGHUP`
reloads configuration between child sessions. The daemon checks the legacy
`~/.config/actionq-dispatcher/config.toml` only when no explicit `--config` is
provided, so existing operators retain a visible migration path.

All state-changing commands return JSON records that are designed to be machine-consumable.

## Data Model

The queue stores two tables inside the selected schema:

- `actions`: the current state of each action.
- `events`: append-only lifecycle and coordination events.

Action records include:

- identity: `id`, `action_type`, `project`, `target_ref`, `source_refs`
- scheduling: `priority`, `status`, `claimed_by`, `claim_deadline`
- lineage: `parent_id`, `chain_depth`, `created_by`
- outcome: `result_ref`, `failure_reason`, `completed_at`

This model is intentionally narrow: mutable action state lives in `actions`, while history and coordination signals live in `events`.

## Coordinator Events

`actionctl emit` supports coordinator-level event types:

- `coordinator_cycle`
- `coordinator_paused`

The payload must be a JSON object.

Example:

```bash
actionctl emit \
	--type coordinator_cycle \
	--actor dispatcher:main \
	--payload '{"claimed": false, "backlog": 3}'
```

## Development

Run the unit test suite:

```bash
uv run pytest -q
```

Run integration tests against a disposable Postgres database:

```bash
uv run pytest tests/test_integration_postgres.py -q
```

The PostgreSQL integration modules start one temporary cluster on a private
Unix socket and create distinct migration and runtime roles before test
collection. The harness requires local `initdb` and `pg_ctl`, creates a fresh
schema per test, refuses to silently skip live database coverage, and never
uses an ambient queue DSN.

## Operational Notes

- The queue schema is created only by the deployment-owned `actionctl migrate`
  entrypoint. It uses a transaction-scoped advisory lock, a version ledger,
  packaged migration checksums, and idempotent retry behavior.
- Runtime identities must not own schema objects or receive schema `CREATE`;
  see [the migration and compatibility runbook](docs/operations/schema-migrations.md).
- `actionq-server` checks compatibility before binding its socket. Its
  read-only `GET /compatibility` endpoint publishes the same record for the
  Vuoro execution adapter.
- Claims use `FOR UPDATE SKIP LOCKED`, so multiple workers can contend safely.
- Automated producers are rate limited when `created_by` starts with `agent:` or `script:`.
- Child actions cannot exceed the configured chain depth.
- Timestamps are emitted as UTC JSON strings.

## Repository

- Source: https://github.com/bayleafwalker/actionq
- Issues: https://github.com/bayleafwalker/actionq/issues
