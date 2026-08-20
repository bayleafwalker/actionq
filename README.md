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
| `ACTIONQ_ARTIFACT_ROOT` | Required for verified settlement | None | Server-owned, owner-only durable CAS root used to verify dispatch-result referents before terminal mutation. |
| `ACTIONQ_RUNTIME_ROLE` | Migration Job | None | Simple PostgreSQL role name that `actionctl migrate` grants queue DML, sequence use, and migration-ledger read access. It never grants schema `CREATE` or ledger writes. |
| `ACTIONQ_MAX_CHAIN_DEPTH` | No | `3` | Maximum allowed parent-child depth for enqueued actions. |
| `ACTIONQ_RATE_LIMIT_PER_HOUR` | No | `20` | Hourly enqueue cap for `agent:` and `script:` producers. |
| `ACTIONQ_TEST_URL` | Test-only | None | Separate Postgres URL used by integration tests. |

`ACTIONQ_SCHEMA` must be a simple Postgres identifier: letters, digits, and underscores, not starting with a digit.

## Vuoro execution adapter

`ActionQApplication` is the adapter-safe application core shared by
`actionctl`, the legacy Actionq HTTP façade, and the Vuoro execution adapter.
The owner-provided `actionq.vuoro` module publishes domain-qualified operation
metadata, JSON Schemas, handlers, and the execution compatibility record for
Vuoro composition. It does not add migrations or database access to the
transport-only Vuoro client.

ActionQ depends on the exact, digest-pinned `vuoro-adapter-kit` 0.1.0 GitHub
Release wheel for pure Draft 2020-12 object-schema construction and the shared
schema feature constants. The kit does not own ActionQ's 26 operation schemas,
handlers, registration order, authorization, provenance, lifecycle, or
compatibility behavior; all of those remain in `actionq.vuoro` and the ActionQ
application core.

ActionQ also pins `vuoro-schema-runtime` 0.1.0 for immutable migration assets,
content digests, identifier quoting, contiguous-asset validation, and the pure
migration-ledger verdict. ActionQ still owns database access, its released
`{{schema}}` SQL renderer, execution-domain ledger selection, schema-3 bridge,
schema-8 dispatch fence, shape and authority checks, grants, migrations, and
the public compatibility record.

Served mutation actors come from authenticated identity, require idempotency
keys, and return durable accepted/rejected Actionq decision references. Runner
effects remain machine-local; the service accepts session evidence and exposes
queue/dispatch/session state. See
[the Vuoro execution adapter contract](docs/contracts/vuoro-execution-adapter.md)
for the catalog and preserved claimant-proof limitation.

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

Normal commands and the Vuoro execution adapter fail closed when this check is
not compatible. They never apply migrations as a startup side effect.

Verified `settle` also requires `ACTIONQ_ARTIFACT_ROOT` (or an explicitly
configured application root). The root is server-owned durable storage; the
runner may write result bytes there, but a caller-supplied root is never
trusted. Missing or corrupt result referents leave the ActionQ row, claim, and
events unchanged.

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

## Execution boundary

ActionQ no longer packages an agent daemon, harness adapters, worktree runner,
session wrapper, or standalone HTTP server. Product-native runtimes execute
work directly. ActionQ retains the current action lifecycle and its Vuoro
execution-domain adapter while the follow-on federation extraction is designed
and verified. The historical qualification records under `docs/evidence/`
describe the deleted adapters; they are evidence about those versions, not
instructions for starting a current worker.

`actionq-dispatcher` remains a historical compatibility package whose launcher
expects the retired `actionq-daemon` executable. Do not install or invoke that
launcher with this ActionQ revision. Retiring its package and the nix-managed
devbox unit is a separate cross-repository rollout action.

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
- Runtime identities must not own schema objects, assume an owner role, or
  receive schema `CREATE` or migration-ledger write authority;
  see [the migration and compatibility runbook](docs/operations/schema-migrations.md).
- The Vuoro execution adapter checks compatibility through the same read-only
  owner contract before opening a runtime-role request connection.
- Claims use `FOR UPDATE SKIP LOCKED`, so multiple workers can contend safely.
- Automated producers are rate limited when `created_by` starts with `agent:` or `script:`.
- Child actions cannot exceed the configured chain depth.
- Timestamps are emitted as UTC JSON strings.

## Repository

- Source: https://github.com/bayleafwalker/actionq
- Issues: https://github.com/bayleafwalker/actionq/issues
