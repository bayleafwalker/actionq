# Actionq deployment migration and compatibility contract

Actionq schema changes are deployment operations. `actionq.schema.migrate` and
`actionctl migrate` are the only packaged migration entrypoints; normal CLI
commands and `actionq-server` never invoke them. The service checks the schema
before binding its socket and exits when the schema is absent, incomplete,
modified, or newer than the adapter supports.

The current execution contract reports:

- domain: `execution`;
- API version: `v1`;
- supported schema versions: `1` through `1`;
- one SHA-256 checksum for every packaged migration.

`actionctl check-compatibility` performs only `SELECT` statements and exits
with status `3` for an incompatible schema. Its JSON object is the Actionq
compatibility record consumed by the Vuoro execution adapter and handshake.
The check validates the ledger and the live queue shape: exact column types,
nullability and default expressions; primary keys; foreign-key columns,
targets, and `NO ACTION` update/delete behavior; the exact status-set
expression; and required index columns, ordering, predicates, access method,
and readiness. A matching ledger never overrides damaged queue objects. A
constraint containing all expected status literals plus a permissive branch
such as `OR true` is incompatible.

## Deployment sequence

1. Verify that the selected DSN and `ACTIONQ_SCHEMA` identify the intended
   environment. Development and production use different secret-backed DSNs
   and identities; never copy one into the other environment.
2. Confirm a usable database backup and record its restore point before the
   first migration for a release.
3. Run `actionctl migrate --json-output` in a foreground deployment Job with
   the migration DSN and `ACTIONQ_RUNTIME_ROLE` set to the runtime identity.
   The command takes a transaction-scoped PostgreSQL advisory lock derived
   from the schema name, applies every missing migration in order, validates
   the required table/constraint/index shape, records its checksum, establishes
   bounded runtime grants, and commits atomically.
4. Run `actionctl check-compatibility` using the runtime DSN.
5. Start or restart the service only after both commands succeed. Startup
   repeats the read-only compatibility check and never migrates automatically.
6. Record the image digest, migration report, compatibility record, database
   identity (without credentials), and backup reference as rollout evidence.

Retries are safe: an already-current migration reports an empty
`applied_versions` list. Concurrent Jobs serialize on the same advisory lock;
the second Job re-reads the ledger after acquiring the lock and does not apply
a migration twice. Deployment orchestration should still create one Job per
domain and gate service rollout on its completion so failures remain visible.

The first version also recognizes the exact unversioned schema shipped before
the ledger existed. It validates that schema semantically and records the v1
checksum without replaying table or index DDL. Both historical index-name
families are accepted when their definitions match; an index with a familiar
name but different columns, order, or predicate is rejected. Partial or
modified unversioned schemas are never stamped automatically.

## Database roles

Appservice creates concrete identities and secrets. The Actionq contract
requires two distinct roles:

- The migration role can connect, take advisory locks, create/alter objects in
  the selected Actionq schema, and read/write `schema_migrations`. It is used
  only by a migration Job. Runtime compatibility rejects any principal with
  `CREATE` authority on the selected schema, so a migration owner cannot start
  the server or dispatch work even if it is presented as a runtime DSN.
- The runtime role has schema `USAGE`, queue table DML, sequence usage, and
  `SELECT` on `schema_migrations`. It has no `CREATE` privilege on the schema,
  no ownership of its objects, and no membership in the migration role.

A representative grant shape, with identifiers supplied by the deployment
owner, is:

```sql
REVOKE CREATE ON SCHEMA actionq FROM PUBLIC;
REVOKE CREATE ON SCHEMA actionq FROM actionq_runtime;
GRANT USAGE ON SCHEMA actionq TO actionq_runtime;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA actionq FROM actionq_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON actionq.actions, actionq.events TO actionq_runtime;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA actionq FROM actionq_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA actionq TO actionq_runtime;
GRANT SELECT ON actionq.schema_migrations TO actionq_runtime;
```

The deployment must also set matching default privileges for objects created
by later migrations. Do not grant `CREATE`, migration-table writes, object
ownership, role inheritance from the migrator, or database-wide DDL to the
runtime identity.

The integration gate starts an isolated PostgreSQL cluster under a temporary
directory, listens on a private Unix socket with trust authentication, creates
distinct migration/runtime roles, and removes the cluster afterward. It
proves that the migration identity cannot pass the service startup/dispatch
gate, while the runtime identity can serve normal queue requests and receives
PostgreSQL `insufficient_privilege` for DDL and migration-ledger writes. It
also runs durable rejection-event and exact default/constraint regressions.
The harness refuses to skip when local `initdb`/`pg_ctl` binaries are expected
but unavailable; it never reads or mutates an ambient queue.

## Recovery and rollback

Migration history is append-only. Never edit a released SQL asset or delete a
ledger row to make compatibility pass. If a migration Job fails, keep the
service stopped, retain its logs, and fix or restore the database before
retrying. A transaction failure rolls back the migration and ledger write.

Rollback of application code uses the previously verified image digest only
when its supported schema range includes the observed version. If it does not,
restore the pre-migration database backup or ship a reviewed forward-fix
migration. After restore, run compatibility using the intended rollback image
before starting service traffic.

Persistent `vuoro-dev` uses its isolated database and development-only
identities for migration, restart, retry, concurrency, and restore rehearsal.
Production migration execution, backup integration, concrete role creation,
and Kubernetes Job ordering remain owned by appservice.
