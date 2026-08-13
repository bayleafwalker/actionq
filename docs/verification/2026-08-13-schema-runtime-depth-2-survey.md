# ActionQ schema-runtime migration: Depth-2 lifecycle survey

This is a `verify-state-protocols` survey of the ActionQ 0.1.22
`vuoro-schema-runtime` migration, based on pre-change Git revision `8ef1fc9`.
It checks whether sharing pure central-schema primitives changes ActionQ's
database, migration, queue-lifecycle, completion, or catalog semantics. It does
not claim those protocols are defect-free and does not redesign them.

## Closed boundary and retained ownership

ActionQ now consumes the exact GitHub Release wheel
`vuoro-schema-runtime` 0.1.0 at SHA-256
`b66c9357c99aa9e1a7353991ce54105a8621958ecfac47f8c121d80b90b77912`.
The shared package owns only immutable `MigrationAsset` values, SHA-256 text
digests, strict identifier quoting, contiguous-asset validation, and the pure
ledger verdict. It has no database driver and performs no I/O.

ActionQ retains its released `{{schema}}` renderer and byte-exact migrations
001 through 011. It also retains the execution-domain-filtered ledger query,
unversioned-v1 adoption checks, exact schema-3 read-only bridge, schema-8
dispatch-root write fence and quiescence check, transaction/advisory-lock
boundary, complete catalog-shape inspection, runtime-principal rejection,
queue/completion grants, role separation, and all public compatibility states
and details. No migration SQL, catalog definition, handler, or authority path
changed.

## Exact compatibility evidence

The focused schema, release, and Vuoro adapter suites completed with 70 passed
and one built-wheel test skipped before the wheel existed. They assert that all
11 migration files retain their exact bytes and released digests, local schema
rendering is byte-equivalent, ledger reads retain `domain = 'execution'`, the
shared verdict maps back to ActionQ's compatibility contract, and the catalog
remains exactly 26 operations at SHA-256
`8d434e8b347e804c90e48a6598304be84b12f2a61ebc2dbed00a26053239a778`.

The final complete workspace suite then ran against a fresh disposable
PostgreSQL 18.4 cluster with the canonical AgentOps completion-artifact
validator and the built release wheel present, completing with 567 passed and
three environment-specific skips. This includes
migration serialization and retry, legacy adoption, schema-3 compatibility,
schema-8 fencing, schema shape/security/role/grant tests, queue lifecycle,
completion authority, and PostgreSQL concurrency coverage.

All eight protected action-resource owner histories were re-executed against
the final candidate tree and their runtime receipts/results rebound to that
tree. The closed-bundle validator passes. These histories cover pruning,
snapshot races, non-disclosure, recursive redaction, response loss, bounded
wait, legacy quarantine, and stale-session fencing.

## Release-candidate evidence

The local `actionq-0.1.22-py3-none-any.whl` was built once and passed the
`v0.1.22` release contract. Its local SHA-256 is
`43a2b9bcb96441958ab30111ca303e5c284f30a74fac74cd0efcbc7019bf64f5`.
The release contract now validates both exact shared GitHub-wheel URLs and
digests in source metadata, the uv lock, and wheel metadata. The built-wheel
test passes 6/6. This is local candidate evidence only: no tag, push, GitHub
Release, image publication, deployment, or composition change was performed.

## Disposition

This migration is `concurrency-tested` for the repository's existing bounded
scenarios and `example-tested` for exact migration bytes, compatibility
mapping, grants, and catalog equality. Unknown database outcomes and all
documented queue fairness limits remain unchanged. The shared runtime does not
become ActionQ schema, role, completion, or execution authority.
