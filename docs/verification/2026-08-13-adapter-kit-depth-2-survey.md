# ActionQ adapter-kit migration: Depth-2 lifecycle survey

This is a read-only `verify-state-protocols` survey of the ActionQ 0.1.20
adapter-kit migration, based on pre-change Git revision `1126233`. It checks
whether the packaging and catalog refactor changes action lifecycle, claim,
settlement, or schema-transition semantics. It does not claim those protocols
are defect-free and does not redesign them.

## Closed boundary and anchors

The authoritative mutable state remains the PostgreSQL `actions` row. The
append-only `events` relation remains its transactional lifecycle history.
The governing contract is `docs/protocols/action-lifecycle.md`; implementation
anchors are `actionq.db:enqueue`, `claim`, `renew`, `_transition_terminal`,
`cancel`, `acknowledge_cancellation`, `reap_cancellations`, and `sweep`.
Deployment schema authority remains `actionq.schema:migrate` plus the immutable
`actionq/migrations/001` through `011` ledger. No alternate backend, clock,
claimant proof, recovery operation, or projection was introduced.

## Depth-2 scenarios retained

The existing PostgreSQL suite remains the concurrency oracle. It covers two
independent claim connections, cancel/complete row-lock serialization, stale
receipt settlement after reassignment, wrong-worker and expired renewal,
renewal response loss, shutdown/cancellation fencing, migration serialization,
rollback/retry, and runtime-role/schema compatibility. The relevant protected
tests are `tests/test_claim_authority.py`,
`tests/test_cross_authority_fault_matrix.py`, and
`tests/test_integration_postgres.py`.

## Disposition

The product diff is limited to `actionq.vuoro`'s pure schema-builder import and
fresh catalog-data copying, the immutable adapter-kit dependency, version and
release metadata, catalog/release tests, and documentation. It does not modify
`actionq/db.py`, `actionq/application.py`, `actionq/schema.py`, any migration,
or the CLI/server/daemon authority paths. Therefore the transition
preconditions, commit boundaries, event writes, claim receipt and lease checks,
cancellation fencing, migration history, and runtime-role compatibility logic
are unchanged by construction.

The full-suite pass also repaired two stale test-harness expectations without
changing product code: the daemon and its real `actionctl` subprocess now use
the same owner-only test CAS, and the cross-authority oracle expects the frozen
privacy-safe `verification-failed` stop reason instead of the private
`branch-ahead-of-base` detail. The protected cross-authority matrix passes 7/7.
All eight closed action-resource owner histories were re-executed and their
receipts/results rebound to the final candidate-tree digest; none was edited to
claim an unexecuted result.

The post-change full workspace run completed with 558 passed and 5 skipped,
including the 44-test PostgreSQL integration module and the 14-test protected
claim-authority module. This is evidence class `concurrency-tested` for the
existing bounded scenarios. Catalog equality and the frozen canonical hash are
`example-tested`. Unknown outcomes after a lost database response and the
documented fairness limits remain unchanged; this survey makes no stronger
consistency claim.
