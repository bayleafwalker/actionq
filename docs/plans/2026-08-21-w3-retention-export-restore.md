# W3 prerequisite — federation retention, export, restore and destructive-archive contract

Status: ratified by the repository owner on 2026-08-21. Required by
`docs/plans/2026-08-20-tranche4-federation-storage-contract-freeze.md` frozen invariant
"Historical data and migration" #6, which makes this contract a prerequisite to W3 rather
than a W7 deferral. W3 exercises the export and restore paths named here.

## 1. Retention duration

Federation v1 data — `federation_resources`, `federation_resource_changes`,
`federation_relations`, `federation_execution_refs`, `federation_evidence`,
`federation_acceptance_decisions`, `federation_settlements`,
`federation_idempotency_bindings`, `federation_command_decisions` and the federation
migration ledger — is retained **indefinitely**. There is no expiry job, no TTL column and
no scheduled deletion. V1 chose no pruning of federation changes (CAS/projection invariant
#4); indefinite retention is the storage-side consequence of that choice.

Expiring any of it requires the separate ratified destructive-retirement plan that W7 is
still waiting on. This contract does not authorize it.
<!-- claim: w3-retention-no-deletion -->

## 2. Durable-authoritative export target

Two distinct mechanisms exist and are **not** substitutes for each other:

| Mechanism | Scope | Retention | Authority |
|---|---|---|---|
| CNPG/Barman continuous backup (`actionq-cnpg.yaml`, `retentionPolicy: "30d"`, Hetzner S3) | whole cluster, physical | 30 days | operational recovery of the live database |
| federation logical export (this contract) | federation tables only, logical | indefinite | durable-authoritative record of federation facts |

The existing 30-day whole-cluster CNPG policy is **unchanged by W3**. It is a database
operations concern with a 30-day window and cannot satisfy indefinite retention, so W3 adds
its own export rather than reinterpreting it.

The federation export is a canonical logical dump of the federation tables produced by
`actionq.federation_backfill.export_federation`, written under an ActionQ-owned path on the
TrueNAS `storage_layer` share:

```
/mnt/truenas/storage_layer/projects/dev/actionq/federation-export/<environment>/<UTC-timestamp>-federation-v1.json
```

The export bytes are `actionq-contracts.canonical_bytes` over a fully ordered
`federation-export/v1` document, so two exports of identical database content are
byte-identical and independently digestible. Text order keys are compared under
`COLLATE "C"`, so byte-identity survives a restore onto a cluster with a different
`lc_collate` and a glibc/ICU collation update on this one — neither of which is a
property of the data.

The document is self-describing. Besides the nine tables it carries the schema version
and compatibility label it was produced under, the producing ActionQ package version, and
the federation migration ledger's `(version, name, checksum)` triples. That last matters
because the first frozen invariant is that those triples never change, and a *restored*
schema's own ledger is written by the restoring wheel — so without carrying them, the
artifact could not evidence the invariant it is retained to evidence. `produced_at` and
`source` are caller-supplied rather than read from the environment, so an export stays a
pure function of its inputs and the byte-identity property above holds when they are
omitted.
<!-- claim: w3-export-byte-identical -->

**Known gap, owned by W5.** This designates a single TrueNAS path as the durable-
authoritative record while demoting a mechanism (CNPG/Barman) that is compressed,
versioned and offsite. The retention *windows* are as stated, but the engineering around
the designated artifact is not yet equivalent: redundancy, snapshot policy, an offsite
copy, a stored digest sidecar and a periodic integrity check are all W5 operator
obligations and none of them exist today, and no export has yet been written to that path.
Scheduling the periodic write is W5 operator deployment work; W3 owns and proves the code
path and the format, and names the target path here so W5 has nothing left to invent.

## 3. Restore objective

Best effort. There is **no** hard RTO or RPO commitment for the federation logical export.

The objective W3 does commit to and prove is correctness, not speed: restoring an export
into a freshly migrated, empty federation schema reproduces the canonical projection of
every federation resource exactly, and re-exporting the restored schema yields byte-identical
export bytes. `actionq.federation_backfill.restore_federation` refuses to write into a
schema that already holds federation resources, so a restore can never partially overlay
live data.
<!-- claim: w3-restore-preserves-projection -->

## 4. Destructive-archive authorization

Manual only, with no automation. No scheduled job, migration, CLI command, or service path
may delete federation rows or an exported artifact. The code carries no delete-capable
federation surface at all, which W3 asserts as a test rather than documenting as a
convention.

Any destructive archive action requires explicit written owner approval recorded against the
separate W7 destructive-retirement plan, which remains unauthorized.

This is proved at the database rather than by convention: W3 asserts that no role the
frozen boundary installs — migration, command, or a denied end-actor role — holds `DELETE`
or `TRUNCATE` on any of the nine federation tables. A source-level check that no module
contains a delete statement would not survive a third module, a later migration, or an
operator at a `psql` prompt; a privilege assertion does.
<!-- claim: w3-no-delete-capable-surface -->

## 5. Backfilled versus native facts

Invariant #5 of "CAS, fencing, projection and rebuild" requires backfilled archive facts to
be distinguishable from post-cutover native federation changes. `federation_resource_changes`
has no dedicated provenance column — its columns are fixed by the frozen
`federation-schema/v1` migration and the `create` change payload is fixed by the module — so
the distinction is **convention over reserved values**, not a schema feature:

1. every backfilled change carries `actor_principal_id = "federation-backfill/v1"`
   (`actionq.federation_backfill.BACKFILL_PRINCIPAL_ID`), a principal id reserved for
   backfill and never issued to a native actor; and
2. every backfilled execution reference carries an `assurance_type` beginning
   `legacy-provenance/` (`actionq.federation_backfill.LEGACY_ASSURANCE_PREFIX`).

`actionq.federation_backfill.is_backfilled_change` is the single reader for (1), and the
principal id is pinned in code rather than being a caller-supplied parameter. W3 asserts
both, and asserts that no backfilled resource ever reaches `accepted`, `rejected` or a
settlement.
<!-- claim: w3-backfilled-distinguishable -->

## 6. What W3 preserves, and what it does not

W3 preserves legacy **identity and shape**, not content. From a federation resource plus
the export you can recover that an action existed, its status at import time, the ids and
types of its events, its action-resource root and recovery floor, its candidate request,
and its parent. You cannot recover anything descriptive or temporal: `action_type`,
`project`, `target_ref`, `created_by`, `priority`, `result_ref`, `failure_reason` and
every legacy timestamp are deliberately not imported, and `federation_execution_refs.
created_at` records the *import* time, not the legacy time. Execution groups, dispatch
requests, managed-dispatch envelopes and individual session-completion events are outside
the mapping entirely; only the completion watermark is imported.
<!-- claim: w3-preserves-identity-and-shape -->

That is correct while the execution schema still exists, and it is the reason W3 is safe
to run before any retirement decision. It stops being sufficient at the point the
execution schema is retired, when this export becomes the only surviving record — at which
point an archive that can say action 4711 completed, but not when or what it was, is
probably not what the owner wants. **Extending the mapping to preserve content is
therefore a prerequisite of any W7 destructive-retirement plan, not of W3.** W7 remains
unauthorized; this contract records the dependency so that decision is made deliberately
rather than discovered afterwards.

Backfilled resource references are deterministic and therefore precomputable by anyone who
knows the environment name. `create` is first-writer-wins with no delete and no
supersede-and-retry path, so a principal holding `federation.create` could occupy them and
permanently block the import. **The backfill must therefore complete before any native
principal is granted `federation.create` in an environment.** The freeze doc's release
ordering already arranges this; stating it here makes the dependency explicit rather than
incidental.

## 7. Backfill is a cutover-time import, not a scheduled job

The mapping records moving legacy values — an action's status, action-resource and completion
recovery floors, the completion log's `last_cursor` — as facts. Each run against a live system
therefore contributes new references for whichever of those have moved, and §1's indefinite
retention plus §4's absence of any delete-capable surface means that growth is permanent and
unreclaimable by design.

That is correct for what backfill is: a one-time import run at cutover, per the freeze doc's
release ordering, possibly resumed a few times. **It must not be scheduled to run
periodically against a live execution database**, which would grow `federation_execution_refs`
and `federation_resource_changes` without bound and with no sanctioned way to reclaim them.
The periodic job that W5 owns is the *export*, not the import.
<!-- claim: w3-cutover-time-import -->

Identity is `(mapping_version, environment, source_id, legacy id)`. Changing any of the first
three starts a new import namespace rather than continuing an existing one — deliberately, so
that re-importing a different database, or under a revised mapping, can never merge into
records already written. `source_id` is a required, explicit operator statement precisely
because "these two databases are the same source" must never be inferred from a shared
environment name.
<!-- claim: w3-source-scoped-identity -->

The `federation-backfill/v1` mapping **has never been applied to any database**. Its
definition — including the reference format and the idempotency-key composition — was still
being corrected up to this pull request's merge, and is frozen from that point. Any change
after it must bump the mapping version, because from then on a schema may exist that was
imported under the old one.

## 8. What this contract does not do

It does not change the execution-schema retention window, the CNPG policy, any Vuoro
catalog, or W7's authorization state.

## 9. Falsifiers

Every claim carrying an inline `claim:` HTML-comment marker above appears here with the test
that would fail if it were untrue, and the scope that test actually establishes. `tests/test_falsifier_coverage.py`
enforces that the two sets agree, that each named test exists, and that each declared scope is
restated in that test's own docstring — so a claim cannot be broadened without touching the
test that is supposed to prove it.

```falsifiers
{
  "minimum_coverage": 0.85,
  "falsifiers": [
  {
    "id": "w3-retention-no-deletion",
    "claim": "Federation v1 data is retained indefinitely; nothing expires or deletes it.",
    "scope": "no role the frozen boundary installs holds DELETE or TRUNCATE on any federation table",
    "test": "tests/test_federation_backfill_rebuild.py::test_no_federation_pruning_capability_exists"
  },
  {
    "id": "w3-export-byte-identical",
    "claim": "Two exports of identical database content are byte-identical, so the artifact is independently digestible.",
    "scope": "within one producing wheel, with produced_at and source omitted",
    "test": "tests/test_federation_backfill_rebuild.py::test_export_restore_round_trip_preserves_the_canonical_projection"
  },
  {
    "id": "w3-restore-preserves-projection",
    "claim": "Restoring an export into a freshly migrated empty schema reproduces every resource's canonical projection exactly.",
    "scope": "into a freshly migrated, empty federation schema",
    "test": "tests/test_federation_backfill_rebuild.py::test_export_restore_round_trip_preserves_the_canonical_projection"
  },
  {
    "id": "w3-no-delete-capable-surface",
    "claim": "Destructive archive action is manual only; no code or role can prune a federation row.",
    "scope": "It says nothing about a superuser or the table owner, which is why destructive archive stays a manual, owner-approved action rather than something the schema alone can prevent.",
    "test": "tests/test_federation_backfill_rebuild.py::test_no_federation_pruning_capability_exists"
  },
  {
    "id": "w3-backfilled-distinguishable",
    "claim": "Backfilled archive facts are distinguishable from post-cutover native federation changes.",
    "scope": "by reserved actor principal id and legacy-provenance assurance prefix",
    "test": "tests/test_federation_backfill_rebuild.py::test_backfilled_changes_are_distinguishable_from_native_changes"
  },
  {
    "id": "w3-preserves-identity-and-shape",
    "claim": "W3 preserves legacy identity and shape: every lifecycle state, reclaims, renewals, stale receipts, cancellations, candidate publications, cursor pruning and completion floors.",
    "scope": "identity and shape only; descriptive and temporal legacy content is deliberately not imported",
    "test": "tests/test_federation_backfill_rebuild.py::test_backfill_imports_every_legacy_lifecycle_state_as_provenance"
  },
  {
    "id": "w3-source-scoped-identity",
    "claim": "Two unrelated legacy databases sharing an environment name cannot merge their provenance.",
    "scope": "deterministic references are a function of the source, so the merge is impossible rather than detected",
    "test": "tests/test_federation_backfill_rebuild.py::test_a_second_legacy_database_under_one_environment_cannot_merge"
  },
  {
    "id": "w3-cutover-time-import",
    "claim": "Backfill is a cutover-time import and must not be scheduled periodically against a live execution database.",
    "scope": "operator-procedural: no code surface schedules the import, and the periodic job W5 owns is the export",
    "test": null,
    "gap": "Procedural, not mechanisable here. Nothing in the package schedules a backfill, so there is no code path to assert against; the enforceable half is W5 deployment configuration, which this repository does not contain. Revisit when W5 lands a scheduler."
    }
  ]
}
```
