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
byte-identical and independently digestible. Scheduling the periodic write is W5 operator
deployment work; W3 owns and proves the code path and the format, and names the target path
here so W5 has nothing left to invent.

## 3. Restore objective

Best effort. There is **no** hard RTO or RPO commitment for the federation logical export.

The objective W3 does commit to and prove is correctness, not speed: restoring an export
into a freshly migrated, empty federation schema reproduces the canonical projection of
every federation resource exactly, and re-exporting the restored schema yields byte-identical
export bytes. `actionq.federation_backfill.restore_federation` refuses to write into a
schema that already holds federation resources, so a restore can never partially overlay
live data.

## 4. Destructive-archive authorization

Manual only, with no automation. No scheduled job, migration, CLI command, or service path
may delete federation rows or an exported artifact. The code carries no delete-capable
federation surface at all, which W3 asserts as a test rather than documenting as a
convention.

Any destructive archive action requires explicit written owner approval recorded against the
separate W7 destructive-retirement plan, which remains unauthorized.

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

`actionq.federation_backfill.is_backfilled_change` is the single reader for (1). W3 asserts
both, and asserts that no backfilled resource ever reaches `accepted`, `rejected` or a
settlement.

## 6. What this contract does not do

It does not change the execution-schema retention window, the CNPG policy, any Vuoro
catalog, or W7's authorization state.
