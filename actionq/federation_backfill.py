"""Deterministic legacy-provenance backfill, rebuild, export and restore for federation v1.

This module is deliberately separate from ``actionq.federation``: backfill has its
own authority (``federation.backfill``), its own read dependency (the legacy
execution schema) and its own determinism requirements, none of which belong in
the hand-reviewed command authority itself.

It records **provenance only**.  Legacy facts reach federation v1 through
``create``/``record-execution-ref``/``add-relation`` and nothing else; a legacy
``claimed``/``completed`` action never becomes a federation acceptance or
settlement.  ``backfill_principal`` enforces that in code rather than by
convention, and ``_PROVENANCE_OPERATIONS`` keeps the command dispatcher from
being widened later.

Retention, export target, restore objective and destructive-archive approval are
fixed by ``docs/plans/2026-08-21-w3-retention-export-restore.md``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

from actionq_contracts import canonical_bytes, sha256_digest as contract_digest

from . import db
from . import federation_schema
from .federation import CommandDecision, FederationAuthority, FederationPrincipal, RESOURCE_RE


BACKFILL_MAPPING_VERSION = "federation-backfill/v1"
# Reserved principal id.  federation_resource_changes has no provenance column
# -- its shape is frozen by federation-schema/v1 -- so "backfilled, not native"
# is carried by this reserved actor id plus the assurance prefix below, exactly
# as the checked-in retention/export/restore contract records.
BACKFILL_PRINCIPAL_ID = "federation-backfill/v1"
BACKFILL_AUTHORITY = "federation.backfill"
# federation.create and federation.relate are the authorities the module's three
# sanctioned commands actually require (federation.py's _require_authority);
# federation.backfill is the ACL row this principal is issued under.
BACKFILL_AUTHORITIES = frozenset({BACKFILL_AUTHORITY, "federation.create", "federation.relate"})
FORBIDDEN_BACKFILL_AUTHORITIES = frozenset({
    "federation.acceptance.decide", "federation.settlement.record",
    "federation.supersede", "federation.evidence.ingest",
})

LEGACY_ASSURANCE_PREFIX = "legacy-provenance/"
LEGACY_EXECUTION_NAMESPACE = "actionq-execution/v1"
LEGACY_COMPLETION_NAMESPACE = "actionq-completion/v1"
# The environment is joined into legacy references with ":" separators, so the
# mapping is injective only while the environment cannot contain one.
ENVIRONMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

_PROVENANCE_OPERATIONS = ("create", "record-execution-ref", "add-relation")
_PROJECTION_SCHEMA_VERSION = "federation-projection/v1"
_EXPORT_SCHEMA_VERSION = "federation-export/v1"
_REBUILD_STATES = {
    "create": "registered", "record-evidence": "evidence-recorded", "supersede": "superseded",
}


class BackfillDrift(db.ActionQError):
    """The legacy source changed under an import that is already part-recorded."""

    def __init__(self, message: str, *, resource_ref: str, fact: str):
        super().__init__(message)
        self.resource_ref = resource_ref
        self.fact = fact


class BackfillRejected(db.ActionQError):
    """A provenance command was rejected; the import is not deterministic."""

    def __init__(self, message: str, *, code: str, operation: str, resource_ref: str | None):
        super().__init__(message)
        self.code = code
        self.operation = operation
        self.resource_ref = resource_ref


class RebuildError(db.ActionQError):
    """A change ledger could not be replayed into a canonical projection."""

    def __init__(self, message: str, *, code: str, resource_ref: str, revision: int | None = None):
        super().__init__(message)
        self.code = code
        self.resource_ref = resource_ref
        self.revision = revision


class ExportError(db.ActionQError):
    """An export document could not be produced or restored."""


def _require_environment(environment: Any) -> str:
    if not isinstance(environment, str) or not ENVIRONMENT_RE.fullmatch(environment):
        raise db.ActionQError("backfill environment must match " + ENVIRONMENT_RE.pattern)
    return environment


def _require_source_id(source_id: Any) -> str:
    """Names the legacy database an import came from; part of every identity."""
    if not isinstance(source_id, str) or not ENVIRONMENT_RE.fullmatch(source_id):
        raise db.ActionQError("backfill source_id must match " + ENVIRONMENT_RE.pattern)
    return source_id


def _require_action_id(action_id: Any) -> int:
    if isinstance(action_id, bool) or not isinstance(action_id, int) or action_id <= 0:
        raise db.ActionQError("legacy action id must be a positive integer")
    return action_id


def legacy_execution_ref(*, environment: str, source_id: str, action_id: int) -> str:
    """Stable opaque legacy identity for one execution-schema action.

    Recorded as an execution *reference string* only.  Nothing joins on it and
    no federation row carries an execution-schema foreign key, which is what the
    frozen invariant forbids; the legacy id is content here, not a key.  Keying
    off ``action_resources.resource_ref`` instead would silently exclude every
    action enqueued through ``db.enqueue``, which never gets one.

    ``source_id`` names *which* legacy database this action came from, and is
    part of the identity rather than a checked attribute.  Scoping by
    environment alone made two unrelated legacy databases sharing an
    environment name map onto the same federation resources, merging two
    actions' provenance into one with nothing to detect it.  Putting the source
    in the identity makes that impossible instead of merely detectable.
    """
    return (f"{LEGACY_EXECUTION_NAMESPACE}:{_require_environment(environment)}"
            f":{_require_source_id(source_id)}:{_require_action_id(action_id)}")


def legacy_completion_ref(*, environment: str, source_id: str) -> str:
    """Stable opaque legacy identity for one database's completion watermark."""
    return (f"{LEGACY_COMPLETION_NAMESPACE}:{_require_environment(environment)}"
            f":{_require_source_id(source_id)}")


def legacy_assurance_type(kind: str, *, mapping_version: str = BACKFILL_MAPPING_VERSION) -> str:
    if not isinstance(kind, str) or not kind:
        raise db.ActionQError("legacy assurance kind is required")
    if not isinstance(mapping_version, str) or not mapping_version:
        raise db.ActionQError("mapping version is required")
    return f"{LEGACY_ASSURANCE_PREFIX}{mapping_version}:{kind}"


def deterministic_resource_ref(legacy_ref: str, *, mapping_version: str = BACKFILL_MAPPING_VERSION) -> str:
    """Map a legacy reference onto a stable federation v1 resource reference.

    sha256 over ``mapping_version`` and the legacy reference, base64url-encoded
    to the exact 43-character body ``federation.RESOURCE_RE`` accepts -- the
    same shape and length as ``db.new_opaque_ref``'s random scheme, so a
    backfilled reference is indistinguishable in *shape* from a native one while
    being reproducible from its source row alone.
    """
    if not isinstance(legacy_ref, str) or not legacy_ref:
        raise db.ActionQError("legacy reference is required")
    if not isinstance(mapping_version, str) or not mapping_version:
        raise db.ActionQError("mapping version is required")
    digest = hashlib.sha256(f"{mapping_version}\x1f{legacy_ref}".encode("utf-8")).digest()
    selected = "aqf1_" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    if not RESOURCE_RE.fullmatch(selected):
        raise db.ActionQError("deterministic resource reference is not a federation v1 reference")
    return selected


def backfill_principal(*, environment: str) -> FederationPrincipal:
    """The only sanctioned backfill principal.

    The principal id is pinned, not a parameter: it is half of the convention
    that makes backfilled changes distinguishable from native ones (the frozen
    change ledger has no provenance column, so ``is_backfilled_change`` has
    nothing else to read).  A caller-supplied id would let a backfill run write
    changes that report as native, which is exactly what the checked-in
    retention contract's §5 says cannot happen.

    The intersection check is a runtime assertion rather than a comment: a later
    edit that widens BACKFILL_AUTHORITIES into acceptance, settlement, supersede
    or evidence ingestion fails here instead of quietly letting legacy terminal
    state be inferred as a federation decision.
    """
    overreach = BACKFILL_AUTHORITIES & FORBIDDEN_BACKFILL_AUTHORITIES
    if overreach:
        raise db.ActionQError(
            "the backfill principal must never hold decision authority: " + ",".join(sorted(overreach))
        )
    if BACKFILL_AUTHORITY not in BACKFILL_AUTHORITIES:
        raise db.ActionQError("the backfill principal must hold federation.backfill")
    return FederationPrincipal.authenticated(
        environment=_require_environment(environment),
        principal_id=BACKFILL_PRINCIPAL_ID,
        authorities=BACKFILL_AUTHORITIES,
    )


def is_backfilled_change(row: Any) -> bool:
    """True when a federation_resource_changes row was written by backfill.

    The contract doc names this the single reader for the backfilled-vs-native
    distinction, so a row shape it cannot read must raise rather than answer
    "native" with confidence.  db.row_value handles both the dict rows
    db.connect produces and positional rows.
    """
    return db.row_value(row, "actor_principal_id", 4) == BACKFILL_PRINCIPAL_ID


@dataclass(frozen=True)
class BackfillCommand:
    operation: str
    idempotency_key: str
    resource_ref: str
    expected_revision: int
    arguments: dict[str, Any]


@dataclass
class _RecordedResource:
    """What federation already holds for one planned resource."""
    revision: int
    facts: dict[tuple[str, ...], int]
    assurances: dict[tuple[str, ...], str]
    foreign: bool


@dataclass(frozen=True)
class BackfillReport:
    mapping_version: str
    environment: str
    principal_id: str
    planned: int
    applied: int
    replayed: int
    resource_refs: tuple[str, ...]


class LegacySource:
    """Read-only legacy execution-schema reader: the W6 extraction seam.

    Every legacy read the backfill performs goes through this one class.
    ``ActionResourceOwner`` cannot serve it: it has no enumeration API, requires
    a known resource ref plus principal scope, and its rows exist only for
    actions enqueued through it -- so action-resource roots, candidate requests
    and the session-completion watermark, which have no public reader at all,
    are read here with plain SELECTs, marked, in one place.

    This is the only place that *reaches* the legacy schema, so W6's extraction
    of ``actionq.storage`` lands here.  It is not, however, the only place that
    knows the legacy row *shape*: ``_source_facts`` reads specific keys off the
    dicts returned below, so the seam is a data contract between the two, not a
    boundary one of them can move across alone.  Every column selected here is
    consumed by that mapping.  The one exception is db.list_actions, the public
    action reader, which is SELECT * -- descriptive columns cross the wire there
    and are dropped by db.redact_action and the mapping, never imported.

    Every read is bounded by ``limit`` and every read *refuses* a result that
    reaches it.  A silently truncated read would produce a quietly incomplete
    import that ``verify()`` still passes, because verification compares
    federation state against federation state and has no way to see a legacy row
    that was never offered to it.
    """

    def __init__(self, *, connection: Callable[[], Any], schema: str, limit: int = 100_000,
                 event_limit: int | None = None):
        for name, value in (("limit", limit), ("event_limit", event_limit)):
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise db.ActionQError(f"legacy read {name} must be a positive integer")
        self.connection = connection
        self.schema = db.schema_name(schema)
        self.limit = limit
        # Events outnumber actions by roughly an order of magnitude, so one
        # shared bound would refuse the whole import on event count long before
        # the action count came near it -- and raising it would multiply every
        # other read at the same time.
        self.event_limit = event_limit if event_limit is not None else limit * 20

    def _q(self, table: str) -> str:
        return db.qname(self.schema, table)

    def _bounded(self, rows: list[Any], table: str, limit: int | None = None) -> list[Any]:
        """Refuse a read that was actually truncated.

        Reads ask for one row more than the bound, so returning exactly the
        bound is proof that nothing was lost -- refusing there would make a
        legacy database with exactly `limit` actions permanently unimportable
        and tell the operator to raise a limit that cost them nothing.
        """
        bound = self.limit if limit is None else limit
        if len(rows) > bound:
            raise db.ActionQError(
                f"legacy read of {table} exceeded the {bound}-row limit; "
                "raise LegacySource(limit=...) rather than importing a truncated history"
            )
        return rows

    def actions(self, conn: Any) -> list[dict[str, Any]]:
        """Every legacy action, redacted, in stable ascending id order.

        db.list_actions orders by (priority, created_at), which is not a total
        order over the table; the backfill plan has to be byte-stable across
        runs, so identity order is imposed here.  db.redact_action strips claim
        receipts and runner-auth digests: legacy claim authority fences legacy
        transitions only and must never cross into federation v1.
        """
        rows = self._bounded(db.list_actions(conn, self.schema, limit=self.limit + 1), "actions")
        return [db.redact_action(row) for row in sorted(rows, key=lambda item: int(item["id"]))]

    def events(self, conn: Any, action_ids: Sequence[int]) -> dict[int, list[dict[str, Any]]]:
        """Events for the selected actions, grouped, in ascending id order.

        Deliberately not db.action_events: that is one round trip per action
        (up to ``limit`` of them, all holding this read's snapshot open) and it
        selects every column, pulling event payloads -- receipts, proofs, runner
        tokens -- into memory for a module whose whole contract is that it never
        imports them.  Only the three columns the plan can use are read.
        """
        selected = [_require_action_id(action_id) for action_id in action_ids]
        grouped: dict[int, list[dict[str, Any]]] = {action_id: [] for action_id in selected}
        if not selected:
            return grouped
        rows = self._bounded(conn.execute(
            f"SELECT id, action_id, event_type FROM {self._q('events')} "
            "WHERE action_id = ANY(%s) ORDER BY action_id, id LIMIT %s",
            (selected, self.event_limit + 1),
        ).fetchall(), "events", self.event_limit)
        for row in rows:
            grouped[int(row["action_id"])].append(dict(row))
        return grouped

    def action_resources(self, conn: Any, action_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
        """Action-resource roots for the selected actions (no public reader).

        Keyed off the same action ids the plan selected, never an independent
        LIMIT: a differently-ordered slice would drop the root of an action that
        *was* imported, and nothing downstream could detect the omission.
        """
        return self._by_action(conn, "action_resources", "action_id, resource_ref, recovery_floor", action_ids)

    def candidate_requests(self, conn: Any, action_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
        """Immutable candidate requests for the selected actions (no public reader)."""
        return self._by_action(conn, "immutable_action_requests", "action_id, request_ref", action_ids)

    def _by_action(self, conn: Any, table: str, columns: str,
                   action_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
        selected = [_require_action_id(action_id) for action_id in action_ids]
        if not selected:
            return {}
        rows = self._bounded(conn.execute(
            f"SELECT {columns} FROM {self._q(table)} WHERE action_id = ANY(%s) ORDER BY action_id LIMIT %s",
            (selected, self.limit + 1),
        ).fetchall(), table)
        return {int(row["action_id"]): dict(row) for row in rows}

    def completion_watermark(self, conn: Any) -> dict[str, Any] | None:
        """The session-completion watermark singleton (no public reader).

        CompletionLog.health() reports the same numbers but builds its own
        connection and mixes in liveness detail that is not a durable fact.
        """
        row = conn.execute(
            f"SELECT recovery_floor, last_cursor, retention_seconds "
            f"FROM {self._q('session_completion_watermarks')} WHERE singleton = 1"
        ).fetchone()
        return dict(row) if row else None


class FederationBackfill:
    """Deterministic provenance import of legacy execution history."""

    def __init__(self, *, authority: FederationAuthority, source: LegacySource, environment: str,
                 source_id: str, mapping_version: str = BACKFILL_MAPPING_VERSION):
        if not isinstance(mapping_version, str) or not mapping_version:
            raise db.ActionQError("mapping version is required")
        self.authority = authority
        self.source = source
        self.environment = _require_environment(environment)
        # Required, with no default: "these two databases are the same source"
        # must be an explicit operator statement, never an accident of two
        # deployments sharing an environment name.
        self.source_id = _require_source_id(source_id)
        self.mapping_version = mapping_version
        self.principal = backfill_principal(environment=environment)

    def _ref(self, legacy_ref: str) -> str:
        return deterministic_resource_ref(legacy_ref, mapping_version=self.mapping_version)

    def _key(self, operation: str, resource_ref: str, fact: tuple[str, ...], expected_revision: int) -> str:
        """Globally unique idempotency key for one attempt at one fact.

        A fact tuple is only unique *within* its resource -- ("create",) is the
        same tuple for every resource -- while the command ledger's identity is
        (environment, principal, operation, key).  The resource reference is
        therefore part of the key, not just the fact.

        The expected revision is part of it too, because it is part of the
        request digest federation.py binds to this key permanently.  Without it,
        a command that was once *rejected* (a stale revision under a concurrent
        run, say) is bound to the digest carrying its old expected revision;
        every later plan computes a new expected revision from the advanced live
        state, so the key would be re-issued under a different digest and come
        back as an idempotency-key conflict forever -- that fact permanently
        unimportable by any run. Including it means a genuine retry is a new
        attempt, while a replay -- which always re-derives the same expected
        revision from the revision the fact was recorded at -- is still the same
        key and still replays byte for byte.
        """
        return "\x1f".join(
            ("backfill", self.mapping_version, operation, resource_ref, *fact, str(expected_revision))
        )

    def _assurance(self, kind: str) -> str:
        """This instance's assurance type.

        Scoped to the instance's mapping version, not the module default: refs
        and idempotency keys already honour a caller-supplied mapping_version,
        and an assurance type that did not would silently label a v2 import as
        v1 -- a mislabelled import rather than an error, since assurance_type is
        part of the record-execution-ref request digest.
        """
        return legacy_assurance_type(kind, mapping_version=self.mapping_version)

    def _read(self) -> dict[str, Any]:
        with self.source.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            actions = self.source.actions(conn)
            action_ids = [int(action["id"]) for action in actions]
            return {
                "actions": actions,
                "events": self.source.events(conn, action_ids),
                "action_resources": self.source.action_resources(conn, action_ids),
                "candidate_requests": self.source.candidate_requests(conn, action_ids),
                "watermark": self.source.completion_watermark(conn),
            }

    def _source_facts(self) -> list[tuple[str, list[tuple[tuple[str, ...], str, dict[str, Any]]]]]:
        """The legacy snapshot as ordered per-resource facts, before revisions.

        Pure function of the legacy source: identity, order and arguments only.
        Revision numbers are assigned separately by ``plan()``, which is what
        lets an already-recorded fact keep the revision it was first written at.
        """
        legacy = self._read()
        facts: dict[str, list[tuple[tuple[str, ...], str, dict[str, Any]]]] = {}
        order: list[str] = []

        def resource(legacy_ref: str) -> str:
            resource_ref = self._ref(legacy_ref)
            facts[resource_ref] = [(("create",), "create", {"resource_ref": resource_ref})]
            order.append(resource_ref)
            return resource_ref

        def execution_ref(resource_ref: str, reference: str, kind: str) -> None:
            facts[resource_ref].append((
                ("execution-ref", reference), "record-execution-ref",
                {"resource_ref": resource_ref, "execution_ref": reference,
                 "assurance_type": self._assurance(kind)},
            ))

        def relation(source_ref: str, target_ref: str) -> None:
            facts[source_ref].append((
                ("relation", "parent-of", target_ref), "add-relation",
                {"source_ref": source_ref, "relation_type": "parent-of", "target_ref": target_ref},
            ))

        planned: dict[int, str] = {}
        for action in legacy["actions"]:
            action_id = int(action["id"])
            legacy_ref = legacy_execution_ref(
                environment=self.environment, source_id=self.source_id, action_id=action_id,
            )
            resource_ref = resource(legacy_ref)
            planned[action_id] = resource_ref
            execution_ref(resource_ref, legacy_ref, "action")
            root = legacy["action_resources"].get(action_id)
            if root is not None:
                execution_ref(resource_ref, f"{legacy_ref}#action-resource:{root['resource_ref']}", "action-resource")
            candidate = legacy["candidate_requests"].get(action_id)
            if candidate is not None:
                execution_ref(
                    resource_ref, f"{legacy_ref}#candidate-request:{candidate['request_ref']}", "candidate-request",
                )
            # Facts whose *value* is mutable, and facts whose cardinality
            # grows, are emitted last in each chain.  This is defence in depth,
            # not a property the plan depends on: expected revisions come from
            # the revision each fact was *recorded* at, never from its position
            # here, so reordering this list cannot change what any run writes
            # (verified by test).  Keeping the stable facts first still means a
            # fresh import's chains read in a sensible order.
            execution_ref(resource_ref, f"{legacy_ref}#status:{action['status']}", "status")
            if root is not None:
                execution_ref(
                    resource_ref, f"{legacy_ref}#action-resource-recovery-floor:{int(root['recovery_floor'])}",
                    "action-resource-recovery-floor",
                )
            for event in legacy["events"][action_id]:
                execution_ref(
                    resource_ref, f"{legacy_ref}#event:{int(event['id'])}", f"event:{event['event_type']}",
                )

        for action in legacy["actions"]:
            parent_id = action.get("parent_id")
            if parent_id is None:
                continue
            parent_ref = planned.get(int(parent_id))
            if parent_ref is None:
                # The parent fell outside the read limit.  Emitting the edge
                # anyway would be rejected as target-not-found and abort an
                # otherwise-correct import; the child is still imported whole.
                continue
            relation(parent_ref, planned[int(action["id"])])

        watermark = legacy["watermark"]
        if watermark is not None:
            legacy_ref = legacy_completion_ref(environment=self.environment, source_id=self.source_id)
            resource_ref = resource(legacy_ref)
            execution_ref(resource_ref, legacy_ref, "completion-watermark")
            # Every remaining watermark fact is a moving counter, so each run
            # against a live system contributes new refs here.  Backfill is a
            # cutover-time import, not a scheduled job -- see the contract doc.
            execution_ref(
                resource_ref, f"{legacy_ref}#recovery-floor:{int(watermark['recovery_floor'])}",
                "completion-recovery-floor",
            )
            execution_ref(
                resource_ref, f"{legacy_ref}#last-cursor:{int(watermark['last_cursor'])}",
                "completion-last-cursor",
            )
            execution_ref(
                resource_ref, f"{legacy_ref}#retention-seconds:{int(watermark['retention_seconds'])}",
                "completion-retention-seconds",
            )
        return [(resource_ref, facts[resource_ref]) for resource_ref in order]

    def plan(self) -> tuple[BackfillCommand, ...]:
        """The command sequence for the current legacy snapshot and live state.

        Expected revisions are reconciled against what is already recorded, not
        counted from zero.  A fact already written keeps the revision it was
        written at, so its request bytes are unchanged and the command-decision
        ledger replays it; a fact the source has since gained is appended after
        the resource's current revision.

        That is what makes the import genuinely restartable and monotonic
        against a *moving* legacy database, not only a frozen one.  Counting
        from zero instead would renumber every fact after any inserted one,
        re-issuing their idempotency keys under different canonical requests --
        which federation.py durably rejects as an idempotency-key conflict, and
        permanently, since each later run would replan the same conflict.

        Obsolete facts are not removed and not an error.  "At import time this
        action was pending" stays true after the action completes; an append-only
        provenance ledger keeps both, which is more faithful than either
        rewriting the first or refusing the second.
        """
        creates: list[BackfillCommand] = []
        rest: list[BackfillCommand] = []
        source = self._source_facts()
        with self.authority.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            live = self._recorded(conn, [resource_ref for resource_ref, _ in source])
        for resource_ref, facts in source:
            record = live[resource_ref]
            revision, recorded, foreign = record.revision, record.facts, record.foreign
            if foreign:
                raise BackfillDrift(
                    "a principal other than backfill has written to this backfilled resource; "
                    "its deterministic identity has been taken over",
                    resource_ref=resource_ref, fact="",
                )
            for fact, _, arguments in facts:
                # assurance_type is in the request digest but not in the fact
                # key, so a legacy value that moved under an already-recorded
                # reference would re-issue a bound key under a different digest
                # and be rejected permanently.  Stop, diagnosably, instead.
                recorded_assurance = record.assurances.get(fact)
                if recorded_assurance is not None and recorded_assurance != arguments["assurance_type"]:
                    raise BackfillDrift(
                        f"legacy fact {fact[1]} is recorded with assurance {recorded_assurance!r} "
                        f"but the source now yields {arguments['assurance_type']!r}",
                        resource_ref=resource_ref, fact=":".join(fact),
                    )
            replays = sorted(
                ((recorded[fact], fact, operation, arguments)
                 for fact, operation, arguments in facts if fact in recorded),
                key=lambda item: item[0],
            )
            for source_revision, fact, operation, arguments in replays:
                expected_revision = source_revision - 1
                (creates if operation == "create" else rest).append(BackfillCommand(
                    operation, self._key(operation, resource_ref, fact, expected_revision), resource_ref,
                    expected_revision, arguments,
                ))
            expected = revision
            for fact, operation, arguments in facts:
                if fact in recorded:
                    continue
                (creates if operation == "create" else rest).append(BackfillCommand(
                    operation, self._key(operation, resource_ref, fact, expected), resource_ref,
                    expected, arguments,
                ))
                expected += 1
        # Every create precedes every other command: add-relation names a target
        # resource that must already exist, and relations are planned inside the
        # source resource's own chain.
        return tuple(creates + rest)

    def _dispatch(self, command: BackfillCommand) -> CommandDecision:
        if command.operation not in _PROVENANCE_OPERATIONS:
            raise db.ActionQError(f"backfill may only issue provenance commands: {command.operation}")
        method = {
            "create": self.authority.create,
            "record-execution-ref": self.authority.record_execution_ref,
            "add-relation": self.authority.add_relation,
        }[command.operation]
        return method(
            principal=self.principal, idempotency_key=command.idempotency_key,
            expected_revision=command.expected_revision, **command.arguments,
        )

    def _recorded(self, conn: Any, resource_refs: Sequence[str]) -> dict[str, _RecordedResource]:
        """Live state for every planned resource, in four queries total.

        Batched rather than per resource: at LegacySource's default bound this
        would otherwise be hundreds of thousands of round trips inside one
        repeatable-read snapshot, holding it open (and pinning xmin) for the
        whole import.

        Carries the assurance type each execution ref was recorded with, not
        only the revision, because assurance_type is part of the
        record-execution-ref request digest while the fact key is the reference
        alone -- so a legacy event_type that moved would otherwise re-issue a
        bound key under a different digest.
        """
        schema = self.authority.schema
        selected = list(resource_refs)
        live: dict[str, _RecordedResource] = {
            resource_ref: _RecordedResource(0, {}, {}, False) for resource_ref in selected
        }
        if not selected:
            return live
        rows = conn.execute(
            f"SELECT resource_ref, revision FROM {db.qname(schema, 'federation_resources')} "
            "WHERE resource_ref = ANY(%s)", (selected,),
        ).fetchall()
        existing = {str(row["resource_ref"]): int(row["revision"]) for row in rows}
        for resource_ref, revision in existing.items():
            live[resource_ref] = _RecordedResource(revision, {("create",): 1}, {}, False)
        if not existing:
            return live
        present = sorted(existing)
        for row in conn.execute(
            f"SELECT resource_ref, execution_ref, assurance_type, source_revision "
            f"FROM {db.qname(schema, 'federation_execution_refs')} WHERE resource_ref = ANY(%s)",
            (present,),
        ).fetchall():
            record = live[str(row["resource_ref"])]
            fact = ("execution-ref", str(row["execution_ref"]))
            record.facts[fact] = int(row["source_revision"])
            record.assurances[fact] = str(row["assurance_type"])
        for row in conn.execute(
            f"SELECT source_ref, relation_type, target_ref, source_revision "
            f"FROM {db.qname(schema, 'federation_relations')} WHERE source_ref = ANY(%s)",
            (present,),
        ).fetchall():
            record = live[str(row["source_ref"])]
            record.facts["relation", str(row["relation_type"]), str(row["target_ref"])] = int(row["source_revision"])
        for row in conn.execute(
            f"SELECT DISTINCT resource_ref FROM {db.qname(schema, 'federation_resource_changes')} "
            "WHERE resource_ref = ANY(%s) AND actor_principal_id <> %s",
            (present, self.principal.principal_id),
        ).fetchall():
            live[str(row["resource_ref"])].foreign = True
        return live

    def run(self) -> BackfillReport:
        """Apply the plan.

        Restartable and incremental: every fact already recorded replays its
        stored decision byte for byte, and every fact the legacy source has
        gained since is appended.  Nothing is rewritten and nothing is removed.

        Plan and dispatch are serialized against other backfill runs by one
        advisory lock held for the whole cycle.  Two concurrent runs over
        divergent legacy snapshots would otherwise plan different revisions for
        the same fact; the loser's command is rejected as stale, and a rejection
        is durable, so that fact could never be imported by any later run.
        """
        with self.authority.connection() as guard, guard.transaction():
            db.lock(guard, "\x1f".join(
                ("federation-backfill/v1", self.mapping_version, self.environment, self.source_id)
            ))
            return self._run_locked()

    def _run_locked(self) -> BackfillReport:
        commands = self.plan()
        applied = replayed = 0
        resource_refs: list[str] = []
        for command in commands:
            decision = self._dispatch(command)
            if decision.status != "accepted":
                raise BackfillRejected(
                    f"backfill command was rejected: {decision.code}: {decision.message}",
                    code=decision.code, operation=command.operation, resource_ref=command.resource_ref,
                )
            if decision.replayed:
                replayed += 1
            else:
                applied += 1
            if command.operation == "create":
                resource_refs.append(command.resource_ref)
        return BackfillReport(
            mapping_version=self.mapping_version, environment=self.environment,
            principal_id=self.principal.principal_id, planned=len(commands),
            applied=applied, replayed=replayed, resource_refs=tuple(resource_refs),
        )

    def verify(self, resource_refs: Sequence[str] | None = None) -> tuple[dict[str, Any], ...]:
        """Rebuild the given resources from their changes and compare.

        Pass ``BackfillReport.resource_refs`` to verify exactly what a run
        wrote.  With no argument the current source is replanned, and resources
        that do not exist yet are skipped rather than raising -- a legacy action
        enqueued between run() and verify() is not a verification failure.

        One repeatable-read snapshot for the whole comparison: rebuild and live
        projection are separate statements, and a federation command committing
        between them would report a spurious mismatch on healthy data.
        """
        selected = list(resource_refs) if resource_refs is not None else [
            resource_ref for resource_ref, _ in self._source_facts()
        ]
        with self.authority.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            existing = [
                resource_ref for resource_ref in selected
                if conn.execute(
                    f"SELECT 1 FROM {db.qname(self.authority.schema, 'federation_resources')} WHERE resource_ref=%s",
                    (resource_ref,),
                ).fetchone() is not None
            ]
            return tuple(verify_rebuild(conn, self.authority.schema, resource_ref) for resource_ref in existing)


def _q(schema: str, table: str) -> str:
    return db.qname(db.schema_name(schema), table)


def _empty_projection(resource_ref: str) -> dict[str, Any]:
    return {
        "schema_version": _PROJECTION_SCHEMA_VERSION, "resource_ref": resource_ref,
        "owner_principal_id": None, "state": None, "revision": 0, "recovery_floor": 0,
        "relations": [], "execution_refs": [], "evidence": [],
        "acceptance_decisions": [], "settlements": [],
    }


def _sorted(rows: list[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: tuple(str(row[key]) for key in keys))


def rebuild_from_changes(conn: Any, schema: str, resource_ref: str, *,
                         changes: Sequence[Any] | None = None) -> dict[str, Any]:
    """Replay a resource's change ledger into a canonical projection.

    Replays operation and payload -- not just revision continuity -- so that
    comparing the result against the live projection is a real check rather than
    a tautology.  Detects gaps, duplicates, non-canonical payload bytes, payload
    digest conflicts and change rows whose recorded state disagrees with the
    replay.  ``changes`` exists so a caller can replay a deliberately corrupted
    sequence; the frozen schema's primary key makes a duplicate revision
    unwritable, and no pruning surface exists to create a gap.
    """
    if not isinstance(resource_ref, str) or not RESOURCE_RE.fullmatch(resource_ref):
        raise db.ActionQError("resource_ref is not a federation v1 reference")
    if changes is None:
        changes = conn.execute(
            f"SELECT revision, operation, state, actor_principal_id, payload_bytes, payload_digest "
            f"FROM {_q(schema, 'federation_resource_changes')} WHERE resource_ref=%s ORDER BY revision",
            (resource_ref,),
        ).fetchall()
    rows = list(changes)
    if not rows:
        raise RebuildError("federation resource has no changes", code="resource-not-found", resource_ref=resource_ref)

    projection = _empty_projection(resource_ref)
    expected_revision = 0
    for row in rows:
        revision = int(row["revision"])
        expected_revision += 1
        if revision != expected_revision:
            code = "duplicate-revision" if revision < expected_revision else "revision-gap"
            raise RebuildError(
                f"change ledger is not contiguous at revision {revision}",
                code=code, resource_ref=resource_ref, revision=revision,
            )
        payload_bytes = bytes(row["payload_bytes"])
        try:
            payload = json.loads(payload_bytes)
        except (ValueError, UnicodeDecodeError) as malformed:
            raise RebuildError(
                f"change payload is not JSON: {malformed}",
                code="payload-not-canonical", resource_ref=resource_ref, revision=revision,
            ) from malformed
        if canonical_bytes(payload) != payload_bytes:
            raise RebuildError(
                "change payload bytes are not canonical",
                code="payload-not-canonical", resource_ref=resource_ref, revision=revision,
            )
        if contract_digest(payload) != str(row["payload_digest"]):
            raise RebuildError(
                "change payload digest does not match its bytes",
                code="payload-digest-mismatch", resource_ref=resource_ref, revision=revision,
            )
        operation = str(row["operation"])
        actor = str(row["actor_principal_id"])
        if operation == "create":
            if revision != 1:
                raise RebuildError(
                    "create is only valid as revision 1",
                    code="invalid-replay-operation", resource_ref=resource_ref, revision=revision,
                )
            projection["owner_principal_id"] = payload["owner_principal_id"]
        elif revision == 1:
            raise RebuildError(
                "the first change must be a create",
                code="invalid-replay-operation", resource_ref=resource_ref, revision=revision,
            )
        elif operation == "add-relation":
            projection["relations"].append({
                "relation_type": payload["relation_type"], "target_ref": payload["target_ref"],
                "source_revision": revision,
            })
        elif operation == "record-execution-ref":
            projection["execution_refs"].append({
                "execution_ref": payload["execution_ref"], "assurance_type": payload["assurance_type"],
                "source_revision": revision,
            })
        elif operation == "record-evidence":
            projection["evidence"].append({
                "evidence_ref": payload["evidence_ref"], "evidence_digest": payload["evidence_digest"],
                "assurance_type": payload["assurance_type"], "source_revision": revision,
            })
        elif operation == "decide-acceptance":
            projection["acceptance_decisions"].append({
                "source_revision": revision, "outcome": payload["outcome"],
                "policy_ref": payload["policy_ref"], "evidence_ref": payload["evidence_ref"],
                "decided_by": actor,
            })
        elif operation == "record-settlement":
            projection["settlements"].append({
                "source_revision": revision, "fact_ref": payload["fact_ref"], "reconciled_by": actor,
            })
        elif operation != "supersede":
            raise RebuildError(
                f"unknown change operation: {operation}",
                code="invalid-replay-operation", resource_ref=resource_ref, revision=revision,
            )
        if operation == "decide-acceptance":
            projection["state"] = payload["outcome"]
        elif operation in _REBUILD_STATES:
            projection["state"] = _REBUILD_STATES[operation]
        if projection["state"] != str(row["state"]):
            raise RebuildError(
                "replayed state does not match the recorded change state",
                code="change-state-mismatch", resource_ref=resource_ref, revision=revision,
            )
    projection["revision"] = expected_revision
    projection["relations"] = _sorted(projection["relations"], "relation_type", "target_ref")
    projection["execution_refs"] = _sorted(projection["execution_refs"], "execution_ref")
    projection["evidence"] = _sorted(projection["evidence"], "evidence_ref")
    return projection


def live_projection(conn: Any, schema: str, resource_ref: str) -> dict[str, Any]:
    """The canonical projection as the live federation tables hold it."""
    if not isinstance(resource_ref, str) or not RESOURCE_RE.fullmatch(resource_ref):
        raise db.ActionQError("resource_ref is not a federation v1 reference")
    root = conn.execute(
        f"SELECT owner_principal_id, state, revision, recovery_floor "
        f"FROM {_q(schema, 'federation_resources')} WHERE resource_ref=%s",
        (resource_ref,),
    ).fetchone()
    if root is None:
        raise RebuildError("federation resource does not exist", code="resource-not-found", resource_ref=resource_ref)
    projection = _empty_projection(resource_ref)
    projection.update({
        "owner_principal_id": root["owner_principal_id"], "state": str(root["state"]),
        "revision": int(root["revision"]), "recovery_floor": int(root["recovery_floor"]),
    })
    projection["relations"] = _sorted([
        {"relation_type": str(row["relation_type"]), "target_ref": str(row["target_ref"]),
         "source_revision": int(row["source_revision"])}
        for row in conn.execute(
            f"SELECT relation_type, target_ref, source_revision FROM {_q(schema, 'federation_relations')} WHERE source_ref=%s",
            (resource_ref,),
        ).fetchall()
    ], "relation_type", "target_ref")
    projection["execution_refs"] = _sorted([
        {"execution_ref": str(row["execution_ref"]), "assurance_type": str(row["assurance_type"]),
         "source_revision": int(row["source_revision"])}
        for row in conn.execute(
            f"SELECT execution_ref, assurance_type, source_revision FROM {_q(schema, 'federation_execution_refs')} WHERE resource_ref=%s",
            (resource_ref,),
        ).fetchall()
    ], "execution_ref")
    projection["evidence"] = _sorted([
        {"evidence_ref": str(row["evidence_ref"]), "evidence_digest": str(row["evidence_digest"]),
         "assurance_type": str(row["assurance_type"]), "source_revision": int(row["source_revision"])}
        for row in conn.execute(
            f"SELECT evidence_ref, evidence_digest, assurance_type, source_revision FROM {_q(schema, 'federation_evidence')} WHERE resource_ref=%s",
            (resource_ref,),
        ).fetchall()
    ], "evidence_ref")
    projection["acceptance_decisions"] = [
        {"source_revision": int(row["source_revision"]), "outcome": str(row["outcome"]),
         "policy_ref": str(row["policy_ref"]), "evidence_ref": row["evidence_ref"],
         "decided_by": str(row["decided_by"])}
        for row in conn.execute(
            f"SELECT source_revision, outcome, policy_ref, evidence_ref, decided_by "
            f"FROM {_q(schema, 'federation_acceptance_decisions')} WHERE resource_ref=%s ORDER BY source_revision",
            (resource_ref,),
        ).fetchall()
    ]
    projection["settlements"] = [
        {"source_revision": int(row["source_revision"]), "fact_ref": str(row["fact_ref"]),
         "reconciled_by": str(row["reconciled_by"])}
        for row in conn.execute(
            f"SELECT source_revision, fact_ref, reconciled_by FROM {_q(schema, 'federation_settlements')} "
            f"WHERE resource_ref=%s ORDER BY source_revision",
            (resource_ref,),
        ).fetchall()
    ]
    return projection


def verify_rebuild(conn: Any, schema: str, resource_ref: str) -> dict[str, Any]:
    """Compare a clean rebuild against the live projection, byte for byte.

    The two reads are separate statements, so under a concurrent writer they
    must share one snapshot or a healthy resource can report a mismatch.  Call
    this on a connection whose transaction is repeatable-read (as
    ``FederationBackfill.verify`` does) whenever writers may be active.
    """
    rebuilt = rebuild_from_changes(conn, schema, resource_ref)
    live = live_projection(conn, schema, resource_ref)
    rebuilt_bytes, live_bytes = canonical_bytes(rebuilt), canonical_bytes(live)
    return {
        "resource_ref": resource_ref, "revision": rebuilt["revision"],
        "matches": rebuilt_bytes == live_bytes,
        "rebuilt": rebuilt, "live": live,
        "rebuilt_digest": contract_digest(rebuilt), "live_digest": contract_digest(live),
    }


def require_rebuild_matches(conn: Any, schema: str, resource_ref: str) -> dict[str, Any]:
    result = verify_rebuild(conn, schema, resource_ref)
    if not result["matches"]:
        raise RebuildError(
            "rebuilt projection differs from the live projection",
            code="projection-mismatch", resource_ref=resource_ref, revision=result["revision"],
        )
    return result


# Export column shape: table -> ((column, kind), ...).  Kinds are explicit
# because canonical_bytes accepts neither bytes nor datetimes, and an export
# that guessed per value would not round-trip a NULL column back to its type.
# Export shape: table -> (columns, primary key).  Kinds are explicit because
# canonical_bytes accepts neither bytes nor datetimes, and an export that
# guessed per value would not round-trip a NULL column back to its type.  The
# column set is pinned against federation_schema._COLUMN_SHAPE by test, so a
# migration that adds a column fails loudly instead of silently dropping it
# from the artifact the retention contract calls the indefinite record.
_EXPORT_TABLES: tuple[tuple[str, tuple[tuple[str, str], ...], tuple[str, ...]], ...] = (
    ("federation_resources", (
        ("resource_ref", "text"), ("owner_principal_id", "text"), ("state", "text"),
        ("revision", "int"), ("recovery_floor", "int"), ("created_at", "time"), ("updated_at", "time"),
    ), ("resource_ref",)),
    ("federation_resource_changes", (
        ("resource_ref", "text"), ("revision", "int"), ("operation", "text"), ("state", "text"),
        ("actor_principal_id", "text"), ("payload_bytes", "bytes"), ("payload_digest", "text"),
        ("occurred_at", "time"),
    ), ("resource_ref", "revision")),
    ("federation_relations", (
        ("source_ref", "text"), ("relation_type", "text"), ("target_ref", "text"),
        ("source_revision", "int"), ("created_at", "time"),
    ), ("source_ref", "relation_type", "target_ref")),
    ("federation_execution_refs", (
        ("resource_ref", "text"), ("execution_ref", "text"), ("assurance_type", "text"),
        ("source_revision", "int"), ("created_at", "time"),
    ), ("resource_ref", "execution_ref")),
    ("federation_evidence", (
        ("resource_ref", "text"), ("evidence_ref", "text"), ("evidence_digest", "text"),
        ("assurance_type", "text"), ("source_revision", "int"), ("created_at", "time"),
    ), ("resource_ref", "evidence_ref")),
    ("federation_acceptance_decisions", (
        ("resource_ref", "text"), ("source_revision", "int"), ("outcome", "text"),
        ("policy_ref", "text"), ("evidence_ref", "text"), ("decided_by", "text"), ("created_at", "time"),
    ), ("resource_ref", "source_revision")),
    ("federation_settlements", (
        ("resource_ref", "text"), ("source_revision", "int"), ("fact_ref", "text"),
        ("reconciled_by", "text"), ("created_at", "time"),
    ), ("resource_ref", "source_revision")),
    ("federation_idempotency_bindings", (
        ("environment", "text"), ("principal_id", "text"), ("operation", "text"),
        ("idempotency_key", "text"), ("request_digest", "text"), ("created_at", "time"),
    ), ("environment", "principal_id", "operation", "idempotency_key")),
    ("federation_command_decisions", (
        ("environment", "text"), ("principal_id", "text"), ("operation", "text"),
        ("idempotency_key", "text"), ("request_digest", "text"), ("status", "text"),
        ("code", "text"), ("message", "text"), ("response_bytes", "bytes"),
        ("response_digest", "text"), ("resource_ref", "text"),
        ("before_revision", "int"), ("after_revision", "int"), ("decided_at", "time"),
    ), ("environment", "principal_id", "operation", "idempotency_key", "request_digest")),
)


def _encode(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "text":
        return str(value)
    if kind == "int":
        return int(value)
    if kind == "bytes":
        return base64.b64encode(bytes(value)).decode("ascii")
    if kind == "time":
        moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        if moment.tzinfo is None:
            raise ExportError("federation timestamps must be timezone-aware")
        return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raise ExportError(f"unknown export column kind: {kind}")


def _decode(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "bytes":
        try:
            return base64.b64decode(str(value), validate=True)
        except (ValueError, TypeError) as malformed:
            raise ExportError(f"export column is not valid base64: {malformed}") from malformed
    if kind == "time":
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as malformed:
            raise ExportError(f"export column is not an ISO-8601 timestamp: {malformed}") from malformed
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ExportError("export integer column is not an integer")
        return value
    if not isinstance(value, str):
        raise ExportError("export text column is not a string")
    return value


def _require_idle(conn: Any, action: str) -> None:
    """Refuse a connection that is already inside a transaction.

    Both export and restore need their own transaction: the isolation level can
    only be selected before a transaction's first query, and a nested
    conn.transaction() is a savepoint that does not commit.  psycopg's own error
    for this ("SET TRANSACTION ISOLATION LEVEL must be called before any query")
    does not say which contract was broken.
    """
    status = getattr(getattr(conn, "info", None), "transaction_status", None)
    if status is None:
        return
    if getattr(status, "name", str(status)) != "IDLE":
        raise ExportError(
            f"federation {action} requires a connection with no open transaction; "
            "commit or roll back before calling it"
        )


def _order_by(columns: tuple[tuple[str, str], ...], keys: tuple[str, ...]) -> str:
    """Ordering over the primary key, with text keys forced to C collation.

    Database collation is not a property of the data.  Ordering text keys under
    the cluster's lc_collate makes two exports of identical content differ
    byte-for-byte across clusters -- and across a glibc/ICU collation update on
    one cluster, which matters on the multi-decade horizon this artifact claims.
    """
    kinds = dict(columns)
    return ", ".join(
        f'{key} COLLATE "C"' if kinds[key] == "text" else key for key in keys
    )


def export_federation(conn: Any, schema: str, *, produced_at: str | None = None,
                      source: str | None = None) -> bytes:
    """Canonical logical dump of every federation v1 table.

    The durable-authoritative export named by the checked-in retention contract:
    independent of, and additional to, the whole-cluster physical backup, whose
    retention window is far shorter than federation data's.

    The document is self-describing.  A reader decades from now, holding these
    bytes and nothing else, gets the schema version and compatibility label it
    was produced under, the producing package version, the federation migration
    ledger's name/checksum triples (which the first frozen invariant says never
    change, and which a restored schema's own ledger cannot evidence because it
    is written by the *restoring* wheel), and the source it was taken from.

    ``produced_at`` and ``source`` are caller-supplied rather than read from the
    environment, so an export is a pure function of its inputs: omit them and
    two exports of identical content are byte-identical, which is the property
    the retention contract relies on for restore-and-compare.

    All nine tables are read in one repeatable-read snapshot.  Under the default
    read-committed isolation each statement takes a fresh snapshot, so a
    concurrent command could otherwise tear the document -- a change row for a
    resource missing from the resources dump, or a resource at revision N
    carrying only N-1 changes, either of which fails restore or rebuild.  The
    transaction is opened here rather than assumed: SET TRANSACTION outside a
    transaction block is a silent no-op, so a caller-supplied autocommit
    connection would have lost the protection without any error.  The other
    direction cannot be repaired here at all -- a connection that has already
    run a statement is inside a transaction whose snapshot is fixed, and
    conn.transaction() would only open a savepoint -- so it is refused with a
    clear error rather than a psycopg one.

    Byte-identity: omitting produced_at and source makes the document a pure
    function of the database *for one wheel*.  producer_version and the
    migration ledger are inside the digested bytes, so a cross-version
    restore-and-compare must compare the "tables" block, not the whole document.
    """
    from . import __version__ as producer_version

    _require_idle(conn, "export")
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        compatibility = federation_schema.require_compatible(conn, schema)
        ledger = [
            {"version": int(row["version"]), "name": str(row["name"]), "checksum": str(row["checksum"])}
            for row in conn.execute(
                f"SELECT version, name, checksum FROM {_q(schema, federation_schema.MIGRATION_TABLE)} "
                "WHERE domain=%s ORDER BY version", (federation_schema.DOMAIN,),
            ).fetchall()
        ]
        tables: dict[str, Any] = {}
        for table, columns, keys in _EXPORT_TABLES:
            names = [name for name, _ in columns]
            rows = conn.execute(
                f"SELECT {','.join(names)} FROM {_q(schema, table)} ORDER BY {_order_by(columns, keys)}"
            ).fetchall()
            tables[table] = {
                "columns": names,
                "rows": [[_encode(row[name], kind) for name, kind in columns] for row in rows],
            }
    return canonical_bytes({
        "schema_version": _EXPORT_SCHEMA_VERSION,
        "compatibility_label": compatibility.compatibility_label,
        "federation_schema_version": compatibility.observed_schema_version,
        "provenance": {
            "producer": "actionq",
            "producer_version": producer_version,
            "migration_ledger": ledger,
            "source": source,
            "produced_at": produced_at,
        },
        "tables": tables,
    })


def restore_federation(conn: Any, schema: str, payload: bytes) -> dict[str, int]:
    """Restore an export into a freshly migrated, empty federation schema.

    Refuses a non-empty target: a restore must never partially overlay live
    federation data.  Tables are written in declared order, which is the
    foreign-key order every dependent table needs.

    Requires an idle connection so its transaction is a real one rather than a
    savepoint that never commits, and leaves the commit to the caller, as every
    other write path in this package does.
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise ExportError("export payload must be bytes")
    try:
        document = json.loads(bytes(payload))
    except (ValueError, UnicodeDecodeError) as malformed:
        raise ExportError(f"export payload is not JSON: {malformed}") from malformed
    if not isinstance(document, dict) or document.get("schema_version") != _EXPORT_SCHEMA_VERSION:
        raise ExportError("export payload is not a federation-export/v1 document")
    provenance = document.get("provenance")
    if not isinstance(provenance, dict):
        raise ExportError("export payload carries no provenance block")
    tables = document.get("tables")
    if not isinstance(tables, dict) or set(tables) != {name for name, _, _ in _EXPORT_TABLES}:
        raise ExportError("export payload does not describe every federation table")

    restored: dict[str, int] = {}
    # One transaction for the compatibility verdict, the emptiness probe and
    # every insert.  Without it a failure partway through (a hand-edited export
    # violating a CHECK, a dropped connection) leaves earlier tables populated,
    # and the non-empty guard then blocks every retry -- the operator would have
    # to hand-truncate the schema.
    _require_idle(conn, "restore")
    with conn.transaction():
        compatibility = federation_schema.require_compatible(conn, schema)
        if document.get("compatibility_label") != compatibility.compatibility_label:
            raise ExportError("export payload was produced under a different compatibility label")
        if document.get("federation_schema_version") != compatibility.observed_schema_version:
            raise ExportError("export payload was produced under a different federation schema version")
        # The ledger triples are carried precisely so the artifact can evidence
        # the frozen "migrations never change" invariant; verifying them here is
        # what makes carrying them more than decoration.
        target_ledger = [
            {"version": int(row["version"]), "name": str(row["name"]), "checksum": str(row["checksum"])}
            for row in conn.execute(
                f"SELECT version, name, checksum FROM {_q(schema, federation_schema.MIGRATION_TABLE)} "
                "WHERE domain=%s ORDER BY version", (federation_schema.DOMAIN,),
            ).fetchall()
        ]
        if provenance.get("migration_ledger") != target_ledger:
            raise ExportError(
                "export payload's federation migration ledger differs from the target schema's"
            )
        for table, _, _ in _EXPORT_TABLES:
            occupied = conn.execute(f"SELECT 1 FROM {_q(schema, table)} LIMIT 1").fetchone()
            if occupied is not None:
                raise ExportError(f"restore requires an empty federation schema: {table} is not empty")
        for table, columns, _ in _EXPORT_TABLES:
            names = [name for name, _ in columns]
            described = tables[table]
            if not isinstance(described, dict) or described.get("columns") != names:
                raise ExportError(f"export payload column shape does not match {table}")
            rows = described.get("rows")
            if not isinstance(rows, list):
                raise ExportError(f"export payload rows for {table} are not a list")
            placeholders = ",".join(["%s"] * len(names))
            statement = f"INSERT INTO {_q(schema, table)} ({','.join(names)}) VALUES ({placeholders})"
            for row in rows:
                if not isinstance(row, list) or len(row) != len(columns):
                    raise ExportError(f"export payload row for {table} has the wrong arity")
                conn.execute(statement, tuple(_decode(value, kind) for value, (_, kind) in zip(row, columns)))
            restored[table] = len(rows)
    return restored
