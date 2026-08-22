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


def _require_action_id(action_id: Any) -> int:
    if isinstance(action_id, bool) or not isinstance(action_id, int) or action_id <= 0:
        raise db.ActionQError("legacy action id must be a positive integer")
    return action_id


def legacy_execution_ref(*, environment: str, action_id: int) -> str:
    """Stable opaque legacy identity for one execution-schema action.

    Recorded as an execution *reference string* only.  Nothing joins on it and
    no federation row carries an execution-schema foreign key, which is what the
    frozen invariant forbids; the legacy id is content here, not a key.  Keying
    off ``action_resources.resource_ref`` instead would silently exclude every
    action enqueued through ``db.enqueue``, which never gets one.
    """
    return f"{LEGACY_EXECUTION_NAMESPACE}:{_require_environment(environment)}:{_require_action_id(action_id)}"


def legacy_completion_ref(*, environment: str) -> str:
    """Stable opaque legacy identity for the session-completion watermark."""
    return f"{LEGACY_COMPLETION_NAMESPACE}:{_require_environment(environment)}"


def legacy_assurance_type(kind: str) -> str:
    if not isinstance(kind, str) or not kind:
        raise db.ActionQError("legacy assurance kind is required")
    return f"{LEGACY_ASSURANCE_PREFIX}{BACKFILL_MAPPING_VERSION}:{kind}"


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
    """True when a federation_resource_changes row was written by backfill."""
    try:
        actor = row["actor_principal_id"]
    except (TypeError, KeyError, IndexError):
        return False
    return actor == BACKFILL_PRINCIPAL_ID


@dataclass(frozen=True)
class BackfillCommand:
    operation: str
    idempotency_key: str
    resource_ref: str
    expected_revision: int
    arguments: dict[str, Any]


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
    are read here with plain SELECTs, marked, in one place.  When W6 extracts
    ``actionq.storage`` this class is the only thing that moves.

    Every read is bounded by ``limit`` and every read *refuses* a result that
    reaches it.  A silently truncated read would produce a quietly incomplete
    import that ``verify()`` still passes, because verification compares
    federation state against federation state and has no way to see a legacy row
    that was never offered to it.
    """

    def __init__(self, *, connection: Callable[[], Any], schema: str, limit: int = 100_000):
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise db.ActionQError("legacy read limit must be a positive integer")
        self.connection = connection
        self.schema = db.schema_name(schema)
        self.limit = limit

    def _q(self, table: str) -> str:
        return db.qname(self.schema, table)

    def _bounded(self, rows: list[Any], table: str) -> list[Any]:
        if len(rows) >= self.limit:
            raise db.ActionQError(
                f"legacy read of {table} reached the {self.limit}-row limit; "
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
        rows = self._bounded(db.list_actions(conn, self.schema, limit=self.limit), "actions")
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
            (selected, self.limit),
        ).fetchall(), "events")
        for row in rows:
            grouped[int(row["action_id"])].append(dict(row))
        return grouped

    def action_resources(self, conn: Any, action_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
        """Action-resource roots for the selected actions (no public reader).

        Keyed off the same action ids the plan selected, never an independent
        LIMIT: a differently-ordered slice would drop the root of an action that
        *was* imported, and nothing downstream could detect the omission.
        """
        return self._by_action(
            conn, "action_resources", "action_id, resource_ref, revision, recovery_floor, state", action_ids,
        )

    def candidate_requests(self, conn: Any, action_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
        """Immutable candidate requests for the selected actions (no public reader)."""
        return self._by_action(
            conn, "immutable_action_requests",
            "action_id, request_ref, plan_ref, topology, role, subject", action_ids,
        )

    def _by_action(self, conn: Any, table: str, columns: str,
                   action_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
        selected = [_require_action_id(action_id) for action_id in action_ids]
        if not selected:
            return {}
        rows = self._bounded(conn.execute(
            f"SELECT {columns} FROM {self._q(table)} WHERE action_id = ANY(%s) ORDER BY action_id LIMIT %s",
            (selected, self.limit),
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
                 mapping_version: str = BACKFILL_MAPPING_VERSION):
        if not isinstance(mapping_version, str) or not mapping_version:
            raise db.ActionQError("mapping version is required")
        self.authority = authority
        self.source = source
        self.environment = _require_environment(environment)
        self.mapping_version = mapping_version
        self.principal = backfill_principal(environment=environment)

    def _ref(self, legacy_ref: str) -> str:
        return deterministic_resource_ref(legacy_ref, mapping_version=self.mapping_version)

    def _key(self, operation: str, fact: str) -> str:
        return f"backfill:{self.mapping_version}:{operation}:{fact}"

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

    def plan(self) -> tuple[BackfillCommand, ...]:
        """The full command sequence for the current legacy snapshot.

        Expected revisions come from this plan's own counters, never from live
        federation state: a rerun must re-issue byte-identical requests so the
        command-decision ledger replays them, and a request digest that embedded
        an already-advanced live revision would not replay.
        """
        legacy = self._read()
        commands: list[BackfillCommand] = []
        revisions: dict[str, int] = {}

        def create(legacy_ref: str) -> str:
            resource_ref = self._ref(legacy_ref)
            commands.append(BackfillCommand(
                "create", self._key("create", legacy_ref), resource_ref, 0, {"resource_ref": resource_ref},
            ))
            revisions[resource_ref] = 1
            return resource_ref

        def execution_ref(resource_ref: str, reference: str, kind: str) -> None:
            expected = revisions[resource_ref]
            commands.append(BackfillCommand(
                "record-execution-ref", self._key("record-execution-ref", reference), resource_ref, expected,
                {"resource_ref": resource_ref, "execution_ref": reference,
                 "assurance_type": legacy_assurance_type(kind)},
            ))
            revisions[resource_ref] = expected + 1

        def relation(source_ref: str, target_ref: str, fact: str) -> None:
            expected = revisions[source_ref]
            commands.append(BackfillCommand(
                "add-relation", self._key("add-relation", fact), source_ref, expected,
                {"source_ref": source_ref, "relation_type": "parent-of", "target_ref": target_ref},
            ))
            revisions[source_ref] = expected + 1

        planned: dict[int, str] = {}
        for action in legacy["actions"]:
            action_id = int(action["id"])
            legacy_ref = legacy_execution_ref(environment=self.environment, action_id=action_id)
            resource_ref = create(legacy_ref)
            planned[action_id] = resource_ref
            execution_ref(resource_ref, legacy_ref, "action")
            execution_ref(resource_ref, f"{legacy_ref}#status:{action['status']}", "status")
            for event in legacy["events"][action_id]:
                execution_ref(
                    resource_ref, f"{legacy_ref}#event:{int(event['id'])}", f"event:{event['event_type']}",
                )
            root = legacy["action_resources"].get(action_id)
            if root is not None:
                execution_ref(resource_ref, f"{legacy_ref}#action-resource:{root['resource_ref']}", "action-resource")
                execution_ref(
                    resource_ref, f"{legacy_ref}#action-resource-recovery-floor:{int(root['recovery_floor'])}",
                    "action-resource-recovery-floor",
                )
            candidate = legacy["candidate_requests"].get(action_id)
            if candidate is not None:
                execution_ref(
                    resource_ref, f"{legacy_ref}#candidate-request:{candidate['request_ref']}", "candidate-request",
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
            child_id = int(action["id"])
            relation(parent_ref, planned[child_id], f"parent-of:{int(parent_id)}:{child_id}")

        watermark = legacy["watermark"]
        if watermark is not None:
            legacy_ref = legacy_completion_ref(environment=self.environment)
            resource_ref = create(legacy_ref)
            planned[0] = resource_ref
            execution_ref(resource_ref, legacy_ref, "completion-watermark")
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
        return tuple(commands)

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

    @staticmethod
    def _fact(command: BackfillCommand) -> tuple[str, ...]:
        """This command's identity within its resource's revision chain."""
        if command.operation == "create":
            return ("create",)
        if command.operation == "record-execution-ref":
            return ("execution-ref", command.arguments["execution_ref"])
        return ("relation", command.arguments["relation_type"], command.arguments["target_ref"])

    def _recorded(self, conn: Any, resource_ref: str) -> tuple[int, dict[tuple[str, ...], int]]:
        schema = self.authority.schema
        root = conn.execute(
            f"SELECT revision FROM {db.qname(schema, 'federation_resources')} WHERE resource_ref=%s",
            (resource_ref,),
        ).fetchone()
        if root is None:
            return 0, {}
        recorded: dict[tuple[str, ...], int] = {("create",): 1}
        for row in conn.execute(
            f"SELECT execution_ref, source_revision FROM {db.qname(schema, 'federation_execution_refs')} "
            "WHERE resource_ref=%s", (resource_ref,),
        ).fetchall():
            recorded["execution-ref", str(row["execution_ref"])] = int(row["source_revision"])
        for row in conn.execute(
            f"SELECT relation_type, target_ref, source_revision FROM {db.qname(schema, 'federation_relations')} "
            "WHERE source_ref=%s", (resource_ref,),
        ).fetchall():
            recorded["relation", str(row["relation_type"]), str(row["target_ref"])] = int(row["source_revision"])
        return int(root["revision"]), recorded

    def _require_no_drift(self, commands: tuple[BackfillCommand, ...]) -> None:
        """Refuse, before writing anything, an import whose source has moved.

        Expected revisions come from the plan's own counters, so a legacy row
        that changes between runs shifts every later command in that resource's
        chain: the same idempotency key is re-issued under a different canonical
        request, which federation.py durably rejects as an idempotency-key
        conflict -- permanently, since every later run replans the same
        conflicting command.  A resume must therefore see the same source
        snapshot as the run it is resuming.  Detecting that here turns a cryptic
        mid-import abort into one upfront error naming the exact fact, and
        leaves no partial write behind.
        """
        by_resource: dict[str, list[BackfillCommand]] = {}
        for command in commands:
            by_resource.setdefault(command.resource_ref, []).append(command)
        with self.authority.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            for resource_ref, planned in by_resource.items():
                revision, recorded = self._recorded(conn, resource_ref)
                if revision == 0:
                    continue
                for command in planned:
                    fact = self._fact(command)
                    after = command.expected_revision + 1
                    position = recorded.pop(fact, None)
                    if after <= revision and position != after:
                        raise BackfillDrift(
                            f"legacy source changed under a part-recorded import: {fact} is planned at "
                            f"revision {after} but is recorded at {position}",
                            resource_ref=resource_ref, fact=":".join(fact),
                        )
                    if after > revision and position is not None:
                        raise BackfillDrift(
                            f"legacy source changed under a part-recorded import: {fact} is already "
                            f"recorded at revision {position} but is planned at {after}",
                            resource_ref=resource_ref, fact=":".join(fact),
                        )
                if recorded:
                    lost = sorted(recorded)[0]
                    raise BackfillDrift(
                        f"legacy source no longer offers a fact this import already recorded: {lost}",
                        resource_ref=resource_ref, fact=":".join(lost),
                    )

    def run(self) -> BackfillReport:
        """Apply the plan.

        Restartable against the same legacy snapshot: a resumed run replays the
        stored decision for every command already applied.  A source that has
        changed since a part-recorded run is refused up front by
        ``_require_no_drift`` rather than aborting partway through.
        """
        commands = self.plan()
        self._require_no_drift(commands)
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

    def verify(self) -> tuple[dict[str, Any], ...]:
        """Rebuild every planned resource from its changes and compare.

        One repeatable-read snapshot for the whole comparison: rebuild and live
        projection are separate statements, and a federation command committing
        between them would report a spurious mismatch on healthy data.
        """
        refs = [command.resource_ref for command in self.plan() if command.operation == "create"]
        with self.authority.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            return tuple(verify_rebuild(conn, self.authority.schema, resource_ref) for resource_ref in refs)


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
_EXPORT_TABLES: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
    ("federation_resources", (
        ("resource_ref", "text"), ("owner_principal_id", "text"), ("state", "text"),
        ("revision", "int"), ("recovery_floor", "int"), ("created_at", "time"), ("updated_at", "time"),
    )),
    ("federation_resource_changes", (
        ("resource_ref", "text"), ("revision", "int"), ("operation", "text"), ("state", "text"),
        ("actor_principal_id", "text"), ("payload_bytes", "bytes"), ("payload_digest", "text"),
        ("occurred_at", "time"),
    )),
    ("federation_relations", (
        ("source_ref", "text"), ("relation_type", "text"), ("target_ref", "text"),
        ("source_revision", "int"), ("created_at", "time"),
    )),
    ("federation_execution_refs", (
        ("resource_ref", "text"), ("execution_ref", "text"), ("assurance_type", "text"),
        ("source_revision", "int"), ("created_at", "time"),
    )),
    ("federation_evidence", (
        ("resource_ref", "text"), ("evidence_ref", "text"), ("evidence_digest", "text"),
        ("assurance_type", "text"), ("source_revision", "int"), ("created_at", "time"),
    )),
    ("federation_acceptance_decisions", (
        ("resource_ref", "text"), ("source_revision", "int"), ("outcome", "text"),
        ("policy_ref", "text"), ("evidence_ref", "text"), ("decided_by", "text"), ("created_at", "time"),
    )),
    ("federation_settlements", (
        ("resource_ref", "text"), ("source_revision", "int"), ("fact_ref", "text"),
        ("reconciled_by", "text"), ("created_at", "time"),
    )),
    ("federation_idempotency_bindings", (
        ("environment", "text"), ("principal_id", "text"), ("operation", "text"),
        ("idempotency_key", "text"), ("request_digest", "text"), ("created_at", "time"),
    )),
    ("federation_command_decisions", (
        ("environment", "text"), ("principal_id", "text"), ("operation", "text"),
        ("idempotency_key", "text"), ("request_digest", "text"), ("status", "text"),
        ("code", "text"), ("message", "text"), ("response_bytes", "bytes"),
        ("response_digest", "text"), ("resource_ref", "text"),
        ("before_revision", "int"), ("after_revision", "int"), ("decided_at", "time"),
    )),
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


def export_federation(conn: Any, schema: str) -> bytes:
    """Canonical logical dump of every federation v1 table.

    The durable-authoritative export named by the checked-in retention contract:
    independent of, and additional to, the whole-cluster physical backup, whose
    retention window is far shorter than federation data's.  Two exports of
    identical content are byte-identical.

    All nine tables are read in one repeatable-read snapshot.  Under the default
    read-committed isolation each statement takes a fresh snapshot even inside a
    single transaction, so a concurrent command could otherwise tear the
    document -- a change row for a resource missing from the resources dump, or
    a resource at revision N carrying only N-1 changes, either of which fails
    restore or rebuild.  Because a snapshot can only be selected before the
    transaction's first query, pass a connection with no statements yet issued
    in the current transaction.
    """
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    compatibility = federation_schema.require_compatible(conn, schema)
    tables: dict[str, Any] = {}
    for table, columns in _EXPORT_TABLES:
        names = [name for name, _ in columns]
        order = ",".join(str(position) for position in range(1, len(names) + 1))
        rows = conn.execute(
            f"SELECT {','.join(names)} FROM {_q(schema, table)} ORDER BY {order}"
        ).fetchall()
        tables[table] = {
            "columns": names,
            "rows": [[_encode(row[name], kind) for name, kind in columns] for row in rows],
        }
    return canonical_bytes({
        "schema_version": _EXPORT_SCHEMA_VERSION,
        "compatibility_label": compatibility.compatibility_label,
        "federation_schema_version": compatibility.observed_schema_version,
        "tables": tables,
    })


def restore_federation(conn: Any, schema: str, payload: bytes) -> dict[str, int]:
    """Restore an export into a freshly migrated, empty federation schema.

    Refuses a non-empty target: a restore must never partially overlay live
    federation data.  Tables are written in declared order, which is the
    foreign-key order every dependent table needs.
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise ExportError("export payload must be bytes")
    try:
        document = json.loads(bytes(payload))
    except (ValueError, UnicodeDecodeError) as malformed:
        raise ExportError(f"export payload is not JSON: {malformed}") from malformed
    if not isinstance(document, dict) or document.get("schema_version") != _EXPORT_SCHEMA_VERSION:
        raise ExportError("export payload is not a federation-export/v1 document")
    compatibility = federation_schema.require_compatible(conn, schema)
    if document.get("compatibility_label") != compatibility.compatibility_label:
        raise ExportError("export payload was produced under a different compatibility label")
    if document.get("federation_schema_version") != compatibility.observed_schema_version:
        raise ExportError("export payload was produced under a different federation schema version")
    tables = document.get("tables")
    if not isinstance(tables, dict) or set(tables) != {name for name, _ in _EXPORT_TABLES}:
        raise ExportError("export payload does not describe every federation table")

    restored: dict[str, int] = {}
    for table, _ in _EXPORT_TABLES:
        occupied = conn.execute(f"SELECT 1 FROM {_q(schema, table)} LIMIT 1").fetchone()
        if occupied is not None:
            raise ExportError(f"restore requires an empty federation schema: {table} is not empty")
    for table, columns in _EXPORT_TABLES:
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
