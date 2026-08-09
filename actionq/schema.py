"""Deployment-owned schema migration and runtime compatibility contract.

Migration callers need a database role with DDL authority. Runtime callers use
``check_compatibility``/``require_compatible`` only; those functions never
issue DDL and work with a role that can only read the migration ledger.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import asdict, dataclass
from importlib import resources
from typing import Any

from . import db


DOMAIN = "execution"
API_VERSION = "v1"
MIN_SCHEMA_VERSION = 1
MAX_SCHEMA_VERSION = 9
PRE_MIGRATION_BRIDGE_VERSION = 3
MIGRATION_TABLE = "schema_migrations"
_MIGRATION_RE = re.compile(r"^(?P<version>[0-9]{3})_[a-z0-9_]+\.sql$")
_COLUMN_SHAPE = {
    "actions": {
        "id": ("bigint", "NO", "sequence:actions_id_seq"),
        "action_type": ("text", "NO", None),
        "project": ("text", "YES", None),
        "target_ref": ("text", "YES", None),
        "source_refs": ("jsonb", "NO", "'[]'::jsonb"),
        "priority": ("integer", "NO", "100"),
        "status": ("text", "NO", "'pending'::text"),
        "parent_id": ("bigint", "YES", None),
        "chain_depth": ("integer", "NO", "0"),
        "created_at": ("timestamp with time zone", "NO", "now()"),
        "claimed_at": ("timestamp with time zone", "YES", None),
        "claimed_by": ("text", "YES", None),
        "claim_deadline": ("timestamp with time zone", "YES", None),
        "claim_receipt": ("text", "YES", None),
        "cancel_request_id": ("uuid", "YES", None),
        "cancel_stop_deadline": ("timestamp with time zone", "YES", None),
        "stop_acknowledged": ("boolean", "YES", None),
        "runner_auth_digest": ("text", "YES", None),
        "cancel_former_claimed_by": ("text", "YES", None),
        "cancel_former_receipt_digest": ("text", "YES", None),
        "cancel_runner_auth_digest": ("text", "YES", None),
        "cancel_terminal_kind": ("text", "YES", None),
        "completed_at": ("timestamp with time zone", "YES", None),
        "result_ref": ("text", "YES", None),
        "failure_reason": ("text", "YES", None),
        "created_by": ("text", "NO", None),
    },
    "events": {
        "id": ("bigint", "NO", "sequence:events_id_seq"),
        "action_id": ("bigint", "YES", None),
        "event_type": ("text", "NO", None),
        "timestamp": ("timestamp with time zone", "NO", "now()"),
        "actor": ("text", "YES", None),
        "payload": ("jsonb", "NO", "'{}'::jsonb"),
    },
    "dispatch_requests": {
        "action_id": ("bigint", "NO", None),
        "request_ref": ("text", "NO", None),
        "normalized_snapshot": ("bytea", "NO", None),
        "schema_version": ("text", "NO", None),
        "canonicalization_version": ("text", "NO", None),
        "request_sha256": ("text", "NO", None),
        "identity": ("text", "NO", None),
        "environment": ("text", "NO", None),
        "operation": ("text", "NO", None),
        "idempotency_key": ("text", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "dispatch_observation_watermarks": {
        "action_id": ("bigint", "NO", None),
        "first_retained_event_id": ("bigint", "NO", None),
        "last_observed_event_id": ("bigint", "NO", None),
        "watermark_updated_at": ("timestamp with time zone", "NO", "now()"),
    },
    "action_resources": {
        "resource_ref": ("text", "NO", None),
        "action_id": ("bigint", "NO", None),
        "principal_scope": ("text", "NO", None),
        "operation": ("text", "NO", None),
        "idempotency_key": ("text", "NO", None),
        "request_digest": ("text", "NO", None),
        "request_snapshot": ("bytea", "NO", None),
        "revision": ("bigint", "NO", None),
        "enqueue_revision": ("bigint", "NO", None),
        "recovery_floor": ("bigint", "NO", "0"),
        "state": ("text", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
        "updated_at": ("timestamp with time zone", "NO", "now()"),
    },
    "action_resource_changes": {
        "resource_ref": ("text", "NO", None),
        "revision": ("bigint", "NO", None),
        "kind": ("text", "NO", None),
        "state": ("text", "NO", None),
        "occurred_at": ("timestamp with time zone", "NO", "now()"),
    },
    "action_resource_sessions": {
        "resource_ref": ("text", "NO", None),
        "ordinal": ("integer", "NO", None),
        "state": ("text", "NO", None),
        "started_at": ("timestamp with time zone", "YES", None),
        "ended_at": ("timestamp with time zone", "YES", None),
    },
    "execution_groups": {
        "id": ("uuid", "NO", None),
        "plan_ref": ("text", "NO", None),
        "spec_sha256": ("text", "NO", None),
        "spec_snapshot": ("bytea", "NO", None),
        "max_parallel": ("integer", "NO", None),
        "failure_policy": ("text", "NO", None),
        "claim_state": ("text", "NO", "'active'::text"),
        "created_at": ("timestamp with time zone", "NO", "now()"),
        "created_by": ("text", "NO", None),
        "stopped_at": ("timestamp with time zone", "YES", None),
        "stopped_by": ("text", "YES", None),
        "stop_reason": ("text", "YES", None),
    },
    "execution_group_members": {
        "group_id": ("uuid", "NO", None),
        "action_id": ("bigint", "NO", None),
        "ordinal": ("integer", "NO", None),
        "envelope_digest": ("text", "NO", None),
        "envelope_snapshot": ("bytea", "NO", None),
    },
    "immutable_action_specs": {
        "spec_ref": ("text", "NO", None),
        "spec_digest": ("text", "NO", None),
        "spec_snapshot": ("bytea", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "immutable_action_requests": {
        "action_id": ("bigint", "NO", None),
        "request_ref": ("text", "NO", None),
        "request_digest": ("text", "NO", None),
        "request_snapshot": ("bytea", "NO", None),
        "plan_ref": ("text", "NO", None),
        "topology": ("text", "NO", None),
        "role": ("text", "NO", None),
        "subject": ("text", "NO", None),
        "spec_ref": ("text", "NO", None),
        "spec_digest": ("text", "NO", None),
        "input_set_digest": ("text", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "immutable_action_runtime_grants": {
        "action_id": ("bigint", "NO", None),
        "request_digest": ("text", "NO", None),
        "spec_digest": ("text", "NO", None),
        "input_set_digest": ("text", "NO", None),
        "grant_digest": ("text", "NO", None),
        "issued_at": ("timestamp with time zone", "NO", "now()"),
    },
}
_REQUIRED_COLUMNS = {
    table: set(columns) for table, columns in _COLUMN_SHAPE.items()
}
_REQUIRED_CONSTRAINT_COUNTS = {
    "actions": {"p": 1, "f": 1, "c": 1},
    "events": {"p": 1, "f": 1},
    "dispatch_requests": {"p": 1, "f": 1, "u": 2, "c": 3},
    "dispatch_observation_watermarks": {"p": 1, "f": 1, "c": 2},
    "action_resources": {"p": 1, "f": 1, "u": 3, "c": 5},
    "action_resource_changes": {"p": 1, "f": 1, "c": 3},
    "action_resource_sessions": {"p": 1, "f": 1, "c": 1},
    "execution_groups": {"p": 1, "u": 2, "c": 3},
    "execution_group_members": {"p": 1, "f": 2, "u": 2, "c": 1},
    "immutable_action_specs": {"p": 1, "u": 1, "c": 3},
    "immutable_action_requests": {"p": 1, "f": 2, "u": 3, "c": 5},
    "immutable_action_runtime_grants": {"p": 1, "f": 1, "c": 4},
}
_REQUIRED_INDEXES = {
    "actions.claim-lookup": (
        "actions",
        (("status", False, False), ("priority", False, False), ("created_at", False, False)),
        "status = 'pending'::text",
    ),
    "actions.parent": ("actions", (("parent_id", False, False),), None),
    "actions.project": ("actions", (("project", False, False),), None),
    "actions.deadline": (
        "actions",
        (("claim_deadline", False, False),),
        "status = 'claimed'::text",
    ),
    "events.action": ("events", (("action_id", False, False),), None),
    "events.timestamp": ("events", (("timestamp", True, True),), None),
    "events.type-time": (
        "events",
        (("event_type", False, False), ("timestamp", True, True)),
        None,
    ),
    "dispatch-requests.created": (
        "dispatch_requests", (("created_at", False, False), ("action_id", False, False)), None,
    ),
    "action-resource-changes.lookup": (
        "action_resource_changes", (("resource_ref", False, False), ("revision", False, False)), None,
    ),
    "execution-group-members.group": (
        "execution_group_members", (("group_id", False, False),), None,
    ),
    "execution-group-members.action": (
        "execution_group_members", (("action_id", False, False),), None,
    ),
    "immutable-action-requests.created": (
        "immutable_action_requests", (("created_at", False, False), ("action_id", False, False)), None,
    ),
}


class SchemaCompatibilityError(db.ActionQError):
    """The selected schema cannot safely serve this actionq release."""


class SchemaMigrationError(db.ActionQError):
    """A deployment migration could not establish the expected schema."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


@dataclass(frozen=True)
class Compatibility:
    domain: str
    api_version: str
    minimum_schema_version: int
    maximum_schema_version: int
    observed_schema_version: int | None
    state: str
    compatible: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _migration_root():
    return resources.files("actionq").joinpath("migrations")


def load_migrations() -> tuple[Migration, ...]:
    migrations: list[Migration] = []
    for path in sorted(_migration_root().iterdir(), key=lambda item: item.name):
        match = _MIGRATION_RE.fullmatch(path.name)
        if match is None:
            continue
        raw = path.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=int(match.group("version")),
                name=path.name,
                sql=raw,
                checksum=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
            )
        )
    versions = [migration.version for migration in migrations]
    expected = list(range(1, MAX_SCHEMA_VERSION + 1))
    if versions != expected:
        raise SchemaMigrationError(
            f"migration assets must contain exactly versions {expected}; found {versions}"
        )
    return tuple(migrations)


def _render(migration: Migration, schema: str) -> str:
    quoted_schema = f'"{db.schema_name(schema)}"'
    rendered = migration.sql.replace("{{schema}}", quoted_schema)
    if "{{schema}}" in rendered:
        raise SchemaMigrationError(f"unresolved schema placeholder in {migration.name}")
    return rendered


def _statements(rendered_sql: str) -> tuple[str, ...]:
    """Split the repository's plain DDL assets into one statement per execute.

    Actionq migration assets intentionally contain no procedural blocks or
    semicolons inside literals. Keeping each execute to one statement works
    with Psycopg's extended query protocol and keeps the transaction boundary
    in the migration runner.
    """

    return tuple(statement.strip() for statement in rendered_sql.split(";") if statement.strip())


def _row_value(row: Any, key: str, index: int = 0) -> Any:
    if isinstance(row, dict):
        return row[key]
    return row[index]


def _ledger_exists(conn, schema: str) -> bool:
    row = conn.execute(
        "SELECT to_regclass(%s) AS relation",
        (db.qname(schema, MIGRATION_TABLE),),
    ).fetchone()
    return bool(row and _row_value(row, "relation"))


def _applied_migrations(conn, schema: str) -> dict[int, str]:
    rows = conn.execute(
        f"SELECT version, checksum FROM {db.qname(schema, MIGRATION_TABLE)} "
        "WHERE domain = %s ORDER BY version",
        (DOMAIN,),
    ).fetchall()
    return {
        int(_row_value(row, "version")): str(_row_value(row, "checksum", 1))
        for row in rows
    }


def _require_schema8_dispatch_root_quiescence(conn, schema: str) -> None:
    """Refuse v8 while a v7-era observer root still exists.

    Version 7 backfilled observer floors from retained events.  Once events
    have been pruned, no subsequent migration can prove the earlier floor, so
    v8 intentionally does not rewrite those rows.  The deployment preflight
    must retire every dispatch root first; ordinary terminal action/event
    history is not a root and remains untouched.
    """

    row = conn.execute(
        f"SELECT COUNT(*) AS dispatch_roots FROM {db.qname(schema, 'dispatch_requests')}"
    ).fetchone()
    count = int(_row_value(row, "dispatch_roots")) if row is not None else 0
    if count:
        raise SchemaMigrationError(
            "schema 8 requires zero pre-v7 dispatch roots; "
            f"found {count} row(s) in dispatch_requests"
        )


def _lock_schema8_dispatch_roots(conn, schema: str) -> None:
    """Fence writes to v7 dispatch roots for the v8 quiescence check.

    ``SHARE ROW EXCLUSIVE`` conflicts with the ``ROW EXCLUSIVE`` lock taken by
    INSERT.  The migrator holds it from the root count through the v8 ledger
    write, so an enqueue cannot slip between the zero-root observation and the
    migration. Deployment preflight still owns draining already-open clients.
    """

    conn.execute(
        f"LOCK TABLE {db.qname(schema, 'dispatch_requests')} IN SHARE ROW EXCLUSIVE MODE"
    )


def _data_tables_exist(conn, schema: str) -> bool:
    rows = conn.execute(
        "SELECT to_regclass(%s) AS actions, to_regclass(%s) AS events",
        (db.qname(schema, "actions"), db.qname(schema, "events")),
    ).fetchone()
    return bool(
        rows
        and (
            _row_value(rows, "actions")
            or _row_value(rows, "events", 1)
        )
    )


def _canonical_sql(value: Any) -> str:
    """Collapse only insignificant SQL whitespace outside quoted tokens.

    PostgreSQL catalog deparsers already normalize unquoted identifiers. This
    helper deliberately preserves case and contents inside string and quoted
    identifier tokens, and it never removes ordering or other SQL clauses.
    """

    output: list[str] = []
    quote: str | None = None
    pending_space = False
    raw = str(value).strip()
    index = 0
    while index < len(raw):
        character = raw[index]
        if quote is not None:
            output.append(character)
            if character == quote:
                if index + 1 < len(raw) and raw[index + 1] == quote:
                    index += 1
                    output.append(raw[index])
                else:
                    quote = None
            index += 1
            continue
        if character in ("'", '"'):
            if pending_space and output:
                output.append(" ")
            pending_space = False
            quote = character
            output.append(character)
        elif character.isspace():
            pending_space = True
        else:
            if pending_space and output:
                output.append(" ")
            pending_space = False
            output.append(character)
        index += 1
    return "".join(output).strip()


def _without_redundant_outer_parentheses(value: Any) -> str:
    canonical = _canonical_sql(value)
    while canonical.startswith("(") and canonical.endswith(")"):
        depth = 0
        quote: str | None = None
        wraps_entire_expression = True
        index = 0
        while index < len(canonical):
            character = canonical[index]
            if quote is not None:
                if character == quote:
                    if index + 1 < len(canonical) and canonical[index + 1] == quote:
                        index += 1
                    else:
                        quote = None
            elif character in ("'", '"'):
                quote = character
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0 and index != len(canonical) - 1:
                    wraps_entire_expression = False
                    break
            index += 1
        if not wraps_entire_expression or depth != 0:
            break
        canonical = _canonical_sql(canonical[1:-1])
    return canonical


def _canonical_default(schema: str, expected: str | None, value: Any) -> str | None:
    if value is None:
        return None
    canonical = _canonical_sql(value)
    if expected and expected.startswith("sequence:"):
        sequence = expected.removeprefix("sequence:")
        accepted = {
            _canonical_sql(f"nextval('{sequence}'::regclass)"),
            _canonical_sql(f"nextval('{schema}.{sequence}'::regclass)"),
            _canonical_sql(f"nextval('\"{schema}\".{sequence}'::regclass)"),
        }
        return expected if canonical in accepted else canonical
    return canonical


def _runtime_principal_issue(conn, schema: str) -> str | None:
    row = conn.execute(
        """
        SELECT current_user AS principal,
               principal_record.rolsuper AS is_superuser,
               schema_record.nspowner = principal_record.oid AS owns_schema,
               pg_has_role(current_user, schema_record.nspowner, 'MEMBER')
                   AS can_assume_schema_owner,
               has_schema_privilege(current_user, schema_record.oid, 'CREATE')
                   AS can_create,
               COALESCE(bool_or(
                   relation_record.relowner = principal_record.oid
               ), false) AS owns_relation,
               COALESCE(bool_or(
                   relation_record.relowner IS NOT NULL
                   AND pg_has_role(current_user, relation_record.relowner, 'MEMBER')
               ), false) AS can_assume_relation_owner,
               COALESCE(bool_or(
                   relation_record.relname = %s
                   AND relation_record.relkind IN ('r', 'p')
                   AND (
                       has_table_privilege(current_user, relation_record.oid, 'INSERT')
                       OR has_table_privilege(current_user, relation_record.oid, 'UPDATE')
                       OR has_table_privilege(current_user, relation_record.oid, 'DELETE')
                       OR has_table_privilege(current_user, relation_record.oid, 'TRUNCATE')
                       OR has_table_privilege(current_user, relation_record.oid, 'REFERENCES')
                       OR has_table_privilege(current_user, relation_record.oid, 'TRIGGER')
                   )
               ), false) AS can_write_ledger
        FROM pg_namespace AS schema_record
        JOIN pg_roles AS principal_record ON principal_record.rolname = current_user
        LEFT JOIN pg_class AS relation_record
          ON relation_record.relnamespace = schema_record.oid
         AND relation_record.relkind IN ('r', 'p', 'S', 'i', 'I')
        WHERE schema_record.nspname = %s
        GROUP BY principal_record.oid,
                 principal_record.rolsuper,
                 schema_record.oid,
                 schema_record.nspowner
        """,
        (MIGRATION_TABLE, schema),
    ).fetchone()
    if row is None:
        return "runtime principal authority could not be established"
    checks = (
        ("is_superuser", 1, "runtime principal is superuser"),
        ("owns_schema", 2, "runtime principal owns the domain schema"),
        (
            "can_assume_schema_owner",
            3,
            "runtime principal can assume the domain schema owner",
        ),
        ("can_create", 4, "runtime principal has schema CREATE authority"),
        ("owns_relation", 5, "runtime principal owns a domain relation"),
        (
            "can_assume_relation_owner",
            6,
            "runtime principal can assume a domain relation owner",
        ),
        ("can_write_ledger", 7, "runtime principal can mutate the migration ledger"),
    )
    for key, index, detail in checks:
        if bool(_row_value(row, key, index)):
            return detail
    return None


def _shape_issues(conn, schema: str) -> tuple[str, ...]:
    """Return deterministic schema-shape issues using SELECT statements only."""

    issues: list[str] = []
    rows = conn.execute(
        """
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = ANY(%s)
        """,
        (schema, list(_REQUIRED_COLUMNS)),
    ).fetchall()
    observed: dict[str, dict[str, tuple[str, str, str | None]]] = {
        table: {} for table in _COLUMN_SHAPE
    }
    for row in rows:
        table = str(_row_value(row, "table_name"))
        column = str(_row_value(row, "column_name", 1))
        if table in observed and column in _COLUMN_SHAPE[table]:
            expected_default = _COLUMN_SHAPE[table][column][2]
            observed[table][column] = (
                str(_row_value(row, "data_type", 2)),
                str(_row_value(row, "is_nullable", 3)),
                _canonical_default(
                    schema,
                    expected_default,
                    _row_value(row, "column_default", 4),
                ),
            )
        elif table in observed:
            issues.append(f"column-unexpected:{table}.{column}")
    for table, columns in _COLUMN_SHAPE.items():
        for column, expected in columns.items():
            actual = observed[table].get(column)
            if actual is None:
                issues.append(f"column-missing:{table}.{column}")
            else:
                if actual[0] != expected[0]:
                    issues.append(f"column-type:{table}.{column}")
                if actual[1] != expected[1]:
                    issues.append(f"column-nullability:{table}.{column}")
                if actual[2] != expected[2]:
                    issues.append(f"column-default:{table}.{column}")

    constraint_rows = conn.execute(
        """
        SELECT relation.relname AS table_name,
               relation.oid AS relation_oid,
               constraint_record.contype,
               ARRAY(
                   SELECT attribute_record.attname
                   FROM unnest(constraint_record.conkey) WITH ORDINALITY AS key_record(attnum, position)
                   JOIN pg_attribute AS attribute_record
                     ON attribute_record.attrelid = constraint_record.conrelid
                    AND attribute_record.attnum = key_record.attnum
                   ORDER BY key_record.position
               ) AS columns,
               foreign_namespace.nspname AS foreign_namespace,
               foreign_relation.relname AS foreign_table,
               foreign_relation.oid AS foreign_oid,
               ARRAY(
                   SELECT foreign_attribute.attname
                   FROM unnest(constraint_record.confkey) WITH ORDINALITY AS key_record(attnum, position)
                   JOIN pg_attribute AS foreign_attribute
                     ON foreign_attribute.attrelid = constraint_record.confrelid
                    AND foreign_attribute.attnum = key_record.attnum
                   ORDER BY key_record.position
               ) AS foreign_columns,
               constraint_record.confupdtype AS update_action,
               constraint_record.confdeltype AS delete_action,
               constraint_record.confmatchtype AS match_action,
               constraint_record.condeferrable AS is_deferrable,
               constraint_record.condeferred AS is_initially_deferred,
               constraint_record.convalidated AS is_validated,
               pg_get_expr(
                   constraint_record.conbin,
                   constraint_record.conrelid,
                   true
               ) AS expression
        FROM pg_constraint AS constraint_record
        JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid
        JOIN pg_namespace AS namespace_record ON namespace_record.oid = relation.relnamespace
        LEFT JOIN pg_class AS foreign_relation ON foreign_relation.oid = constraint_record.confrelid
        LEFT JOIN pg_namespace AS foreign_namespace
          ON foreign_namespace.oid = foreign_relation.relnamespace
        WHERE namespace_record.nspname = %s AND relation.relname = ANY(%s)
        """,
        (schema, list(_REQUIRED_CONSTRAINT_COUNTS)),
    ).fetchall()
    constraints: list[dict[str, Any]] = []
    for row in constraint_rows:
        constraints.append(
            {
                "table": str(_row_value(row, "table_name")),
                "relation_oid": _row_value(row, "relation_oid", 1),
                "type": str(_row_value(row, "contype", 2)),
                "columns": tuple(_row_value(row, "columns", 3) or ()),
                "foreign_namespace": _row_value(row, "foreign_namespace", 4),
                "foreign_table": _row_value(row, "foreign_table", 5),
                "foreign_oid": _row_value(row, "foreign_oid", 6),
                "foreign_columns": tuple(_row_value(row, "foreign_columns", 7) or ()),
                "update_action": str(_row_value(row, "update_action", 8)),
                "delete_action": str(_row_value(row, "delete_action", 9)),
                "match_action": str(_row_value(row, "match_action", 10)),
                "is_deferrable": bool(_row_value(row, "is_deferrable", 11)),
                "is_initially_deferred": bool(
                    _row_value(row, "is_initially_deferred", 12)
                ),
                "is_validated": bool(_row_value(row, "is_validated", 13)),
                "expression": str(_row_value(row, "expression", 14) or ""),
            }
        )
    table_oids = {
        constraint["table"]: constraint["relation_oid"]
        for constraint in constraints
        if constraint["table"] in _REQUIRED_CONSTRAINT_COUNTS
    }

    def has_constraint(
        table: str,
        constraint_type: str,
        columns: tuple[str, ...],
        *,
        foreign_namespace: str | None = None,
        foreign_table: str | None = None,
        foreign_oid: Any = None,
        foreign_columns: tuple[str, ...] = (),
        update_action: str = "a",
        delete_action: str = "a",
        match_action: str = "s",
        is_deferrable: bool = False,
        is_initially_deferred: bool = False,
        is_validated: bool = True,
    ) -> bool:
        return any(
            constraint["table"] == table
            and constraint["type"] == constraint_type
            and constraint["columns"] == columns
            and (
                foreign_table is None
                or (
                    constraint["foreign_namespace"] == foreign_namespace
                    and constraint["foreign_table"] == foreign_table
                    and constraint["foreign_oid"] == foreign_oid
                    and constraint["foreign_columns"] == foreign_columns
                    and constraint["update_action"] == update_action
                    and constraint["delete_action"] == delete_action
                    and constraint["match_action"] == match_action
                    and constraint["is_deferrable"] is is_deferrable
                    and constraint["is_initially_deferred"] is is_initially_deferred
                    and constraint["is_validated"] is is_validated
                )
            )
            for constraint in constraints
        )

    required_constraints = (
        ("actions-primary-key", has_constraint("actions", "p", ("id",))),
        (
            "actions-parent-foreign-key",
            has_constraint(
                "actions",
                "f",
                ("parent_id",),
                foreign_namespace=schema,
                foreign_table="actions",
                foreign_oid=table_oids.get("actions"),
                foreign_columns=("id",),
            ),
        ),
        ("events-primary-key", has_constraint("events", "p", ("id",))),
        (
            "events-action-foreign-key",
            has_constraint(
                "events",
                "f",
                ("action_id",),
                foreign_namespace=schema,
                foreign_table="actions",
                foreign_oid=table_oids.get("actions"),
                foreign_columns=("id",),
            ),
        ),
        ("dispatch-requests-primary-key", has_constraint("dispatch_requests", "p", ("action_id",))),
        (
            "dispatch-requests-action-foreign-key",
            has_constraint(
                "dispatch_requests", "f", ("action_id",),
                foreign_namespace=schema,
                foreign_table="actions",
                foreign_oid=table_oids.get("actions"),
                foreign_columns=("id",),
            ),
        ),
        ("dispatch-requests-request-ref-unique", has_constraint("dispatch_requests", "u", ("request_ref",))),
        (
            "dispatch-requests-idempotency-binding-unique",
            has_constraint("dispatch_requests", "u", ("identity", "environment", "operation", "idempotency_key")),
        ),
        ("dispatch-observation-watermarks-primary-key", has_constraint("dispatch_observation_watermarks", "p", ("action_id",))),
        (
            "dispatch-observation-watermarks-action-foreign-key",
            has_constraint(
                "dispatch_observation_watermarks", "f", ("action_id",),
                foreign_namespace=schema,
                foreign_table="actions",
                foreign_oid=table_oids.get("actions"),
                foreign_columns=("id",),
                delete_action="r",
            ),
        ),
        ("execution-groups-primary-key", has_constraint("execution_groups", "p", ("id",))),
        ("execution-groups-plan-ref-unique", has_constraint("execution_groups", "u", ("plan_ref",))),
        ("execution-groups-spec-sha256-unique", has_constraint("execution_groups", "u", ("spec_sha256",))),
        (
            "execution-group-members-primary-key",
            has_constraint("execution_group_members", "p", ("group_id", "action_id")),
        ),
        (
            "execution-group-members-group-foreign-key",
            has_constraint(
                "execution_group_members", "f", ("group_id",),
                foreign_namespace=schema, foreign_table="execution_groups",
                foreign_oid=table_oids.get("execution_groups"), foreign_columns=("id",),
                delete_action="r",
            ),
        ),
        (
            "execution-group-members-action-foreign-key",
            has_constraint(
                "execution_group_members", "f", ("action_id",),
                foreign_namespace=schema, foreign_table="actions",
                foreign_oid=table_oids.get("actions"), foreign_columns=("id",),
                delete_action="r",
            ),
        ),
        (
            "execution-group-members-action-unique",
            has_constraint("execution_group_members", "u", ("action_id",)),
        ),
        (
            "execution-group-members-ordinal-unique",
            has_constraint("execution_group_members", "u", ("group_id", "ordinal")),
        ),
        ("immutable-action-specs-primary-key", has_constraint("immutable_action_specs", "p", ("spec_ref",))),
        ("immutable-action-specs-digest-unique", has_constraint("immutable_action_specs", "u", ("spec_digest",))),
        ("immutable-action-requests-primary-key", has_constraint("immutable_action_requests", "p", ("action_id",))),
        (
            "immutable-action-requests-action-foreign-key",
            has_constraint("immutable_action_requests", "f", ("action_id",),
                           foreign_namespace=schema, foreign_table="actions",
                           foreign_oid=table_oids.get("actions"), foreign_columns=("id",), delete_action="r"),
        ),
        (
            "immutable-action-requests-spec-foreign-key",
            has_constraint("immutable_action_requests", "f", ("spec_ref",),
                           foreign_namespace=schema, foreign_table="immutable_action_specs",
                           foreign_oid=table_oids.get("immutable_action_specs"), foreign_columns=("spec_ref",), delete_action="r"),
        ),
        ("immutable-action-requests-request-ref-unique", has_constraint("immutable_action_requests", "u", ("request_ref",))),
        ("immutable-action-requests-request-digest-unique", has_constraint("immutable_action_requests", "u", ("request_digest",))),
        ("immutable-action-requests-logical-identity-unique", has_constraint("immutable_action_requests", "u", ("plan_ref", "topology", "role", "subject"))),
        ("immutable-action-runtime-grants-primary-key", has_constraint("immutable_action_runtime_grants", "p", ("action_id",))),
        (
            "immutable-action-runtime-grants-action-foreign-key",
            has_constraint("immutable_action_runtime_grants", "f", ("action_id",),
                           foreign_namespace=schema, foreign_table="actions",
                           foreign_oid=table_oids.get("actions"), foreign_columns=("id",), delete_action="r"),
        ),
    )
    for name, present in required_constraints:
        if not present:
            issues.append(f"constraint-missing-or-invalid:{name}")
    status_checks = [
        constraint
        for constraint in constraints
        if constraint["table"] == "actions"
        and constraint["type"] == "c"
        and constraint["columns"] == ("status",)
    ]
    expected_status_expression = _without_redundant_outer_parentheses(
        "status = ANY (ARRAY["
        + ", ".join(f"'{status}'::text" for status in (
            "pending",
            "claimed",
            "cancelling",
            "completed",
            "failed",
            "rejected",
            "cancelled",
        ))
        + "])"
    )
    actual_status_expression = (
        _without_redundant_outer_parentheses(status_checks[0]["expression"])
        if len(status_checks) == 1
        else ""
    )
    if len(status_checks) != 1 or actual_status_expression != expected_status_expression:
        issues.append("constraint-invalid:actions.status")
    # These checks and uniqueness constraints make the v2 binding append-only
    # in meaning: an Action cannot be rebound to another ref, digest, or
    # idempotency owner by malformed schema drift.  Match the complete
    # canonical expression: a permissive `OR true` is not an equivalent check.
    dispatch_checks = [
        constraint for constraint in constraints
        if constraint["table"] == "dispatch_requests" and constraint["type"] == "c"
    ]
    required_dispatch_checks = {
        "schema_version": "schema_version = 'v2'::text",
        "request_sha256": "request_sha256 ~ '^[a-f0-9]{64}$'::text",
        "operation": "operation = 'execution.dispatch.enqueue'::text",
    }
    for column, expected_expression in required_dispatch_checks.items():
        actual = [
            (
                _without_redundant_outer_parentheses(constraint["expression"]),
                constraint["is_validated"],
            )
            for constraint in dispatch_checks
            if constraint["columns"] == (column,)
        ]
        if (
            len(actual) != 1
            or actual[0][0] != _without_redundant_outer_parentheses(expected_expression)
            or actual[0][1] is not True
        ):
            issues.append(f"constraint-missing-or-invalid:dispatch_requests.{column}")

    required_watermark_checks = {
        "first_retained_event_id": "first_retained_event_id >= 0",
        "last_observed_event_id": "last_observed_event_id >= 0",
    }
    for column, expected_expression in required_watermark_checks.items():
        actual = [
            (_without_redundant_outer_parentheses(constraint["expression"]), constraint["is_validated"])
            for constraint in constraints
            if constraint["table"] == "dispatch_observation_watermarks"
            and constraint["type"] == "c"
            and constraint["columns"] == (column,)
        ]
        if (
            len(actual) != 1
            or actual[0][0] != _without_redundant_outer_parentheses(expected_expression)
            or actual[0][1] is not True
        ):
            issues.append(f"constraint-missing-or-invalid:dispatch_observation_watermarks.{column}")

    required_group_checks = {
        ("execution_groups", "max_parallel"): "max_parallel >= 1 AND max_parallel <= 32",
        ("execution_groups", "failure_policy"): "failure_policy = 'continue-independent'::text",
        ("execution_groups", "claim_state"): "claim_state = ANY (ARRAY['active'::text, 'stopped'::text])",
        ("execution_group_members", "ordinal"): "ordinal >= 0",
    }
    for (table, column), expected_expression in required_group_checks.items():
        actual = [
            (_without_redundant_outer_parentheses(constraint["expression"]), constraint["is_validated"])
            for constraint in constraints
            if constraint["table"] == table
            and constraint["type"] == "c"
            and constraint["columns"] == (column,)
        ]
        if (
            len(actual) != 1
            or actual[0][0] != _without_redundant_outer_parentheses(expected_expression)
            or actual[0][1] is not True
        ):
            issues.append(f"constraint-missing-or-invalid:{table}.{column}")

    index_rows = conn.execute(
        """
        SELECT relation.relname AS table_name,
               index_record.indisvalid,
               index_record.indisready,
               index_record.indisunique,
               access_method.amname AS access_method,
               ARRAY(
                   SELECT COALESCE(
                       (
                           SELECT attribute_record.attname
                           FROM pg_attribute AS attribute_record
                           WHERE attribute_record.attrelid = index_record.indrelid
                             AND attribute_record.attnum = index_record.indkey[position - 1]
                       ),
                       pg_get_indexdef(index_record.indexrelid, position, true)
                   )
                   FROM generate_series(1, index_record.indnkeyatts) AS position
                   ORDER BY position
               ) AS expressions,
               ARRAY(
                   SELECT (index_record.indoption[position - 1] & 1) = 1
                   FROM generate_series(1, index_record.indnkeyatts) AS position
                   ORDER BY position
               ) AS descending,
               ARRAY(
                   SELECT (index_record.indoption[position - 1] & 2) = 2
                   FROM generate_series(1, index_record.indnkeyatts) AS position
                   ORDER BY position
               ) AS nulls_first,
               pg_get_expr(index_record.indpred, index_record.indrelid, true) AS predicate
        FROM pg_index AS index_record
        JOIN pg_class AS relation ON relation.oid = index_record.indrelid
        JOIN pg_class AS index_relation ON index_relation.oid = index_record.indexrelid
        JOIN pg_am AS access_method ON access_method.oid = index_relation.relam
        JOIN pg_namespace AS namespace_record ON namespace_record.oid = relation.relnamespace
        WHERE namespace_record.nspname = %s
          AND relation.relname = ANY(%s)
          AND NOT index_record.indisprimary
        """,
        (schema, list(_REQUIRED_COLUMNS)),
    ).fetchall()
    indexes = []
    for row in index_rows:
        expressions = tuple(_row_value(row, "expressions", 5) or ())
        descending = tuple(_row_value(row, "descending", 6) or ())
        nulls_first = tuple(_row_value(row, "nulls_first", 7) or ())
        indexes.append(
            {
                "table": str(_row_value(row, "table_name")),
                "valid": bool(_row_value(row, "indisvalid", 1)),
                "ready": bool(_row_value(row, "indisready", 2)),
                "unique": bool(_row_value(row, "indisunique", 3)),
                "access_method": str(_row_value(row, "access_method", 4)),
                "keys": tuple(
                    (_canonical_sql(expression), bool(is_desc), bool(is_nulls_first))
                    for expression, is_desc, is_nulls_first in zip(
                        expressions, descending, nulls_first, strict=True
                    )
                ),
                "predicate": (
                    _without_redundant_outer_parentheses(
                        _row_value(row, "predicate", 8)
                    )
                    if _row_value(row, "predicate", 8) is not None
                    else None
                ),
            }
        )
    for name, (table, keys, predicate) in _REQUIRED_INDEXES.items():
        if not any(
            index["table"] == table
            and index["valid"]
            and index["ready"]
            and not index["unique"]
            and index["access_method"] == "btree"
            and index["keys"] == keys
            and index["predicate"]
            == (_without_redundant_outer_parentheses(predicate) if predicate else None)
            for index in indexes
        ):
            issues.append(f"index-missing-or-invalid:{name}")
    return tuple(sorted(issues))


def _unversioned_v1_shape_issues(issues: tuple[str, ...] | list[str]) -> list[str]:
    """Keep legacy-v1 adoption strict while allowing later packaged deltas.

    An unledgered installation can legitimately contain only the v1 action and
    event relations.  Claim receipts arrive in v2 and dispatch bindings in v3;
    their *absence* must not prevent migration 1 from recording the pre-v2
    baseline.  Any malformed existing v3 relation remains a refusal.
    """
    expected_later_absence = (
        "column-missing:actions.claim_receipt",
        "column-missing:actions.cancel_request_id",
        "column-missing:actions.cancel_stop_deadline",
        "column-missing:actions.stop_acknowledged",
        "column-missing:actions.runner_auth_digest",
        "column-missing:actions.cancel_former_claimed_by",
        "column-missing:actions.cancel_former_receipt_digest",
        "column-missing:actions.cancel_runner_auth_digest",
        "column-missing:actions.cancel_terminal_kind",
        "column-missing:dispatch_requests.",
        "column-missing:dispatch_observation_watermarks.",
        "constraint-missing-or-invalid:dispatch-requests-",
        "constraint-missing-or-invalid:dispatch_requests.",
        "constraint-missing-or-invalid:dispatch-observation-watermarks-",
        "constraint-missing-or-invalid:dispatch_observation_watermarks.",
        "index-missing-or-invalid:dispatch-requests.created",
        "column-missing:execution_groups.",
        "column-missing:execution_group_members.",
        "constraint-missing-or-invalid:execution-groups-",
        "constraint-missing-or-invalid:execution-group-members-",
        "constraint-missing-or-invalid:execution_groups.",
        "constraint-missing-or-invalid:execution_group_members.",
        "index-missing-or-invalid:execution-group-members.",
        "column-missing:immutable_action_specs.",
        "column-missing:immutable_action_requests.",
        "column-missing:immutable_action_runtime_grants.",
        "constraint-missing-or-invalid:immutable-action-",
        "constraint-missing-or-invalid:immutable_action_",
        "index-missing-or-invalid:immutable-action-requests.",
        "column-missing:action_resources.",
        "column-missing:action_resource_changes.",
        "column-missing:action_resource_sessions.",
        "constraint-missing-or-invalid:action-resource-",
        "constraint-missing-or-invalid:action_resource_",
        "index-missing-or-invalid:action-resource-",
    )
    return [
        issue for issue in issues
        if not issue.startswith(expected_later_absence)
    ]


def _schema3_bridge_shape_issues(conn, schema: str) -> tuple[str, ...]:
    """Validate the exact released v3 shape without accepting v4+ fragments."""

    all_issues = list(_shape_issues(conn, schema))
    issues = _unversioned_v1_shape_issues(all_issues)
    # Unlike legacy-v1 adoption, v3 requires the v2 claim receipt column.
    if "column-missing:actions.claim_receipt" in all_issues:
        issues.append("column-missing:actions.claim_receipt")
    if (
        "constraint-invalid:actions.status" in issues
        and _has_legacy_v1_status_constraint(conn, schema)
    ):
        issues.remove("constraint-invalid:actions.status")

    later_action_columns = (
        "cancel_request_id",
        "cancel_stop_deadline",
        "stop_acknowledged",
        "runner_auth_digest",
        "cancel_former_claimed_by",
        "cancel_former_receipt_digest",
        "cancel_runner_auth_digest",
        "cancel_terminal_kind",
    )
    later_tables = (
        "execution_groups",
        "execution_group_members",
        "immutable_action_specs",
        "immutable_action_requests",
        "immutable_action_runtime_grants",
        "dispatch_observation_watermarks",
        "action_resources",
        "action_resource_changes",
        "action_resource_sessions",
    )
    rows = conn.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND (
              (table_name = 'actions' AND column_name = ANY(%s))
              OR table_name = ANY(%s)
          )
        ORDER BY table_name, ordinal_position
        """,
        (schema, list(later_action_columns), list(later_tables)),
    ).fetchall()
    issues.extend(
        f"post-v3-object-present:{_row_value(row, 'table_name')}."
        f"{_row_value(row, 'column_name', 1)}"
        for row in rows
    )
    return tuple(sorted(set(issues)))


def _has_legacy_v1_status_constraint(conn, schema: str) -> bool:
    row = conn.execute(
        """SELECT pg_get_expr(c.conbin, c.conrelid, true) AS expression
           FROM pg_constraint c
           JOIN pg_class r ON r.oid=c.conrelid
           JOIN pg_namespace n ON n.oid=r.relnamespace
           WHERE n.nspname=%s AND r.relname='actions' AND c.conname='actions_status_check'""",
        (schema,),
    ).fetchone()
    if row is None:
        return False
    actual = _without_redundant_outer_parentheses(str(_row_value(row, "expression")))
    expected = _without_redundant_outer_parentheses(
        "status = ANY (ARRAY[" + ", ".join(
            f"'{status}'::text" for status in
            ("pending", "claimed", "completed", "failed", "rejected", "cancelled")
        ) + "])"
    )
    return actual == expected


def check_compatibility(
    conn,
    schema: str,
    *,
    require_runtime_principal: bool = True,
) -> Compatibility:
    """Inspect schema compatibility using SELECT statements only."""

    schema = db.schema_name(schema)
    if not _ledger_exists(conn, schema):
        return Compatibility(
            domain=DOMAIN,
            api_version=API_VERSION,
            minimum_schema_version=MIN_SCHEMA_VERSION,
            maximum_schema_version=MAX_SCHEMA_VERSION,
            observed_schema_version=None,
            state="uninitialized",
            compatible=False,
            detail="migration ledger is absent; run the deployment migration entrypoint",
        )

    applied = _applied_migrations(conn, schema)
    observed = max(applied, default=0)
    expected = {migration.version: migration.checksum for migration in load_migrations()}
    if not applied:
        state = "uninitialized"
        detail = "migration ledger contains no execution-domain versions"
    elif observed > MAX_SCHEMA_VERSION:
        state = "too-new"
        detail = (
            f"schema version {observed} exceeds supported maximum "
            f"{MAX_SCHEMA_VERSION}"
        )
    elif observed < MIN_SCHEMA_VERSION:
        state = "too-old"
        detail = (
            f"schema version {observed} is below supported minimum "
            f"{MIN_SCHEMA_VERSION}"
        )
    elif any(
        version not in expected or applied[version] != expected[version]
        for version in applied
    ):
        state = "checksum-mismatch"
        detail = "an applied migration checksum does not match the packaged asset"
    elif set(applied) not in (set(expected), set(range(1, PRE_MIGRATION_BRIDGE_VERSION + 1))):
        state = "incomplete"
        detail = (
            f"applied migration versions {sorted(applied)} do not match "
            f"expected versions {sorted(expected)}"
        )
    elif require_runtime_principal and (
        principal_issue := _runtime_principal_issue(conn, schema)
    ):
        state = "role-mismatch"
        detail = principal_issue
    else:
        shape_issues = (
            _schema3_bridge_shape_issues(conn, schema)
            if set(applied) == set(range(1, PRE_MIGRATION_BRIDGE_VERSION + 1))
            else _shape_issues(conn, schema)
        )
        if shape_issues:
            state = "shape-mismatch"
            detail = "required queue shape is invalid: " + ",".join(shape_issues)
        else:
            state = "compatible"
            detail = (
                "schema 3 is compatible through the read-only pre-migration bridge"
                if observed == PRE_MIGRATION_BRIDGE_VERSION
                else "schema is compatible with the packaged execution adapter"
            )

    return Compatibility(
        domain=DOMAIN,
        api_version=API_VERSION,
        minimum_schema_version=MIN_SCHEMA_VERSION,
        maximum_schema_version=MAX_SCHEMA_VERSION,
        observed_schema_version=observed or None,
        state=state,
        compatible=state == "compatible",
        detail=detail,
    )


def require_compatible(
    conn,
    schema: str,
    *,
    require_runtime_principal: bool = True,
) -> Compatibility:
    compatibility = check_compatibility(
        conn,
        schema,
        require_runtime_principal=require_runtime_principal,
    )
    if not compatibility.compatible:
        raise SchemaCompatibilityError(
            f"actionq schema {schema!r} is {compatibility.state}: "
            f"{compatibility.detail}"
        )
    return compatibility


def _grant_runtime_privileges(conn, schema: str, runtime_role: str | None) -> None:
    if runtime_role is None:
        return
    if not db.SCHEMA_RE.fullmatch(runtime_role):
        raise SchemaMigrationError(
            "ACTIONQ_RUNTIME_ROLE must be a simple PostgreSQL identifier"
        )
    from psycopg import sql

    schema_identifier = sql.Identifier(schema)
    role_identifier = sql.Identifier(runtime_role)
    conn.execute(
        sql.SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC").format(schema_identifier)
    )
    conn.execute(
        sql.SQL("REVOKE CREATE ON SCHEMA {} FROM {}").format(
            schema_identifier, role_identifier
        )
    )
    conn.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            schema_identifier, role_identifier
        )
    )
    conn.execute(
        sql.SQL(
            "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} FROM {}"
        ).format(schema_identifier, role_identifier)
    )
    conn.execute(
        sql.SQL(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {}.{}, {}.{} TO {}"
        ).format(
            schema_identifier,
            sql.Identifier("actions"),
            schema_identifier,
            sql.Identifier("events"),
            role_identifier,
        )
    )
    # The byte-exact dispatch snapshot is append-only.  Runtime code can bind
    # it once and read it, but cannot replace or erase it after enqueue.
    conn.execute(
        sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(
            schema_identifier, sql.Identifier("dispatch_requests"), role_identifier
        )
    )
    conn.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {}.{} TO {}").format(
            schema_identifier,
            sql.Identifier("dispatch_observation_watermarks"),
            role_identifier,
        )
    )
    for table in ("action_resources", "action_resource_changes", "action_resource_sessions"):
        conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {}.{} TO {}").format(
                schema_identifier, sql.Identifier(table), role_identifier,
            )
        )
    conn.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {}.{} TO {}").format(
            schema_identifier, sql.Identifier("execution_groups"), role_identifier
        )
    )
    conn.execute(
        sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(
            schema_identifier, sql.Identifier("execution_group_members"), role_identifier
        )
    )
    # Immutable compiler records are append-only. Runtime claim validation
    # reads the canonical bytes and may create a first runtime grant, but it
    # cannot rewrite either a request/spec binding or a grant.
    for table in (
        "immutable_action_specs",
        "immutable_action_requests",
        "immutable_action_runtime_grants",
    ):
        conn.execute(
            sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(
                schema_identifier, sql.Identifier(table), role_identifier
            )
        )
    conn.execute(
        sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
            schema_identifier,
            sql.Identifier(MIGRATION_TABLE),
            role_identifier,
        )
    )
    conn.execute(
        sql.SQL(
            "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {} FROM {}"
        ).format(schema_identifier, role_identifier)
    )
    conn.execute(
        sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
            schema_identifier, role_identifier
        )
    )
    conn.execute(
        sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} "
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
        ).format(schema_identifier, role_identifier)
    )
    conn.execute(
        sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} "
            "GRANT USAGE, SELECT ON SEQUENCES TO {}"
        ).format(schema_identifier, role_identifier)
    )


def migrate(
    conn,
    schema: str,
    *,
    runtime_role: str | None = None,
) -> dict[str, Any]:
    """Apply packaged migrations once under a transaction-scoped advisory lock."""

    schema = db.schema_name(schema)
    runtime_role = runtime_role or os.environ.get("ACTIONQ_RUNTIME_ROLE")
    migrations = load_migrations()
    applied_now: list[int] = []
    adopted_legacy_schema = False
    with conn.transaction():
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"actionq:{schema}:schema-migration",),
        )
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        data_tables_existed = _data_tables_exist(conn, schema)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {db.qname(schema, MIGRATION_TABLE)} (
                domain      TEXT        NOT NULL,
                version     INTEGER     NOT NULL CHECK (version > 0),
                name        TEXT        NOT NULL,
                checksum    TEXT        NOT NULL,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (domain, version)
            )
            """
        )
        applied = _applied_migrations(conn, schema)
        known = {migration.version: migration for migration in migrations}
        unknown = sorted(set(applied) - set(known))
        if unknown:
            raise SchemaMigrationError(
                f"database contains migration versions newer than this release: {unknown}"
            )
        for version, checksum in applied.items():
            if known[version].checksum != checksum:
                raise SchemaMigrationError(
                    f"applied migration {version} checksum differs from packaged asset"
                )
        for migration in migrations:
            if migration.version in applied:
                continue
            if migration.version == 8:
                _lock_schema8_dispatch_roots(conn, schema)
                _require_schema8_dispatch_root_quiescence(conn, schema)
            if migration.version == 1 and data_tables_existed:
                shape_issues = _shape_issues(conn, schema)
                # A genuine v1 schema predates the claim-receipt column
                # added by v2.  It may be adopted only when that is its sole
                # difference; all other drift remains a refusal before any
                # migration ledger is stamped.
                shape_issues = _unversioned_v1_shape_issues(shape_issues)
                if (_has_legacy_v1_status_constraint(conn, schema)
                        and "constraint-invalid:actions.status" in shape_issues):
                    shape_issues.remove("constraint-invalid:actions.status")
                if shape_issues:
                    raise SchemaMigrationError(
                        "refusing to stamp incompatible unversioned actionq schema: "
                        + ",".join(shape_issues)
                    )
                adopted_legacy_schema = True
            else:
                for statement in _statements(_render(migration, schema)):
                    conn.execute(statement)
                shape_issues = _shape_issues(conn, schema)
                if shape_issues and migration.version == MAX_SCHEMA_VERSION:
                    raise SchemaMigrationError(
                        "migration did not establish the required actionq schema: "
                        + ",".join(shape_issues)
                    )
            conn.execute(
                f"INSERT INTO {db.qname(schema, MIGRATION_TABLE)} "
                "(domain, version, name, checksum) VALUES (%s, %s, %s, %s)",
                (DOMAIN, migration.version, migration.name, migration.checksum),
            )
            applied_now.append(migration.version)

        _grant_runtime_privileges(conn, schema, runtime_role)
        compatibility = require_compatible(
            conn,
            schema,
            require_runtime_principal=False,
        )

    return {
        "domain": DOMAIN,
        "schema": schema,
        "target_schema_version": MAX_SCHEMA_VERSION,
        "applied_versions": applied_now,
        "adopted_legacy_schema": adopted_legacy_schema,
        "runtime_role": runtime_role,
        "compatibility": compatibility.as_dict(),
    }
