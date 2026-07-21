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
MAX_SCHEMA_VERSION = 1
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
}
_REQUIRED_COLUMNS = {
    table: set(columns) for table, columns in _COLUMN_SHAPE.items()
}
_REQUIRED_CONSTRAINT_COUNTS = {
    "actions": {"p": 1, "f": 1, "c": 1},
    "events": {"p": 1, "f": 1},
}
_REQUIRED_INDEXES = {
    "actions.claim-lookup": ("actions", ("status", "priority", "created_at"), "pending"),
    "actions.parent": ("actions", ("parent_id",), None),
    "actions.project": ("actions", ("project",), None),
    "actions.deadline": ("actions", ("claim_deadline",), "claimed"),
    "events.action": ("events", ("action_id",), None),
    "events.timestamp": ("events", ("timestamp desc",), None),
    "events.type-time": ("events", ("event_type", "timestamp desc"), None),
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


def _normalized_identifier(value: Any) -> str:
    normalized = " ".join(str(value).replace('"', "").strip().lower().split())
    return re.sub(r"\s+nulls\s+(first|last)$", "", normalized)


def _normalized_default(schema: str, expected: str | None, value: Any) -> str | None:
    if value is None:
        return None
    normalized = _normalized_identifier(value)
    if expected and expected.startswith("sequence:"):
        sequence = expected.removeprefix("sequence:")
        pattern = (
            r"nextval\('(?:"
            + re.escape(schema.lower())
            + r"\.)?"
            + re.escape(sequence)
            + r"'::regclass\)"
        )
        return expected if re.fullmatch(pattern, normalized) else normalized
    return normalized


def _runtime_principal_issue(conn, schema: str) -> str | None:
    row = conn.execute(
        """
        SELECT current_user AS principal,
               has_schema_privilege(current_user, %s, 'CREATE') AS can_create
        """,
        (schema,),
    ).fetchone()
    if row and bool(_row_value(row, "can_create", 1)):
        return "runtime principal has schema CREATE authority"
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
                _normalized_default(
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
               constraint_record.contype,
               ARRAY(
                   SELECT attribute_record.attname
                   FROM unnest(constraint_record.conkey) WITH ORDINALITY AS key_record(attnum, position)
                   JOIN pg_attribute AS attribute_record
                     ON attribute_record.attrelid = constraint_record.conrelid
                    AND attribute_record.attnum = key_record.attnum
                   ORDER BY key_record.position
               ) AS columns,
               foreign_relation.relname AS foreign_table,
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
               pg_get_expr(
                   constraint_record.conbin,
                   constraint_record.conrelid,
                   true
               ) AS expression
        FROM pg_constraint AS constraint_record
        JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid
        JOIN pg_namespace AS namespace_record ON namespace_record.oid = relation.relnamespace
        LEFT JOIN pg_class AS foreign_relation ON foreign_relation.oid = constraint_record.confrelid
        WHERE namespace_record.nspname = %s AND relation.relname = ANY(%s)
        """,
        (schema, list(_REQUIRED_CONSTRAINT_COUNTS)),
    ).fetchall()
    constraints: list[dict[str, Any]] = []
    for row in constraint_rows:
        constraints.append(
            {
                "table": str(_row_value(row, "table_name")),
                "type": str(_row_value(row, "contype", 1)),
                "columns": tuple(_row_value(row, "columns", 2) or ()),
                "foreign_table": _row_value(row, "foreign_table", 3),
                "foreign_columns": tuple(_row_value(row, "foreign_columns", 4) or ()),
                "update_action": str(_row_value(row, "update_action", 5)),
                "delete_action": str(_row_value(row, "delete_action", 6)),
                "expression": str(_row_value(row, "expression", 7) or ""),
            }
        )

    def has_constraint(
        table: str,
        constraint_type: str,
        columns: tuple[str, ...],
        *,
        foreign_table: str | None = None,
        foreign_columns: tuple[str, ...] = (),
        update_action: str = "a",
        delete_action: str = "a",
    ) -> bool:
        return any(
            constraint["table"] == table
            and constraint["type"] == constraint_type
            and constraint["columns"] == columns
            and (
                foreign_table is None
                or (
                    constraint["foreign_table"] == foreign_table
                    and constraint["foreign_columns"] == foreign_columns
                    and constraint["update_action"] == update_action
                    and constraint["delete_action"] == delete_action
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
                foreign_table="actions",
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
                foreign_table="actions",
                foreign_columns=("id",),
            ),
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
    expected_status_expression = _normalized_identifier(
        "status = ANY (ARRAY["
        + ", ".join(f"'{status}'::text" for status in (
            "pending",
            "claimed",
            "completed",
            "failed",
            "rejected",
            "cancelled",
        ))
        + "])"
    )
    actual_status_expression = (
        _normalized_identifier(status_checks[0]["expression"])
        if len(status_checks) == 1
        else ""
    )
    if actual_status_expression.startswith("(") and actual_status_expression.endswith(")"):
        actual_status_expression = actual_status_expression[1:-1]
    if len(status_checks) != 1 or actual_status_expression != expected_status_expression:
        issues.append("constraint-invalid:actions.status")

    index_rows = conn.execute(
        """
        SELECT relation.relname AS table_name,
               index_record.indisvalid,
               index_record.indisready,
               index_record.indisunique,
               access_method.amname AS access_method,
               ARRAY(
                   SELECT pg_get_indexdef(index_record.indexrelid, position, true)
                       || CASE
                              WHEN (index_record.indoption[position - 1] & 1) = 1
                              THEN ' DESC'
                              ELSE ''
                          END
                   FROM generate_series(1, index_record.indnkeyatts) AS position
                   ORDER BY position
               ) AS columns,
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
        predicate = _row_value(row, "predicate", 6)
        normalized_predicate = _normalized_identifier(predicate or "")
        predicate_match = re.fullmatch(
            r"\(?status\s*=\s*'([^']+)'(?:::text)?\)?",
            normalized_predicate,
        )
        indexes.append(
            {
                "table": str(_row_value(row, "table_name")),
                "valid": bool(_row_value(row, "indisvalid", 1)),
                "ready": bool(_row_value(row, "indisready", 2)),
                "unique": bool(_row_value(row, "indisunique", 3)),
                "access_method": str(_row_value(row, "access_method", 4)),
                "columns": tuple(
                    _normalized_identifier(column)
                    for column in (_row_value(row, "columns", 5) or ())
                ),
                "predicate_status": predicate_match.group(1) if predicate_match else None,
            }
        )
    for name, (table, columns, predicate_status) in _REQUIRED_INDEXES.items():
        if not any(
            index["table"] == table
            and index["valid"]
            and index["ready"]
            and not index["unique"]
            and index["access_method"] == "btree"
            and index["columns"] == columns
            and index["predicate_status"] == predicate_status
            for index in indexes
        ):
            issues.append(f"index-missing-or-invalid:{name}")
    return tuple(sorted(issues))


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
    elif set(applied) != set(expected):
        state = "incomplete"
        detail = (
            f"applied migration versions {sorted(applied)} do not match "
            f"expected versions {sorted(expected)}"
        )
    elif any(applied[version] != checksum for version, checksum in expected.items()):
        state = "checksum-mismatch"
        detail = "an applied migration checksum does not match the packaged asset"
    elif require_runtime_principal and (
        principal_issue := _runtime_principal_issue(conn, schema)
    ):
        state = "role-mismatch"
        detail = principal_issue
    else:
        shape_issues = _shape_issues(conn, schema)
        if shape_issues:
            state = "shape-mismatch"
            detail = "required queue shape is invalid: " + ",".join(shape_issues)
        else:
            state = "compatible"
            detail = "schema is compatible with the packaged execution adapter"

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
            if migration.version == 1 and data_tables_existed:
                shape_issues = _shape_issues(conn, schema)
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
                if shape_issues:
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
