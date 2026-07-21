"""Deployment-owned schema migration and runtime compatibility contract.

Migration callers need a database role with DDL authority. Runtime callers use
``check_compatibility``/``require_compatible`` only; those functions never
issue DDL and work with a role that can only read the migration ledger.
"""

from __future__ import annotations

import hashlib
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
        "id": ("bigint", "NO", True),
        "action_type": ("text", "NO", False),
        "project": ("text", "YES", False),
        "target_ref": ("text", "YES", False),
        "source_refs": ("jsonb", "NO", True),
        "priority": ("integer", "NO", True),
        "status": ("text", "NO", True),
        "parent_id": ("bigint", "YES", False),
        "chain_depth": ("integer", "NO", True),
        "created_at": ("timestamp with time zone", "NO", True),
        "claimed_at": ("timestamp with time zone", "YES", False),
        "claimed_by": ("text", "YES", False),
        "claim_deadline": ("timestamp with time zone", "YES", False),
        "completed_at": ("timestamp with time zone", "YES", False),
        "result_ref": ("text", "YES", False),
        "failure_reason": ("text", "YES", False),
        "created_by": ("text", "NO", False),
    },
    "events": {
        "id": ("bigint", "NO", True),
        "action_id": ("bigint", "YES", False),
        "event_type": ("text", "NO", False),
        "timestamp": ("timestamp with time zone", "NO", True),
        "actor": ("text", "YES", False),
        "payload": ("jsonb", "NO", True),
    },
}
_REQUIRED_COLUMNS = {
    table: set(columns) for table, columns in _COLUMN_SHAPE.items()
}
_REQUIRED_CONSTRAINT_COUNTS = {
    "actions": {"p": 1, "f": 1, "c": 1},
    "events": {"p": 1, "f": 1},
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


def check_compatibility(conn, schema: str) -> Compatibility:
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


def require_compatible(conn, schema: str) -> Compatibility:
    compatibility = check_compatibility(conn, schema)
    if not compatibility.compatible:
        raise SchemaCompatibilityError(
            f"actionq schema {schema!r} is {compatibility.state}: "
            f"{compatibility.detail}"
        )
    return compatibility


def _validate_required_shape(conn, schema: str) -> None:
    rows = conn.execute(
        """
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = ANY(%s)
        """,
        (schema, list(_REQUIRED_COLUMNS)),
    ).fetchall()
    observed: dict[str, dict[str, tuple[str, str, bool]]] = {
        table: {} for table in _COLUMN_SHAPE
    }
    for row in rows:
        table = str(_row_value(row, "table_name"))
        column = str(_row_value(row, "column_name", 1))
        if table in observed:
            observed[table][column] = (
                str(_row_value(row, "data_type", 2)),
                str(_row_value(row, "is_nullable", 3)),
                _row_value(row, "column_default", 4) is not None,
            )
    missing = {
        table: sorted(required - set(observed[table]))
        for table, required in _REQUIRED_COLUMNS.items()
        if required - set(observed[table])
    }
    mismatched = {
        f"{table}.{column}": {
            "expected": expected,
            "observed": observed[table].get(column),
        }
        for table, columns in _COLUMN_SHAPE.items()
        for column, expected in columns.items()
        if column in observed[table] and observed[table][column] != expected
    }
    if missing or mismatched:
        raise SchemaMigrationError(
            "migration did not establish the required actionq table shape: "
            f"missing={missing}, mismatched={mismatched}"
        )
    constraint_rows = conn.execute(
        """
        SELECT relation.relname AS table_name, constraint_record.contype,
               count(*) AS constraint_count
        FROM pg_constraint AS constraint_record
        JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid
        JOIN pg_namespace AS namespace_record ON namespace_record.oid = relation.relnamespace
        WHERE namespace_record.nspname = %s AND relation.relname = ANY(%s)
        GROUP BY relation.relname, constraint_record.contype
        """,
        (schema, list(_REQUIRED_CONSTRAINT_COUNTS)),
    ).fetchall()
    observed_constraints = {
        (str(_row_value(row, "table_name")), str(_row_value(row, "contype", 1))): int(
            _row_value(row, "constraint_count", 2)
        )
        for row in constraint_rows
    }
    missing_constraints = {
        f"{table}.{constraint_type}": expected_count
        for table, constraint_types in _REQUIRED_CONSTRAINT_COUNTS.items()
        for constraint_type, expected_count in constraint_types.items()
        if observed_constraints.get((table, constraint_type), 0) < expected_count
    }
    if missing_constraints:
        raise SchemaMigrationError(
            "migration did not establish the required actionq constraints: "
            f"{missing_constraints}"
        )


def migrate(conn, schema: str) -> dict[str, Any]:
    """Apply packaged migrations once under a transaction-scoped advisory lock."""

    schema = db.schema_name(schema)
    migrations = load_migrations()
    applied_now: list[int] = []
    with conn.transaction():
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"actionq:{schema}:schema-migration",),
        )
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
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
            for statement in _statements(_render(migration, schema)):
                conn.execute(statement)
            _validate_required_shape(conn, schema)
            conn.execute(
                f"INSERT INTO {db.qname(schema, MIGRATION_TABLE)} "
                "(domain, version, name, checksum) VALUES (%s, %s, %s, %s)",
                (DOMAIN, migration.version, migration.name, migration.checksum),
            )
            applied_now.append(migration.version)

        _validate_required_shape(conn, schema)
        compatibility = require_compatible(conn, schema)

    return {
        "domain": DOMAIN,
        "schema": schema,
        "target_schema_version": MAX_SCHEMA_VERSION,
        "applied_versions": applied_now,
        "compatibility": compatibility.as_dict(),
    }
