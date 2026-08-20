"""Migration and compatibility authority for ``federation-schema/v1``.

This domain is physically and logically independent of the frozen execution
schema.  In particular, it never imports or delegates to ``actionq.schema``.
"""

from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass
from importlib import resources
from typing import Any

from vuoro_schema_runtime import MigrationAsset, sha256_text

from . import db


DOMAIN = "federation"
API_VERSION = "v1"
COMPATIBILITY_LABEL = "federation-schema/v1"
MIN_SCHEMA_VERSION = 1
MAX_SCHEMA_VERSION = 1
MIGRATION_TABLE = "schema_migrations"
DEFAULT_SCHEMA = "actionq_federation"
_MIGRATION_RE = re.compile(r"^(?P<version>[0-9]{3})_[a-z0-9_]+\.sql$")
REQUIRED_TABLES = (
    MIGRATION_TABLE, "federation_resources", "federation_resource_changes",
    "federation_relations", "federation_execution_refs", "federation_evidence",
    "federation_acceptance_decisions", "federation_settlements",
    "federation_idempotency_bindings", "federation_command_decisions",
)

# Column shape: table -> column -> (data_type, is_nullable, canonical_default).
# canonical_default is compared against information_schema.column_default after
# stripping type casts and quoting -- None means "no default".
_COLUMN_SHAPE: dict[str, dict[str, tuple[str, str, str | None]]] = {
    MIGRATION_TABLE: {
        "domain": ("text", "NO", None),
        "version": ("integer", "NO", None),
        "name": ("text", "NO", None),
        "checksum": ("text", "NO", None),
        "applied_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_resources": {
        "resource_ref": ("text", "NO", None),
        "owner_principal_id": ("text", "NO", None),
        "state": ("text", "NO", None),
        "revision": ("bigint", "NO", None),
        "recovery_floor": ("bigint", "NO", "0"),
        "created_at": ("timestamp with time zone", "NO", "now()"),
        "updated_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_resource_changes": {
        "resource_ref": ("text", "NO", None),
        "revision": ("bigint", "NO", None),
        "operation": ("text", "NO", None),
        "state": ("text", "NO", None),
        "actor_principal_id": ("text", "NO", None),
        "payload_bytes": ("bytea", "NO", None),
        "payload_digest": ("text", "NO", None),
        "occurred_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_relations": {
        "source_ref": ("text", "NO", None),
        "relation_type": ("text", "NO", None),
        "target_ref": ("text", "NO", None),
        "source_revision": ("bigint", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_execution_refs": {
        "resource_ref": ("text", "NO", None),
        "execution_ref": ("text", "NO", None),
        "assurance_type": ("text", "NO", None),
        "source_revision": ("bigint", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_evidence": {
        "resource_ref": ("text", "NO", None),
        "evidence_ref": ("text", "NO", None),
        "evidence_digest": ("text", "NO", None),
        "assurance_type": ("text", "NO", None),
        "source_revision": ("bigint", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_acceptance_decisions": {
        "resource_ref": ("text", "NO", None),
        "source_revision": ("bigint", "NO", None),
        "outcome": ("text", "NO", None),
        "policy_ref": ("text", "NO", None),
        "evidence_ref": ("text", "YES", None),
        "decided_by": ("text", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_settlements": {
        "resource_ref": ("text", "NO", None),
        "source_revision": ("bigint", "NO", None),
        "fact_ref": ("text", "NO", None),
        "reconciled_by": ("text", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_idempotency_bindings": {
        "environment": ("text", "NO", None),
        "principal_id": ("text", "NO", None),
        "operation": ("text", "NO", None),
        "idempotency_key": ("text", "NO", None),
        "request_digest": ("text", "NO", None),
        "created_at": ("timestamp with time zone", "NO", "now()"),
    },
    "federation_command_decisions": {
        "environment": ("text", "NO", None),
        "principal_id": ("text", "NO", None),
        "operation": ("text", "NO", None),
        "idempotency_key": ("text", "NO", None),
        "request_digest": ("text", "NO", None),
        "status": ("text", "NO", None),
        "code": ("text", "NO", None),
        "message": ("text", "NO", None),
        "response_bytes": ("bytea", "NO", None),
        "response_digest": ("text", "NO", None),
        "resource_ref": ("text", "YES", None),
        "before_revision": ("bigint", "YES", None),
        "after_revision": ("bigint", "YES", None),
        "decided_at": ("timestamp with time zone", "NO", "now()"),
    },
}

# Constraint counts by pg_constraint.contype ('p'=primary key, 'f'=foreign key,
# 'c'=check, 'u'=unique). PostgreSQL 18 also emits 'n' (not-null) catalog
# entries for every NOT NULL column; those are validated via the nullability
# field in _COLUMN_SHAPE instead, so 'n' is deliberately excluded here.
_REQUIRED_CONSTRAINT_COUNTS: dict[str, dict[str, int]] = {
    MIGRATION_TABLE: {"p": 1, "c": 1},
    "federation_resources": {"p": 1, "c": 3},
    "federation_resource_changes": {"p": 1, "f": 1, "c": 3},
    "federation_relations": {"p": 1, "f": 2, "c": 3},
    "federation_execution_refs": {"p": 1, "f": 1, "c": 1},
    "federation_evidence": {"p": 1, "f": 1, "c": 2},
    "federation_acceptance_decisions": {"p": 1, "f": 1, "c": 2},
    "federation_settlements": {"p": 1, "f": 1, "c": 1},
    "federation_idempotency_bindings": {"p": 1, "c": 1},
    "federation_command_decisions": {"p": 1, "c": 5},
}

# Exact CHECK constraint bodies (pg_get_constraintdef output, deparsed by
# PostgreSQL itself) per table. A count match alone lets any CHECK be
# swapped for a same-count vacuous one (e.g. CHECK (true)) without being
# noticed, so the deparsed expression is compared too.
_REQUIRED_CHECK_EXPRESSIONS: dict[str, frozenset[str]] = {
    MIGRATION_TABLE: frozenset({"CHECK ((version > 0))"}),
    "federation_resources": frozenset({
        "CHECK ((state = ANY (ARRAY['registered'::text, 'evidence-recorded'::text, 'accepted'::text, 'rejected'::text, 'superseded'::text])))",
        "CHECK ((revision > 0))",
        "CHECK ((recovery_floor = 0))",
    }),
    "federation_resource_changes": frozenset({
        "CHECK ((revision > 0))",
        "CHECK ((state = ANY (ARRAY['registered'::text, 'evidence-recorded'::text, 'accepted'::text, 'rejected'::text, 'superseded'::text])))",
        "CHECK ((payload_digest ~ '^sha256:[0-9a-f]{64}$'::text))",
    }),
    "federation_relations": frozenset({
        "CHECK ((relation_type = ANY (ARRAY['parent-of'::text, 'depends-on'::text, 'derived-from'::text, 'supersedes'::text])))",
        "CHECK ((source_revision > 0))",
        "CHECK ((source_ref <> target_ref))",
    }),
    "federation_execution_refs": frozenset({"CHECK ((source_revision > 0))"}),
    "federation_evidence": frozenset({
        "CHECK ((evidence_digest ~ '^sha256:[0-9a-f]{64}$'::text))",
        "CHECK ((source_revision > 0))",
    }),
    "federation_acceptance_decisions": frozenset({
        "CHECK ((outcome = ANY (ARRAY['accepted'::text, 'rejected'::text])))",
        "CHECK ((source_revision > 0))",
    }),
    "federation_settlements": frozenset({"CHECK ((source_revision > 0))"}),
    "federation_idempotency_bindings": frozenset({"CHECK ((request_digest ~ '^sha256:[0-9a-f]{64}$'::text))"}),
    "federation_command_decisions": frozenset({
        "CHECK ((request_digest ~ '^sha256:[0-9a-f]{64}$'::text))",
        "CHECK ((status = ANY (ARRAY['accepted'::text, 'rejected'::text])))",
        "CHECK ((response_digest ~ '^sha256:[0-9a-f]{64}$'::text))",
        "CHECK (((before_revision IS NULL) OR (before_revision >= 0)))",
        "CHECK (((after_revision IS NULL) OR (after_revision >= 0)))",
    }),
}

# Exact PRIMARY KEY bodies (pg_get_constraintdef output), same rationale as
# _REQUIRED_CHECK_EXPRESSIONS: a count match alone lets a PK's column
# composition be swapped for anything of the same count. This is the sole
# uniqueness guarantee behind, e.g., "one idempotency key binds one digest".
_REQUIRED_PRIMARY_KEYS: dict[str, str] = {
    MIGRATION_TABLE: "PRIMARY KEY (domain, version)",
    "federation_resources": "PRIMARY KEY (resource_ref)",
    "federation_resource_changes": "PRIMARY KEY (resource_ref, revision)",
    "federation_relations": "PRIMARY KEY (source_ref, relation_type, target_ref)",
    "federation_execution_refs": "PRIMARY KEY (resource_ref, execution_ref)",
    "federation_evidence": "PRIMARY KEY (resource_ref, evidence_ref)",
    "federation_acceptance_decisions": "PRIMARY KEY (resource_ref, source_revision)",
    "federation_settlements": "PRIMARY KEY (resource_ref, source_revision)",
    "federation_idempotency_bindings": "PRIMARY KEY (environment, principal_id, operation, idempotency_key)",
    "federation_command_decisions": "PRIMARY KEY (environment, principal_id, operation, idempotency_key, request_digest)",
}

# Exact FOREIGN KEY bodies, same rationale. "{schema}" is substituted with
# the schema under test before comparison -- pg_get_constraintdef always
# qualifies the referenced table with its schema.
_REQUIRED_FOREIGN_KEYS: dict[str, frozenset[str]] = {
    "federation_resource_changes": frozenset({
        "FOREIGN KEY (resource_ref) REFERENCES {schema}.federation_resources(resource_ref) ON DELETE RESTRICT",
    }),
    "federation_relations": frozenset({
        "FOREIGN KEY (source_ref) REFERENCES {schema}.federation_resources(resource_ref) ON DELETE RESTRICT",
        "FOREIGN KEY (target_ref) REFERENCES {schema}.federation_resources(resource_ref) ON DELETE RESTRICT",
    }),
    "federation_execution_refs": frozenset({
        "FOREIGN KEY (resource_ref) REFERENCES {schema}.federation_resources(resource_ref) ON DELETE RESTRICT",
    }),
    "federation_evidence": frozenset({
        "FOREIGN KEY (resource_ref) REFERENCES {schema}.federation_resources(resource_ref) ON DELETE RESTRICT",
    }),
    "federation_acceptance_decisions": frozenset({
        "FOREIGN KEY (resource_ref) REFERENCES {schema}.federation_resources(resource_ref) ON DELETE RESTRICT",
    }),
    "federation_settlements": frozenset({
        "FOREIGN KEY (resource_ref) REFERENCES {schema}.federation_resources(resource_ref) ON DELETE RESTRICT",
    }),
}

# Non-primary-key indexes: index name -> (table, unique?, ordered column/
# expression tuple as pg_get_indexdef renders each key, partial predicate).
_REQUIRED_INDEXES: dict[str, tuple[str, bool, tuple[str, ...], str | None]] = {
    "federation_relations_target_idx": ("federation_relations", False, ("target_ref", "relation_type", "source_ref"), None),
    "federation_changes_order_idx": ("federation_resource_changes", False, ("resource_ref", "revision"), None),
}

_DEFAULT_CAST_RE = re.compile(r"::[A-Za-z0-9_ \"]+(\([^)]*\))?\s*$")


class FederationSchemaError(db.ActionQError):
    """The selected federation schema cannot be migrated or served safely."""


@dataclass(frozen=True)
class Compatibility:
    domain: str
    api_version: str
    compatibility_label: str
    observed_schema_version: int | None
    state: str
    compatible: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def configured_schema() -> str:
    return db.schema_name(os.environ.get("ACTIONQ_FEDERATION_SCHEMA", DEFAULT_SCHEMA))


def load_migrations() -> tuple[MigrationAsset, ...]:
    root = resources.files("actionq").joinpath("federation_migrations")
    result: list[MigrationAsset] = []
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        match = _MIGRATION_RE.fullmatch(path.name)
        if match is None:
            continue
        raw = path.read_text(encoding="utf-8")
        result.append(MigrationAsset(int(match.group("version")), path.name, raw, sha256_text(raw)))
    if [asset.version for asset in result] != list(range(1, MAX_SCHEMA_VERSION + 1)):
        raise FederationSchemaError("federation migration assets are not exactly contiguous v1")
    return tuple(result)


def _statements(asset: MigrationAsset, schema: str) -> tuple[str, ...]:
    quoted = f'"{db.schema_name(schema)}"'
    rendered = asset.sql.replace("{{schema}}", quoted)
    if "{{schema}}" in rendered:
        raise FederationSchemaError(f"unresolved schema placeholder in {asset.name}")
    return tuple(statement.strip() for statement in rendered.split(";") if statement.strip())


def _row(row: Any, key: str, index: int = 0) -> Any:
    return row[key] if isinstance(row, dict) else row[index]


def _ledger_exists(conn: Any, schema: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) AS relation", (db.qname(schema, MIGRATION_TABLE),)).fetchone()
    return bool(row and _row(row, "relation"))


def _canonical_default(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    while True:
        match = _DEFAULT_CAST_RE.search(text)
        if match is None:
            break
        text = text[: match.start()].strip()
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        text = text[1:-1]
    if text.lower().startswith("now("):
        return "now()"
    return text


def _column_issues(conn: Any, schema: str) -> list[str]:
    issues: list[str] = []
    tables = tuple(_COLUMN_SHAPE)
    rows = conn.execute(
        "SELECT table_name, column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns WHERE table_schema=%s AND table_name = ANY(%s)",
        (schema, list(tables)),
    ).fetchall()
    observed: dict[str, dict[str, tuple[str, str, str | None]]] = {table: {} for table in tables}
    for row in rows:
        table = str(_row(row, "table_name"))
        column = str(_row(row, "column_name", 1))
        if table not in observed:
            continue
        if column not in _COLUMN_SHAPE[table]:
            issues.append(f"column-unexpected:{table}.{column}")
            continue
        observed[table][column] = (
            str(_row(row, "data_type", 2)),
            str(_row(row, "is_nullable", 3)),
            _canonical_default(_row(row, "column_default", 4)),
        )
    for table, columns in _COLUMN_SHAPE.items():
        for column, expected in columns.items():
            actual = observed[table].get(column)
            if actual is None:
                issues.append(f"column-missing:{table}.{column}")
                continue
            expected_type, expected_nullable, expected_default = expected
            if actual[0] != expected_type:
                issues.append(f"column-type:{table}.{column}")
            if actual[1] != expected_nullable:
                issues.append(f"column-nullability:{table}.{column}")
            if actual[2] != expected_default:
                issues.append(f"column-default:{table}.{column}")
    return issues


def _constraint_issues(conn: Any, schema: str) -> list[str]:
    issues: list[str] = []
    tables = tuple(_REQUIRED_CONSTRAINT_COUNTS)
    rows = conn.execute(
        """SELECT relation.relname AS table_name, constraint_record.contype AS contype,
                  pg_get_constraintdef(constraint_record.oid) AS definition
           FROM pg_constraint constraint_record
           JOIN pg_class relation ON relation.oid=constraint_record.conrelid
           JOIN pg_namespace namespace_record ON namespace_record.oid=relation.relnamespace
           WHERE namespace_record.nspname=%s AND relation.relname = ANY(%s)""",
        (schema, list(tables)),
    ).fetchall()
    counts: dict[str, dict[str, int]] = {table: {} for table in tables}
    checks: dict[str, set[str]] = {table: set() for table in tables}
    primary_keys: dict[str, str] = {}
    foreign_keys: dict[str, set[str]] = {table: set() for table in tables}
    for row in rows:
        table = str(_row(row, "table_name"))
        contype = str(_row(row, "contype", 1))
        definition = str(_row(row, "definition", 2))
        if table not in counts:
            continue
        counts[table][contype] = counts[table].get(contype, 0) + 1
        if contype == "c":
            checks[table].add(definition)
        elif contype == "p":
            primary_keys[table] = definition
        elif contype == "f":
            foreign_keys[table].add(definition)
    for table, expected_counts in _REQUIRED_CONSTRAINT_COUNTS.items():
        actual_counts = counts.get(table, {})
        for contype, expected_n in expected_counts.items():
            actual_n = actual_counts.get(contype, 0)
            if actual_n != expected_n:
                issues.append(f"constraint-count:{table}.{contype}:{actual_n}!={expected_n}")
        for contype, actual_n in actual_counts.items():
            # 'n' (not-null) catalog entries are validated via column
            # nullability instead; anything else unexpected is a real drift.
            if contype != "n" and contype not in expected_counts and actual_n:
                issues.append(f"constraint-unexpected:{table}.{contype}")
    for table, expected_checks in _REQUIRED_CHECK_EXPRESSIONS.items():
        # A count match alone lets any CHECK be swapped for a same-count
        # vacuous one (e.g. CHECK (true)); compare PostgreSQL's own
        # deparsed expression text so a body swap is caught even when the
        # count above is unchanged. The same applies to PRIMARY KEY and
        # FOREIGN KEY column composition below -- e.g. a same-count PK swap
        # would otherwise silently drop the sole DB-level guarantee that one
        # idempotency key binds one request digest.
        actual_checks = checks.get(table, set())
        if actual_checks != expected_checks:
            issues.append(f"check-expression:{table}")
    for table, expected_pk in _REQUIRED_PRIMARY_KEYS.items():
        if primary_keys.get(table) != expected_pk:
            issues.append(f"primary-key:{table}")
    if _REQUIRED_FOREIGN_KEYS:
        # pg_get_constraintdef quotes the schema qualifier only when the
        # identifier needs it (mixed case, reserved word, ...); db.SCHEMA_RE
        # permits both, so a raw f"{schema}." template would mismatch a
        # correctly-migrated schema whose name happens to need quoting.
        # quote_ident() asks PostgreSQL for the exact same rendering
        # pg_get_constraintdef itself would use, rather than reimplementing
        # its quoting rules.
        quoted_schema = str(_row(conn.execute("SELECT quote_ident(%s) AS quoted", (schema,)).fetchone(), "quoted"))
        for table, expected_fks in _REQUIRED_FOREIGN_KEYS.items():
            expected = {template.format(schema=quoted_schema) for template in expected_fks}
            if foreign_keys.get(table, set()) != expected:
                issues.append(f"foreign-key:{table}")
    return issues


def _index_issues(conn: Any, schema: str) -> list[str]:
    issues: list[str] = []
    tables = tuple(_COLUMN_SHAPE)
    rows = conn.execute(
        """SELECT index_class.relname AS index_name, table_class.relname AS table_name,
                  index_record.indisunique AS is_unique,
                  pg_get_expr(index_record.indpred, index_record.indrelid, true) AS predicate,
                  index_record.indexrelid AS index_oid, index_record.indrelid AS table_oid,
                  index_record.indnatts AS key_count
           FROM pg_index index_record
           JOIN pg_class index_class ON index_class.oid=index_record.indexrelid
           JOIN pg_class table_class ON table_class.oid=index_record.indrelid
           JOIN pg_namespace namespace_record ON namespace_record.oid=table_class.relnamespace
           WHERE namespace_record.nspname=%s AND table_class.relname = ANY(%s) AND NOT index_record.indisprimary""",
        (schema, list(tables)),
    ).fetchall()
    observed: dict[str, tuple[str, bool, tuple[str, ...], str | None]] = {}
    for row in rows:
        name = str(_row(row, "index_name"))
        table = str(_row(row, "table_name", 1))
        is_unique = bool(_row(row, "is_unique", 2))
        predicate = _row(row, "predicate", 3)
        index_oid = _row(row, "index_oid", 4)
        key_count = int(_row(row, "key_count", 6))
        # pg_get_indexdef per key position renders both plain columns and
        # expression keys as text, unlike joining pg_attribute directly
        # (which silently drops expression keys, attnum=0).
        keys = tuple(
            str(_row(conn.execute(
                "SELECT pg_get_indexdef(%s, %s, true) AS key", (index_oid, position),
            ).fetchone(), "key"))
            for position in range(1, key_count + 1)
        )
        observed[name] = (table, is_unique, keys, str(predicate) if predicate is not None else None)
    for name, expected in _REQUIRED_INDEXES.items():
        actual = observed.get(name)
        if actual is None:
            issues.append(f"index-missing:{name}")
        elif actual != expected:
            issues.append(f"index-shape:{name}")
    issues.extend(
        f"index-unexpected:{name}" for name in sorted(set(observed) - set(_REQUIRED_INDEXES))
    )
    return issues


def _shape_issues(conn: Any, schema: str) -> tuple[str, ...]:
    issues: list[str] = []
    for table in REQUIRED_TABLES:
        row = conn.execute("SELECT to_regclass(%s) AS relation", (db.qname(schema, table),)).fetchone()
        if not row or not _row(row, "relation"):
            issues.append(f"table-missing:{table}")
    if issues:
        # Column/constraint/index checks assume every required table exists;
        # a missing table already fails compatibility on its own.
        return tuple(sorted(issues))
    issues.extend(_column_issues(conn, schema))
    issues.extend(_constraint_issues(conn, schema))
    issues.extend(_index_issues(conn, schema))
    rows = conn.execute(
        """SELECT source.relname AS source_table, target_namespace.nspname AS target_schema
           FROM pg_constraint constraint_record
           JOIN pg_class source ON source.oid=constraint_record.conrelid
           JOIN pg_namespace source_namespace ON source_namespace.oid=source.relnamespace
           JOIN pg_class target ON target.oid=constraint_record.confrelid
           JOIN pg_namespace target_namespace ON target_namespace.oid=target.relnamespace
           WHERE constraint_record.contype='f' AND source_namespace.nspname=%s
             AND target_namespace.nspname<>%s""",
        (schema, schema),
    ).fetchall()
    issues.extend(
        f"cross-domain-foreign-key:{_row(row, 'source_table')}:{_row(row, 'target_schema', 1)}"
        for row in rows
    )
    return tuple(sorted(issues))


def check_compatibility(conn: Any, schema: str | None = None) -> Compatibility:
    selected = db.schema_name(schema or configured_schema())
    if not _ledger_exists(conn, selected):
        return Compatibility(DOMAIN, API_VERSION, COMPATIBILITY_LABEL, None, "uninitialized", False, "federation migration ledger is absent")
    rows = conn.execute(
        f"SELECT version, name, checksum FROM {db.qname(selected, MIGRATION_TABLE)} WHERE domain=%s ORDER BY version",
        (DOMAIN,),
    ).fetchall()
    applied = [(int(_row(item, "version")), str(_row(item, "name", 1)), str(_row(item, "checksum", 2))) for item in rows]
    expected = [(asset.version, asset.name, asset.sha256) for asset in load_migrations()]
    observed = max((item[0] for item in applied), default=0)
    if applied == expected:
        issues = _shape_issues(conn, selected)
        if issues:
            state, detail = "shape-mismatch", "federation schema shape is invalid: " + ",".join(issues)
        else:
            state, detail = "compatible", "schema is compatible with federation-schema/v1"
    elif observed > MAX_SCHEMA_VERSION:
        state, detail = "too-new", f"federation schema version {observed} exceeds {MAX_SCHEMA_VERSION}"
    elif applied and [item[0] for item in applied] != list(range(1, observed + 1)):
        state, detail = "incomplete", "federation migration ledger is not contiguous"
    elif applied:
        state, detail = "checksum-mismatch", "federation migration name or checksum differs from packaged v1"
    else:
        state, detail = "uninitialized", "federation migration ledger has no federation rows"
    return Compatibility(DOMAIN, API_VERSION, COMPATIBILITY_LABEL, observed or None, state, state == "compatible", detail)


def require_compatible(conn: Any, schema: str | None = None) -> Compatibility:
    result = check_compatibility(conn, schema)
    if not result.compatible:
        raise FederationSchemaError(f"federation schema is {result.state}: {result.detail}")
    return result


def migrate(conn: Any, schema: str | None = None) -> dict[str, Any]:
    selected = db.schema_name(schema or configured_schema())
    assets = load_migrations()
    applied_now: list[int] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"actionq:federation:{selected}:schema-migration",))
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{selected}"')
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {db.qname(selected, MIGRATION_TABLE)} ("
            "domain TEXT NOT NULL, version INTEGER NOT NULL CHECK (version > 0), name TEXT NOT NULL, "
            "checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(domain, version))"
        )
        rows = conn.execute(
            f"SELECT version, name, checksum FROM {db.qname(selected, MIGRATION_TABLE)} WHERE domain=%s ORDER BY version",
            (DOMAIN,),
        ).fetchall()
        applied = {int(_row(item, "version")): (str(_row(item, "name", 1)), str(_row(item, "checksum", 2))) for item in rows}
        known = {asset.version: asset for asset in assets}
        for version, recorded in applied.items():
            if version not in known or recorded != (known[version].name, known[version].sha256):
                raise FederationSchemaError(f"federation migration ledger drift at version {version}")
        for asset in assets:
            if asset.version in applied:
                continue
            for statement in _statements(asset, selected):
                conn.execute(statement)
            conn.execute(
                f"INSERT INTO {db.qname(selected, MIGRATION_TABLE)} (domain, version, name, checksum) VALUES (%s,%s,%s,%s)",
                (DOMAIN, asset.version, asset.name, asset.sha256),
            )
            applied_now.append(asset.version)
    result = check_compatibility(conn, selected)
    if not result.compatible:
        raise FederationSchemaError(result.detail)
    return {"domain": DOMAIN, "api_version": API_VERSION, "schema": selected, "applied": applied_now, "compatibility": result.as_dict()}


def configure_role_boundaries(
    conn: Any,
    schema: str | None = None,
    *,
    object_owner_role: str,
    migration_role: str,
    command_role: str,
    denied_roles: tuple[str, ...] = (),
) -> None:
    """Install the frozen v1 database-role boundary as a deployment action.

    The caller must be a role administrator.  ``object_owner_role`` is a
    NOLOGIN ownership role which is not granted to either service role.  The
    migration role keeps schema CREATE and migration-ledger DML, while the
    command role gets only federation fact/decision DML.  End-actor and legacy
    roles belong in ``denied_roles`` and receive no direct federation access.
    """
    from psycopg import sql

    selected = db.schema_name(schema or configured_schema())
    names = (object_owner_role, migration_role, command_role, *denied_roles)
    if not all(db.SCHEMA_RE.fullmatch(name) for name in names):
        raise FederationSchemaError("federation database roles must be simple identifiers")
    role_rows = conn.execute(
        "SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname=ANY(%s)",
        (list(names),),
    ).fetchall()
    observed_roles = {_row(row, "rolname"): bool(_row(row, "rolcanlogin", 1)) for row in role_rows}
    missing_roles = sorted(set(names) - set(observed_roles))
    if missing_roles:
        raise FederationSchemaError("federation database roles are absent: " + ",".join(missing_roles))
    if observed_roles[object_owner_role]:
        raise FederationSchemaError("federation object owner must be NOLOGIN")
    for service_role in (migration_role, command_role):
        membership = conn.execute("SELECT pg_has_role(%s,%s,'MEMBER') AS member", (service_role, object_owner_role)).fetchone()
        if membership and bool(_row(membership, "member")):
            raise FederationSchemaError(f"{service_role} must not be a member of the federation object-owner role")
    for denied_role in denied_roles:
        for service_role in (migration_role, command_role):
            # REVOKE ALL below is issued directly against denied_role; a role
            # that also inherits from migration_role/command_role would keep
            # that role's privileges through inheritance regardless, making
            # the direct REVOKE cosmetic rather than an actual denial.
            membership = conn.execute("SELECT pg_has_role(%s,%s,'MEMBER') AS member", (denied_role, service_role)).fetchone()
            if membership and bool(_row(membership, "member")):
                raise FederationSchemaError(f"{denied_role} must not be a member of {service_role} (would inherit federation access despite the direct REVOKE)")
    owner = sql.Identifier(object_owner_role)
    migrator = sql.Identifier(migration_role)
    command = sql.Identifier(command_role)
    schema_id = sql.Identifier(selected)
    tables = REQUIRED_TABLES[1:]
    conn.execute(sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(schema_id, owner))
    for table in (MIGRATION_TABLE, *tables):
        conn.execute(sql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(schema_id, sql.Identifier(table), owner))
    conn.execute(sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(schema_id))
    conn.execute(sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM PUBLIC").format(schema_id))
    for role_name in (migration_role, command_role, *denied_roles):
        role = sql.Identifier(role_name)
        conn.execute(sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(schema_id, role))
        conn.execute(sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM {}").format(schema_id, role))
    conn.execute(sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}").format(schema_id, migrator))
    conn.execute(sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(schema_id, sql.Identifier(MIGRATION_TABLE), migrator))
    conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema_id, command))
    conn.execute(sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(schema_id, sql.Identifier(MIGRATION_TABLE), command))
    conn.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {}.{} TO {}").format(schema_id, sql.Identifier("federation_resources"), command))
    append_only_tables = sql.SQL(", ").join(
        sql.SQL("{}.{}").format(schema_id, sql.Identifier(table))
        for table in tables if table != "federation_resources"
    )
    conn.execute(sql.SQL("GRANT SELECT, INSERT ON TABLE {} TO {}").format(append_only_tables, command))
