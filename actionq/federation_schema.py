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


def _shape_issues(conn: Any, schema: str) -> tuple[str, ...]:
    issues: list[str] = []
    for table in REQUIRED_TABLES:
        row = conn.execute("SELECT to_regclass(%s) AS relation", (db.qname(schema, table),)).fetchone()
        if not row or not _row(row, "relation"):
            issues.append(f"table-missing:{table}")
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
