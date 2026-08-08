from __future__ import annotations

import json
from collections.abc import Iterable

import pytest
from click.testing import CliRunner

from actionq import db, schema, server
from actionq.cli import cli


class _Rows:
    def __init__(self, rows: Iterable[dict] = ()):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _Transaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class FakeSchemaConnection:
    def __init__(
        self,
        *,
        ledger_exists: bool = False,
        applied: dict[int, str] | None = None,
        tables_exist: bool | None = None,
        valid_indexes: bool = True,
        can_create_schema: bool = False,
        owns_schema: bool = False,
        owns_relation: bool = False,
        can_assume_owner: bool = False,
        can_write_ledger: bool = False,
        is_superuser: bool = False,
        permissive_status: bool = False,
        status_expression: str | None = None,
        default_overrides: dict[tuple[str, str], str] | None = None,
        cascading_foreign_keys: bool = False,
        foreign_namespace: str | None = None,
        foreign_oid: int = 101,
        foreign_match_action: str = "s",
        foreign_deferrable: bool = False,
        foreign_initially_deferred: bool = False,
        foreign_validated: bool = True,
        index_overrides: dict[str, dict] | None = None,
        dispatch_relation: bool = True,
        watermark_relation: bool = True,
        dispatch_binding_constraints: bool = True,
        dispatch_check_overrides: dict[str, str] | None = None,
        dispatch_check_validated: dict[str, bool] | None = None,
        watermark_check_overrides: dict[str, str] | None = None,
        watermark_check_validated: dict[str, bool] | None = None,
        dispatch_root_count: int = 0,
    ):
        self.ledger_exists = ledger_exists
        self.applied = dict(applied or {})
        self.tables_exist = ledger_exists if tables_exist is None else tables_exist
        self.valid_indexes = valid_indexes
        self.can_create_schema = can_create_schema
        self.owns_schema = owns_schema
        self.owns_relation = owns_relation
        self.can_assume_owner = can_assume_owner
        self.can_write_ledger = can_write_ledger
        self.is_superuser = is_superuser
        self.permissive_status = permissive_status
        self.status_expression = status_expression
        self.default_overrides = default_overrides or {}
        self.cascading_foreign_keys = cascading_foreign_keys
        self.foreign_namespace = foreign_namespace
        self.foreign_oid = foreign_oid
        self.foreign_match_action = foreign_match_action
        self.foreign_deferrable = foreign_deferrable
        self.foreign_initially_deferred = foreign_initially_deferred
        self.foreign_validated = foreign_validated
        self.index_overrides = index_overrides or {}
        self.dispatch_relation = dispatch_relation
        self.watermark_relation = watermark_relation
        self.dispatch_binding_constraints = dispatch_binding_constraints
        self.dispatch_check_overrides = dispatch_check_overrides or {}
        self.dispatch_check_validated = dispatch_check_validated or {}
        self.watermark_check_overrides = watermark_check_overrides or {}
        self.watermark_check_validated = watermark_check_validated or {}
        self.dispatch_root_count = dispatch_root_count
        self.executed: list[tuple[str, object]] = []
        self.closed = False
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def close(self):
        self.closed = True

    def rollback(self):
        self.rollbacks += 1

    def transaction(self):
        return _Transaction()

    def execute(self, statement, params=None):
        normalized = " ".join(str(statement).split())
        self.executed.append((normalized, params))
        if normalized.startswith("SELECT to_regclass") and "AS actions" in normalized:
            relation = "present" if self.tables_exist else None
            return _Rows([{"actions": relation, "events": relation}])
        if normalized.startswith("SELECT to_regclass"):
            return _Rows([{"relation": "aq.schema_migrations" if self.ledger_exists else None}])
        if normalized.startswith("CREATE TABLE IF NOT EXISTS \"aq\".\"schema_migrations\""):
            self.ledger_exists = True
            return _Rows()
        if normalized.startswith("SELECT version, checksum"):
            return _Rows(
                {"version": version, "checksum": checksum}
                for version, checksum in sorted(self.applied.items())
            )
        if normalized.startswith("SELECT COUNT(*) AS dispatch_roots"):
            return _Rows([{"dispatch_roots": self.dispatch_root_count}])
        if normalized.startswith("SELECT current_user AS principal"):
            return _Rows(
                [
                    {
                        "principal": "runtime",
                        "is_superuser": self.is_superuser,
                        "owns_schema": self.owns_schema,
                        "can_assume_schema_owner": self.can_assume_owner,
                        "can_create": self.can_create_schema,
                        "owns_relation": self.owns_relation,
                        "can_assume_relation_owner": self.can_assume_owner,
                        "can_write_ledger": self.can_write_ledger,
                    }
                ]
            )
        if "CREATE TABLE IF NOT EXISTS \"aq\".actions" in normalized:
            self.tables_exist = True
            return _Rows()
        if normalized.startswith("SELECT table_name, column_name"):
            rows = []
            if self.tables_exist:
                query_schema = params[0]
                if len(params) == 3:
                    post_v3_tables = set(params[2])
                    return _Rows(
                        {"table_name": table, "column_name": "present"}
                        for table in post_v3_tables
                        if not (set(self.applied) == {1, 2, 3})
                    )
                for table, columns in schema._COLUMN_SHAPE.items():
                    if table == "dispatch_requests" and not self.dispatch_relation:
                        continue
                    if table == "dispatch_observation_watermarks" and not self.watermark_relation:
                        continue
                    rows.extend(
                        {
                            "table_name": table,
                            "column_name": column,
                            "data_type": expected[0],
                            "is_nullable": expected[1],
                            "column_default": self.default_overrides.get(
                                (table, column),
                                (
                                    f"nextval('{query_schema}.{expected[2].removeprefix('sequence:')}'::regclass)"
                                    if expected[2] and expected[2].startswith("sequence:")
                                    else expected[2]
                                ),
                            ),
                        }
                        for column, expected in columns.items()
                    )
            return _Rows(rows)
        if normalized.startswith("SELECT relation.relname AS table_name") and "pg_constraint" in normalized:
            rows = []
            if self.tables_exist:
                query_schema = params[0]
                foreign_namespace = self.foreign_namespace or query_schema
                rows = [
                    {
                        "table_name": "actions",
                        "relation_oid": 101,
                        "contype": "p",
                        "columns": ["id"],
                        "foreign_namespace": None,
                        "foreign_table": None,
                        "foreign_oid": None,
                        "foreign_columns": [],
                        "update_action": " ",
                        "delete_action": " ",
                        "match_action": " ",
                        "is_deferrable": False,
                        "is_initially_deferred": False,
                        "is_validated": True,
                        "expression": "",
                    },
                    {
                        "table_name": "actions",
                        "relation_oid": 101,
                        "contype": "f",
                        "columns": ["parent_id"],
                        "foreign_namespace": foreign_namespace,
                        "foreign_table": "actions",
                        "foreign_oid": self.foreign_oid,
                        "foreign_columns": ["id"],
                        "update_action": "c" if self.cascading_foreign_keys else "a",
                        "delete_action": "c" if self.cascading_foreign_keys else "a",
                        "match_action": self.foreign_match_action,
                        "is_deferrable": self.foreign_deferrable,
                        "is_initially_deferred": self.foreign_initially_deferred,
                        "is_validated": self.foreign_validated,
                        "expression": "",
                    },
                    {
                        "table_name": "actions",
                        "relation_oid": 101,
                        "contype": "c",
                        "columns": ["status"],
                        "foreign_namespace": None,
                        "foreign_table": None,
                        "foreign_oid": None,
                        "foreign_columns": [],
                        "update_action": " ",
                        "delete_action": " ",
                        "match_action": " ",
                        "is_deferrable": False,
                        "is_initially_deferred": False,
                        "is_validated": True,
                        "expression": (
                            self.status_expression
                            or (
                                "status = ANY (ARRAY['pending'::text, 'claimed'::text, "
                                "'cancelling'::text, "
                                "'completed'::text, 'failed'::text, 'rejected'::text, "
                                "'cancelled'::text])"
                                + (" OR true" if self.permissive_status else "")
                            )
                        ),
                    },
                    {
                        "table_name": "events",
                        "relation_oid": 102,
                        "contype": "p",
                        "columns": ["id"],
                        "foreign_namespace": None,
                        "foreign_table": None,
                        "foreign_oid": None,
                        "foreign_columns": [],
                        "update_action": " ",
                        "delete_action": " ",
                        "match_action": " ",
                        "is_deferrable": False,
                        "is_initially_deferred": False,
                        "is_validated": True,
                        "expression": "",
                    },
                    {
                        "table_name": "events",
                        "relation_oid": 102,
                        "contype": "f",
                        "columns": ["action_id"],
                        "foreign_namespace": foreign_namespace,
                        "foreign_table": "actions",
                        "foreign_oid": self.foreign_oid,
                        "foreign_columns": ["id"],
                        "update_action": "c" if self.cascading_foreign_keys else "a",
                        "delete_action": "c" if self.cascading_foreign_keys else "a",
                        "match_action": self.foreign_match_action,
                        "is_deferrable": self.foreign_deferrable,
                        "is_initially_deferred": self.foreign_initially_deferred,
                        "is_validated": self.foreign_validated,
                        "expression": "",
                    },
                ]
                if self.dispatch_relation:
                    def dispatch_constraint(kind, columns, *, foreign=False, expression="", validated=True):
                        return {
                            "table_name": "dispatch_requests", "relation_oid": 103,
                            "contype": kind, "columns": columns,
                            "foreign_namespace": foreign_namespace if foreign else None,
                            "foreign_table": "actions" if foreign else None,
                            "foreign_oid": self.foreign_oid if foreign else None,
                            "foreign_columns": ["id"] if foreign else [],
                            "update_action": "a", "delete_action": "a", "match_action": "s",
                            "is_deferrable": False, "is_initially_deferred": False,
                            "is_validated": validated, "expression": expression,
                        }
                    rows.extend([
                        dispatch_constraint("p", ["action_id"]),
                        dispatch_constraint("f", ["action_id"], foreign=True),
                        *([
                            dispatch_constraint("u", ["request_ref"]),
                            dispatch_constraint("u", ["identity", "environment", "operation", "idempotency_key"]),
                        ] if self.dispatch_binding_constraints else []),
                        dispatch_constraint("c", ["schema_version"], expression=self.dispatch_check_overrides.get("schema_version", "schema_version = 'v2'::text"), validated=self.dispatch_check_validated.get("schema_version", True)),
                        dispatch_constraint("c", ["request_sha256"], expression=self.dispatch_check_overrides.get("request_sha256", "request_sha256 ~ '^[a-f0-9]{64}$'::text"), validated=self.dispatch_check_validated.get("request_sha256", True)),
                        dispatch_constraint("c", ["operation"], expression=self.dispatch_check_overrides.get("operation", "operation = 'execution.dispatch.enqueue'::text"), validated=self.dispatch_check_validated.get("operation", True)),
                    ])
                    if self.watermark_relation:
                        rows.extend([
                            dispatch_constraint("p", ["action_id"]),
                            dispatch_constraint("f", ["action_id"], foreign=True),
                            dispatch_constraint("c", ["first_retained_event_id"], expression=self.watermark_check_overrides.get("first_retained_event_id", "first_retained_event_id >= 0"), validated=self.watermark_check_validated.get("first_retained_event_id", True)),
                            dispatch_constraint("c", ["last_observed_event_id"], expression=self.watermark_check_overrides.get("last_observed_event_id", "last_observed_event_id >= 0"), validated=self.watermark_check_validated.get("last_observed_event_id", True)),
                        ])
                        for constraint in rows[-4:]:
                            constraint["table_name"] = "dispatch_observation_watermarks"
                            constraint["relation_oid"] = 108
                        rows[-3]["delete_action"] = "r"
                def group_constraint(
                    table,
                    relation_oid,
                    kind,
                    columns,
                    *,
                    foreign_table=None,
                    foreign_oid=None,
                    foreign_columns=None,
                    expression="",
                ):
                    return {
                        "table_name": table,
                        "relation_oid": relation_oid,
                        "contype": kind,
                        "columns": columns,
                        "foreign_namespace": query_schema if foreign_table else None,
                        "foreign_table": foreign_table,
                        "foreign_oid": foreign_oid,
                        "foreign_columns": foreign_columns or (["id"] if foreign_table else []),
                        "update_action": "a",
                        "delete_action": "r" if foreign_table else "a",
                        "match_action": "s",
                        "is_deferrable": False,
                        "is_initially_deferred": False,
                        "is_validated": True,
                        "expression": expression,
                    }

                rows.extend([
                    group_constraint("execution_groups", 104, "p", ["id"]),
                    group_constraint("execution_groups", 104, "u", ["plan_ref"]),
                    group_constraint("execution_groups", 104, "u", ["spec_sha256"]),
                    group_constraint(
                        "execution_groups", 104, "c", ["max_parallel"],
                        expression="max_parallel >= 1 AND max_parallel <= 32",
                    ),
                    group_constraint(
                        "execution_groups", 104, "c", ["failure_policy"],
                        expression="failure_policy = 'continue-independent'::text",
                    ),
                    group_constraint(
                        "execution_groups", 104, "c", ["claim_state"],
                        expression="claim_state = ANY (ARRAY['active'::text, 'stopped'::text])",
                    ),
                    group_constraint(
                        "execution_group_members", 105, "p", ["group_id", "action_id"]
                    ),
                    group_constraint(
                        "execution_group_members", 105, "f", ["group_id"],
                        foreign_table="execution_groups", foreign_oid=104,
                    ),
                    group_constraint(
                        "execution_group_members", 105, "f", ["action_id"],
                        foreign_table="actions", foreign_oid=101,
                    ),
                    group_constraint("execution_group_members", 105, "u", ["action_id"]),
                    group_constraint(
                        "execution_group_members", 105, "u", ["group_id", "ordinal"]
                    ),
                    group_constraint(
                        "execution_group_members", 105, "c", ["ordinal"],
                        expression="ordinal >= 0",
                    ),
                    group_constraint("immutable_action_specs", 106, "p", ["spec_ref"]),
                    group_constraint("immutable_action_specs", 106, "u", ["spec_digest"]),
                    group_constraint("immutable_action_specs", 106, "c", ["spec_ref"]),
                    group_constraint("immutable_action_specs", 106, "c", ["spec_digest"]),
                    group_constraint("immutable_action_specs", 106, "c", ["spec_ref", "spec_digest"]),
                    group_constraint("immutable_action_requests", 107, "p", ["action_id"]),
                    group_constraint(
                        "immutable_action_requests", 107, "f", ["action_id"],
                        foreign_table="actions", foreign_oid=101,
                    ),
                    group_constraint(
                        "immutable_action_requests", 107, "f", ["spec_ref"],
                        foreign_table="immutable_action_specs", foreign_oid=106,
                        foreign_columns=["spec_ref"],
                    ),
                    group_constraint("immutable_action_requests", 107, "u", ["request_ref"]),
                    group_constraint("immutable_action_requests", 107, "u", ["request_digest"]),
                    group_constraint("immutable_action_requests", 107, "u", ["plan_ref", "topology", "role", "subject"]),
                    group_constraint("immutable_action_requests", 107, "c", ["plan_ref"]),
                    group_constraint("immutable_action_requests", 107, "c", ["topology"]),
                    group_constraint("immutable_action_requests", 107, "c", ["role"]),
                    group_constraint("immutable_action_requests", 107, "c", ["request_ref", "request_digest"]),
                    group_constraint("immutable_action_requests", 107, "c", ["spec_ref", "spec_digest"]),
                    group_constraint("immutable_action_runtime_grants", 108, "p", ["action_id"]),
                    group_constraint(
                        "immutable_action_runtime_grants", 108, "f", ["action_id"],
                        foreign_table="actions", foreign_oid=101,
                    ),
                    group_constraint("immutable_action_runtime_grants", 108, "c", ["request_digest"]),
                    group_constraint("immutable_action_runtime_grants", 108, "c", ["spec_digest"]),
                    group_constraint("immutable_action_runtime_grants", 108, "c", ["input_set_digest"]),
                    group_constraint("immutable_action_runtime_grants", 108, "c", ["grant_digest"]),
                ])
            return _Rows(rows)
        if normalized.startswith("SELECT relation.relname AS table_name") and "pg_index" in normalized:
            rows = []
            if self.tables_exist:
                for name, (table, keys, predicate) in schema._REQUIRED_INDEXES.items():
                    if table == "dispatch_requests" and not self.dispatch_relation:
                        continue
                    override = self.index_overrides.get(name, {})
                    rows.append(
                        {
                            "table_name": table,
                            "indisvalid": self.valid_indexes,
                            "indisready": True,
                            "indisunique": False,
                            "access_method": "btree",
                            "expressions": [key[0] for key in keys],
                            "descending": [key[1] for key in keys],
                            "nulls_first": override.get(
                                "nulls_first", [key[2] for key in keys]
                            ),
                            "predicate": override.get("predicate", predicate),
                        }
                    )
            return _Rows(rows)
        if normalized.startswith("INSERT INTO \"aq\".\"schema_migrations\""):
            _, version, _, checksum = params
            self.applied[int(version)] = str(checksum)
            return _Rows()
        return _Rows()


def _packaged_checksums() -> dict[int, str]:
    return {migration.version: migration.checksum for migration in schema.load_migrations()}


def test_migration_assets_are_contiguous_and_render_only_validated_schema():
    migrations = schema.load_migrations()

    assert [migration.version for migration in migrations] == [1, 2, 3, 4, 5, 6, 7, 8]
    rendered = schema._render(migrations[0], "aq")
    assert "{{schema}}" not in rendered
    assert '"aq".actions' in rendered
    assert len(schema._statements(rendered)) == 9
    with pytest.raises(db.ActionQError):
        schema._render(migrations[0], "unsafe-name")


def test_v3_migration_comment_is_attached_to_valid_sql_statement():
    migration = next(item for item in schema.load_migrations() if item.version == 3)
    statements = schema._statements(schema._render(migration, "aq"))

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS \"aq\".dispatch_requests" in statements[0]
    assert statements[1].startswith("CREATE INDEX IF NOT EXISTS idx_dispatch_requests_created")
    assert all("jsonb is intentionally" not in statement for statement in statements)


def test_compatibility_is_read_only_and_fails_closed_without_ledger():
    conn = FakeSchemaConnection()

    result = schema.check_compatibility(conn, "aq")

    assert result.state == "uninitialized"
    assert result.compatible is False
    assert all(statement.startswith("SELECT") for statement, _ in conn.executed)
    with pytest.raises(schema.SchemaCompatibilityError, match="uninitialized"):
        schema.require_compatible(conn, "aq")


def test_compatibility_accepts_exact_packaged_version_and_checksum():
    conn = FakeSchemaConnection(ledger_exists=True, applied=_packaged_checksums())

    result = schema.require_compatible(conn, "aq")

    assert result.as_dict() == {
        "domain": "execution",
        "api_version": "v1",
        "minimum_schema_version": 1,
        "maximum_schema_version": 8,
        "observed_schema_version": 8,
        "state": "compatible",
        "compatible": True,
        "detail": "schema is compatible with the packaged execution adapter",
    }


def test_v3_dispatch_relation_missing_fails_runtime_compatibility():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        dispatch_relation=False,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert result.state == "shape-mismatch"
    assert "column-missing:dispatch_requests.action_id" in result.detail
    with pytest.raises(schema.SchemaCompatibilityError, match="shape-mismatch"):
        schema.require_compatible(conn, "aq")


def test_v3_dispatch_binding_index_corruption_fails_runtime_compatibility():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        index_overrides={"dispatch-requests.created": {"predicate": "action_id > 0"}},
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert "index-missing-or-invalid:dispatch-requests.created" in result.detail


def test_v3_dispatch_binding_constraint_corruption_fails_runtime_compatibility():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        dispatch_binding_constraints=False,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert "constraint-missing-or-invalid:dispatch-requests-request-ref-unique" in result.detail


@pytest.mark.parametrize(
    ("column", "expression"),
    [
        ("schema_version", "schema_version = 'v2'::text OR true"),
        ("request_sha256", "request_sha256 ~ '^[a-f0-9]{64}$'::text OR true"),
        ("operation", "operation = 'execution.dispatch.enqueue'::text OR true"),
    ],
)
def test_v3_permissive_dispatch_check_variants_fail_runtime_compatibility(column, expression):
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        dispatch_check_overrides={column: expression},
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert f"constraint-missing-or-invalid:dispatch_requests.{column}" in result.detail


def test_v3_not_valid_dispatch_check_fails_runtime_compatibility():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        dispatch_check_validated={"operation": False},
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert "constraint-missing-or-invalid:dispatch_requests.operation" in result.detail
    assert all(statement.startswith("SELECT") for statement, _ in conn.executed)


def test_v5_execution_group_column_shape_drift_fails_runtime_compatibility():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        default_overrides={("execution_groups", "claim_state"): "'stopped'::text"},
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert result.state == "shape-mismatch"
    assert "column-default:execution_groups.claim_state" in result.detail


def test_unversioned_v1_adoption_allows_only_expected_later_relation_absence():
    remaining = schema._unversioned_v1_shape_issues(
        [
            "column-missing:actions.claim_receipt",
            "column-missing:dispatch_requests.request_ref",
            "column-missing:dispatch_observation_watermarks.last_observed_event_id",
            "constraint-missing-or-invalid:dispatch-requests-request-ref-unique",
            "constraint-missing-or-invalid:dispatch_requests.operation",
            "constraint-missing-or-invalid:dispatch_observation_watermarks.last_observed_event_id",
            "index-missing-or-invalid:dispatch-requests.created",
            "column-type:actions.priority",
            "column-unexpected:dispatch_requests.forged",
        ]
    )

    assert remaining == [
        "column-type:actions.priority",
        "column-unexpected:dispatch_requests.forged",
    ]


def test_schema3_bridge_accepts_absent_post_v3_watermark_relation():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied={version: checksum for version, checksum in _packaged_checksums().items() if version <= 3},
        watermark_relation=False,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is True
    assert result.observed_schema_version == 3


@pytest.mark.parametrize(
    ("column", "expression", "validated"),
    [
        ("first_retained_event_id", "first_retained_event_id >= 0 OR true", True),
        ("last_observed_event_id", "last_observed_event_id >= 0", False),
    ],
)
def test_compatibility_rejects_permissive_or_unvalidated_watermark_checks(column, expression, validated):
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        watermark_check_overrides={column: expression},
        watermark_check_validated={column: validated},
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert f"constraint-missing-or-invalid:dispatch_observation_watermarks.{column}" in result.detail


def test_compatibility_rejects_valid_ledger_when_queue_shape_is_missing():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        tables_exist=False,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.state == "shape-mismatch"
    assert result.compatible is False
    assert "column-missing:actions.id" in result.detail
    assert all(statement.startswith("SELECT") for statement, _ in conn.executed)


def test_compatibility_rejects_semantically_invalid_index():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        valid_indexes=False,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.state == "shape-mismatch"
    assert "index-missing-or-invalid:actions.claim-lookup" in result.detail


@pytest.mark.parametrize(
    ("authority", "expected_detail"),
    [
        ({"can_create_schema": True}, "schema CREATE authority"),
        ({"owns_schema": True}, "owns the domain schema"),
        ({"owns_relation": True}, "owns a domain relation"),
        ({"can_assume_owner": True}, "can assume the domain schema owner"),
        ({"can_write_ledger": True}, "mutate the migration ledger"),
        ({"is_superuser": True}, "is superuser"),
    ],
)
def test_compatibility_rejects_authority_capable_principal_even_with_valid_schema(
    authority, expected_detail
):
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        **authority,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.state == "role-mismatch"
    assert expected_detail in result.detail


@pytest.mark.parametrize(
    ("connection_options", "expected_issue"),
    [
        ({"permissive_status": True}, "constraint-invalid:actions.status"),
        (
            {"default_overrides": {("actions", "priority"): "50"}},
            "column-default:actions.priority",
        ),
        (
            {"default_overrides": {("events", "payload"): "'{\"x\": 1}'::jsonb"}},
            "column-default:events.payload",
        ),
        (
            {"default_overrides": {("actions", "status"): "'PENDING'::text"}},
            "column-default:actions.status",
        ),
        (
            {"cascading_foreign_keys": True},
            "constraint-missing-or-invalid:actions-parent-foreign-key",
        ),
        (
            {"foreign_namespace": "shadow"},
            "constraint-missing-or-invalid:actions-parent-foreign-key",
        ),
        (
            {"foreign_oid": 999},
            "constraint-missing-or-invalid:actions-parent-foreign-key",
        ),
        (
            {"foreign_match_action": "f"},
            "constraint-missing-or-invalid:actions-parent-foreign-key",
        ),
        (
            {"foreign_deferrable": True, "foreign_initially_deferred": True},
            "constraint-missing-or-invalid:actions-parent-foreign-key",
        ),
        (
            {"foreign_validated": False},
            "constraint-missing-or-invalid:actions-parent-foreign-key",
        ),
        (
            {
                "status_expression": (
                    "status = ANY (ARRAY['PENDING'::text, 'CLAIMED'::text, "
                    "'COMPLETED'::text, 'FAILED'::text, 'REJECTED'::text, "
                    "'CANCELLED'::text])"
                )
            },
            "constraint-invalid:actions.status",
        ),
        (
            {
                "index_overrides": {
                    "actions.claim-lookup": {
                        "predicate": "status = 'PENDING'::text"
                    }
                }
            },
            "index-missing-or-invalid:actions.claim-lookup",
        ),
        (
            {
                "index_overrides": {
                    "events.timestamp": {"nulls_first": [False]}
                }
            },
            "index-missing-or-invalid:events.timestamp",
        ),
    ],
)
def test_compatibility_rejects_semantically_permissive_shape(
    connection_options, expected_issue
):
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        **connection_options,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.state == "shape-mismatch"
    assert expected_issue in result.detail


def test_sql_canonicalization_preserves_semantic_tokens():
    assert schema._canonical_sql(" status   =  'PENDING'::text ") == (
        "status = 'PENDING'::text"
    )
    assert schema._canonical_sql('"Status" DESC NULLS LAST') == (
        '"Status" DESC NULLS LAST'
    )
    assert schema._without_redundant_outer_parentheses(
        "((status = 'pending'::text))"
    ) == "status = 'pending'::text"


@pytest.mark.parametrize(
    ("applied", "state"),
    [
        (
            {
                1: "wrong",
                2: _packaged_checksums()[2],
                    3: _packaged_checksums()[3],
                    4: _packaged_checksums()[4],
                    5: _packaged_checksums()[5],
                    6: _packaged_checksums()[6],
                },
                "checksum-mismatch",
            ),
            ({**_packaged_checksums(), 9: "future"}, "too-new"),
    ],
)
def test_compatibility_rejects_unsupported_schema(applied, state):
    conn = FakeSchemaConnection(ledger_exists=True, applied=applied)

    result = schema.check_compatibility(conn, "aq")

    assert result.compatible is False
    assert result.state == state


def test_migration_is_serialized_idempotent_and_returns_compatibility():
    conn = FakeSchemaConnection()

    first = schema.migrate(conn, "aq")
    second = schema.migrate(conn, "aq")

    assert first["applied_versions"] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert second["applied_versions"] == []
    assert second["compatibility"]["compatible"] is True
    locks = [
        params
        for statement, params in conn.executed
        if statement.startswith("SELECT pg_advisory_xact_lock")
    ]
    assert locks == [
        ("actionq:aq:schema-migration",),
        ("actionq:aq:schema-migration",),
    ]


def test_schema8_migration_refuses_dispatch_roots_without_touching_terminal_history():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied={version: checksum for version, checksum in _packaged_checksums().items() if version < 8},
        dispatch_root_count=1,
    )

    with pytest.raises(schema.SchemaMigrationError, match="zero pre-v7 dispatch roots; found 1"):
        schema.migrate(conn, "aq")

    assert 8 not in conn.applied
    # The guard is only about dispatch roots. It neither queries nor deletes
    # terminal action/event records, which remain deployment evidence.
    assert not any(statement.startswith("DELETE FROM") for statement, _ in conn.executed)


def test_check_compatibility_cli_uses_read_only_contract(monkeypatch):
    conn = FakeSchemaConnection(ledger_exists=True, applied=_packaged_checksums())
    monkeypatch.setattr(db, "connect", lambda: conn)

    result = CliRunner().invoke(cli, ["--schema", "aq", "check-compatibility"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["state"] == "compatible"
    assert all(statement.startswith("SELECT") for statement, _ in conn.executed)


def test_check_compatibility_cli_exits_three_for_incompatible_schema(monkeypatch):
    conn = FakeSchemaConnection()
    monkeypatch.setattr(db, "connect", lambda: conn)

    result = CliRunner().invoke(cli, ["--schema", "aq", "check-compatibility"])

    assert result.exit_code == 3
    assert json.loads(result.output)["state"] == "uninitialized"


def test_server_checks_compatibility_before_binding(monkeypatch):
    calls: list[str] = []

    def compatible():
        calls.append("compatibility")
        return {"state": "compatible", "observed_schema_version": 1}

    class RefusingServer:
        def __init__(self, *args, **kwargs):
            calls.append("bind")
            raise RuntimeError("stop after bind")

    monkeypatch.setattr(server, "_require_runtime_compatibility", compatible)
    monkeypatch.setattr(server, "HTTPServer", RefusingServer)

    with pytest.raises(RuntimeError, match="stop after bind"):
        server.main()

    assert calls == ["compatibility", "bind"]


def test_server_never_binds_when_schema_is_incompatible(monkeypatch, capsys):
    def incompatible():
        raise schema.SchemaCompatibilityError("unsupported schema")

    monkeypatch.setattr(server, "_require_runtime_compatibility", incompatible)
    monkeypatch.setattr(
        server,
        "HTTPServer",
        lambda *args, **kwargs: pytest.fail("server bound before compatibility check"),
    )

    with pytest.raises(SystemExit) as excinfo:
        server.main()
    assert excinfo.value.code == 3
    assert "startup refused" in capsys.readouterr().err


def test_server_request_connections_recheck_compatibility_without_ddl(monkeypatch):
    conn = FakeSchemaConnection(ledger_exists=True, applied=_packaged_checksums())
    monkeypatch.setattr(db, "connect", lambda: conn)

    assert server._sessions("") == []

    assert conn.rollbacks == 1

    assert any(statement.startswith("SELECT to_regclass") for statement, _ in conn.executed)
    assert all(
        not statement.startswith(("CREATE", "ALTER", "DROP"))
        for statement, _ in conn.executed
    )


def test_server_reports_schema_incompatibility_as_unavailable(monkeypatch):
    def incompatible(_query):
        raise schema.SchemaCompatibilityError("too new")

    responses = []
    handler = object.__new__(server._Handler)
    handler.path = "/sessions"
    handler._send_json = lambda status, body: responses.append((status, body))
    monkeypatch.setattr(server, "_sessions", incompatible)

    handler.do_GET()

    assert responses == [(503, {"error": "schema incompatible"})]
