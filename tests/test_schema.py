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
        permissive_status: bool = False,
        default_overrides: dict[tuple[str, str], str] | None = None,
        cascading_foreign_keys: bool = False,
    ):
        self.ledger_exists = ledger_exists
        self.applied = dict(applied or {})
        self.tables_exist = ledger_exists if tables_exist is None else tables_exist
        self.valid_indexes = valid_indexes
        self.can_create_schema = can_create_schema
        self.permissive_status = permissive_status
        self.default_overrides = default_overrides or {}
        self.cascading_foreign_keys = cascading_foreign_keys
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
        if normalized.startswith("SELECT current_user AS principal"):
            return _Rows(
                [{"principal": "runtime", "can_create": self.can_create_schema}]
            )
        if "CREATE TABLE IF NOT EXISTS \"aq\".actions" in normalized:
            self.tables_exist = True
            return _Rows()
        if normalized.startswith("SELECT table_name, column_name"):
            rows = []
            if self.tables_exist:
                for table, columns in schema._COLUMN_SHAPE.items():
                    rows.extend(
                        {
                            "table_name": table,
                            "column_name": column,
                            "data_type": expected[0],
                            "is_nullable": expected[1],
                            "column_default": self.default_overrides.get(
                                (table, column),
                                (
                                    f"nextval('{expected[2].removeprefix('sequence:')}'::regclass)"
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
                rows = [
                    {
                        "table_name": "actions",
                        "contype": "p",
                        "columns": ["id"],
                        "foreign_table": None,
                        "foreign_columns": [],
                        "update_action": " ",
                        "delete_action": " ",
                        "expression": "",
                    },
                    {
                        "table_name": "actions",
                        "contype": "f",
                        "columns": ["parent_id"],
                        "foreign_table": "actions",
                        "foreign_columns": ["id"],
                        "update_action": "c" if self.cascading_foreign_keys else "a",
                        "delete_action": "c" if self.cascading_foreign_keys else "a",
                        "expression": "",
                    },
                    {
                        "table_name": "actions",
                        "contype": "c",
                        "columns": ["status"],
                        "foreign_table": None,
                        "foreign_columns": [],
                        "update_action": " ",
                        "delete_action": " ",
                        "expression": (
                            "status = ANY (ARRAY['pending'::text, 'claimed'::text, "
                            "'completed'::text, 'failed'::text, 'rejected'::text, "
                            "'cancelled'::text])"
                            + (" OR true" if self.permissive_status else "")
                        ),
                    },
                    {
                        "table_name": "events",
                        "contype": "p",
                        "columns": ["id"],
                        "foreign_table": None,
                        "foreign_columns": [],
                        "update_action": " ",
                        "delete_action": " ",
                        "expression": "",
                    },
                    {
                        "table_name": "events",
                        "contype": "f",
                        "columns": ["action_id"],
                        "foreign_table": "actions",
                        "foreign_columns": ["id"],
                        "update_action": "c" if self.cascading_foreign_keys else "a",
                        "delete_action": "c" if self.cascading_foreign_keys else "a",
                        "expression": "",
                    },
                ]
            return _Rows(rows)
        if normalized.startswith("SELECT relation.relname AS table_name") and "pg_index" in normalized:
            rows = []
            if self.tables_exist:
                for table, columns, predicate in schema._REQUIRED_INDEXES.values():
                    rows.append(
                        {
                            "table_name": table,
                            "indisvalid": self.valid_indexes,
                            "indisready": True,
                            "indisunique": False,
                            "access_method": "btree",
                            "columns": list(columns),
                            "predicate": f"status = '{predicate}'::text" if predicate else None,
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

    assert [migration.version for migration in migrations] == [1]
    rendered = schema._render(migrations[0], "aq")
    assert "{{schema}}" not in rendered
    assert '"aq".actions' in rendered
    assert len(schema._statements(rendered)) == 9
    with pytest.raises(db.ActionQError):
        schema._render(migrations[0], "unsafe-name")


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
        "maximum_schema_version": 1,
        "observed_schema_version": 1,
        "state": "compatible",
        "compatible": True,
        "detail": "schema is compatible with the packaged execution adapter",
    }
    assert all(statement.startswith("SELECT") for statement, _ in conn.executed)


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


def test_compatibility_rejects_ddl_capable_principal_even_with_valid_schema():
    conn = FakeSchemaConnection(
        ledger_exists=True,
        applied=_packaged_checksums(),
        can_create_schema=True,
    )

    result = schema.check_compatibility(conn, "aq")

    assert result.state == "role-mismatch"
    assert "schema CREATE authority" in result.detail


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
            {"cascading_foreign_keys": True},
            "constraint-missing-or-invalid:actions-parent-foreign-key",
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


@pytest.mark.parametrize(
    ("applied", "state"),
    [
        ({1: "wrong"}, "checksum-mismatch"),
        ({1: _packaged_checksums()[1], 2: "future"}, "too-new"),
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

    assert first["applied_versions"] == [1]
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
