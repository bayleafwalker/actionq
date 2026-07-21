import getpass
import hashlib
import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

import psycopg
from psycopg import sql

from actionq.cli import cli
from actionq import db, schema as schema_contract, server


MIGRATION_ROLE = "actionq_migration"
RUNTIME_ROLE = "actionq_runtime"
LEGACY_V1_PATH = Path(__file__).parent / "fixtures" / "actionq_legacy_v1.sql"
LEGACY_V1_SHA256 = "49b1a8a316ca8de67d404e000a9a35d7de3ce2263ed1f490bfa66a2b01b7954f"


@pytest.fixture(scope="module", autouse=True)
def postgres_urls():
    binaries = {command: shutil.which(command) for command in ("initdb", "pg_ctl")}
    missing = sorted(command for command, path in binaries.items() if path is None)
    if missing:
        pytest.fail(f"PostgreSQL server binaries are required: {missing}")

    root = Path(tempfile.mkdtemp(prefix="actionq-pg-"))
    data = root / "data"
    socket_dir = root / "socket"
    socket_dir.mkdir()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    initdb = Path(binaries["initdb"] or "initdb").resolve()
    pg_ctl = Path(binaries["pg_ctl"] or "pg_ctl").resolve()
    initdb_args = [
        str(initdb),
        "--no-locale",
        "--encoding=UTF8",
        "--auth=trust",
        "-D",
        str(data),
    ]
    adjacent_share = initdb.parents[1] / "share" / "postgresql"
    if (adjacent_share / "postgres.bki").exists():
        initdb_args.extend(["-L", str(adjacent_share)])
    subprocess.run(initdb_args, check=True, capture_output=True, text=True)
    subprocess.run(
        [
            str(pg_ctl),
            "-D",
            str(data),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-F -h '' -k {socket_dir} -p {port}",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    base = f"dbname=postgres host={socket_dir} port={port}"
    urls = {
        "admin": f"{base} user={getpass.getuser()}",
        "migration": f"{base} user={MIGRATION_ROLE}",
        "runtime": f"{base} user={RUNTIME_ROLE}",
    }
    previous = {
        name: os.environ.get(name)
        for name in (
            "ACTIONQ_TEST_URL",
            "ACTIONQ_TEST_MIGRATION_URL",
            "ACTIONQ_TEST_RUNTIME_URL",
        )
    }
    try:
        with psycopg.connect(urls["admin"], autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(MIGRATION_ROLE))
            )
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(RUNTIME_ROLE)))
            conn.execute(
                sql.SQL("GRANT CREATE ON DATABASE postgres TO {}").format(
                    sql.Identifier(MIGRATION_ROLE)
                )
            )
        os.environ["ACTIONQ_TEST_URL"] = urls["admin"]
        os.environ["ACTIONQ_TEST_MIGRATION_URL"] = urls["migration"]
        os.environ["ACTIONQ_TEST_RUNTIME_URL"] = urls["runtime"]
        yield urls
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-m", "fast", "-w", "stop"],
            check=False,
            capture_output=True,
            text=True,
        )
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def runner_env(monkeypatch):
    schema = "aqtest_" + uuid.uuid4().hex
    monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_URL"])
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    return CliRunner(), schema


def _invoke_json(runner, args):
    result = runner.invoke(cli, args)
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def _install_legacy_v1(conn, schema: str) -> None:
    raw = LEGACY_V1_PATH.read_text(encoding="utf-8")
    assert hashlib.sha256(raw.encode()).hexdigest() == LEGACY_V1_SHA256
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(f'SET search_path TO "{schema}"')
    try:
        for statement in schema_contract._statements(raw):
            conn.execute(statement)
    finally:
        conn.execute("RESET search_path")
    conn.commit()


def test_lifecycle_claim_complete_show(runner_env):
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    action = _invoke_json(
        runner,
        [
            "add",
            "--type",
            "scope-iterate",
            "--project",
            "sprintctl",
            "--target",
            "42",
            "--source",
            "doc:plan",
            "--created-by",
            "human:test",
        ],
    )
    assert action["status"] == "pending"

    claimed = _invoke_json(runner, ["claim", "--worker", "worker:test"])
    assert claimed["id"] == action["id"]
    assert claimed["status"] == "claimed"

    completed = _invoke_json(
        runner,
        ["complete", str(action["id"]), "--result", "branch=agent/scope-iterate/1"],
    )
    assert completed["status"] == "completed"

    detail = _invoke_json(runner, ["show", str(action["id"])])
    assert detail["action"]["status"] == "completed"
    assert [event["event_type"] for event in detail["events"]] == [
        "action_enqueued",
        "action_claimed",
        "action_completed",
    ]


def test_claim_exits_nonzero_when_empty(runner_env):
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, ["claim", "--worker", "worker:test"])
    assert result.exit_code == 2
    assert "no pending actions" in result.output


def test_manual_usage_limit_pause_then_resume_drill(runner_env):
    """One manual resume/re-dispatch drill for work item #976: pause an
    action with a confirmed usage-limit reason and handoff reference, fail
    it (checkpoint-and-fail), enqueue a fresh re-dispatch action, and emit
    the correlating ``session.resumed`` -- all through the real
    ``actionctl`` CLI against a disposable schema, not just in-process
    fakes."""
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    action = _invoke_json(
        runner,
        ["add", "--type", "scope-iterate", "--project", "sprintctl", "--target", "42", "--created-by", "human:test"],
    )
    claimed = _invoke_json(runner, ["claim", "--worker", "actionq-daemon:test"])
    assert claimed["id"] == action["id"]

    paused = _invoke_json(
        runner,
        [
            "emit", "--type", "session.paused", "--action", str(action["id"]), "--actor", "actionq-daemon:test",
            "--payload", json.dumps({
                "session_id": "aqs:old", "reason": "usage-limit", "mechanism": "checkpoint-and-fail",
                "handoff_ref": "/home/agent/.local/state/actionq/handoff/aqs_old.md", "resumable": True,
            }),
        ],
    )
    assert paused["event_type"] == "session.paused"

    failed = _invoke_json(
        runner,
        ["fail", str(action["id"]), "--reason", "usage-limit-paused: confirmed usage-limit signal matched"],
    )
    assert failed["status"] == "failed"

    redispatch = _invoke_json(
        runner,
        ["add", "--type", "scope-iterate", "--project", "sprintctl", "--target", "42",
         "--source", f"handoff:aqs:old", "--created-by", "human:test"],
    )
    resumed = _invoke_json(
        runner,
        [
            "emit", "--type", "session.resumed", "--action", str(redispatch["id"]), "--actor", "actionq-daemon:test",
            "--payload", json.dumps({
                "session_id": "aqs:new", "resumed_from_session_id": "aqs:old",
                "handoff_ref": "/home/agent/.local/state/actionq/handoff/aqs_old.md", "mechanism": "redispatch",
            }),
        ],
    )
    assert resumed["event_type"] == "session.resumed"
    assert resumed["payload"]["resumed_from_session_id"] == "aqs:old"

    original_history = _invoke_json(runner, ["show", str(action["id"])])
    assert [event["event_type"] for event in original_history["events"]] == [
        "action_enqueued", "action_claimed", "session.paused", "action_failed",
    ]
    redispatch_history = _invoke_json(runner, ["show", str(redispatch["id"])])
    assert [event["event_type"] for event in redispatch_history["events"]] == [
        "action_enqueued", "session.resumed",
    ]


def test_emit_coordinator_cycle(runner_env):
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    event = _invoke_json(
        runner,
        [
            "emit",
            "--type",
            "coordinator_cycle",
            "--actor",
            "dispatcher:test",
            "--payload",
            '{"claimed": false}',
        ],
    )
    assert event["event_type"] == "coordinator_cycle"
    assert event["payload"]["claimed"] is False


def test_deployment_migration_empty_current_retry_and_compatibility(runner_env):
    runner, schema = runner_env

    first = runner.invoke(cli, ["migrate", "--json-output"])
    assert first.exit_code == 0, first.output
    assert json.loads(first.output)["applied_versions"] == [1]

    second = runner.invoke(cli, ["migrate", "--json-output"])
    assert second.exit_code == 0, second.output
    assert json.loads(second.output)["applied_versions"] == []

    compatibility = runner.invoke(cli, ["check-compatibility"])
    assert compatibility.exit_code == 0, compatibility.output
    assert json.loads(compatibility.output)["state"] == "compatible"


def test_deployment_migration_adopts_unversioned_current_schema(runner_env):
    runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    _install_legacy_v1(conn, schema)
    before = {
        row["indexname"]
        for row in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND indexname NOT LIKE %s",
            (schema, "%_pkey"),
        ).fetchall()
    }
    conn.close()

    result = runner.invoke(cli, ["migrate", "--json-output"])

    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["applied_versions"] == [1]
    assert report["adopted_legacy_schema"] is True
    assert runner.invoke(cli, ["check-compatibility"]).exit_code == 0
    verify_conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    after = {
        row["indexname"]
        for row in verify_conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND indexname NOT LIKE %s",
            (schema, "%_pkey"),
        ).fetchall()
    }
    verify_conn.close()
    assert before == after == {
        "idx_actions_claim_lookup",
        "idx_actions_parent",
        "idx_actions_project",
        "idx_actions_deadline",
        "idx_events_action",
        "idx_events_timestamp",
        "idx_events_type_time",
    }


def test_unversioned_legacy_wrong_index_definition_is_not_stamped(runner_env):
    _runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    _install_legacy_v1(conn, schema)
    conn.execute(f'DROP INDEX "{schema}".idx_actions_project')
    conn.execute(
        f'CREATE INDEX idx_actions_project ON "{schema}".actions(target_ref)'
    )
    conn.commit()

    with pytest.raises(
        schema_contract.SchemaMigrationError, match="index-missing-or-invalid"
    ):
        schema_contract.migrate(conn, schema)
    conn.rollback()
    assert conn.execute(
        "SELECT to_regclass(%s) AS relation",
        (f'"{schema}"."schema_migrations"',),
    ).fetchone()["relation"] is None
    conn.close()


def test_unversioned_legacy_wrong_status_constraint_is_not_stamped(runner_env):
    _runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    _install_legacy_v1(conn, schema)
    conn.execute(
        f'ALTER TABLE "{schema}".actions DROP CONSTRAINT actions_status_check'
    )
    conn.execute(
        f'ALTER TABLE "{schema}".actions ADD CONSTRAINT actions_status_check '
        "CHECK (status IN ('pending', 'claimed'))"
    )
    conn.commit()

    with pytest.raises(
        schema_contract.SchemaMigrationError,
        match="constraint-invalid:actions.status",
    ):
        schema_contract.migrate(conn, schema)
    conn.rollback()
    assert conn.execute(
        "SELECT to_regclass(%s) AS relation",
        (f'"{schema}"."schema_migrations"',),
    ).fetchone()["relation"] is None
    conn.close()


def test_failed_migration_rolls_back_ledger_and_can_retry_after_repair(runner_env):
    _runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    migration = schema_contract.load_migrations()[0]
    statements = schema_contract._statements(schema_contract._render(migration, schema))
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(statements[0])
    conn.execute(
        f'CREATE TABLE "{schema}".events ('
        f'id BIGSERIAL PRIMARY KEY, action_id BIGINT REFERENCES "{schema}".actions(id), '
        "event_type TEXT NOT NULL, timestamp TIMESTAMPTZ NOT NULL DEFAULT now())"
    )
    conn.commit()

    with pytest.raises(schema_contract.SchemaMigrationError, match="incompatible unversioned"):
        schema_contract.migrate(conn, schema)
    conn.rollback()
    ledger = conn.execute(
        "SELECT to_regclass(%s) AS relation",
        (f'"{schema}"."schema_migrations"',),
    ).fetchone()["relation"]
    assert ledger is None

    conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
    conn.commit()
    report = schema_contract.migrate(conn, schema)
    assert report["applied_versions"] == [1]
    conn.close()


def test_deployment_migrations_serialize_across_connections(runner_env):
    _runner, schema = runner_env
    barrier = threading.Barrier(2)
    reports: list[dict] = []
    errors: list[Exception] = []

    def migrate_once():
        conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
        try:
            barrier.wait(timeout=5)
            reports.append(schema_contract.migrate(conn, schema))
        except Exception as exc:  # pragma: no cover - surfaced through errors
            errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=migrate_once) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors, errors
    assert len(reports) == 2
    assert sorted(len(report["applied_versions"]) for report in reports) == [0, 1]
    assert all(report["compatibility"]["compatible"] for report in reports)


def test_runtime_contract_rejects_future_schema_without_running_ddl(runner_env):
    runner, schema = runner_env
    assert runner.invoke(cli, ["migrate"]).exit_code == 0
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    migration = schema_contract.load_migrations()[0]
    conn.execute(
        f'INSERT INTO "{schema}"."schema_migrations" '
        "(domain, version, name, checksum) VALUES (%s, %s, %s, %s)",
        (schema_contract.DOMAIN, 2, "002_future.sql", migration.checksum),
    )
    conn.commit()
    conn.close()

    result = runner.invoke(cli, ["ls"])

    assert result.exit_code != 0
    assert "too-new" in result.output


def test_valid_ledger_with_damaged_queue_shape_fails_runtime_and_restart(runner_env):
    runner, schema = runner_env
    assert runner.invoke(cli, ["migrate"]).exit_code == 0
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    conn.execute(f'DROP INDEX "{schema}".idx_actions_project')
    conn.commit()
    conn.close()

    check = runner.invoke(cli, ["check-compatibility"])

    assert check.exit_code == 3
    compatibility = json.loads(check.output)
    assert compatibility["state"] == "shape-mismatch"
    assert "index-missing-or-invalid:actions.project" in compatibility["detail"]
    with pytest.raises(schema_contract.SchemaCompatibilityError, match="shape-mismatch"):
        server._require_runtime_compatibility()


def test_service_restart_repeats_compatibility_without_migration(runner_env):
    runner, schema = runner_env
    assert runner.invoke(cli, ["migrate"]).exit_code == 0

    first_start = server._require_runtime_compatibility()
    second_start = server._require_runtime_compatibility()

    assert first_start["state"] == "compatible"
    assert second_start == first_start


def test_runtime_role_can_check_compatibility_but_cannot_run_ddl():
    schema = "aqroles_" + uuid.uuid4().hex
    migration_conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    runtime_conn = db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"])
    runtime_role = runtime_conn.execute("SELECT current_user AS role").fetchone()["role"]
    migration_role = migration_conn.execute("SELECT current_user AS role").fetchone()["role"]
    assert migration_role == MIGRATION_ROLE
    assert runtime_role == RUNTIME_ROLE
    assert migration_role != runtime_role
    try:
        schema_contract.migrate(migration_conn, schema)
        migration_conn.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(runtime_role)
            )
        )
        migration_conn.execute(
            sql.SQL("GRANT SELECT ON {}.{} TO {}").format(
                sql.Identifier(schema),
                sql.Identifier("schema_migrations"),
                sql.Identifier(runtime_role),
            )
        )
        migration_conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(runtime_role)
            )
        )
        migration_conn.execute(
            sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(runtime_role)
            )
        )
        migration_conn.commit()

        assert schema_contract.require_compatible(runtime_conn, schema).compatible
        with pytest.raises(Exception) as excinfo:
            runtime_conn.execute(
                sql.SQL("CREATE TABLE {}.forbidden_runtime_ddl (id INTEGER)").format(
                    sql.Identifier(schema)
                )
            )
        assert getattr(excinfo.value, "sqlstate", None) == "42501"
        runtime_conn.rollback()
    finally:
        runtime_conn.close()
        migration_conn.close()
