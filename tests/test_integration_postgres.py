import hashlib
import json
import os
import socket
import threading
import uuid
from http.server import HTTPServer
from pathlib import Path

import pytest

from psycopg import errors as pg_errors, sql

from actionq.cli import cli
from actionq import db, schema as schema_contract, server
from actionq.action_resource import ActionResourceOwner, CursorExpired, IdempotencyConflict
import actionq.action_resource as action_resource_module
from actionq.application import ActionQApplication, AuthenticatedActionResourcePrincipal, InvocationProvenance

MIGRATION_ROLE = "actionq_migration"
RUNTIME_ROLE = "actionq_runtime"
LEGACY_V1_PATH = Path(__file__).parent / "fixtures" / "actionq_legacy_v1.sql"
LEGACY_V1_SHA256 = "ae2b2166dc841b06793de6a6f706a01056725cd674aa018bb98aba36eab47de0"


@pytest.fixture
def runner_env(monkeypatch, actionq_cli_runner):
    schema = "aqtest_" + uuid.uuid4().hex
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    return actionq_cli_runner, schema


def _invoke_json(runner, args, *, input=None):
    result = runner.invoke(cli, args, input=input)
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def _install_legacy_v1(conn, schema: str) -> None:
    raw = LEGACY_V1_PATH.read_text(encoding="utf-8")
    assert hashlib.sha256(raw.encode()).hexdigest() == LEGACY_V1_SHA256
    rendered = raw.replace("{{schema}}", f'"{schema}"')
    assert "{{schema}}" not in rendered
    conn.execute(f'CREATE SCHEMA "{schema}"')
    for statement in schema_contract._statements(rendered):
        conn.execute(statement)
    conn.commit()


def _run_action_resource_owner_history(owner_history):
    """Disposable-PG falsifying history for the frozen owner contract."""
    resource_schema = "aqresource_" + uuid.uuid4().hex
    migration_conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    schema_contract.migrate(migration_conn, resource_schema)
    migration_conn.close()

    def connect():
        return db.connect(os.environ["ACTIONQ_TEST_URL"])

    def connect_runtime():
        return db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"])

    secret = b"owner-test-cursor-secret".ljust(32, b"!")
    owner = ActionResourceOwner(schema=resource_schema, connection=connect, cursor_secret=secret)
    application = ActionQApplication(schema=resource_schema, connection_factory=connect_runtime, resource_cursor_secret=secret, authorizer=lambda *_: True)
    boundary_principal = AuthenticatedActionResourcePrincipal.derive(InvocationProvenance(actor="principal-boundary", environment="test", request_id="r", catalog_revision="c", idempotency_key="boundary-key"))

    def create_action(conn):
        return conn.execute(
            f'INSERT INTO "{resource_schema}".actions '
            "(action_type, project, created_by) VALUES ('scope-iterate','actionq','test') RETURNING id"
        ).fetchone()

    canary_request = {"repo": "actionq", "secret": "canary-secret", "result_ref": "canary-result", "failure_detail": "canary-failure", "claim": "canary-claim", "provenance": "canary-provenance", "request_snapshot": "canary-request"}
    first = owner.enqueue(principal_scope="principal-a", operation="enqueue", idempotency_key="key-a",
                          request=canary_request, create_action=create_action)
    retry = owner.enqueue(principal_scope="principal-a", operation="enqueue", idempotency_key="key-a",
                          request=dict(reversed(list(canary_request.items()))), create_action=create_action)
    assert retry.original_retry and (retry.resource_ref, retry.revision) == (first.resource_ref, first.revision)
    with pytest.raises(IdempotencyConflict):
        owner.enqueue(principal_scope="principal-a", operation="enqueue", idempotency_key="key-a",
                      request={"repo": "different"}, create_action=create_action)

    boundary_first = application.enqueue_action_resource(
        principal=boundary_principal, operation="execution.action.enqueue",
        idempotency_key="boundary-key", request={"repo": "actionq"},
        action_type="scope-iterate", project="actionq", target_ref=None,
        source_refs=[], priority=100, parent_id=None, created_by="boundary-test",
    )
    boundary_retry = application.enqueue_action_resource(
        principal=boundary_principal, operation="execution.action.enqueue",
        idempotency_key="boundary-key", request={"repo": "actionq"},
        action_type="scope-iterate", project="actionq", target_ref=None,
        source_refs=[], priority=100, parent_id=None, created_by="boundary-test",
    )
    assert boundary_retry == boundary_first
    assert json.loads(application.serialize_action_resource_envelope(boundary_first)) == boundary_first
    if owner_history == "response-loss":
        committed = {}
        def commit_then_lose_response():
            committed["value"] = application.enqueue_action_resource(
                principal=boundary_principal, operation="execution.action.enqueue",
                idempotency_key="lost-response-key", request={"repo": "actionq", "fault": "response-loss"},
                action_type="scope-iterate", project="actionq", target_ref=None,
                source_refs=[], priority=100, parent_id=None, created_by="boundary-test",
            )
            raise ConnectionError("post-commit pre-response fault")
        with pytest.raises(ConnectionError, match="post-commit pre-response"):
            commit_then_lose_response()
        recovered = application.enqueue_action_resource(
            principal=boundary_principal, operation="execution.action.enqueue",
            idempotency_key="lost-response-key", request={"fault": "response-loss", "repo": "actionq"},
            action_type="scope-iterate", project="actionq", target_ref=None,
            source_refs=[], priority=100, parent_id=None, created_by="boundary-test",
        )
        assert recovered == committed["value"]

    initial = owner.snapshot(resource_ref=first.resource_ref, principal_scope="principal-a")
    assert initial["revision"] == 1 and initial["recovery_floor"] == 0
    assert not any(value in json.dumps(initial) for key, value in canary_request.items() if key != "repo")
    if owner_history == "redaction":
        with connect() as conn:
            stored = dict(conn.execute(f'SELECT * FROM "{resource_schema}".action_resources WHERE resource_ref=%s', (first.resource_ref,)).fetchone())
            for field, value in canary_request.items():
                if field == "repo":
                    continue
                with pytest.raises(db.ActionQError, match="unknown action resource owner field"):
                    action_resource_module._projection({**stored, f"unknown_{field}": value}, [])
        with pytest.raises(db.ActionQError, match="unknown action resource event field"):
            action_resource_module._event({"revision": 9, "kind": "state_changed", "state": "failed", "occurred_at": stored["updated_at"], "unknown": "canary-unknown"})
    action_id = None
    with connect() as conn:
        action_id = conn.execute(f'SELECT action_id FROM "{resource_schema}".action_resources WHERE resource_ref=%s', (first.resource_ref,)).fetchone()["action_id"]
    if owner_history == "snapshot-race":
        projection_read = threading.Event()
        terminal_committed = threading.Event()
        original_projection = action_resource_module._projection
        captured = {}
        def blocked_projection(row, sessions):
            projection_read.set()
            assert terminal_committed.wait(5)
            return original_projection(row, sessions)
        action_resource_module._projection = blocked_projection
        thread = threading.Thread(target=lambda: captured.setdefault("snapshot", owner.snapshot(resource_ref=first.resource_ref, principal_scope="principal-a")))
        try:
            thread.start()
            assert projection_read.wait(5)
            with connect() as conn, conn.transaction():
                conn.execute(f'UPDATE "{resource_schema}".actions SET status=\'claimed\', claim_receipt=\'race-receipt\' WHERE id=%s', (action_id,))
                owner.project_state(conn, action_id=action_id, expected_claim_receipt="race-receipt")
                conn.execute(f'UPDATE "{resource_schema}".actions SET status=\'completed\', completed_at=now() WHERE id=%s', (action_id,))
                owner.project_state(conn, action_id=action_id)
            terminal_committed.set()
            thread.join(5)
            assert not thread.is_alive()
        finally:
            terminal_committed.set()
            action_resource_module._projection = original_projection
        raced = captured["snapshot"]
        assert raced["revision"] == 1 and raced["projection"]["terminal"] is False
        raced_changes = owner.changes(resource_ref=first.resource_ref, principal_scope="principal-a", cursor=raced["cursor"])
        assert [event["revision"] for event in raced_changes["changes"]] == [2, 3]
        assert raced_changes["changes"][-1]["terminal"] is True
    with connect() as conn, conn.transaction():
        conn.execute(f'UPDATE "{resource_schema}".actions SET status=\'claimed\', claim_receipt=\'receipt-a\' WHERE id=%s', (action_id,))
        with pytest.raises(db.ClaimRejected):
            application.project_action_resource(conn, action_id=action_id, expected_claim_receipt="stale-receipt")
        assert application.project_action_resource(conn, action_id=action_id, expected_claim_receipt="receipt-a") >= 2
        if owner_history == "fencing":
            conn.execute(f'UPDATE "{resource_schema}".actions SET claim_receipt=\'receipt-b\' WHERE id=%s', (action_id,))
            with pytest.raises(db.ClaimRejected):
                application.project_action_resource(conn, action_id=action_id, expected_claim_receipt="receipt-a")
            application.project_action_resource(conn, action_id=action_id, expected_claim_receipt="receipt-b")
        conn.execute(f'UPDATE "{resource_schema}".actions SET status=\'completed\', completed_at=now() WHERE id=%s', (action_id,))
        assert application.project_action_resource(conn, action_id=action_id) >= 3
    owner_principal = AuthenticatedActionResourcePrincipal.derive(InvocationProvenance(actor="principal-a", environment="test", request_id="r2", catalog_revision="c", idempotency_key="k"))
    # The direct owner fixture uses its literal principal scope; retain that
    # low-level proof while application-boundary roots use derived scopes.
    changes = owner.changes(resource_ref=first.resource_ref, principal_scope="principal-a", cursor=initial["cursor"])
    assert [event["revision"] for event in changes["changes"]] == list(range(2, changes["changes"][-1]["revision"] + 1))
    assert changes["changes"][-1]["terminal"] is True
    if owner_history == "fencing":
        restarted_fencing = ActionQApplication(schema=resource_schema, connection_factory=connect_runtime, resource_cursor_secret=secret, authorizer=lambda *_: True)
        with connect() as conn, conn.transaction(), pytest.raises(db.ClaimRejected):
            restarted_fencing.project_action_resource(conn, action_id=action_id, expected_claim_receipt="receipt-a")
        claim_a_provenance = InvocationProvenance(actor="runner:a", environment="test", request_id="claim-a", catalog_revision="c", idempotency_key="claim-a")
        claim_a = application.claim(worker="runner:a", timeout_minutes=1, provenance=claim_a_provenance)["result"]
        with connect() as conn, conn.transaction():
            conn.execute(f'UPDATE "{resource_schema}".actions SET claim_deadline=now()-interval \'1 minute\' WHERE id=%s', (claim_a["id"],))
        application.sweep()
        restarted_lifecycle = ActionQApplication(schema=resource_schema, connection_factory=connect_runtime, resource_cursor_secret=secret, authorizer=lambda *_: True)
        claim_b_provenance = InvocationProvenance(actor="runner:b", environment="test", request_id="claim-b", catalog_revision="c", idempotency_key="claim-b")
        claim_b = restarted_lifecycle.claim(worker="runner:b", timeout_minutes=1, provenance=claim_b_provenance)["result"]
        assert claim_b["id"] == claim_a["id"] and claim_b["claim_receipt"] != claim_a["claim_receipt"]
        stale_decision = restarted_lifecycle.complete(action_id=claim_a["id"], result_ref="result:stale", actor="runner:a", claim_receipt=claim_a["claim_receipt"], provenance=InvocationProvenance(actor="runner:a", environment="test", request_id="stale", catalog_revision="c", idempotency_key="stale"))
        assert stale_decision["decision"]["status"] == "rejected"
        restarted_lifecycle.complete(action_id=claim_b["id"], result_ref="result:ok", actor="runner:b", claim_receipt=claim_b["claim_receipt"], provenance=InvocationProvenance(actor="runner:b", environment="test", request_id="complete", catalog_revision="c", idempotency_key="complete"))
        terminal_boundary = restarted_lifecycle.action_resource_snapshot(resource_ref=boundary_first["resource_ref"], principal=boundary_principal)
        assert terminal_boundary["projection"]["state"] == "completed"
        for outcome in ("fail", "reject"):
            root = restarted_lifecycle.enqueue_action_resource(principal=boundary_principal, operation="execution.action.enqueue", idempotency_key=f"{outcome}-root", request={"outcome": outcome}, action_type="scope-iterate", project="actionq", target_ref=None, source_refs=[], priority=100, parent_id=None, created_by="boundary-test")
            actor = f"runner:{outcome}"
            claimed = restarted_lifecycle.claim(worker=actor, timeout_minutes=1, provenance=InvocationProvenance(actor=actor, environment="test", request_id=f"claim-{outcome}", catalog_revision="c", idempotency_key=f"claim-{outcome}"))["result"]
            terminal_provenance = InvocationProvenance(actor=actor, environment="test", request_id=outcome, catalog_revision="c", idempotency_key=outcome)
            if outcome == "fail":
                restarted_lifecycle.fail(action_id=claimed["id"], reason="bounded", actor=actor, claim_receipt=claimed["claim_receipt"], provenance=terminal_provenance)
            else:
                restarted_lifecycle.reject(action_id=claimed["id"], reason="bounded", validator="test", actor=actor, claim_receipt=claimed["claim_receipt"], provenance=terminal_provenance)
            assert restarted_lifecycle.action_resource_snapshot(resource_ref=root["resource_ref"], principal=boundary_principal)["projection"]["state"] == ("failed" if outcome == "fail" else "rejected")

    terminal_before_pruning = application.serialize_action_resource_envelope(owner.snapshot(resource_ref=first.resource_ref, principal_scope="principal-a")["projection"])
    if owner_history == "pruning":
        assert application.serialize_action_resource_envelope(owner.snapshot(resource_ref=first.resource_ref, principal_scope="principal-a")["projection"]) == terminal_before_pruning
        assert owner.prune(resource_ref=first.resource_ref, through_revision=1) == 1
        assert owner.snapshot(resource_ref=first.resource_ref, principal_scope="principal-a")["recovery_floor"] == 1
        assert application.serialize_action_resource_envelope(owner.snapshot(resource_ref=first.resource_ref, principal_scope="principal-a")["projection"]) == terminal_before_pruning
        owner.prune(resource_ref=first.resource_ref, through_revision=2)
    owner.prune(resource_ref=first.resource_ref, through_revision=999)
    restarted = ActionResourceOwner(schema=resource_schema, connection=connect, cursor_secret=secret)
    terminal = restarted.snapshot(resource_ref=first.resource_ref, principal_scope="principal-a")
    assert terminal["recovery_floor"] == terminal["revision"] and terminal["projection"]["outcome"] == "completed"
    with pytest.raises(CursorExpired) as expired:
        restarted.changes(resource_ref=first.resource_ref, principal_scope="principal-a", cursor=initial["cursor"])
    assert expired.value.recovery_floor == terminal["revision"]
    assert restarted.changes(resource_ref=first.resource_ref, principal_scope="principal-a", cursor=terminal["cursor"])["changes"] == []
    terminal_started = __import__("time").monotonic()
    assert restarted.changes(resource_ref=first.resource_ref, principal_scope="principal-a", cursor=terminal["cursor"], wait_seconds=30)["changes"] == []
    assert __import__("time").monotonic() - terminal_started < 1
    assert application.serialize_action_resource_envelope(terminal["projection"]) == terminal_before_pruning
    assert application.serialize_action_resource_envelope(terminal).endswith(b"\n")
    if owner_history == "non-disclosure":
        foreign = AuthenticatedActionResourcePrincipal.derive(InvocationProvenance(actor="foreign", environment="test", request_id="f", catalog_revision="c", idempotency_key="f"))
        cases = [
            application.action_resource_snapshot_response(resource_ref="aqr1_bad", principal=boundary_principal),
            application.action_resource_snapshot_response(resource_ref="aqr1_" + "A" * 43, principal=boundary_principal),
            application.action_resource_snapshot_response(resource_ref=boundary_first["resource_ref"], principal=foreign),
            ActionQApplication(schema=resource_schema, connection_factory=connect_runtime, resource_cursor_secret=secret, authorizer=lambda *_: False).action_resource_snapshot_response(resource_ref=boundary_first["resource_ref"], principal=boundary_principal),
        ]
        assert len(set(cases)) == 1
        assert cases[0] == (404, (("content-type", "application/json"), ("content-length", "112"), ("cache-control", "no-store")), b'{"error":{"code":"resource_not_found","message":"resource not found"},"schema_version":"resource-reference/v1"}\n')
    if owner_history == "bounded-wait":
        pending = application.action_resource_snapshot(resource_ref=boundary_first["resource_ref"], principal=boundary_principal)
        assert application.action_resource_changes(resource_ref=boundary_first["resource_ref"], principal=boundary_principal, cursor=pending["cursor"], wait_seconds=0)["changes"] == []
        clock = [0.0]
        original_monotonic, original_sleep = action_resource_module.time.monotonic, action_resource_module.time.sleep
        action_resource_module.time.monotonic = lambda: clock[0]
        action_resource_module.time.sleep = lambda duration: clock.__setitem__(0, clock[0] + duration)
        try:
            empty = application.action_resource_changes(resource_ref=boundary_first["resource_ref"], principal=boundary_principal, cursor=pending["cursor"], wait_seconds=30)
        finally:
            action_resource_module.time.monotonic, action_resource_module.time.sleep = original_monotonic, original_sleep
        assert empty["changes"] == [] and 30 <= clock[0] <= 31
        early = {}
        waiter = threading.Thread(target=lambda: early.setdefault("value", application.action_resource_changes(resource_ref=boundary_first["resource_ref"], principal=boundary_principal, cursor=pending["cursor"], wait_seconds=30)))
        waiter.start()
        __import__("time").sleep(.2)
        with connect() as conn, conn.transaction():
            boundary_action_id = conn.execute(f'SELECT action_id FROM "{resource_schema}".action_resources WHERE resource_ref=%s', (boundary_first["resource_ref"],)).fetchone()["action_id"]
            conn.execute(f'UPDATE "{resource_schema}".actions SET status=\'claimed\', claim_receipt=\'boundary-receipt\' WHERE id=%s', (boundary_action_id,))
            owner.project_state(conn, action_id=boundary_action_id, expected_claim_receipt="boundary-receipt")
        waiter.join(5)
        assert not waiter.is_alive() and early["value"]["changes"][0]["revision"] == 2
        cancel = threading.Event()
        disconnected_result = {}
        def cancelled_wait():
            try:
                application.action_resource_changes(resource_ref=boundary_first["resource_ref"], principal=boundary_principal, cursor=early["value"]["cursor"], wait_seconds=30, cancel_event=cancel)
            except ConnectionError as exc:
                disconnected_result["error"] = str(exc)
        observer = threading.Thread(target=cancelled_wait)
        observer.start()
        __import__("time").sleep(.2)
        cancel.set()
        observer.join(2)
        assert disconnected_result == {"error": "observer disconnected"}
        restarted_application = ActionQApplication(schema=resource_schema, connection_factory=connect_runtime, resource_cursor_secret=secret, authorizer=lambda *_: True)
        assert restarted_application.action_resource_snapshot(resource_ref=boundary_first["resource_ref"], principal=boundary_principal)["revision"] == 2
    if owner_history == "legacy-quarantine":
        fixture = json.loads((Path(__file__).parents[1] / "verification/fixtures/action-resource-owner-v1/legacy-quarantine.json").read_text())
        counters = {"body_read": 0, "auth": 0, "application": 0, "db": 0}
        original_auth, original_app, original_connect = server._served_authenticator, server.ActionQApplication, server._db.connect
        server._served_authenticator = lambda _headers: counters.__setitem__("auth", counters["auth"] + 1)
        server.ActionQApplication = lambda **_kwargs: counters.__setitem__("application", counters["application"] + 1)
        server._db.connect = lambda *_a, **_k: counters.__setitem__("db", counters["db"] + 1)
        class BodyReadSpy:
            def __init__(self, wrapped): self.wrapped = wrapped
            def read(self, *args, **kwargs): counters["body_read"] += 1; return self.wrapped.read(*args, **kwargs)
            def __getattr__(self, name): return getattr(self.wrapped, name)
        class HistoryHandler(server._Handler):
            def setup(self):
                super().setup(); self.rfile = BodyReadSpy(self.rfile)
            def log_message(self, *_args): pass
        httpd = HTTPServer(("127.0.0.1", 0), HistoryHandler)
        worker = threading.Thread(target=httpd.serve_forever, daemon=True)
        worker.start()
        exercised = 0
        try:
            for method in fixture["methods"]:
                for case in fixture["path_cases"]:
                    payload = b"{invalid-json"
                    raw = f"{method} {case['raw_target']} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode() + payload
                    with socket.create_connection(httpd.server_address, timeout=2) as connection:
                        connection.sendall(raw)
                        response = b""
                        while chunk := connection.recv(65536):
                            response += chunk
                    if case["quarantined"]:
                        assert b" 404 Not Found\r\n" in response and response.endswith(server.V2_DISPATCH_QUARANTINE_BYTES)
                    exercised += 1
        finally:
            httpd.shutdown(); worker.join(2); httpd.server_close()
            server._served_authenticator, server.ActionQApplication, server._db.connect = original_auth, original_app, original_connect
        assert exercised == fixture["cross_product_count"] == 64
        assert counters == {"body_read": 0, "auth": 0, "application": 0, "db": 0}
        assert server.V2_DISPATCH_QUARANTINE_BYTES.decode() == fixture["response"]["body_utf8"]

    cleanup = connect()
    cleanup.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(resource_schema)))
    cleanup.commit()
    cleanup.close()
    assertion_id = f"action-resource-owner.{owner_history}.v1"
    print(f"OWNER_ASSERTION {assertion_id}")
    return {assertion_id}


def test_action_resource_history_pruning_none_some_all_restart():
    assert _run_action_resource_owner_history("pruning") == {"action-resource-owner.pruning.v1"}


def test_action_resource_history_threaded_concurrent_terminal_handoff():
    assert _run_action_resource_owner_history("snapshot-race") == {"action-resource-owner.snapshot-race.v1"}


def test_action_resource_history_four_case_application_not_found_bytes():
    assert _run_action_resource_owner_history("non-disclosure") == {"action-resource-owner.non-disclosure.v1"}


def test_action_resource_history_recursive_six_class_redaction_canaries():
    assert _run_action_resource_owner_history("redaction") == {"action-resource-owner.redaction.v1"}


def test_action_resource_history_commit_response_loss_original_retry():
    assert _run_action_resource_owner_history("response-loss") == {"action-resource-owner.response-loss.v1"}


def test_action_resource_history_wait_zero_thirty_early_spurious_disconnect_restart():
    assert _run_action_resource_owner_history("bounded-wait") == {"action-resource-owner.bounded-wait.v1"}


def test_action_resource_history_legacy_raw_64_case_preaccess_quarantine():
    assert _run_action_resource_owner_history("legacy-quarantine") == {"action-resource-owner.legacy-quarantine.v1"}


def test_action_resource_history_two_incarnation_stale_session_fencing():
    assert _run_action_resource_owner_history("fencing") == {"action-resource-owner.fencing.v1"}


def _install_exact_v3(schema: str) -> None:
    """Install the released 1..3 ledger and its least-privilege runtime grants."""
    conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(
        f'''CREATE TABLE "{schema}".schema_migrations (
            domain TEXT NOT NULL, version INTEGER NOT NULL CHECK (version > 0),
            name TEXT NOT NULL, checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (domain, version))'''
    )
    for migration in schema_contract.load_migrations()[:3]:
        for statement in schema_contract._statements(
            schema_contract._render(migration, schema)
        ):
            conn.execute(statement)
        conn.execute(
            f'INSERT INTO "{schema}".schema_migrations '
            "(domain, version, name, checksum) VALUES (%s, %s, %s, %s)",
            (schema_contract.DOMAIN, migration.version, migration.name, migration.checksum),
        )
    conn.execute(f'REVOKE CREATE ON SCHEMA "{schema}" FROM PUBLIC')
    conn.execute(f'REVOKE CREATE ON SCHEMA "{schema}" FROM {RUNTIME_ROLE}')
    conn.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO {RUNTIME_ROLE}')
    conn.execute(
        f'GRANT SELECT, INSERT, UPDATE, DELETE ON "{schema}".actions, '
        f'"{schema}".events TO {RUNTIME_ROLE}'
    )
    conn.execute(
        f'GRANT SELECT, INSERT ON "{schema}".dispatch_requests TO {RUNTIME_ROLE}'
    )
    conn.execute(
        f'GRANT SELECT ON "{schema}".schema_migrations TO {RUNTIME_ROLE}'
    )
    conn.execute(
        f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "{schema}" TO {RUNTIME_ROLE}'
    )
    conn.commit()
    conn.close()


def _install_exact_v7(conn, schema: str) -> None:
    """Install the released v1..v7 ledger without running v8."""

    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(
        f'''CREATE TABLE "{schema}".schema_migrations (
            domain TEXT NOT NULL, version INTEGER NOT NULL CHECK (version > 0),
            name TEXT NOT NULL, checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (domain, version))'''
    )
    for migration in schema_contract.load_migrations()[:7]:
        for statement in schema_contract._statements(
            schema_contract._render(migration, schema)
        ):
            conn.execute(statement)
        conn.execute(
            f'INSERT INTO "{schema}".schema_migrations '
            "(domain, version, name, checksum) VALUES (%s, %s, %s, %s)",
            (schema_contract.DOMAIN, migration.version, migration.name, migration.checksum),
        )
    conn.commit()


def test_exact_v3_bridge_lifecycle_then_exact_migration_to_current(
    runner_env, signed_runner_proof
):
    runner, schema = runner_env
    _install_exact_v3(schema)

    compatibility = _invoke_json(runner, ["check-compatibility"])
    assert compatibility["state"] == "compatible"
    assert compatibility["observed_schema_version"] == 3
    assert "pre-migration bridge" in compatibility["detail"]

    action = _invoke_json(
        runner,
        ["add", "--type", "scope-iterate", "--created-by", "human:test"],
    )
    claimed = _invoke_json(
        runner,
        ["claim", "--proof-stdin"],
        input=json.dumps(
            signed_runner_proof("worker:test", "execution.action.claim", "queue:next")
        ),
    )
    assert claimed["id"] == action["id"]
    renewed = _invoke_json(
        runner,
        ["renew", str(action["id"]), "--timeout", "45", "--proof-stdin"],
        input=json.dumps(
            {
                "claim_receipt": claimed["claim_receipt"],
                "runner_proof": signed_runner_proof(
                    "worker:test", "execution.action.renew", f"action:{action['id']}"
                ),
            }
        ),
    )
    assert renewed["status"] == "claimed"
    completed = _invoke_json(
        runner,
        ["complete", str(action["id"]), "--result", "bridge:v3", "--proof-stdin"],
        input=json.dumps(
            {
                "claim_receipt": claimed["claim_receipt"],
                "runner_proof": signed_runner_proof(
                    "worker:test", "execution.action.complete", f"action:{action['id']}"
                ),
            }
        ),
    )
    assert completed["status"] == "completed"

    for command, expected in (("fail", "failed"), ("reject", "rejected")):
        candidate = _invoke_json(
            runner,
            ["add", "--type", f"bridge-{command}", "--created-by", "human:test"],
        )
        candidate_claim = _invoke_json(
            runner,
            ["claim", "--proof-stdin"],
            input=json.dumps(
                signed_runner_proof("worker:test", "execution.action.claim", "queue:next")
            ),
        )
        assert candidate_claim["id"] == candidate["id"]
        args = [command, str(candidate["id"]), "--reason", f"bridge:{command}"]
        if command == "reject":
            args.extend(["--validator", "validator:test"])
        args.append("--proof-stdin")
        terminal = _invoke_json(
            runner,
            args,
            input=json.dumps(
                {
                    "claim_receipt": candidate_claim["claim_receipt"],
                    "runner_proof": signed_runner_proof(
                        "worker:test",
                        f"execution.action.{command}",
                        f"action:{candidate['id']}",
                    ),
                }
            ),
        )
        assert terminal["status"] == expected

    pending_cancel = _invoke_json(
        runner,
        ["add", "--type", "bridge-cancel", "--created-by", "human:test"],
    )
    cancelled = _invoke_json(
        runner,
        ["cancel", str(pending_cancel["id"]), "--reason", "bridge:cancel"],
    )
    assert cancelled["status"] == "cancelled"

    retry = _invoke_json(
        runner,
        ["add", "--type", "bridge-retry", "--created-by", "human:test"],
    )
    first_claim = _invoke_json(
        runner,
        ["claim", "--proof-stdin"],
        input=json.dumps(
            signed_runner_proof("worker:test", "execution.action.claim", "queue:next")
        ),
    )
    assert first_claim["id"] == retry["id"]
    migration_conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    migration_conn.execute(
        f'UPDATE "{schema}".actions SET claim_deadline=now() - interval \'1 second\' '
        "WHERE id=%s",
        (retry["id"],),
    )
    migration_conn.commit()
    migration_conn.close()
    swept = _invoke_json(runner, ["sweep"])
    assert [row["id"] for row in swept["reclaimed"]] == [retry["id"]]
    assert swept["cancelled"] == []
    replacement = _invoke_json(
        runner,
        ["claim", "--proof-stdin"],
        input=json.dumps(
            signed_runner_proof("worker:test", "execution.action.claim", "queue:next")
        ),
    )
    assert replacement["id"] == retry["id"]
    assert replacement["claim_receipt"] != first_claim["claim_receipt"]

    migration = runner.invoke(cli, ["migrate", "--json-output"])
    assert migration.exit_code == 0, migration.output
    assert json.loads(migration.output)["applied_versions"] == [4, 5, 6, 7, 8, 9]
    final = _invoke_json(runner, ["check-compatibility"])
    assert final["observed_schema_version"] == 9
    assert final["state"] == "compatible"


def test_schema8_rendered_guard_executes_against_postgres(runner_env):
    _runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    _install_exact_v7(conn, schema)

    report = schema_contract.migrate(conn, schema, runtime_role=RUNTIME_ROLE)

    assert report["applied_versions"] == [8, 9]
    assert conn.execute(
        f'SELECT MAX(version) AS version FROM "{schema}".schema_migrations WHERE domain=%s',
        (schema_contract.DOMAIN,),
    ).fetchone()["version"] == 9
    conn.close()


def test_schema8_root_gate_lock_blocks_concurrent_dispatch_request_insert(runner_env):
    _runner, schema = runner_env
    migrator = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    _install_exact_v7(migrator, schema)
    action_id = migrator.execute(
        f'INSERT INTO "{schema}".actions (action_type, created_by) VALUES (%s, %s) RETURNING id',
        ("scope-iterate", "test:migrator"),
    ).fetchone()["id"]
    migrator.commit()
    writer = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    writer_errors: list[Exception] = []

    def concurrent_insert() -> None:
        try:
            writer.execute("SET lock_timeout = '250ms'")
            writer.execute(
                f'''INSERT INTO "{schema}".dispatch_requests
                    (action_id, request_ref, normalized_snapshot, schema_version,
                     canonicalization_version, request_sha256, identity,
                     environment, operation, idempotency_key)
                    VALUES (%s, %s, %s, 'v2', 'test', %s, 'test:writer',
                            'test', 'execution.dispatch.enqueue', 'test-key')''',
                (action_id, "req:" + uuid.uuid4().hex, b"{}", "a" * 64),
            )
            writer.commit()
        except Exception as exc:  # asserted after the lock-holder observes it
            writer_errors.append(exc)
            writer.rollback()

    with migrator.transaction():
        schema_contract._lock_schema8_dispatch_roots(migrator, schema)
        thread = threading.Thread(target=concurrent_insert)
        thread.start()
        schema_contract._require_schema8_dispatch_root_quiescence(migrator, schema)
        migration = schema_contract.load_migrations()[7]
        for statement in schema_contract._statements(
            schema_contract._render(migration, schema)
        ):
            migrator.execute(statement)
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert len(writer_errors) == 1
        assert isinstance(writer_errors[0], pg_errors.LockNotAvailable)

    assert migrator.execute(
        f'SELECT COUNT(*) AS count FROM "{schema}".dispatch_requests'
    ).fetchone()["count"] == 0
    writer.close()
    migrator.close()


def test_v3_bridge_fails_closed_on_partial_checksum_and_post_v3_shape(runner_env):
    runner, schema = runner_env
    _install_exact_v3(schema)
    migration_conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])

    migration_conn.execute(
        f'UPDATE "{schema}".schema_migrations SET checksum=%s '
        "WHERE domain=%s AND version=3",
        ("0" * 64, schema_contract.DOMAIN),
    )
    migration_conn.commit()
    mismatch = runner.invoke(cli, ["check-compatibility"])
    assert mismatch.exit_code == 3
    assert json.loads(mismatch.output)["state"] == "checksum-mismatch"

    migration_conn.execute(
        f'UPDATE "{schema}".schema_migrations SET checksum=%s '
        "WHERE domain=%s AND version=3",
        (schema_contract.load_migrations()[2].checksum, schema_contract.DOMAIN),
    )
    migration_conn.execute(
        f'ALTER TABLE "{schema}".actions ADD COLUMN cancel_request_id UUID'
    )
    migration_conn.commit()
    drift = runner.invoke(cli, ["check-compatibility"])
    assert drift.exit_code == 3
    record = json.loads(drift.output)
    assert record["state"] == "shape-mismatch"
    assert "post-v3-object-present:actions.cancel_request_id" in record["detail"]
    migration_conn.close()


def test_v3_bridge_rejects_incomplete_prefix(runner_env):
    runner, schema = runner_env
    _install_exact_v3(schema)
    conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    conn.execute(
        f'DELETE FROM "{schema}".schema_migrations WHERE domain=%s AND version=3',
        (schema_contract.DOMAIN,),
    )
    conn.commit()
    conn.close()

    result = runner.invoke(cli, ["check-compatibility"])
    assert result.exit_code == 3
    assert json.loads(result.output)["state"] == "incomplete"


def test_lifecycle_claim_complete_show(runner_env, signed_runner_proof):
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

    claimed = _invoke_json(runner, ["claim", "--proof-stdin"],
                           input=json.dumps(signed_runner_proof("worker:test", "execution.action.claim", "queue:next")))
    assert claimed["id"] == action["id"]
    assert claimed["status"] == "claimed"

    completed = _invoke_json(
        runner,
        ["complete", str(action["id"]), "--result", "branch=agent/scope-iterate/1", "--proof-stdin"],
        input=json.dumps({"claim_receipt": claimed["claim_receipt"], "runner_proof": signed_runner_proof(
            "worker:test", "execution.action.complete", f"action:{action['id']}"
        )}),
    )
    assert completed["status"] == "completed"

    detail = _invoke_json(runner, ["show", str(action["id"])])
    assert detail["action"]["status"] == "completed"
    assert [event["event_type"] for event in detail["events"]] == [
        "action_enqueued",
        "action_claimed",
        "action_completed",
    ]


def test_claim_exits_nonzero_when_empty(runner_env, signed_runner_proof):
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, ["claim", "--proof-stdin"],
                           input=json.dumps(signed_runner_proof("worker:test", "execution.action.claim", "queue:next")))
    assert result.exit_code == 2
    assert "no pending actions" in result.output


def test_manual_usage_limit_pause_then_resume_drill(runner_env, signed_runner_proof):
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
    claimed = _invoke_json(runner, ["claim", "--proof-stdin"], input=json.dumps(
        signed_runner_proof("actionq-daemon:test", "execution.action.claim", "queue:next")
    ))
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
        ["fail", str(action["id"]), "--reason", "usage-limit-paused: confirmed usage-limit signal matched", "--proof-stdin"],
        input=json.dumps({"claim_receipt": claimed["claim_receipt"], "runner_proof": signed_runner_proof(
            "actionq-daemon:test", "execution.action.fail", f"action:{action['id']}"
        )}),
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
    assert json.loads(first.output)["applied_versions"] == list(
        range(schema_contract.MIN_SCHEMA_VERSION, schema_contract.MAX_SCHEMA_VERSION + 1)
    )

    second = runner.invoke(cli, ["migrate", "--json-output"])
    assert second.exit_code == 0, second.output
    assert json.loads(second.output)["applied_versions"] == []

    compatibility = runner.invoke(cli, ["check-compatibility"])
    assert compatibility.exit_code == 0, compatibility.output
    assert json.loads(compatibility.output)["state"] == "compatible"


def test_deployment_migration_adopts_unversioned_current_schema(runner_env):
    runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
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
    assert report["applied_versions"] == list(
        range(schema_contract.MIN_SCHEMA_VERSION, schema_contract.MAX_SCHEMA_VERSION + 1)
    )
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
    expected_indexes = {
        "idx_actionq_actions_claim_lookup",
        "idx_actionq_actions_parent",
        "idx_actionq_actions_project",
        "idx_actionq_actions_deadline",
        "idx_actionq_events_action",
        "idx_actionq_events_timestamp",
        "idx_actionq_events_type_time",
    }
    if schema_contract.MAX_SCHEMA_VERSION >= 3:
        expected_indexes.update(
            {
                "dispatch_requests_identity_environment_operation_idempotenc_key",
                "dispatch_requests_request_ref_key",
                "idx_dispatch_requests_created",
            }
        )
        if schema_contract.MAX_SCHEMA_VERSION >= 4:
            expected_indexes.add("idx_actions_cancelling_deadline")
            if schema_contract.MAX_SCHEMA_VERSION >= 5:
                expected_indexes.update(
                {
                    "execution_groups_plan_ref_key",
                    "execution_groups_spec_sha256_key",
                    "execution_group_members_action_id_key",
                    "execution_group_members_group_id_ordinal_key",
                    "idx_execution_group_members_group",
                    "idx_execution_group_members_action",
                    }
                )
            if schema_contract.MAX_SCHEMA_VERSION >= 6:
                expected_indexes.update(
                    {
                        "immutable_action_specs_spec_digest_key",
                        "immutable_action_requests_request_ref_key",
                        "immutable_action_requests_request_digest_key",
                        "immutable_action_requests_plan_ref_topology_role_subject_key",
                        "idx_immutable_action_requests_created",
                    }
                )
            if schema_contract.MAX_SCHEMA_VERSION >= 9:
                expected_indexes.update(
                    {
                        "action_resources_action_id_key",
                        "action_resources_principal_scope_operation_idempotency_key_key",
                        "idx_action_resource_changes_lookup",
                    }
                )
    assert before <= expected_indexes
    assert after == expected_indexes


def test_unversioned_legacy_wrong_index_definition_is_not_stamped(runner_env):
    _runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    _install_legacy_v1(conn, schema)
    conn.execute(f'DROP INDEX "{schema}".idx_actionq_actions_project')
    conn.execute(
        f'CREATE INDEX idx_actionq_actions_project ON "{schema}".actions(target_ref)'
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
    conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    _install_legacy_v1(conn, schema)
    conn.execute(
        f'ALTER TABLE "{schema}".actions DROP CONSTRAINT actions_status_check'
    )
    conn.execute(
        f'ALTER TABLE "{schema}".actions ADD CONSTRAINT actions_status_check '
        "CHECK (status IN ('pending', 'claimed', 'completed', 'failed', "
        "'rejected', 'cancelled') OR true)"
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


@pytest.mark.parametrize(
    ("statements", "expected_issue"),
    [
        (
            ("ALTER TABLE {schema}.actions ALTER COLUMN priority SET DEFAULT 50",),
            "column-default:actions.priority",
        ),
        (
            (
                "ALTER TABLE {schema}.events ALTER COLUMN payload "
                "SET DEFAULT '{\"unexpected\": true}'::jsonb",
            ),
            "column-default:events.payload",
        ),
        (
            (
                "ALTER TABLE {schema}.events DROP CONSTRAINT events_action_id_fkey",
                "ALTER TABLE {schema}.events ADD CONSTRAINT events_action_id_fkey "
                "FOREIGN KEY (action_id) REFERENCES {schema}.actions(id) "
                "ON UPDATE CASCADE ON DELETE CASCADE",
            ),
            "constraint-missing-or-invalid:events-action-foreign-key",
        ),
    ],
)
def test_unversioned_legacy_semantic_drift_is_not_stamped(
    runner_env, statements, expected_issue
):
    _runner, schema = runner_env
    conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    _install_legacy_v1(conn, schema)
    quoted_schema = f'"{schema}"'
    for statement in statements:
        conn.execute(statement.replace("{schema}", quoted_schema))
    conn.commit()

    with pytest.raises(schema_contract.SchemaMigrationError, match=expected_issue):
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
    assert report["applied_versions"] == list(
        range(schema_contract.MIN_SCHEMA_VERSION, schema_contract.MAX_SCHEMA_VERSION + 1)
    )
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
    assert sorted(len(report["applied_versions"]) for report in reports) == [0, schema_contract.MAX_SCHEMA_VERSION]
    assert all(report["compatibility"]["compatible"] for report in reports)


def test_runtime_contract_rejects_future_schema_without_running_ddl(runner_env):
    runner, schema = runner_env
    assert runner.invoke(cli, ["migrate"]).exit_code == 0
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    migration = schema_contract.load_migrations()[0]
    conn.execute(
        f'INSERT INTO "{schema}"."schema_migrations" '
        "(domain, version, name, checksum) VALUES (%s, %s, %s, %s)",
        (
            schema_contract.DOMAIN,
            schema_contract.MAX_SCHEMA_VERSION + 1,
            "future.sql",
            migration.checksum,
        ),
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


@pytest.mark.parametrize(
    ("statements", "expected_issue"),
    [
        (
            ("ALTER TABLE {schema}.actions ALTER COLUMN priority SET DEFAULT 50",),
            "column-default:actions.priority",
        ),
        (
            (
                "ALTER TABLE {schema}.events ALTER COLUMN payload "
                "SET DEFAULT '{\"unexpected\": true}'::jsonb",
            ),
            "column-default:events.payload",
        ),
        (
            (
                "ALTER TABLE {schema}.actions ALTER COLUMN status "
                "SET DEFAULT 'PENDING'",
            ),
            "column-default:actions.status",
        ),
        (
            (
                "ALTER TABLE {schema}.actions DROP CONSTRAINT actions_status_check",
                "ALTER TABLE {schema}.actions ADD CONSTRAINT actions_status_check "
                "CHECK (status IN ('pending', 'claimed', 'completed', 'failed', "
                "'rejected', 'cancelled') OR true)",
            ),
            "constraint-invalid:actions.status",
        ),
        (
            (
                "ALTER TABLE {schema}.actions DROP CONSTRAINT actions_status_check",
                "ALTER TABLE {schema}.actions ADD CONSTRAINT actions_status_check "
                "CHECK (status IN ('PENDING', 'CLAIMED', 'COMPLETED', 'FAILED', "
                "'REJECTED', 'CANCELLED'))",
            ),
            "constraint-invalid:actions.status",
        ),
        (
            (
                "DROP INDEX {schema}.idx_actions_claim_lookup",
                "CREATE INDEX idx_actions_claim_lookup ON {schema}.actions"
                "(status, priority, created_at) WHERE status = 'PENDING'",
            ),
            "index-missing-or-invalid:actions.claim-lookup",
        ),
        (
            (
                "DROP INDEX {schema}.idx_events_timestamp",
                "CREATE INDEX idx_events_timestamp ON {schema}.events"
                "(timestamp DESC NULLS LAST)",
            ),
            "index-missing-or-invalid:events.timestamp",
        ),
        (
            (
                "ALTER TABLE {schema}.events DROP CONSTRAINT events_action_id_fkey",
                "ALTER TABLE {schema}.events ADD CONSTRAINT events_action_id_fkey "
                "FOREIGN KEY (action_id) REFERENCES {schema}.actions(id) "
                "ON UPDATE CASCADE ON DELETE CASCADE",
            ),
            "constraint-missing-or-invalid:events-action-foreign-key",
        ),
        (
            (
                "CREATE SCHEMA {shadow}",
                "CREATE TABLE {shadow}.actions (id BIGINT PRIMARY KEY)",
                "ALTER TABLE {schema}.events DROP CONSTRAINT events_action_id_fkey",
                "ALTER TABLE {schema}.events ADD CONSTRAINT events_action_id_fkey "
                "FOREIGN KEY (action_id) REFERENCES {shadow}.actions(id)",
            ),
            "constraint-missing-or-invalid:events-action-foreign-key",
        ),
        (
            (
                "ALTER TABLE {schema}.events DROP CONSTRAINT events_action_id_fkey",
                "ALTER TABLE {schema}.events ADD CONSTRAINT events_action_id_fkey "
                "FOREIGN KEY (action_id) REFERENCES {schema}.actions(id) "
                "DEFERRABLE INITIALLY DEFERRED",
            ),
            "constraint-missing-or-invalid:events-action-foreign-key",
        ),
        (
            (
                "ALTER TABLE {schema}.events DROP CONSTRAINT events_action_id_fkey",
                "ALTER TABLE {schema}.events ADD CONSTRAINT events_action_id_fkey "
                "FOREIGN KEY (action_id) REFERENCES {schema}.actions(id) NOT VALID",
            ),
            "constraint-missing-or-invalid:events-action-foreign-key",
        ),
        (
            (
                "ALTER TABLE {schema}.events DROP CONSTRAINT events_action_id_fkey",
                "ALTER TABLE {schema}.events ADD CONSTRAINT events_action_id_fkey "
                "FOREIGN KEY (action_id) REFERENCES {schema}.actions(id) MATCH FULL",
            ),
            "constraint-missing-or-invalid:events-action-foreign-key",
        ),
    ],
)
def test_runtime_rejects_semantically_permissive_schema_shape(
    runner_env, statements, expected_issue
):
    runner, schema = runner_env
    assert runner.invoke(cli, ["migrate"]).exit_code == 0
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    quoted_schema = f'"{schema}"'
    quoted_shadow = f'"{schema}_shadow"'
    for statement in statements:
        conn.execute(
            statement.replace("{schema}", quoted_schema).replace(
                "{shadow}", quoted_shadow
            )
        )
    conn.commit()
    conn.close()

    result = runner.invoke(cli, ["check-compatibility"])

    assert result.exit_code == 3
    compatibility = json.loads(result.output)
    assert compatibility["state"] == "shape-mismatch"
    assert expected_issue in compatibility["detail"]


def test_service_restart_repeats_compatibility_without_migration(runner_env):
    runner, schema = runner_env
    assert runner.invoke(cli, ["migrate"]).exit_code == 0

    first_start = server._require_runtime_compatibility()
    second_start = server._require_runtime_compatibility()

    assert first_start["state"] == "compatible"
    assert second_start == first_start


def test_migration_role_cannot_serve_and_runtime_role_cannot_run_ddl(monkeypatch):
    schema = "aqroles_" + uuid.uuid4().hex
    migration_conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    runtime_conn = db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"])
    runtime_role = runtime_conn.execute("SELECT current_user AS role").fetchone()["role"]
    migration_role = migration_conn.execute("SELECT current_user AS role").fetchone()["role"]
    assert migration_role == MIGRATION_ROLE
    assert runtime_role == RUNTIME_ROLE
    assert migration_role != runtime_role
    try:
        migration_conn.rollback()
        runtime_conn.rollback()
        report = schema_contract.migrate(
            migration_conn,
            schema,
            runtime_role=runtime_role,
        )
        assert report["runtime_role"] == runtime_role

        migration_compatibility = schema_contract.check_compatibility(
            migration_conn, schema
        )
        assert migration_compatibility.state == "role-mismatch"
        with pytest.raises(schema_contract.SchemaCompatibilityError, match="role-mismatch"):
            schema_contract.require_compatible(migration_conn, schema)
        migration_conn.rollback()

        monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
        monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_MIGRATION_URL"])
        with pytest.raises(schema_contract.SchemaCompatibilityError, match="role-mismatch"):
            server._require_runtime_compatibility()
        with pytest.raises(schema_contract.SchemaCompatibilityError, match="role-mismatch"):
            server._dispatch(
                {
                    "contract_version": "v1",
                    "repo_id": "actionq",
                    "kind": "implement",
                    "title": "must not dispatch as migrator",
                }
            )

        assert schema_contract.require_compatible(runtime_conn, schema).compatible
        runtime_conn.rollback()
        monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_RUNTIME_URL"])
        assert server._require_runtime_compatibility()["state"] == "compatible"
        action = server._dispatch(
            {
                "contract_version": "v1",
                "repo_id": "actionq",
                "kind": "implement",
                "title": "runtime dispatch",
            }
        )
        assert action["status"] == "pending"
        with pytest.raises(Exception) as excinfo:
            runtime_conn.execute(
                sql.SQL("CREATE TABLE {}.forbidden_runtime_ddl (id INTEGER)").format(
                    sql.Identifier(schema)
                )
            )
        assert getattr(excinfo.value, "sqlstate", None) == "42501"
        runtime_conn.rollback()
        with pytest.raises(Exception) as excinfo:
            runtime_conn.execute(
                sql.SQL("UPDATE {}.{} SET checksum = 'forbidden'").format(
                    sql.Identifier(schema), sql.Identifier("schema_migrations")
                )
            )
        assert getattr(excinfo.value, "sqlstate", None) == "42501"
        runtime_conn.rollback()
    finally:
        runtime_conn.close()
        migration_conn.close()


def test_relation_owner_and_assumable_owner_cannot_serve_when_schema_create_revoked(
    monkeypatch,
):
    """Reproduce the split owner topology from the independent PG18 gate."""

    schema = "aqsplitroles_" + uuid.uuid4().hex
    admin_conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    migration_conn = db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"])
    runtime_conn = db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"])
    schema_identifier = sql.Identifier(schema)
    migration_identifier = sql.Identifier(MIGRATION_ROLE)
    runtime_identifier = sql.Identifier(RUNTIME_ROLE)
    membership_granted = False
    try:
        admin_conn.execute(
            sql.SQL("CREATE SCHEMA {} AUTHORIZATION CURRENT_USER").format(
                schema_identifier
            )
        )
        admin_conn.execute(
            sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}").format(
                schema_identifier, migration_identifier
            )
        )
        admin_conn.commit()

        monkeypatch.delenv("ACTIONQ_RUNTIME_ROLE")
        report = schema_contract.migrate(migration_conn, schema)
        assert report["runtime_role"] is None

        admin_conn.execute(
            sql.SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC, {}, {}").format(
                schema_identifier, migration_identifier, runtime_identifier
            )
        )
        admin_conn.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                schema_identifier, runtime_identifier
            )
        )
        admin_conn.execute(
            sql.SQL(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {}.{}, {}.{} TO {}"
            ).format(
                schema_identifier,
                sql.Identifier("actions"),
                schema_identifier,
                sql.Identifier("events"),
                runtime_identifier,
            )
        )
        admin_conn.execute(
            sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(
                schema_identifier,
                sql.Identifier("dispatch_requests"),
                runtime_identifier,
            )
        )
        admin_conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {}.{} TO {}").format(
                schema_identifier,
                sql.Identifier("dispatch_observation_watermarks"),
                runtime_identifier,
            )
        )
        admin_conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {}.{} TO {}").format(
                schema_identifier,
                sql.Identifier("execution_groups"),
                runtime_identifier,
            )
        )
        admin_conn.execute(
            sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(
                schema_identifier,
                sql.Identifier("execution_group_members"),
                runtime_identifier,
            )
        )
        for table in (
            "immutable_action_specs",
            "immutable_action_requests",
            "immutable_action_runtime_grants",
        ):
            admin_conn.execute(
                sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(
                    schema_identifier, sql.Identifier(table), runtime_identifier
                )
            )
        for table in (
            "action_resources",
            "action_resource_changes",
            "action_resource_sessions",
        ):
            admin_conn.execute(
                sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {}.{} TO {}").format(
                    schema_identifier, sql.Identifier(table), runtime_identifier
                )
            )
        admin_conn.execute(
            sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                schema_identifier,
                sql.Identifier("schema_migrations"),
                runtime_identifier,
            )
        )
        admin_conn.execute(
            sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                schema_identifier, runtime_identifier
            )
        )
        admin_conn.commit()

        topology = admin_conn.execute(
            """
            SELECT owner_role.rolname AS schema_owner,
                   array_agg(DISTINCT relation_owner.rolname) AS relation_owners
            FROM pg_namespace AS namespace_record
            JOIN pg_roles AS owner_role ON owner_role.oid = namespace_record.nspowner
            JOIN pg_class AS relation_record
              ON relation_record.relnamespace = namespace_record.oid
             AND relation_record.relname = ANY(%s)
            JOIN pg_roles AS relation_owner
              ON relation_owner.oid = relation_record.relowner
            WHERE namespace_record.nspname = %s
            GROUP BY owner_role.rolname
            """,
            (["actions", "events", "dispatch_requests", "dispatch_observation_watermarks", "schema_migrations"], schema),
        ).fetchone()
        admin_conn.rollback()
        assert topology["schema_owner"] != MIGRATION_ROLE
        assert topology["relation_owners"] == [MIGRATION_ROLE]
        assert migration_conn.execute(
            "SELECT has_schema_privilege(current_user, %s, 'CREATE') AS allowed",
            (schema,),
        ).fetchone()["allowed"] is False
        migration_conn.rollback()

        owner_compatibility = schema_contract.check_compatibility(
            migration_conn, schema
        )
        assert owner_compatibility.state == "role-mismatch"
        assert "owns a domain relation" in owner_compatibility.detail
        migration_conn.rollback()

        # Revoking schema CREATE does not remove ALTER authority from the
        # principal that owns the tables. Prove the verifier topology without
        # retaining the probe mutation.
        migration_conn.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN verifier_probe INTEGER").format(
                schema_identifier, sql.Identifier("actions")
            )
        )
        migration_conn.rollback()

        monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
        monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_MIGRATION_URL"])
        with pytest.raises(schema_contract.SchemaCompatibilityError, match="role-mismatch"):
            server._require_runtime_compatibility()
        with pytest.raises(schema_contract.SchemaCompatibilityError, match="role-mismatch"):
            server._dispatch(
                {
                    "contract_version": "v1",
                    "repo_id": "actionq",
                    "kind": "implement",
                    "title": "relation owner must not dispatch",
                }
            )

        assert schema_contract.require_compatible(runtime_conn, schema).compatible
        runtime_conn.rollback()
        monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_RUNTIME_URL"])
        assert server._require_runtime_compatibility()["state"] == "compatible"
        assert server._dispatch(
            {
                "contract_version": "v1",
                "repo_id": "actionq",
                "kind": "implement",
                "title": "bounded runtime dispatch",
            }
        )["status"] == "pending"

        with pytest.raises(Exception) as excinfo:
            runtime_conn.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN forbidden INTEGER").format(
                    schema_identifier, sql.Identifier("actions")
                )
            )
        assert getattr(excinfo.value, "sqlstate", None) == "42501"
        runtime_conn.rollback()
        with pytest.raises(Exception) as excinfo:
            runtime_conn.execute(
                sql.SQL("UPDATE {}.{} SET checksum = 'forbidden'").format(
                    schema_identifier, sql.Identifier("schema_migrations")
                )
            )
        assert getattr(excinfo.value, "sqlstate", None) == "42501"
        runtime_conn.rollback()

        admin_conn.execute(
            sql.SQL("GRANT {} TO {}").format(
                migration_identifier, runtime_identifier
            )
        )
        admin_conn.commit()
        membership_granted = True

        assumable_compatibility = schema_contract.check_compatibility(
            runtime_conn, schema
        )
        assert assumable_compatibility.state == "role-mismatch"
        assert "can assume a domain relation owner" in assumable_compatibility.detail
        runtime_conn.rollback()
        with pytest.raises(schema_contract.SchemaCompatibilityError, match="role-mismatch"):
            server._require_runtime_compatibility()
        with pytest.raises(schema_contract.SchemaCompatibilityError, match="role-mismatch"):
            server._dispatch(
                {
                    "contract_version": "v1",
                    "repo_id": "actionq",
                    "kind": "implement",
                    "title": "owner member must not dispatch",
                }
            )
    finally:
        if membership_granted:
            admin_conn.rollback()
            admin_conn.execute(
                sql.SQL("REVOKE {} FROM {}").format(
                    migration_identifier, runtime_identifier
                )
            )
            admin_conn.commit()
        runtime_conn.close()
        migration_conn.close()
        admin_conn.close()
