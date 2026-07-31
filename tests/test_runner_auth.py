from __future__ import annotations

import json
import os
import uuid

import pytest

from actionq import db
from actionq.application import ActionQApplication
from actionq.cli import cli


def _schema() -> str:
    return "aqrunnerauth_" + uuid.uuid4().hex


def test_signed_claim_rejects_tampering_and_replay(
    monkeypatch, actionq_cli_runner, signed_runner_proof,
):
    schema = _schema()
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    runner = actionq_cli_runner
    assert runner.invoke(cli, ["migrate"]).exit_code == 0
    for target in ("one", "two"):
        assert runner.invoke(cli, [
            "add", "--type", "scope-iterate", "--target", target,
            "--created-by", "human:test",
        ]).exit_code == 0
    proof = signed_runner_proof("worker:one", "execution.action.claim", "queue:next")
    first = runner.invoke(cli, ["claim", "--proof-stdin"], input=json.dumps(proof))
    assert first.exit_code == 0, first.output
    replay = runner.invoke(cli, ["claim", "--proof-stdin"], input=json.dumps(proof))
    assert replay.exit_code != 0
    tampered = {**signed_runner_proof("worker:one", "execution.action.claim", "queue:next"),
                "resource": "queue:other"}
    rejected = runner.invoke(cli, ["claim", "--proof-stdin"], input=json.dumps(tampered))
    assert rejected.exit_code != 0
    rows = json.loads(runner.invoke(cli, ["ls", "--status", "pending"]).output)
    assert len(rows) == 1


def test_ack_response_loss_replays_same_signed_request_once(signed_runner_proof):
    schema = _schema()
    with db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"]) as migration:
        db.migrate(migration, schema)
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        action = db.enqueue(
            conn, schema, action_type="scope-iterate", project="demo", target_ref="1",
            source_refs=[], priority=100, parent_id=None, created_by="human:test",
        )
        claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        cancelling = db.cancel(conn, schema, action["id"], "stop", actor="controller")
    request_id = str(cancelling["cancel_request_id"])
    proof = signed_runner_proof(
        "worker:one", "execution.action.cancel-ack",
        f"action:{action['id']}:cancel:{request_id}",
    )
    app = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]),
    )
    kwargs = dict(
        action_id=action["id"], cancel_request_id=request_id,
        former_claim_receipt=claimed["claim_receipt"],
        runner_auth_token=claimed["runner_auth_token"], runner_proof=proof,
    )
    assert app.acknowledge_cancellation(**kwargs)["status"] == "cancelled"
    assert app.acknowledge_cancellation(**kwargs)["status"] == "cancelled"
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        events = db.action_events(conn, schema, action["id"])
    assert sum(event["event_type"] == "runner.authenticated" for event in events) == 1
    assert sum(event["event_type"] == "action_cancelled" for event in events) == 1


def test_unregistered_signed_runner_is_rejected(monkeypatch, actionq_cli_runner, signed_runner_proof):
    schema = _schema()
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    runner = actionq_cli_runner
    assert runner.invoke(cli, ["migrate"]).exit_code == 0
    proof = signed_runner_proof("runner:unregistered", "execution.action.claim", "queue:next")
    result = runner.invoke(cli, ["claim", "--proof-stdin"], input=json.dumps(proof))
    assert result.exit_code != 0
    assert "not registered" in result.output


def test_runner_registry_rejects_group_writable_file(
    monkeypatch, runner_identity, actionq_cli_runner, signed_runner_proof,
):
    schema = _schema()
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    runner = actionq_cli_runner
    assert runner.invoke(cli, ["migrate"]).exit_code == 0
    runner_identity["registry"].chmod(0o664)
    try:
        proof = signed_runner_proof("worker:one", "execution.action.claim", "queue:next")
        result = runner.invoke(cli, ["claim", "--proof-stdin"], input=json.dumps(proof))
        assert result.exit_code != 0
        assert "owner-controlled" in result.output
    finally:
        runner_identity["registry"].chmod(0o644)


def test_ack_replay_is_bound_to_exact_operation_and_resource(signed_runner_proof):
    schema = _schema()
    with db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"]) as migration:
        db.migrate(migration, schema)
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        proof = signed_runner_proof("worker:one", "execution.action.cancel-ack", "action:1:cancel:first")
        db.consume_runner_request(
            conn, schema, runner_id="worker:one", request_id=proof["request_id"],
            operation="execution.action.cancel-ack", resource="action:1:cancel:first",
            action_id=None,
        )
        with pytest.raises(db.ActionQError, match="already consumed"):
            db.consume_runner_request(
                conn, schema, runner_id="worker:one", request_id=proof["request_id"],
                operation="execution.action.cancel-ack", resource="action:1:cancel:other",
                action_id=None, allow_replay=True,
            )
