from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from actionq import db
from actionq.application import ActionQApplication
from actionq.cli import cli
from actionq.daemon import ActionConfig, Daemon, DaemonConfig


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


def _publication_fixture(schema: str):
    with db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"]) as migration:
        db.migrate(migration, schema)
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        action = db.enqueue(
            conn, schema, action_type="scope-iterate", project="demo", target_ref="2032",
            source_refs=[], priority=100, parent_id=None, created_by="human:test",
        )
        claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
    return action, claimed


def _publication_arguments(action_id: int, receipt: str, proof: dict):
    return {
        "action_id": action_id,
        "attempt_id": "attempt-2032",
        "journal_ref": "artifact:sha256:" + "a" * 64,
        "source_commit": "b" * 40,
        "candidate_commit": "c" * 40,
        "claim_receipt": receipt,
        "runner_proof": proof,
    }


def test_publication_registration_is_fenced_sanitized_and_exactly_replayable(signed_runner_proof):
    schema = _schema()
    action, claimed = _publication_fixture(schema)
    proof = signed_runner_proof(
        "worker:one", "execution.action.register-publication",
        f"action:{action['id']}:publication:attempt-2032",
    )
    app = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]),
    )
    arguments = _publication_arguments(action["id"], claimed["claim_receipt"], proof)
    registered = app.register_publication(**arguments)
    replay = app.register_publication(**arguments)
    assert replay == registered
    assert registered["claim_receipt_digest"] == db.receipt_digest(claimed["claim_receipt"])
    with pytest.raises(db.ActionQError, match="conflicts"):
        app.register_publication(**{**arguments, "journal_ref": "artifact:sha256:" + "d" * 64})
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        events = db.action_events(conn, schema, action["id"])
    publications = [event for event in events if event["event_type"] == "publication.registered"]
    assert len(publications) == 1
    payload = publications[0]["payload"]
    assert payload["journal_ref"] == arguments["journal_ref"]
    assert claimed["claim_receipt"] not in db.to_json(payload)
    assert not any("worktree" in key or "path" in key for key in payload)


@pytest.mark.parametrize("history", ("expired", "reclaimed", "cancelled"))
def test_publication_registration_rejects_nonlive_claim_histories(signed_runner_proof, history):
    schema = _schema()
    action, claimed = _publication_fixture(schema)
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        if history == "expired":
            conn.execute(
                f"UPDATE {db.qname(schema, 'actions')} SET claim_deadline=now() - interval '1 second' WHERE id=%s",
                (action["id"],),
            )
        elif history == "reclaimed":
            conn.execute(
                f"UPDATE {db.qname(schema, 'actions')} SET claim_deadline=now() - interval '1 second' WHERE id=%s",
                (action["id"],),
            )
            db.sweep(conn, schema)
            db.claim(conn, schema, worker="worker:test", timeout_minutes=30)
        else:
            db.cancel(conn, schema, action["id"], "controller cancellation", actor="controller")
    proof = signed_runner_proof(
        "worker:one", "execution.action.register-publication",
        f"action:{action['id']}:publication:attempt-2032",
    )
    app = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]),
    )
    with pytest.raises(db.ActionQError, match="publication registration rejected"):
        app.register_publication(**_publication_arguments(action["id"], claimed["claim_receipt"], proof))
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        events = db.action_events(conn, schema, action["id"])
    assert not any(event["event_type"] == "publication.registered" for event in events)


@pytest.mark.parametrize("history", ("register-then-cancel", "cancel-then-register"))
def test_publication_registration_and_cancellation_serialize_on_action_row(
    signed_runner_proof, history,
):
    """Real PostgreSQL Depth-2 histories at the upload/terminal boundary."""
    schema = _schema()
    action, claimed = _publication_fixture(schema)
    proof = signed_runner_proof(
        "worker:one", "execution.action.register-publication",
        f"action:{action['id']}:publication:attempt-2032",
    )
    app = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]),
    )
    arguments = _publication_arguments(action["id"], claimed["claim_receipt"], proof)
    if history == "register-then-cancel":
        app.register_publication(**arguments)
        with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
            db.cancel(conn, schema, action["id"], "controller cancellation", actor="controller")
    else:
        with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
            db.cancel(conn, schema, action["id"], "controller cancellation", actor="controller")
        with pytest.raises(db.ActionQError, match="publication registration rejected"):
            app.register_publication(**arguments)
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        current = db.get_action(conn, schema, action["id"])
        events = db.action_events(conn, schema, action["id"])
        with pytest.raises(db.ActionQError, match="cannot transition"):
            db.complete(
                conn, schema, action["id"], "artifact:sha256:" + "a" * 64,
                actor="worker:one", worker="worker:one",
                claim_receipt=claimed["claim_receipt"],
            )
    assert current["status"] == "cancelling"
    registrations = [event for event in events if event["event_type"] == "publication.registered"]
    assert len(registrations) == (1 if history == "register-then-cancel" else 0)


@pytest.mark.parametrize("journal_state", ("incomplete", "complete-unregistered"))
def test_reclaimed_claim_resumes_registers_and_settles_real_postgres(
    signed_runner_proof, journal_state,
):
    """Durable journal + ActionQ history suffice; no prior daemon state or packet."""
    schema = _schema()
    action, old_claim = _publication_fixture(schema)
    attempt_id = "attempt-crashed"
    source_commit, candidate_commit = "b" * 40, "c" * 40
    journal_ref = "artifact:sha256:" + "a" * 64
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        db.insert_event(conn, schema, action_id=action["id"],
                        event_type="runner.contract.frozen", actor="runner:old", payload={
                            "contract": {"attempt_id": attempt_id,
                                         "source_commit": source_commit},
                        })
        conn.execute(
            f"UPDATE {db.qname(schema, 'actions')} SET claim_deadline=now() - interval '1 second' WHERE id=%s",
            (action["id"],),
        )
        db.sweep(conn, schema)
        replacement = db.claim(conn, schema, worker="worker:test", timeout_minutes=30)

    app = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]),
    )

    class LiveClient:
        def show(self, action_id):
            return app.show_action(action_id)

        def register_publication(self, action_id, *, publication, actor, claim_receipt):
            proof = signed_runner_proof(
                actor, "execution.action.register-publication",
                f"action:{action_id}:publication:{publication['attempt_id']}",
            )
            return app.register_publication(
                action_id=action_id, claim_receipt=claim_receipt, runner_proof=proof,
                **{key: publication[key] for key in (
                    "attempt_id", "journal_ref", "source_commit", "candidate_commit"
                )},
            )

        def complete(self, action_id, *, result_ref, actor, claim_receipt):
            proof = signed_runner_proof(
                actor, "execution.action.complete", f"action:{action_id}",
            )
            app.complete(action_id=action_id, result_ref=result_ref, actor=None,
                         claim_receipt=claim_receipt, runner_proof=proof)

        def emit(self, event_type, *, action_id, actor, payload):
            app.emit_event(event_type=event_type, action_id=action_id,
                           payload=payload, actor=actor)

        def reconcile_runner_spool(self, action_id, *, attempt_id):
            return None

    publication = {"action_id": action["id"], "attempt_id": attempt_id,
                   "journal_ref": journal_ref, "source_commit": source_commit,
                   "candidate_commit": candidate_commit}

    class RecoveryDaemon(Daemon):
        def _runnerctl_json(self, *args, input_value=None):
            if args[0] == "journal-list":
                return ([{**publication, "status": "incomplete", "latest_stage": "objects"}]
                        if journal_state == "incomplete" else [publication])
            if args[0] == "journal-resume":
                return publication
            if args[0] == "journal-recover":
                return publication
            if args[0] == "settlement-ack":
                return {"ok": True}
            raise AssertionError(args)

    daemon = RecoveryDaemon(
        DaemonConfig(artifact_root=Path("/durable/actionq"), runner_id="worker:test"),
        {"scope-iterate": ActionConfig(runner="scope-iterate", publish_candidate=True)},
        LiveClient(),
    )
    claimed_action = {**replacement, "action_type": "scope-iterate"}
    assert daemon._resume_and_settle_interrupted_publication(claimed_action) is True
    with db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]) as conn:
        current = db.get_action(conn, schema, action["id"])
        events = db.action_events(conn, schema, action["id"])
    assert current["status"] == "completed" and current["result_ref"] == journal_ref
    assert [event["event_type"] for event in events].count("publication.registered") == 1
    assert old_claim["claim_receipt"] != replacement["claim_receipt"]
