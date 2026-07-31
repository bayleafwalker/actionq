from __future__ import annotations

from pathlib import Path
from itertools import product

import pytest

from actionq.daemon import ActionConfig, Daemon, DaemonConfig
from actionq.scope_iterate import VerificationResult
from actionq_contracts import EXECUTION_ENVELOPE_V1, ExecutionEnvelope


JOURNAL_REF = "artifact:sha256:" + "a" * 64


class Client:
    def __init__(self, *, lose_completion_response: bool = False):
        self.completed = []
        self.events = []
        self.lose_completion_response = lose_completion_response
        self.result_ref = None
        self.status = "completed"
        self.reconciled = []
        self.registered = []
        self.frozen = []

    def complete(self, action_id, *, result_ref, actor, claim_receipt):
        self.completed.append((action_id, result_ref, actor, claim_receipt))
        self.result_ref = result_ref
        if self.lose_completion_response:
            raise RuntimeError("response lost after commit")

    def show(self, action_id):
        events = []
        if self.result_ref is not None:
            events.append({"event_type": "action_completed", "payload": {"result_ref": self.result_ref}})
        events.extend(self.frozen)
        events.extend(self.registered)
        return {"action": {"id": action_id, "status": self.status, "result_ref": self.result_ref,
                           "completed_at": "2026-07-31T12:00:00Z"},
                "events": events}

    def register_publication(self, action_id, *, publication, actor, claim_receipt):
        event = {"event_type": "publication.registered", "payload": {
            key: publication[key] for key in (
                "attempt_id", "journal_ref", "source_commit", "candidate_commit"
            )
        }}
        self.registered.append(event)
        return event

    def emit(self, event_type, *, action_id, actor, payload):
        self.events.append((event_type, action_id, actor, payload))

    def reconcile_runner_spool(self, action_id, *, attempt_id):
        self.reconciled.append((action_id, attempt_id))


class PublicationDaemon(Daemon):
    def __init__(self, client, *, recovered=None):
        super().__init__(
            DaemonConfig(artifact_root=Path("/durable/actionq")),
            {"scope-iterate": ActionConfig(runner="scope-iterate", publish_candidate=True)},
            client,
        )
        self.recovered = recovered
        self.commands = []

    def _runnerctl_json(self, *args, input_value=None):
        self.commands.append((args, input_value))
        if args[0] == "journal-recover":
            return self.recovered
        if args[0] == "settlement-query":
            return None
        if args[0] == "journal-list":
            return ([{"action_id": 2032, "attempt_id": "attempt-old",
                      "status": "incomplete", "latest_stage": "bundle",
                      "source_commit": "b" * 40, "candidate_commit": "c" * 40}]
                    if self.recovered == "incomplete" else [])
        if args[0] == "journal-resume":
            return publication()
        if args[0] in {"settlement-ack", "publish"}:
            return ({"ok": True} if args[0] == "settlement-ack" else self.recovered)
        raise AssertionError(args)


def publication():
    return {"action_id": 2032, "attempt_id": "attempt-old", "journal_ref": JOURNAL_REF,
            "source_commit": "b" * 40, "candidate_commit": "c" * 40}


def test_completion_response_loss_is_reconciled_before_settlement_ack():
    client = Client(lose_completion_response=True)
    daemon = PublicationDaemon(client, recovered=publication())
    daemon._complete_published(2032, claim_receipt="memory-only", publication=publication())
    assert client.completed == [(2032, JOURNAL_REF, "runner:devbox", "memory-only")]
    ack = next(value for args, value in daemon.commands if args[0] == "settlement-ack")
    assert ack == {
        "artifact_root": "/durable/actionq", "action_id": 2032,
        "attempt_id": "attempt-old", "journal_ref": JOURNAL_REF,
        "terminal_status": "completed", "result_ref": JOURNAL_REF,
        "authoritative_decision": {"action_id": 2032, "status": "completed",
                                   "result_ref": JOURNAL_REF,
                                   "completed_at": "2026-07-31T12:00:00Z"},
    }
    assert [event[0] for event in client.events] == ["publication.settled"]


def test_reclaimed_action_settles_recovered_publication_without_execution():
    client = Client()
    client.register_publication(2032, publication=publication(), actor="runner:devbox",
                                claim_receipt="old-memory-only")
    daemon = PublicationDaemon(client, recovered=publication())
    action = {
        "id": 2032, "action_type": "scope-iterate", "claim_receipt": "new-live-receipt",
    }
    assert daemon._settle_recovered_publication(action) is True
    assert client.completed[0][1] == JOURNAL_REF
    assert client.reconciled == [(2032, "attempt-old")]
    assert daemon.commands[0][0][0] == "journal-recover"


def test_unregistered_recovered_publication_is_not_adopted_by_new_claim():
    client = Client()
    daemon = PublicationDaemon(client, recovered=publication())
    assert daemon._settle_recovered_publication({
        "id": 2032, "action_type": "scope-iterate", "claim_receipt": "new-live-receipt",
    }) is False
    assert not client.completed


def test_daemon_restart_resumes_exact_interrupted_attempt_without_packet():
    daemon = PublicationDaemon(Client(), recovered="incomplete")
    assert daemon._resume_interrupted_publication(
        2032, "scope-iterate", "attempt-old",
    ) is True
    assert [args[0] for args, _ in daemon.commands] == ["journal-list", "journal-resume"]
    resume_args = daemon.commands[-1][0]
    assert resume_args[-2:] == ("--attempt-id", "attempt-old")


def test_fresh_daemon_claim_resumes_registers_and_settles_without_harness():
    client = Client()
    client.frozen.append({"event_type": "runner.contract.frozen", "payload": {"contract": {
        "attempt_id": "attempt-old", "source_commit": "b" * 40,
    }}})
    daemon = PublicationDaemon(client, recovered="incomplete")
    action = {"id": 2032, "action_type": "scope-iterate",
              "claim_receipt": "new-live-receipt"}
    assert daemon._resume_and_settle_interrupted_publication(action) is True
    assert [args[0] for args, _ in daemon.commands][:2] == ["journal-list", "journal-resume"]
    assert len(client.registered) == 1
    assert client.completed == [(2032, JOURNAL_REF, "runner:devbox", "new-live-receipt")]
    assert client.reconciled == [(2032, "attempt-old")]


def test_no_recovered_publication_falls_through_to_normal_execution():
    client = Client()
    daemon = PublicationDaemon(client, recovered=None)
    assert daemon._settle_recovered_publication({
        "id": 2032, "action_type": "scope-iterate", "claim_receipt": "live",
    }) is False
    assert not client.completed


def test_late_publication_is_not_acknowledged_after_cancellation_fences_claim():
    client = Client()
    client.status = "cancelling"
    client.result_ref = None
    daemon = PublicationDaemon(client, recovered=publication())
    assert daemon._reconcile_terminal_publication(2032, "scope-iterate") is False
    assert not any(args[0] == "settlement-ack" for args, _ in daemon.commands)


def test_startup_reconciles_commit_before_local_settlement_ack():
    client = Client()
    client.result_ref = JOURNAL_REF
    daemon = PublicationDaemon(client, recovered=publication())
    assert daemon._reconcile_terminal_publication(2032, "scope-iterate") is True
    assert any(args[0] == "settlement-ack" for args, _ in daemon.commands)
    assert client.events[-1][3]["recovered_after_response_loss"] is True


def test_publication_packet_contains_no_claim_or_runner_authority(tmp_path: Path):
    client = Client()
    daemon = PublicationDaemon(client, recovered=publication())
    log = tmp_path / "redacted.log"
    log.write_text("safe output\n")
    envelope = ExecutionEnvelope(
        EXECUTION_ENVELOPE_V1, 2032, "attempt-new", "b" * 40,
        "scope-iterate:scope-iterate", ("src/**",),
    )
    result = VerificationResult(
        tmp_path, "agent/test", "b" * 40, "c" * 40, ("src/file.py",),
    )
    daemon._publish_scope_candidate(
        action_id=2032, envelope=envelope, scope_result=result, exit_code=0,
        output_path=log, test_command=("pytest", "-q"),
    )
    packet = next(value for args, value in daemon.commands if args[0] == "publish")
    serialized = repr(packet).lower()
    assert "claim_receipt" not in serialized
    assert "runner_auth" not in serialized
    assert "memory-only" not in serialized
    assert packet["redacted_log"] == "safe output\n"


def test_publish_policy_requires_explicit_durable_root():
    client = Client()
    daemon = Daemon(
        DaemonConfig(artifact_root=None),
        {"scope-iterate": ActionConfig(runner="scope-iterate", publish_candidate=True)},
        client,
    )
    with pytest.raises(RuntimeError, match="artifact_root"):
        daemon._settle_recovered_publication({
            "id": 2032, "action_type": "scope-iterate", "claim_receipt": "live",
        })


def test_bounded_depth3_artifact_postgres_crash_model():
    """Exhaust the publication/terminal boundary without claiming exactly-once work."""
    operations = ("publish", "complete", "cancel", "ack")
    for history in product(operations, repeat=3):
        artifact = False
        terminal = None
        acknowledged = False
        terminal_mutations = 0
        for operation in history:
            if operation == "publish":
                artifact = True  # digest-idempotent even when repeated
            elif operation == "complete" and terminal is None:
                terminal = "completed"
                terminal_mutations += 1
            elif operation == "cancel" and terminal is None:
                terminal = "cancelled"
                terminal_mutations += 1
            elif operation == "ack" and artifact and terminal == "completed":
                acknowledged = True
        assert terminal_mutations <= 1
        assert not acknowledged or (artifact and terminal == "completed")
        if artifact and terminal is None:
            # Crash after upload leaves recoverable content, never a terminal
            # disposition invented by the journal.
            assert acknowledged is False
