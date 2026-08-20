from __future__ import annotations

from copy import deepcopy
import http.client
import json
import os
import threading
import uuid

import pytest

from actionq import db
from actionq.application import ActionQApplication
from actionq.completion_log import CompletionConflictError


def _event(*, event_id: str, stream_id: str, sequence: int, failed: bool = False) -> dict:
    return {
        "schema_version": "session.completion-observed/v1",
        "event_id": event_id,
        "origin_stream_id": stream_id,
        "origin_sequence": sequence,
        "runtime_session_id": f"session-{sequence}",
        "attempt_id": None,
        "action_id": None,
        "repo": {"project": "actionq"},
        "harness": "manual",
        "model": None,
        "terminal": {
            "kind": "failed" if failed else "succeeded",
            "exit_code": 7 if failed else 0,
            "reason_code": "process-exit" if failed else "completed",
            "retryable": failed,
        },
        "started_at": "2026-08-11T10:00:00Z",
        "completed_at": "2026-08-11T10:00:01Z",
        "observed_at": "2026-08-11T10:00:01Z",
        "duration_ms": 1000,
        "refs": [],
        "evidence": {"dirty": False, "commit_count": 0, "verification": {"pass": 0, "fail": 0, "error": 0}},
        "privacy": {
            "prompt_absent": True, "transcript_absent": True, "raw_output_absent": True,
            "environment_absent": True, "credentials_absent": True,
            "absolute_paths_absent": True, "claim_proofs_absent": True,
        },
    }


@pytest.fixture
def completion_application(postgres_urls, monkeypatch):
    schema = "aq_completion_" + uuid.uuid4().hex
    with db.connect(postgres_urls["admin"]) as conn:
        db.migrate(conn, schema)
    monkeypatch.setenv("ACTIONQ_COMPLETION_INGEST_URL", postgres_urls["completion_ingest"])
    monkeypatch.setenv("ACTIONQ_COMPLETION_READ_URL", postgres_urls["completion_read"])
    application = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(postgres_urls["admin"]),
    )
    yield application
    with db.connect(postgres_urls["admin"]) as conn:
        conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        conn.commit()


def test_completion_log_idempotency_positions_conflicts_and_cursor_replay(completion_application):
    stream = "1a2b3c4d-5e6f-4788-9a0b-1c2d3e4f5061"
    first = _event(event_id="4d5e6f70-8192-4a3b-8c0d-3e4f50617283", stream_id=stream, sequence=3)
    second = _event(event_id="4d5e6f70-8192-4a3b-8c0d-3e4f50617284", stream_id=stream, sequence=1, failed=True)

    accepted = completion_application.ingest_session_completion(first)
    replayed = completion_application.ingest_session_completion(deepcopy(first))
    assert accepted["server_cursor"] == replayed["server_cursor"]
    assert accepted["acknowledged"] is True
    assert replayed["replayed"] is True

    # Producer positions may arrive out of order; server ordering is the
    # durable ingest cursor, not an inferred global producer order.
    second_ack = completion_application.ingest_session_completion(second)
    assert second_ack["server_cursor"] > accepted["server_cursor"]
    page = completion_application.list_session_completions(cursor=0, limit=10)
    assert [event["origin_sequence"] for event in page["events"]] == [3, 1]

    conflicting_id = deepcopy(first)
    conflicting_id["terminal"] = {"kind": "failed", "exit_code": 9, "reason_code": "process-exit", "retryable": True}
    with pytest.raises(CompletionConflictError, match="event_id"):
        completion_application.ingest_session_completion(conflicting_id)
    conflicting_position = deepcopy(second)
    conflicting_position["event_id"] = "4d5e6f70-8192-4a3b-8c0d-3e4f50617285"
    with pytest.raises(CompletionConflictError, match="origin stream position"):
        completion_application.ingest_session_completion(conflicting_position)
    assert completion_application.session_completion_health()["quarantine_count"] == 2


def test_completion_log_retention_preserves_ack_and_rejects_stale_cursor(completion_application):
    stream = "1a2b3c4d-5e6f-4788-9a0b-1c2d3e4f5061"
    first = _event(event_id="4d5e6f70-8192-4a3b-8c0d-3e4f50617283", stream_id=stream, sequence=1)
    second = _event(event_id="4d5e6f70-8192-4a3b-8c0d-3e4f50617284", stream_id=stream, sequence=2, failed=True)
    first_ack = completion_application.ingest_session_completion(first)
    completion_application.ingest_session_completion(second)
    schema = completion_application.schema
    with db.connect(os.environ["ACTIONQ_TEST_URL"]) as conn:
        conn.execute(f'UPDATE "{schema}".session_completion_events SET received_at=now()-interval \'2 days\' WHERE server_cursor=%s', (first_ack["server_cursor"],))
        conn.commit()

    assert completion_application.compact_session_completions(older_than_seconds=60) == 1
    stale = completion_application.list_session_completions(cursor=0, limit=10)
    assert stale["status"] == "cursor_expired"
    assert stale["advance_cursor"] is False
    assert stale["next_cursor"] == 0
    assert stale["recovery_floor"] == 1
    repaired = completion_application.list_session_completions(cursor=0, limit=10, replay=True)
    assert repaired["status"] == "cursor_expired"
    assert repaired["events"] == []
    assert repaired["next_cursor"] == 0
    explicit = completion_application.list_session_completions(cursor=1, limit=10, replay=True)
    assert [event["event_id"] for event in explicit["events"]] == [second["event_id"]]
    # The retained position keeps a retry of the compacted event idempotent.
    retry = completion_application.ingest_session_completion(first)
    assert retry["replayed"] is True
    assert retry["server_cursor"] == first_ack["server_cursor"]


def test_completion_roles_have_no_queue_dml_privileges(completion_application, postgres_urls):
    schema = completion_application.schema
    if postgres_urls["completion_ingest"] == postgres_urls["runtime"]:
        pytest.skip("external PostgreSQL fixture does not provide dedicated completion roles")
    checks = (
        ("completion_ingest", "actions", "INSERT"),
        ("completion_ingest", "events", "UPDATE"),
        ("completion_read", "actions", "SELECT"),
        ("completion_read", "events", "DELETE"),
    )
    for role_url, relation, privilege in checks:
        with db.connect(postgres_urls[role_url]) as conn:
            allowed = conn.execute(
                "SELECT has_table_privilege(current_user, %s, %s) AS allowed",
                (f'"{schema}".{relation}', privilege),
            ).fetchone()["allowed"]
            assert allowed is False
    with db.connect(postgres_urls["completion_read"]) as conn:
        assert conn.execute(
            "SELECT has_table_privilege(current_user, %s, 'SELECT') AS allowed",
            (f'"{schema}".session_completion_events',),
        ).fetchone()["allowed"] is True


