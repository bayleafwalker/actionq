from __future__ import annotations

import os
from pathlib import Path
import sys
import uuid

import pytest

from actionq import db
from actionq.daemon import ActionConfig, ActionctlClient, Daemon, DaemonConfig, SessionRecord


pytestmark = pytest.mark.skipif(
    not os.environ.get("ACTIONQ_TEST_URL"),
    reason="ACTIONQ_TEST_URL is required for Postgres daemon integration tests",
)


def _text(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def test_fake_daemon_lifecycle_uses_actionctl_subprocess(monkeypatch, tmp_path: Path):
    schema = "aqdaemon_" + uuid.uuid4().hex
    monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_URL"])
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    with db.connect() as conn:
        db.migrate(conn, schema)
        action = db.enqueue(
            conn,
            schema,
            action_type="scope-iterate",
            project="demo",
            target_ref="42",
            source_refs=[],
            priority=100,
            parent_id=None,
            created_by="human:test",
        )

    actionctl = Path(sys.executable).with_name("actionctl")
    daemon = Daemon(
        DaemonConfig(
            heartbeat_interval_seconds=0.01,
            session_state_path=tmp_path / "session.json",
            pause_file=tmp_path / "PAUSED",
            actionctl_bin=str(actionctl),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=1)},
        ActionctlClient(str(actionctl)),
    )

    assert daemon.run_once() is True

    with db.connect() as conn:
        settled = db.get_action(conn, schema, int(action["id"]))
        events = db.action_events(conn, schema, int(action["id"]))
        dispatches = db.list_dispatches(conn, schema)
    assert _text(settled["status"]) == "completed", repr(settled.get("failure_reason"))
    event_types = [_text(event["event_type"]) for event in events]
    assert event_types[:4] == [
        "action_enqueued", "action_claimed", "session.dispatch", "session.started"
    ]
    assert "session.heartbeat" in event_types
    assert event_types[-2:] == ["session.exited", "action_completed"]
    assert dispatches[0]["status"] == "completed"
    assert dispatches[0]["session"]["status"] == "exited"

    recovery = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "stale.json",
            pause_file=tmp_path / "PAUSED",
            actionctl_bin=str(actionctl),
        ),
        {},
        ActionctlClient(str(actionctl)),
    )
    recovery._write_state(
        SessionRecord(
            session_id="aqs:stale",
            runtime_session_id="aqs:stale",
            daemon_id="old-daemon",
            action_id=int(action["id"]),
            action_type="scope-iterate",
            project="demo",
            target_ref="42",
            runner="fake",
            pid=99999999,
            started_at="2026-07-19T00:00:00Z",
            updated_at="2026-07-19T00:00:01Z",
        )
    )
    assert recovery.recover_stale_state() is False
    with db.connect() as conn:
        sessions = db.list_sessions(conn, schema)
    stale = next(session for session in sessions if session["session_id"] == "aqs:stale")
    assert stale["status"] == "exited"
    assert stale["outcome"] == "end-inferred"
