from __future__ import annotations

import os
from pathlib import Path
import sys
import uuid

import pytest

from actionq import db
from actionq.daemon import ActionConfig, ActionctlClient, Daemon, DaemonConfig


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
    assert _text(settled["status"]) == "completed", repr(settled.get("failure_reason"))
    event_types = [_text(event["event_type"]) for event in events]
    assert event_types[:4] == [
        "action_enqueued", "action_claimed", "session.dispatch", "session.started"
    ]
    assert "session.heartbeat" in event_types
    assert event_types[-2:] == ["session.exited", "action_completed"]
