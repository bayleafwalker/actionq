from __future__ import annotations

from pathlib import Path

from actionq.daemon import ActionConfig, Daemon, DaemonConfig


class FakeClient:
    def __init__(self, action=None):
        self.action = action
        self.claims = []
        self.events = []
        self.completed = []
        self.failed = []

    def claim(self, worker, timeout_minutes):
        self.claims.append((worker, timeout_minutes))
        action, self.action = self.action, None
        return action

    def emit(self, event_type, *, action_id, actor, payload):
        self.events.append((event_type, action_id, actor, payload))

    def complete(self, action_id, *, result_ref, actor):
        self.completed.append((action_id, result_ref, actor))

    def fail(self, action_id, *, reason, actor):
        self.failed.append((action_id, reason, actor))


def test_fake_action_emits_lifecycle_and_clears_state(tmp_path: Path):
    client = FakeClient({"id": 7, "action_type": "scope-iterate", "project": "demo", "target_ref": "42"})
    config = DaemonConfig(
        heartbeat_interval_seconds=0.01,
        session_state_path=tmp_path / "session.json",
        pause_file=tmp_path / "PAUSED",
    )
    daemon = Daemon(config, {"scope-iterate": ActionConfig(fake_duration_seconds=0.05)}, client)

    assert daemon.run_once() is True

    event_types = [event[0] for event in client.events]
    assert event_types[:2] == ["session.dispatch", "session.started"]
    assert "session.heartbeat" in event_types
    assert event_types[-1] == "session.exited"
    session_id = client.events[0][3]["session_id"]
    assert all(event[3]["session_id"] == session_id for event in client.events)
    assert client.completed and client.completed[0][0] == 7
    assert not client.failed
    assert config.session_state_path.read_text() == "{}"


def test_pause_file_prevents_claim(tmp_path: Path):
    pause_file = tmp_path / "PAUSED"
    pause_file.touch()
    client = FakeClient({"id": 7, "action_type": "scope-iterate"})
    daemon = Daemon(DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=pause_file), {}, client)

    assert daemon.run_once() is False
    assert client.claims == []
