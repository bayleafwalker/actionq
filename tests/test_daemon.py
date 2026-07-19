from __future__ import annotations

import os
from pathlib import Path
import threading

from actionq.daemon import ActionConfig, Daemon, DaemonConfig, ProjectConfig, SessionRecord, TakeupConfig, load_config


class FakeClient:
    def __init__(self, action=None):
        self.action = action
        self.claims = []
        self.events = []
        self.completed = []
        self.failed = []
        self.started = threading.Event()

    def claim(self, worker, timeout_minutes):
        self.claims.append((worker, timeout_minutes))
        action, self.action = self.action, None
        return action

    def emit(self, event_type, *, action_id, actor, payload):
        self.events.append((event_type, action_id, actor, payload))
        if event_type == "session.started":
            self.started.set()

    def complete(self, action_id, *, result_ref, actor):
        self.completed.append((action_id, result_ref, actor))

    def fail(self, action_id, *, reason, actor):
        self.failed.append((action_id, reason, actor))


class FakeTakeup:
    def __init__(self):
        self.calls = []

    def take(self, project, *, session_id, actor, pid):
        self.calls.append(("take", project, session_id, actor, pid))
        return {"event_id": 1}

    def release(self, project, *, session_id, actor, reason):
        self.calls.append(("release", project, session_id, actor, reason))
        return {"event_id": 2}


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
    assert client.events[0][3]["ttl_seconds"] == 1800
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
    assert client.events[0][0] == "coordinator_paused"


def test_unconfigured_action_fails_without_starting_child(tmp_path: Path):
    client = FakeClient({"id": 8, "action_type": "unsupported"})
    daemon = Daemon(DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED"), {}, client)

    assert daemon.run_once() is True
    assert client.events == []
    assert client.completed == []
    assert client.failed[0][0] == 8


def test_shutdown_pauses_child_then_records_shutdown_outcome(tmp_path: Path):
    client = FakeClient({"id": 9, "action_type": "scope-iterate"})
    daemon = Daemon(
        DaemonConfig(
            graceful_shutdown_seconds=0.01,
            session_state_path=tmp_path / "state.json",
            pause_file=tmp_path / "PAUSED",
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=10)},
        client,
    )
    worker = threading.Thread(target=daemon.run_once)
    worker.start()
    assert client.started.wait(timeout=2)
    daemon.request_shutdown()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert [event[0] for event in client.events][-2:] == ["session.paused", "session.exited"]
    assert client.events[-1][3]["outcome"] == "shutdown"
    assert client.failed[0][0] == 9


def test_load_config_reads_daemon_and_action_settings(tmp_path: Path):
    config_path = tmp_path / "daemon.toml"
    config_path.write_text(
        "[global]\n"
        "poll_interval_seconds = 5\n"
        "session_state_path = 'state.json'\n"
        "[actions.scope-iterate]\n"
        "runner = 'fake'\n"
        "fake_duration_seconds = 2\n"
    )

    config, actions, projects = load_config(config_path)

    assert config.poll_interval_seconds == 5
    assert config.session_state_path == Path("state.json")
    assert actions["scope-iterate"].fake_duration_seconds == 2
    assert projects == {}


def test_stale_state_emits_one_inferred_end_then_clears(tmp_path: Path):
    client = FakeClient()
    config = DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED")
    daemon = Daemon(config, {}, client)
    daemon._write_state(
        SessionRecord(
            session_id="aqs:stale",
            runtime_session_id="aqs:stale",
            daemon_id="old-daemon",
            action_id=12,
            action_type="scope-iterate",
            project="demo",
            target_ref="42",
            runner="fake",
            pid=99999999,
            started_at="2026-07-19T00:00:00Z",
            updated_at="2026-07-19T00:00:01Z",
        )
    )

    assert daemon.run_once() is False
    assert client.events[0][0] == "session.end-inferred"
    assert client.events[0][1] == 12
    assert config.session_state_path.read_text() == "{}"
    assert daemon.run_once() is False
    assert [event[0] for event in client.events].count("session.end-inferred") == 1


def test_live_state_blocks_a_second_claim_after_restart(tmp_path: Path):
    client = FakeClient({"id": 13, "action_type": "scope-iterate"})
    config = DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED")
    daemon = Daemon(config, {}, client)
    daemon._write_state(
        SessionRecord(
            session_id="aqs:live",
            runtime_session_id="aqs:live",
            daemon_id="old-daemon",
            action_id=12,
            action_type="scope-iterate",
            project="demo",
            target_ref="42",
            runner="fake",
            pid=os.getpid(),
            started_at="2026-07-19T00:00:00Z",
            updated_at="2026-07-19T00:00:01Z",
        )
    )

    assert daemon.run_once() is False
    assert client.claims == []
    assert config.session_state_path.read_text() != "{}"


def test_remote_project_takeup_wraps_fake_session(tmp_path: Path):
    client, takeup = FakeClient({"id": 14, "action_type": "scope-iterate", "project": "demo"}), FakeTakeup()
    config = DaemonConfig(
        session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
        takeup=TakeupConfig(enabled=True),
    )
    daemon = Daemon(
        config, {"scope-iterate": ActionConfig(fake_duration_seconds=0.05)}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={"SPRINTCTL_BACKEND": "remote"})}, takeup,
    )

    assert daemon.run_once() is True
    assert [call[0] for call in takeup.calls] == ["take", "release"]
    assert client.events[1][3]["sprint_takeup"]["status"] == "ok"
    assert client.events[-1][3]["sprint_takeup_release"]["status"] == "ok"
