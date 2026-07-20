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
    def __init__(self, fail_take: bool = False, fail_release: bool = False):
        self.calls = []
        self.fail_take = fail_take
        self.fail_release = fail_release

    def take(self, project, *, session_id, actor, pid):
        self.calls.append(("take", project, session_id, actor, pid))
        if self.fail_take:
            raise RuntimeError("sprintctl takeup take: connection refused")
        return {"event_id": 1}

    def release(self, project, *, session_id, actor, reason):
        self.calls.append(("release", project, session_id, actor, reason))
        if self.fail_release:
            raise RuntimeError("sprintctl takeup release: connection refused")
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


def test_takeup_pre_start_failure_fails_action_without_crashing_daemon(tmp_path: Path):
    client = FakeClient({"id": 20, "action_type": "scope-iterate", "project": "demo"})
    takeup = FakeTakeup(fail_take=True)
    config = DaemonConfig(
        session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
        takeup=TakeupConfig(enabled=True),
    )
    daemon = Daemon(
        config, {"scope-iterate": ActionConfig(fake_duration_seconds=10)}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={"SPRINTCTL_BACKEND": "remote"})}, takeup,
    )

    # A takeup failure before the harness starts must not propagate out of
    # run_once() and kill the daemon loop -- it is an expected external
    # failure mode (sprintctl down/unreachable), not a daemon bug.
    claimed = daemon.run_once()

    assert claimed is True
    assert [call[0] for call in takeup.calls] == ["take"]
    assert client.failed and client.failed[0][0] == 20
    assert "takeup failed before session start" in client.failed[0][1]
    assert not client.completed
    # The child never effectively ran as a tracked session.
    assert "session.started" not in [event[0] for event in client.events]
    assert daemon._child is None
    assert config.session_state_path.read_text() == "{}"


def test_shutdown_releases_takeup_cleanly(tmp_path: Path):
    client, takeup = FakeClient({"id": 21, "action_type": "scope-iterate", "project": "demo"}), FakeTakeup()
    config = DaemonConfig(
        graceful_shutdown_seconds=0.01,
        session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
        takeup=TakeupConfig(enabled=True),
    )
    daemon = Daemon(
        config, {"scope-iterate": ActionConfig(fake_duration_seconds=10)}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={"SPRINTCTL_BACKEND": "remote"})}, takeup,
    )
    worker = threading.Thread(target=daemon.run_once)
    worker.start()
    assert client.started.wait(timeout=2)
    daemon.request_shutdown()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert [call[0] for call in takeup.calls] == ["take", "release"]
    release_call = next(call for call in takeup.calls if call[0] == "release")
    assert release_call[4] == "session-shutdown"
    assert client.events[-1][3]["outcome"] == "shutdown"
    assert client.events[-1][3]["sprint_takeup_release"]["status"] == "ok"


def _stale_takeup_record(session_id: str, action_id: int) -> SessionRecord:
    return SessionRecord(
        session_id=session_id,
        runtime_session_id=session_id,
        daemon_id="old-daemon",
        action_id=action_id,
        action_type="scope-iterate",
        project="demo",
        target_ref="42",
        runner="fake",
        pid=999999999,
        started_at="2026-07-19T00:00:00Z",
        updated_at="2026-07-19T00:00:01Z",
    )


def test_recovery_release_calls_takeup_release_for_stale_session(tmp_path: Path):
    client, takeup = FakeClient(), FakeTakeup()
    config = DaemonConfig(
        session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
        takeup=TakeupConfig(enabled=True),
    )
    daemon = Daemon(
        config, {}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={"SPRINTCTL_BACKEND": "remote"})}, takeup,
    )
    daemon._write_state(_stale_takeup_record("aqs:stale-takeup", 30))

    assert daemon.run_once() is False
    assert [call[0] for call in takeup.calls] == ["release"]
    release_call = takeup.calls[0]
    assert release_call[4] == "daemon-recovered"
    assert client.events[0][0] == "session.end-inferred"
    assert client.events[0][3]["sprint_takeup_release"]["status"] == "ok"
    assert config.session_state_path.read_text() == "{}"


def test_recovery_release_failure_evidence_is_retained_not_masked(tmp_path: Path):
    client, takeup = FakeClient(), FakeTakeup(fail_release=True)
    config = DaemonConfig(
        session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
        takeup=TakeupConfig(enabled=True),
    )
    daemon = Daemon(
        config, {}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={"SPRINTCTL_BACKEND": "remote"})}, takeup,
    )
    daemon._write_state(_stale_takeup_record("aqs:stale-takeup-fail", 31))

    assert daemon.run_once() is False
    release_evidence = client.events[0][3]["sprint_takeup_release"]
    assert release_evidence["status"] == "failed"
    assert "connection refused" in release_evidence["error"]
    # A takeup release failure must not mask or block ordinary
    # crash-recovery idempotency; the stale state still clears, and
    # operator cleanup for a stale takeup remains an explicit, visible step.
    assert config.session_state_path.read_text() == "{}"


def test_recovery_skips_takeup_release_for_local_project(tmp_path: Path):
    client, takeup = FakeClient(), FakeTakeup()
    config = DaemonConfig(
        session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
        takeup=TakeupConfig(enabled=True),
    )
    daemon = Daemon(
        config, {}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={})}, takeup,  # no SPRINTCTL_BACKEND=remote
    )
    daemon._write_state(_stale_takeup_record("aqs:stale-local", 32))

    assert daemon.run_once() is False
    assert takeup.calls == []
    assert client.events[0][3]["sprint_takeup_release"] == {
        "attempted": False, "status": "skipped", "reason": "local-mode",
    }
