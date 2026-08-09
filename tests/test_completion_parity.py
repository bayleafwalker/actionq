from __future__ import annotations

import json
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from actionq.completion_outbox import CompletionOutbox
from actionq.daemon import (
    ActionConfig,
    Daemon,
    DaemonConfig,
    ProjectConfig,
    SessionRecord,
)
from actionq.routing import HarnessRoute, RoutingContext


class _Client:
    def __init__(self, action: dict | None = None):
        self.action = action
        self.completed: list[int] = []
        self.failed: list[str] = []
        self.events: list[tuple[str, dict]] = []
        self.current_status = "claimed"

    def claim(self, worker, timeout_minutes):
        action, self.action = self.action, None
        return ({**action, "claim_receipt": "receipt", "runner_auth_token": "proof"}
                if action else None)

    def renew(self, *args, **kwargs):
        return None

    def emit(self, event_type, *, action_id, actor, payload):
        self.events.append((event_type, payload))

    def complete(self, action_id, **kwargs):
        self.completed.append(action_id)

    def fail(self, action_id, *, reason, **kwargs):
        self.failed.append(reason)

    def show(self, action_id):
        return {
            "action": {
                "id": action_id,
                "status": self.current_status,
                "cancel_request_id": "cancel-1",
            }
        }

    def acknowledge_cancellation(self, *args, **kwargs):
        return None


def _daemon(tmp_path: Path, client: _Client | None = None) -> Daemon:
    return Daemon(
        DaemonConfig(
            heartbeat_interval_seconds=0.01,
            session_state_path=tmp_path / "session.json",
            pause_file=tmp_path / "PAUSED",
            completion_outbox_path=tmp_path / "completion.sqlite3",
        ),
        {"fake": ActionConfig(runner="fake", fake_duration_seconds=0.01)},
        client or _Client(),
    )


def _record(outcome: str = "completed") -> SessionRecord:
    return SessionRecord(
        session_id="aqs:stable",
        runtime_session_id="aqs:stable",
        daemon_id="daemon:test",
        action_id=42,
        action_type="fake",
        project="actionq",
        target_ref=None,
        runner="fake",
        pid=123,
        started_at="2026-08-09T10:00:00Z",
        updated_at="2026-08-09T10:00:00Z",
        harness="opencode",
        model="opencode-go",
    )


@pytest.mark.parametrize(
    ("outcome", "exit_code", "usage_limited", "kind", "reason"),
    [
        ("completed", 0, False, "succeeded", "completed"),
        ("failed", 7, False, "failed", "process-exit"),
        ("start-failed", None, False, "failed", "start-failed"),
        ("timed-out", -15, False, "timed-out", "timeout"),
        ("cancelled", -15, False, "cancelled", "cancelled"),
        ("failed", 1, True, "usage-limited", "usage-limit"),
        ("end-inferred", None, False, "end-inferred", "crash-inferred"),
    ],
)
def test_daemon_terminal_mapping_and_replay_are_stable(
    tmp_path: Path,
    outcome: str,
    exit_code: int | None,
    usage_limited: bool,
    kind: str,
    reason: str,
) -> None:
    daemon = _daemon(tmp_path)
    record = _record()
    assert daemon._record_session_completion(
        record, outcome=outcome, exit_code=exit_code, usage_limited=usage_limited
    )
    assert daemon._record_session_completion(
        record, outcome=outcome, exit_code=exit_code, usage_limited=usage_limited
    )
    events = CompletionOutbox(tmp_path / "completion.sqlite3").pending()
    assert len(events) == 1
    assert events[0]["payload"]["terminal"]["kind"] == kind
    assert events[0]["payload"]["terminal"]["reason_code"] == reason
    assert events[0]["payload"]["refs"] == []


def test_daemon_conflicting_replay_reaches_outbox_quarantine(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path)
    record = _record()
    assert daemon._record_session_completion(record, outcome="completed", exit_code=0)
    assert not daemon._record_session_completion(record, outcome="failed", exit_code=7)
    health = CompletionOutbox(tmp_path / "completion.sqlite3").health()
    assert health["quarantined"] == 1


def test_fake_daemon_records_after_child_exit_and_preserves_action_outcome(
    tmp_path: Path,
) -> None:
    client = _Client({"id": 7, "action_type": "fake", "project": "actionq"})
    daemon = _daemon(tmp_path, client)
    original_recorder = daemon._record_session_completion
    observed_after_exit: list[bool] = []

    def recorder(*args, **kwargs):
        observed_after_exit.append(daemon._child is not None and daemon._child.poll() is not None)
        return original_recorder(*args, **kwargs)

    daemon._record_session_completion = recorder  # type: ignore[method-assign]
    assert daemon.run_once()
    event = CompletionOutbox(tmp_path / "completion.sqlite3").pending()[0]["payload"]
    assert event["terminal"]["kind"] == "succeeded"
    assert client.completed == [7]
    assert observed_after_exit == [True]
    assert [name for name, _ in client.events].index("session.started") < [
        name for name, _ in client.events
    ].index("session.exited")


def test_daemon_completion_validates_with_canonical_agentops_contract(
    tmp_path: Path,
) -> None:
    daemon = _daemon(tmp_path)
    daemon._record_session_completion(_record(), outcome="completed", exit_code=0)
    event = CompletionOutbox(tmp_path / "completion.sqlite3").pending()[0]["payload"]
    target = tmp_path / "session-completions"
    target.mkdir()
    (target / "daemon.json").write_text(json.dumps(event), encoding="utf-8")
    validator = Path(
        "/projects/dev/_projects/vuoro-dispatch-ready/members/agentops/"
        "templates/dispatch/scripts/validate_session_mechanization_artifacts.py"
    )
    completed = subprocess.run(
        [sys.executable, str(validator), "--root", str(tmp_path)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_recorder_failure_does_not_change_fake_daemon_outcome(
    tmp_path: Path, monkeypatch
) -> None:
    client = _Client({"id": 8, "action_type": "fake", "project": "actionq"})
    daemon = _daemon(tmp_path, client)
    monkeypatch.setattr(
        "actionq.completion_outbox.CompletionOutbox.record",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("network unavailable")),
    )
    assert daemon.run_once()
    assert client.completed == [8]
    assert client.failed == []


def test_real_fake_opencode_runs_contained_then_coordinator_publishes(
    tmp_path: Path,
) -> None:
    observed = tmp_path / "worker-observed.json"
    opencode = tmp_path / "opencode"
    opencode.write_text(
        f"#!{sys.executable}\n"
        "import json,os,pathlib,sys\n"
        f"pathlib.Path({str(observed)!r}).write_text(json.dumps({{'argv': sys.argv[1:], 'environment': dict(os.environ)}}))\n",
        encoding="utf-8",
    )
    opencode.chmod(0o755)
    runner = tmp_path / "fake-runner"
    runner.write_text(
        f"#!{sys.executable}\n"
        "import json,subprocess,sys\n"
        "packet=json.load(sys.stdin)\n"
        "raise SystemExit(subprocess.run(packet['command'],cwd=packet['cwd'],env=packet['environment']).returncode)\n",
        encoding="utf-8",
    )
    runner.chmod(0o755)
    sudo = tmp_path / "sudo"
    sudo.write_text(
        f"#!{sys.executable}\n"
        "import os,sys\n"
        "args=sys.argv[1:]; command=args[args.index('--')+1:]; os.execv(command[0],command)\n",
        encoding="utf-8",
    )
    sudo.chmod(0o755)
    repo = tmp_path / "repo"
    repo.mkdir()
    runner_in_repo = repo / "fake-runner"
    runner.replace(runner_in_repo)
    runner = runner_in_repo
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    client = _Client({"id": 10, "action_type": "job", "project": "actionq"})
    config = DaemonConfig(
        heartbeat_interval_seconds=0.01,
        session_state_path=tmp_path / "session.json",
        pause_file=tmp_path / "PAUSED",
        completion_outbox_path=tmp_path / "completion.sqlite3",
        runnerctl_bin=str(runner),
        enforce_worker_isolation=True,
        routing=RoutingContext(
            harnesses={"opencode": HarnessRoute("opencode", bin=str(opencode))}
        ),
    )
    daemon = Daemon(
        config,
        {
            "job": ActionConfig(
                runner="harness", harness="opencode", model="go", prompt="work",
                worker_user="nobody",
            )
        },
        client,
        {
            "actionq": ProjectConfig(
                repo,
                default_harness="opencode",
                default_model="go",
                env={
                    "ACTIONQ_COMPLETION_INGEST_TOKEN": "never-pass",
                    "COMPLETION_INGEST_CREDENTIAL": "alias-never-pass",
                    "COORDINATOR_ONLY_EXTRA": "never-pass",
                    "OPENCODE_CONFIG": "/safe/opencode.json",
                },
            )
        },
    )
    original_recorder = daemon._record_session_completion
    original_which = __import__("actionq.daemon", fromlist=["shutil"]).shutil.which
    __import__("actionq.daemon", fromlist=["shutil"]).shutil.which = (
        lambda name: str(sudo) if name == "sudo" else original_which(name)
    )

    def recorder(*args, **kwargs):
        assert observed.exists(), "coordinator published before OpenCode child exit"
        return original_recorder(*args, **kwargs)

    daemon._record_session_completion = recorder  # type: ignore[method-assign]
    try:
        assert daemon.run_once()
    finally:
        __import__("actionq.daemon", fromlist=["shutil"]).shutil.which = original_which
    assert client.failed == []
    worker_observed = json.loads(observed.read_text(encoding="utf-8"))
    assert worker_observed["argv"] == ["run", "--model", "go", "work"]
    worker_env = worker_observed["environment"]
    assert worker_env.get("OPENCODE_CONFIG") == "/safe/opencode.json"
    assert "ACTIONQ_COMPLETION_INGEST_TOKEN" not in worker_env
    assert "COMPLETION_INGEST_CREDENTIAL" not in worker_env
    assert "COORDINATOR_ONLY_EXTRA" not in worker_env
    event = CompletionOutbox(tmp_path / "completion.sqlite3").pending()[0]["payload"]
    assert event["harness"] == "opencode"
    assert event["terminal"]["kind"] == "succeeded"
    assert client.completed == [10]


@pytest.mark.parametrize(
    ("command", "timeout_minutes", "kind"),
    [
        ((sys.executable, "-c", "import sys; sys.exit(6)"), None, "failed"),
        ((sys.executable, "-c", "import time; time.sleep(10)"), 0.001, "timed-out"),
    ],
)
def test_real_daemon_child_nonzero_and_timeout_paths(
    tmp_path: Path,
    command: tuple[str, ...],
    timeout_minutes: float | None,
    kind: str,
) -> None:
    client = _Client({"id": 11, "action_type": "command", "project": "actionq"})
    config = DaemonConfig(
        heartbeat_interval_seconds=60,
        graceful_shutdown_seconds=0.01,
        session_state_path=tmp_path / "session.json",
        pause_file=tmp_path / "PAUSED",
        completion_outbox_path=tmp_path / "completion.sqlite3",
    )
    daemon = Daemon(
        config,
        {
            "command": ActionConfig(
                runner="command", command=command, timeout_minutes=timeout_minutes  # type: ignore[arg-type]
            )
        },
        client,
    )
    assert daemon.run_once()
    event = CompletionOutbox(tmp_path / "completion.sqlite3").pending()[0]["payload"]
    assert event["terminal"]["kind"] == kind


def test_real_daemon_cancellation_control_records_cancelled(tmp_path: Path) -> None:
    client = _Client({"id": 12, "action_type": "command", "project": "actionq"})
    daemon = Daemon(
        DaemonConfig(
            cancellation_poll_interval_seconds=0.01,
            graceful_shutdown_seconds=0.01,
            session_state_path=tmp_path / "session.json",
            pause_file=tmp_path / "PAUSED",
            completion_outbox_path=tmp_path / "completion.sqlite3",
        ),
        {
            "command": ActionConfig(
                runner="command",
                command=(sys.executable, "-c", "import time; time.sleep(10)"),
            )
        },
        client,
    )
    timer = threading.Timer(0.1, lambda: setattr(client, "current_status", "cancelling"))
    timer.start()
    try:
        assert daemon.run_once()
    finally:
        timer.cancel()
    event = CompletionOutbox(tmp_path / "completion.sqlite3").pending()[0]["payload"]
    assert event["terminal"]["kind"] == "cancelled"


def test_real_daemon_runner_start_failure_records_start_failed(tmp_path: Path) -> None:
    client = _Client({"id": 13, "action_type": "fake", "project": "actionq"})
    daemon = Daemon(
        DaemonConfig(
            runnerctl_bin=str(tmp_path / "missing-runner"),
            session_state_path=tmp_path / "session.json",
            pause_file=tmp_path / "PAUSED",
            completion_outbox_path=tmp_path / "completion.sqlite3",
        ),
        {"fake": ActionConfig(runner="fake")},
        client,
    )
    with pytest.raises(FileNotFoundError):
        daemon.run_once()
    event = CompletionOutbox(tmp_path / "completion.sqlite3").pending()[0]["payload"]
    assert event["terminal"]["reason_code"] == "start-failed"


def test_real_daemon_stale_state_records_inferred_crash(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path)
    record = _record()
    record.pid = 999999999
    daemon._write_state(record)
    assert daemon.recover_stale_state() is False
    event = CompletionOutbox(tmp_path / "completion.sqlite3").pending()[0]["payload"]
    assert event["terminal"]["kind"] == "end-inferred"
