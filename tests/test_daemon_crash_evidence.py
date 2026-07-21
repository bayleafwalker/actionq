from __future__ import annotations

import subprocess
from pathlib import Path

from actionq.daemon import Daemon, DaemonConfig, ProjectConfig, SessionRecord

from tests.test_daemon import FakeClient


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)


def _make_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-q", "-m", "initial commit")
    return repo


def _stale_record(session_id: str, action_id: int, *, worktree: str | None, base_commit: str | None) -> SessionRecord:
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
        worktree=worktree,
        base_commit=base_commit,
    )


def _daemon(tmp_path: Path, client: FakeClient, *, project_path: Path | None = None) -> Daemon:
    config = DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED")
    projects = {"demo": ProjectConfig(project_path or tmp_path, sprint_id=None)} if project_path else {}
    return Daemon(config, {}, client, projects)


def test_recovery_collects_surviving_commit_evidence(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _git(repo, "rev-parse", "HEAD")
    base_commit = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True, capture_output=True
    ).stdout.strip()

    # Simulate work that survived the crash: a commit made after the
    # session's recorded start, before the daemon restarted and swept it.
    (repo / "partial_work.txt").write_text("half-finished\n", encoding="utf-8")
    _git(repo, "add", "partial_work.txt")
    _git(repo, "commit", "-q", "-m", "surviving work before crash")

    client = FakeClient()
    daemon = _daemon(tmp_path, client, project_path=repo)
    daemon._write_state(_stale_record("aqs:crashed", 40, worktree=str(repo), base_commit=base_commit))

    assert daemon.run_once() is False
    event_type, action_id, actor, payload = client.events[0]
    assert event_type == "session.end-inferred"
    assert payload["git"] is not None
    assert len(payload["git"]["commits"]) == 1
    assert "partial_work.txt" in payload["git"]["touched_paths"]
    assert payload["git"]["dirty"] is False


def test_recovery_reports_bounded_evidence_when_worktree_is_gone(tmp_path: Path):
    missing_worktree = str(tmp_path / "deleted-repo")
    client = FakeClient()
    daemon = _daemon(tmp_path, client)
    daemon._write_state(_stale_record("aqs:crashed-no-repo", 41, worktree=missing_worktree, base_commit="0" * 40))

    assert daemon.run_once() is False
    payload = client.events[0][3]
    assert payload["git"]["evidence_unavailable"] is True
    assert payload["git"]["commits"] == []
    # Recovery is not blocked by an unavailable worktree.
    assert Path(daemon.config.session_state_path).read_text() == "{}"


def test_recovery_leaves_git_null_when_no_worktree_was_ever_recorded(tmp_path: Path):
    client = FakeClient()
    daemon = _daemon(tmp_path, client)
    daemon._write_state(_stale_record("aqs:crashed-no-project", 42, worktree=None, base_commit=None))

    assert daemon.run_once() is False
    assert client.events[0][3]["git"] is None


def test_run_action_records_worktree_and_base_commit_for_configured_project(tmp_path: Path):
    import json
    import threading

    from actionq.daemon import ActionConfig

    repo = _make_repo(tmp_path)
    state_path = tmp_path / "live-state.json"
    client = FakeClient({"id": 50, "action_type": "scope-iterate", "project": "demo"})
    daemon = Daemon(
        DaemonConfig(
            heartbeat_interval_seconds=0.01,
            session_state_path=state_path,
            pause_file=tmp_path / "PAUSED",
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=2)},
        client,
        {"demo": ProjectConfig(repo, sprint_id=None)},
    )

    worker = threading.Thread(target=daemon.run_once)
    worker.start()
    assert client.started.wait(timeout=2)
    # While the session is still "running," the on-disk state carries the
    # worktree/base_commit fields collected at dispatch time.
    live_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert live_state["worktree"] == str(repo)
    assert len(live_state["base_commit"]) == 40
    daemon.request_shutdown()
    worker.join(timeout=3)
    assert not worker.is_alive()
