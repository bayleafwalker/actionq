"""Real disposable ActionQ/Postgres + Sprintctl/SQLite daemon coverage."""
from __future__ import annotations

import os
from pathlib import Path
import stat
import sys
import threading
import uuid
import subprocess

import pytest

from actionq import db
from actionq.daemon import ActionConfig, ActionctlClient, ContextConfig, Daemon, DaemonConfig, ProjectConfig, SprintctlReservationClient
from actionq.routing import HarnessRoute, RoutingContext
from actionq.scope_iterate import PathACL, ScopeIteratePolicy, ToolACL


SPRINTCTL_ROOT = Path(os.environ.get("SPRINTCTL_TEST_SOURCE", Path(__file__).resolve().parents[2] / "sprintctl"))
pytestmark = pytest.mark.skipif(not SPRINTCTL_ROOT.joinpath("sprintctl", "cli.py").exists(), reason="Sprintctl source checkout unavailable")


def _wrapper(tmp_path: Path) -> Path:
    path = tmp_path / "sprintctl-test"
    path.write_text("#!" + sys.executable + "\nimport sys\n" + f"sys.path.insert(0, {str(SPRINTCTL_ROOT)!r})\n" + "from sprintctl.cli import cli\ncli()\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _sprint_db(tmp_path: Path) -> tuple[Path, int]:
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    path = tmp_path / "sprintctl.db"
    conn = sprint_db.get_connection(path)
    sprint_db.init_db(conn)
    sprint = sprint_db.create_sprint(conn, "fault matrix", status="active")
    track = sprint_db.get_or_create_track(conn, sprint, "execution")
    item = sprint_db.create_work_item(conn, sprint, track, "bounded", "test scope")
    conn.close()
    return path, item


def _make_daemon(
    monkeypatch,
    tmp_path: Path,
    *,
    daemon_type=Daemon,
    action_client_type=ActionctlClient,
    reservation_client_type=SprintctlReservationClient,
    duration=0.12,
) -> tuple[Daemon, str, int, Path, int]:
    schema = "aqcross_" + uuid.uuid4().hex
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    with db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"]) as conn:
        db.migrate(conn, schema)
    monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_RUNTIME_URL"])
    sprint_path, item_id = _sprint_db(tmp_path)
    with db.connect() as conn:
        action = db.enqueue(conn, schema, action_type="scope-iterate", project="demo", target_ref=str(item_id), source_refs=[], priority=100, parent_id=None, created_by="test:cross")
    actionctl = str(Path(sys.executable).with_name("actionctl"))
    sprintctl = _wrapper(tmp_path)
    daemon = daemon_type(
        DaemonConfig(heartbeat_interval_seconds=0.01, graceful_shutdown_seconds=0.1, session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED", actionctl_bin=actionctl, context=ContextConfig(enabled=True, remote_only=False, sprintctl_bin=str(sprintctl))),
        {"scope-iterate": ActionConfig(fake_duration_seconds=duration)}, action_client_type(actionctl),
        {"demo": ProjectConfig(tmp_path, sprint_id=1, env={"SPRINTCTL_DB": str(sprint_path), "SPRINTCTL_BACKEND": "local"})}, reservation_client=reservation_client_type(str(sprintctl)),
    )
    return daemon, schema, action["id"], sprint_path, item_id


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


def test_real_sprintctl_reservation_touches_and_releases_before_actionq_completion(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(monkeypatch, tmp_path)
    assert daemon.run_once() is True
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
    assert _text(action["status"]) == "completed"
    assert "settlement.sprint_reservation_released" in events, events
    assert events.index("settlement.pending") < events.index("settlement.sprint_reservation_released") < events.index("action_completed")
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_reservations(conn, item_id, active_only=True) == []
    finally:
        conn.close()


class _RevokingSprintctlClient(SprintctlReservationClient):
    """Inject an advisory reservation-touch failure."""

    def touch(self, project, *, reservation_id, session_id, correlation_ref):
        self.release(project, reservation_id=reservation_id, actor="fault-injection")
        return super().touch(
            project, reservation_id=reservation_id, session_id=session_id,
            correlation_ref=correlation_ref,
        )


def test_real_sprintctl_reservation_touch_loss_is_advisory(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(monkeypatch, tmp_path, reservation_client_type=_RevokingSprintctlClient, duration=2)
    assert daemon.run_once() is True
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
    # Sprintctl reservation state is advisory; ActionQ remains the execution
    # authority and can settle despite a failed reservation touch.
    assert _text(action["status"]) == "completed"
    assert "action_completed" in events
    assert "session.reservation_touch_failed" in events
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_reservations(conn, item_id, active_only=True) == []
    finally:
        conn.close()


class _CrashAfterSprintReleaseDaemon(Daemon):
    """Model an abrupt process death at the cross-authority boundary."""

    def _after_sprint_reservation_release(self, reservation):
        raise SystemExit("fault injection: crash after Sprintctl reservation release")


def test_crash_after_sprint_reservation_release_leaves_reclaimable_actionq_work(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(
        monkeypatch, tmp_path, daemon_type=_CrashAfterSprintReleaseDaemon,
    )
    with pytest.raises(SystemExit, match="crash after Sprintctl reservation release"):
        daemon.run_once()
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
        assert _text(action["status"]) == "claimed"
        assert "settlement.sprint_reservation_released" in events
        assert "action_completed" not in events
        conn.execute(f'UPDATE "{schema}".actions SET claim_deadline = now() - interval \'1 second\' WHERE id = %s', (action_id,))
        assert [row["id"] for row in db.sweep(conn, schema)] == [action_id]
        replacement = db.claim(conn, schema, worker="replacement", timeout_minutes=5)
        db.complete(conn, schema, action_id=action_id, worker="replacement", actor="replacement", claim_receipt=replacement["claim_receipt"], result_ref="recovered")
        assert _text(db.get_action(conn, schema, action_id)["status"]) == "completed"
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_reservations(conn, item_id, active_only=True) == []
    finally:
        conn.close()


class _LostActionqRenewResponseClient(ActionctlClient):
    """Accept the renewal in ActionQ, then model loss of its response."""

    def renew(self, action_id, *, worker, timeout_minutes, claim_receipt):
        super().renew(
            action_id, worker=worker, timeout_minutes=timeout_minutes,
            claim_receipt=claim_receipt,
        )
        raise RuntimeError("fault injection: ActionQ renewal response lost")


def test_actionq_renewal_lost_response_stops_child_without_terminal_settlement(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(
        monkeypatch, tmp_path, action_client_type=_LostActionqRenewResponseClient,
        duration=2,
    )
    assert daemon.run_once() is True
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
    assert _text(action["status"]) == "claimed"
    assert "claim_renewed" in events
    assert "session.paused" in events
    assert "settlement.actionq_skipped_claim_lost" in events
    assert not {"action_completed", "action_failed"} & set(events)
    assert daemon._child is None
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_reservations(conn, item_id, active_only=True) == []
    finally:
        conn.close()


class _FailingReleaseSprintctlClient(SprintctlReservationClient):
    """Keep the reservation active while making its release unavailable."""

    def release(self, project, *, reservation_id, actor):
        raise RuntimeError("fault injection: Sprintctl release unavailable")


def test_sprintctl_reservation_release_failure_is_journaled_without_blocking_actionq(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(
        monkeypatch, tmp_path, reservation_client_type=_FailingReleaseSprintctlClient,
    )
    assert daemon.run_once() is True
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
    assert _text(action["status"]) == "completed"
    assert "settlement.pending" in events
    assert "settlement.sprint_reservation_release_failed" in events
    assert "action_completed" in events
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert len(sprint_db.list_reservations(conn, item_id, active_only=True)) == 1
    finally:
        conn.close()


class _CapturingActionctlClient(ActionctlClient):
    def __init__(self, binary):
        super().__init__(binary)
        self.started = threading.Event()
        self.claim_receipt = None

    def claim(self, worker, timeout_minutes):
        action = super().claim(worker, timeout_minutes)
        if action is not None:
            self.claim_receipt = action["claim_receipt"]
            action.setdefault("runner_auth_token", "test-runner-auth")
        return action

    def emit(self, event_type, *, action_id, actor, payload):
        super().emit(event_type, action_id=action_id, actor=actor, payload=payload)
        if event_type == "session.started":
            self.started.set()


def test_shutdown_stops_child_releases_sprint_reservation_and_fences_old_receipt(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(
        monkeypatch, tmp_path, action_client_type=_CapturingActionctlClient,
        duration=10,
    )
    worker = threading.Thread(target=daemon.run_once)
    worker.start()
    assert daemon.client.started.wait(timeout=5)
    daemon.request_shutdown()
    worker.join(timeout=15)
    assert not worker.is_alive()
    assert daemon._child is None
    assert daemon.config.session_state_path.read_text() == "{}"
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
        assert _text(action["status"]) == "failed"
        assert "settlement.sprint_reservation_released" in events
        assert "session.exited" in events
        with pytest.raises(db.ActionQError):
            db.complete(
                conn, schema, action_id=action_id, worker=daemon.actor,
                actor=daemon.actor, claim_receipt=daemon.client.claim_receipt,
                result_ref="stale-after-shutdown",
            )
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_reservations(conn, item_id, active_only=True) == []
    finally:
        conn.close()


def test_scope_kernel_verification_failure_releases_sprint_reservation_before_fenced_failure(
    monkeypatch, tmp_path: Path,
):
    """A zero-exit worker cannot settle success without a verified commit."""
    schema = "aqcross_" + uuid.uuid4().hex
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    with db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"]) as conn:
        db.migrate(conn, schema)
    monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_RUNTIME_URL"])
    sprint_path, item_id = _sprint_db(tmp_path)
    with db.connect() as conn:
        action = db.enqueue(
            conn, schema, action_type="scope-iterate", project="demo",
            target_ref=str(item_id), source_refs=[], priority=100,
            parent_id=None, created_by="test:scope-fault",
        )
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(("git", "init"), cwd=repo, check=True, capture_output=True)
    subprocess.run(("git", "config", "user.name", "Test"), cwd=repo, check=True)
    subprocess.run(("git", "config", "user.email", "test@example.invalid"), cwd=repo, check=True)
    (repo / "README.md").write_text("# demo\n")
    subprocess.run(("git", "add", "README.md"), cwd=repo, check=True)
    subprocess.run(("git", "commit", "-m", "initial"), cwd=repo, check=True, capture_output=True)
    prompt = tmp_path / "prompt.md"
    prompt.write_text(
        "{action_json}\n{sprint_item_json}\n{working_dir}\n{branch_name}\n"
        "{allowed_scope}\n{test_command}\n"
    )
    policy = ScopeIteratePolicy(
        worktree_root=(tmp_path / "worktrees").resolve(),
        prompt_template=prompt.resolve(),
        path_acl=PathACL(
            ("docs/**",),
            (".git/**", ".sprintctl/**", "secrets/**", "**/.env", "**/.env.*"),
        ),
        tool_acl=ToolACL(),
        test_command=("python3", "-c", "pass"),
    )
    actionctl = str(Path(sys.executable).with_name("actionctl"))
    sprintctl = _wrapper(tmp_path)
    daemon = Daemon(
        DaemonConfig(
            heartbeat_interval_seconds=0.01,
            session_state_path=tmp_path / "state.json",
            pause_file=tmp_path / "PAUSED",
            actionctl_bin=actionctl,
            context=ContextConfig(
                enabled=True, remote_only=False, sprintctl_bin=str(sprintctl),
            ),
            routing=RoutingContext(harnesses={"codex": HarnessRoute("codex")}),
        ),
        {"scope-iterate": ActionConfig(
            runner="scope-iterate", harness="codex", model="test-model",
            worker_user="nobody",
            scope_iterate=policy,
        )},
        ActionctlClient(actionctl),
        {"demo": ProjectConfig(
            repo, sprint_id=1,
            env={"SPRINTCTL_DB": str(sprint_path), "SPRINTCTL_BACKEND": "local"},
        )},
        reservation_client=SprintctlReservationClient(str(sprintctl)),
    )

    daemon._start_child = lambda *_args, **_kwargs: subprocess.Popen(
        [sys.executable, "-c", "pass"], text=True, start_new_session=True,
    )
    daemon.client.reconcile_runner_spool = lambda *_args, **_kwargs: None
    assert daemon.run_once() is True

    with db.connect() as conn:
        stored = db.get_action(conn, schema, action["id"])
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action["id"])]
    assert _text(stored["status"]) == "failed"
    # The durable action row exposes only the frozen privacy-safe stop-reason
    # vocabulary; the detailed verifier reason remains in private evidence.
    assert stored["failure_reason"] == "verification-failed"
    assert events.index("settlement.sprint_reservation_released") < events.index("action_failed")
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_reservations(conn, item_id, active_only=True) == []
    finally:
        conn.close()
