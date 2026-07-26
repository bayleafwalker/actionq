"""Real disposable ActionQ/Postgres + Sprintctl/SQLite daemon coverage."""
from __future__ import annotations

import os
from pathlib import Path
import stat
import sys
import uuid

import pytest

from actionq import db
from actionq.daemon import ActionConfig, ActionctlClient, ContextConfig, Daemon, DaemonConfig, ProjectConfig, SprintctlClaimClient


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


def _make_daemon(monkeypatch, tmp_path: Path, *, daemon_type=Daemon, claim_client_type=SprintctlClaimClient, duration=0.12) -> tuple[Daemon, str, int, Path, int]:
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
        DaemonConfig(heartbeat_interval_seconds=0.01, session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED", actionctl_bin=actionctl, context=ContextConfig(enabled=True, remote_only=False, sprintctl_bin=str(sprintctl))),
        {"scope-iterate": ActionConfig(fake_duration_seconds=duration)}, ActionctlClient(actionctl),
        {"demo": ProjectConfig(tmp_path, sprint_id=1, env={"SPRINTCTL_DB": str(sprint_path), "SPRINTCTL_BACKEND": "local"})}, claim_client=claim_client_type(str(sprintctl)),
    )
    return daemon, schema, action["id"], sprint_path, item_id


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


def test_real_sprintctl_authority_renews_and_releases_before_actionq_completion(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(monkeypatch, tmp_path)
    assert daemon.run_once() is True
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
    assert _text(action["status"]) == "completed"
    assert "settlement.sprint_claim_released" in events, events
    assert events.index("settlement.pending") < events.index("settlement.sprint_claim_released") < events.index("action_completed")
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_claims(conn, item_id, active_only=True) == []
    finally:
        conn.close()


class _RevokingSprintctlClient(SprintctlClaimClient):
    """Inject ownership loss through the real Sprintctl authority."""

    def renew(self, project, *, claim_id, claim_token, actor, ttl_seconds, runtime_session_id):
        self.release(project, claim_id=claim_id, claim_token=claim_token, actor=actor)
        return super().renew(
            project, claim_id=claim_id, claim_token=claim_token, actor=actor,
            ttl_seconds=ttl_seconds, runtime_session_id=runtime_session_id,
        )


def test_real_sprintctl_ownership_loss_stops_child_and_fences_actionq_settlement(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(monkeypatch, tmp_path, claim_client_type=_RevokingSprintctlClient, duration=2)
    assert daemon.run_once() is True
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
    # Loss of either authority forbids this worker from terminal settlement;
    # ActionQ remains reclaimable rather than accepting a stale conclusion.
    assert _text(action["status"]) == "claimed"
    assert "action_completed" not in events
    assert "session.paused" in events
    sys.path.insert(0, str(SPRINTCTL_ROOT))
    from sprintctl import db as sprint_db
    conn = sprint_db.get_connection(sprint_path)
    try:
        assert sprint_db.list_claims(conn, item_id, active_only=True) == []
    finally:
        conn.close()


class _CrashAfterSprintReleaseDaemon(Daemon):
    """Model an abrupt process death at the cross-authority boundary."""

    def _after_sprint_claim_release(self, lease):
        raise SystemExit("fault injection: crash after Sprintctl release")


def test_crash_after_sprint_release_leaves_reclaimable_actionq_work(monkeypatch, tmp_path: Path):
    daemon, schema, action_id, sprint_path, item_id = _make_daemon(
        monkeypatch, tmp_path, daemon_type=_CrashAfterSprintReleaseDaemon,
    )
    with pytest.raises(SystemExit, match="crash after Sprintctl release"):
        daemon.run_once()
    with db.connect() as conn:
        action = db.get_action(conn, schema, action_id)
        events = [_text(event["event_type"]) for event in db.action_events(conn, schema, action_id)]
        assert _text(action["status"]) == "claimed"
        assert "settlement.sprint_claim_released" in events
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
        assert sprint_db.list_claims(conn, item_id, active_only=True) == []
    finally:
        conn.close()
