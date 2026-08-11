from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from actionq.daemon import ActionConfig, Daemon, DaemonConfig, ProjectConfig, SessionRecord, _deadline_after, _now
from actionq.lifecycle import BoundedLifecycleProfile
from actionq.routing import HarnessRoute, RoutingContext


class LifecycleClient:
    def __init__(self, action: dict):
        self.action = action
        self.events: list[tuple[str, int | None, str, dict]] = []
        self.settled: list[dict] = []
        self.status = "claimed"

    def claim(self, worker, timeout_minutes):
        action, self.action = self.action, None
        return action

    def renew(self, action_id, *, worker, timeout_minutes, claim_receipt):
        return None

    def show(self, action_id):
        return {"action": {"id": action_id, "status": self.status}}

    def emit(self, event_type, *, action_id, actor, payload):
        self.events.append((event_type, action_id, actor, payload))

    def settle(self, action_id, *, result, actor, claim_receipt):
        self.settled.append(result)
        self.status = "completed" if result["terminal_status"] in {"completed", "no_change"} else "failed"

    def reconcile_runner_spool(self, action_id, *, attempt_id):
        return None


def _fake_opencode(path: Path, *, declaration: bool, terminal_status: str = "completed") -> None:
    active_path = path.parent.parent / f".{path.parent.name}-work-pid"
    declaration_value = {
        "contract_id": "dispatch-finalization/v1", "action_id": 81,
        "attempt_id": "aqs:81", "session_id": "ses_81",
        "terminal_status": terminal_status, "summary": "verified",
    }
    declaration_line = (
        "events.append(" + repr({
            "type": "text", "sessionID": "ses_81", "timestamp": 2,
            "part": {"type": "text", "text": json.dumps(declaration_value, separators=(",", ":"))},
        }) + ")\n"
        if declaration else ""
    )
    path.write_text(
        f"#!{sys.executable}\n"
        "import json,os,sys,time\n"
        "from pathlib import Path\n"
        f"active=Path({str(active_path)!r})\n"
        "if '--continue' in sys.argv and active.exists():\n"
        "    try: os.kill(int(active.read_text()),0)\n"
        "    except ProcessLookupError: pass\n"
        "    else: sys.exit(9)\n"
        "if '--continue' not in sys.argv:\n"
        "    active.write_text(str(os.getpid()))\n"
        "events=[{'type':'step_start','sessionID':'ses_81','timestamp':1,'part':{'type':'step-start'}}]\n"
        "if '--continue' in sys.argv:\n"
        f"    {declaration_line}"
        "events.append({'type':'step_finish','sessionID':'ses_81','timestamp':3,'part':{'type':'step-finish'}})\n"
        "[print(json.dumps(event,separators=(',',':')),flush=True) for event in events]\n"
        "if '--continue' not in sys.argv: time.sleep(10)\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _daemon(
    tmp_path: Path, *, declaration: bool, terminal_status: str = "completed",
) -> tuple[Daemon, LifecycleClient]:
    repo = tmp_path / "repo"
    repo.mkdir()
    fake = repo / "opencode.py"
    _fake_opencode(fake, declaration=declaration, terminal_status=terminal_status)
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    subprocess.run(["git", "add", "opencode.py"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "fake harness"], cwd=repo, check=True)
    client = LifecycleClient({
        "id": 81, "action_type": "opencode", "project": "demo",
        "attempt_id": "aqs:81", "claim_attempt_id": "aqs:81",
        "claim_receipt": "receipt", "runner_auth_token": "runner",
    })
    config = DaemonConfig(
        artifact_root=tmp_path / "cas", runnerctl_bin=str(Path(sys.executable).parent / "actionq-runner"),
        session_state_path=tmp_path / "control/state.json", pause_file=tmp_path / "PAUSED",
        enforce_worker_isolation=False,
        routing=RoutingContext(
            harnesses={"opencode": HarnessRoute("opencode", bin=str(fake), provider="opencode-go")},
        ),
    )
    config.artifact_root.mkdir(mode=0o700)
    config.artifact_root.chmod(0o700)
    daemon = Daemon(
        config,
        {"opencode": ActionConfig(
            runner="harness", harness="opencode", prompt="work", model="test-model",
            worker_user="agentworker", harness_profile="opencode-nixpkgs-devbox-1.18.4",
            lifecycle_profile=BoundedLifecycleProfile(),
        )},
        client,
        {"demo": ProjectConfig(repo)},
    )
    return daemon, client


def test_bounded_lifecycle_requires_finalizer_declaration(tmp_path: Path):
    daemon, client = _daemon(tmp_path, declaration=True)
    assert daemon.run_once() is True
    assert client.settled[0]["terminal_status"] == "completed", [
        (event[0], event[3].get("reason"), event[3].get("outcome")) for event in client.events
    ]
    events = [event[0] for event in client.events]
    assert events.index("session.finalizing") < events.index("session.finalized") < events.index("session.exited")


def test_bounded_lifecycle_exit_without_declaration_cannot_complete(tmp_path: Path):
    daemon, client = _daemon(tmp_path, declaration=False)
    assert daemon.run_once() is True
    assert client.settled[0]["terminal_status"] == "failed"
    assert "session.finalization-rejected" in [event[0] for event in client.events]


def test_bounded_lifecycle_declared_failure_is_not_projected_as_success(tmp_path: Path):
    daemon, client = _daemon(tmp_path, declaration=True, terminal_status="failed")
    assert daemon.run_once() is True
    assert client.settled[0]["terminal_status"] == "failed"
    exited = next(event[3] for event in client.events if event[0] == "session.exited")
    assert exited["outcome"] == "failed"


def _stale_working_record(daemon: Daemon, tmp_path: Path) -> SessionRecord:
    repo = daemon.projects["demo"].path
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, text=True, check=True, capture_output=True,
    ).stdout.strip()
    return SessionRecord(
        session_id="aqs:81", runtime_session_id="aqs:81", daemon_id="old-daemon",
        action_id=81, action_type="opencode", project="demo", target_ref=None,
        runner="harness", pid=99999999, started_at=_now(), updated_at=_now(),
        worktree=str(repo), base_commit=commit, harness="opencode",
        provider="opencode-go", model="test-model", requested_selector="test-model",
        routing_source="action-class", phase="working", harness_session_id="ses_81",
        total_deadline_at=_deadline_after(300),
    )


def test_restart_finalizes_same_session_only_after_receipt_renewal(tmp_path: Path):
    daemon, client = _daemon(tmp_path, declaration=True)
    client.action = None
    record = _stale_working_record(daemon, tmp_path)
    daemon._progress_path(record.session_id).parent.mkdir(parents=True, exist_ok=True)
    daemon._progress_path(record.session_id).write_text("\n".join([
        json.dumps({"type": "step_start", "sessionID": "ses_81", "timestamp": 1, "part": {"type": "step-start"}}),
        json.dumps({"type": "step_finish", "sessionID": "ses_81", "timestamp": 2, "part": {"type": "step-finish"}}),
    ]) + "\n", encoding="utf-8")
    daemon._write_state(record)
    daemon._write_recovery_authority(
        session_id=record.session_id, claim_receipt="receipt", runner_auth_token="runner",
    )

    assert daemon.recover_stale_state() is False
    assert client.settled[0]["terminal_status"] == "completed"
    assert "session.restart-finalized" in [event[0] for event in client.events]
    assert daemon.config.session_state_path.read_text(encoding="utf-8") == "{}"
    assert not daemon._authority_path().exists()


def test_restart_fails_closed_when_claim_receipt_cannot_be_renewed(tmp_path: Path):
    daemon, client = _daemon(tmp_path, declaration=True)
    client.action = None
    record = _stale_working_record(daemon, tmp_path)
    daemon._write_state(record)
    daemon._write_recovery_authority(
        session_id=record.session_id, claim_receipt="stale", runner_auth_token="runner",
    )
    client.renew = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("stale claim"))

    assert daemon.recover_stale_state() is False
    assert client.settled == []
    assert "session.finalizing" not in [event[0] for event in client.events]
    assert "session.end-inferred" in [event[0] for event in client.events]
