from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from actionq.daemon import ActionConfig, Daemon, DaemonConfig, ProjectConfig
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
    declaration_value = {
        "contract_id": "dispatch-finalization/v1", "action_id": 81,
        "attempt_id": "aqs:81", "session_id": "ses_81",
        "terminal_status": terminal_status, "summary": "verified",
    }
    declaration_line = (
        "events.append(" + repr({
            "type": "dispatch.finalization",
            "properties": {"sessionID": "ses_81"},
            "declaration": declaration_value,
        }) + ")\n"
        if declaration else ""
    )
    path.write_text(
        f"#!{sys.executable}\n"
        "import json,sys\n"
        "events=[{'type':'message.updated','properties':{'sessionID':'ses_81','info':{'role':'assistant'}}}]\n"
        "if '--continue' in sys.argv:\n"
        f"    {declaration_line}"
        "[print(json.dumps(event,separators=(',',':'))) for event in events]\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _daemon(
    tmp_path: Path, *, declaration: bool, terminal_status: str = "completed",
) -> tuple[Daemon, LifecycleClient]:
    fake = tmp_path / "opencode.py"
    _fake_opencode(fake, declaration=declaration, terminal_status=terminal_status)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "opencode.py"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "fake harness"], cwd=tmp_path, check=True)
    client = LifecycleClient({
        "id": 81, "action_type": "opencode", "project": "demo",
        "attempt_id": "aqs:81", "claim_attempt_id": "aqs:81",
        "claim_receipt": "receipt", "runner_auth_token": "runner",
    })
    config = DaemonConfig(
        artifact_root=tmp_path / "cas", runnerctl_bin=str(Path(sys.executable).parent / "actionq-runner"),
        session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
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
        {"demo": ProjectConfig(tmp_path)},
    )
    return daemon, client


def test_bounded_lifecycle_requires_finalizer_declaration(tmp_path: Path):
    daemon, client = _daemon(tmp_path, declaration=True)
    assert daemon.run_once() is True
    assert client.settled[0]["terminal_status"] == "completed"
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
