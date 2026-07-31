from __future__ import annotations

import io
import os
import subprocess
from pathlib import Path

from actionq.daemon import ActionConfig, Daemon, DaemonConfig, ProjectConfig
from actionq.scope_iterate import (
    PathACL, PreparedScopeIterate, ScopeIteratePolicy, ScopeIterateRequest,
)
from actionq_contracts import EXECUTION_ENVELOPE_V1, ExecutionEnvelope


IMAGE = "localhost/actionq-worker@sha256:" + "a" * 64


class Sink(io.StringIO):
    def __init__(self):
        super().__init__()
        self.packet = ""

    def close(self):
        self.packet = self.getvalue()
        super().close()


class Child:
    def __init__(self, argv, **kwargs):
        self.argv = list(argv)
        self.kwargs = kwargs
        self.stdin = Sink()
        self.pid = 4242


class Client:
    def __init__(self):
        self.events = []

    def emit(self, event_type, *, action_id, actor, payload):
        self.events.append((event_type, action_id, actor, payload))


def test_daemon_oci_child_packet_has_no_authority_and_exact_frozen_limits(
    tmp_path: Path, monkeypatch,
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    output = tmp_path / "output.log"
    made = []

    def popen(argv, **kwargs):
        child = Child(argv, **kwargs)
        made.append(child)
        return child

    monkeypatch.setattr(subprocess, "Popen", popen)
    daemon = Daemon(DaemonConfig(runnerctl_bin="runnerctl"), {}, Client())
    envelope = ExecutionEnvelope(
        EXECUTION_ENVELOPE_V1, 2033, "attempt-oci", "b" * 40,
        "proof:oci-scope-iterate", ("docs/**",),
    )
    config = ActionConfig(
        runner="oci-scope-iterate", command=("python", "-c", "print('ok')"),
        timeout_minutes=30, oci_engine="/usr/bin/podman", oci_image=IMAGE,
        oci_uid=os.geteuid(), oci_cpus=2, oci_memory_bytes=2 * 1024**3, oci_pids=128,
        oci_disk_bytes=10 * 1024**3,
        oci_seccomp_sha256="c" * 64,
    )
    child = daemon._start_child(
        config, project=ProjectConfig(workspace), output_path=output, envelope=envelope,
    )
    assert child.argv == ["runnerctl", "oci-execute", "--packet-stdin"]
    import json
    packet = json.loads(child.stdin.packet)
    assert packet == {
        "engine": "/usr/bin/podman", "image": IMAGE,
        "seccomp_sha256": "c" * 64,
        "envelope": json.loads(json.dumps(envelope.as_dict())),
        "registered_command_id": envelope.command_id, "deterministic": True,
        "command": ["python", "-c", "print('ok')"], "workspace": str(workspace),
        "network": "none", "uid": os.geteuid(),
        "environment": {"HOME": "/nonexistent", "LANG": "C.UTF-8",
                        "PATH": "/usr/local/bin:/usr/bin:/bin"},
        "limits": {"cpus": 2, "memory_bytes": 2 * 1024**3,
                   "pids": 128, "disk_bytes": 10 * 1024**3, "timeout_seconds": 1800},
    }
    assert not any(marker in child.stdin.packet for marker in (
        "claim_receipt", "runner_auth", "ACTIONQ_URL", "KUBECONFIG", "artifact_root",
    ))


def test_daemon_destroys_only_exact_supervisor_worktree_after_settlement(tmp_path: Path):
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repository, check=True)
    (repository / "README.md").write_text("source\n")
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "source"], cwd=repository, check=True)
    source = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True,
        text=True, capture_output=True,
    ).stdout.strip()
    worktree_root = tmp_path / "worktrees"
    worktree_root.mkdir()
    workspace = worktree_root / "attempt"
    subprocess.run(
        ["git", "worktree", "add", "-q", "-b", "agent/proof", str(workspace), source],
        cwd=repository, check=True,
    )
    prompt = tmp_path / "prompt.txt"
    prompt.write_text("proof")
    policy = ScopeIteratePolicy(
        worktree_root=worktree_root, prompt_template=prompt,
        path_acl=PathACL(
            allow=("docs/**",),
            deny=(".git/**", ".sprintctl/**", "secrets/**", "**/.env", "**/.env.*"),
        ),
    )
    request = ScopeIterateRequest(2033, "demo", repository, {}, {})
    prepared = PreparedScopeIterate(request, policy, workspace, "agent/proof", source, "proof")
    client = Client()
    daemon = Daemon(DaemonConfig(), {}, client)
    daemon._destroy_oci_workspace(2033, "attempt-oci", prepared)
    assert not workspace.exists()
    assert client.events[-1][0] == "workspace.destroyed"
    assert client.events[-1][3] == {"attempt_id": "attempt-oci", "workspace_destroyed": True}


def test_daemon_idempotently_reconciles_deterministic_oci_container(monkeypatch):
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    daemon = Daemon(DaemonConfig(), {}, Client())
    config = ActionConfig(runner="oci-scope-iterate", oci_engine="/usr/bin/podman")
    daemon._cleanup_oci_container(config, action_id=2033, attempt_id="attempt-oci")
    assert calls == [([
        "/usr/bin/podman", "rm", "--force", "--ignore",
        daemon._oci_container_name(2033, "attempt-oci"),
    ], {"text": True, "capture_output": True, "check": False})]
