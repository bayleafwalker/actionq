from __future__ import annotations

import json
import hashlib
import os
import subprocess
import time
from dataclasses import asdict
from pathlib import Path

import pytest

from actionq_contracts import (
    EXECUTION_ENVELOPE_V1, Execution, ExecutionEnvelope, require_compatible,
)
from actionq_runner.oci import oci_execute
from actionq_runner.publisher import FilesystemCAS, publish, recover_publication


ENGINE = os.environ.get("ACTIONQ_OCI_ENGINE")
IMAGE = os.environ.get("ACTIONQ_OCI_IMAGE")
SECCOMP_SHA256 = (
    hashlib.sha256((Path(ENGINE).parent.parent / "share/containers/seccomp.json").read_bytes()).hexdigest()
    if ENGINE else None
)
pytestmark = pytest.mark.skipif(
    not ENGINE or not IMAGE, reason="rootless OCI engine/image proof is not configured",
)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, text=True,
        capture_output=True, env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C"},
    ).stdout.strip()


def test_real_rootless_runner_emits_publishable_equivalent_lifecycle(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir(mode=0o700)
    git(workspace, "init", "-q")
    git(workspace, "config", "user.email", "runner@example.invalid")
    git(workspace, "config", "user.name", "Runner")
    (workspace / "docs").mkdir()
    (workspace / "docs/original.txt").write_text("source\n")
    (workspace / "docs/executable.sh").write_text("#!/bin/sh\nexit 0\n")
    (workspace / "docs/executable.sh").chmod(0o755)
    git(workspace, "add", ".")
    git(workspace, "commit", "-qm", "source")
    source = git(workspace, "rev-parse", "HEAD")

    envelope = ExecutionEnvelope(
        EXECUTION_ENVELOPE_V1, 2033, "oci-real-proof", source,
        "fixture:oci-scope-iterate", ("docs/**",),
    )
    worker_script = r'''
import json, os, pathlib, socket
root = pathlib.Path("docs")
(root / "original.txt").rename(root / "renamed.txt")
(root / "binary.bin").write_bytes(bytes(range(256)))
(root / "link").symlink_to("renamed.txt")
(root / "executable.sh").write_text("#!/bin/sh\nexit 7\n")
probe = {
  "uid": os.getuid(),
  "effective_capabilities": next(line.split()[1] for line in pathlib.Path("/proc/self/status").read_text().splitlines() if line.startswith("CapEff:")),
  "projects_visible": pathlib.Path("/projects/dev").exists(),
  "engine_socket": pathlib.Path("/var/run/docker.sock").exists() or pathlib.Path("/run/podman/podman.sock").exists(),
  "service_account": pathlib.Path("/var/run/secrets/kubernetes.io/serviceaccount/token").exists(),
  "forbidden_env": sorted(k for k in os.environ if any(x in k.upper() for x in ("ACTIONQ", "SPRINT", "KUBE", "SSH_", "TOKEN", "SECRET", "PASSWORD"))),
}
s = socket.socket(); s.settimeout(0.2); probe["network_reachable"] = s.connect_ex(("1.1.1.1", 443)) == 0; s.close()
(root / "isolation.json").write_text(json.dumps(probe, sort_keys=True))
'''
    command = [
        "sh", "-ceu",
        "python -c \"$WORKER_SCRIPT\"; git add docs; git -c user.name=Runner -c user.email=runner@example.invalid commit -qm oci-proof",
    ]
    result = oci_execute({
        "engine": ENGINE, "image": IMAGE, "envelope": envelope.as_dict(),
        "seccomp_sha256": SECCOMP_SHA256,
        "registered_command_id": envelope.command_id, "deterministic": True,
        "command": command, "workspace": str(workspace), "network": "none",
        "uid": os.geteuid(), "environment": {
            "HOME": "/nonexistent", "LANG": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin",
            "WORKER_SCRIPT": worker_script,
        },
        "limits": {"cpus": 2, "memory_bytes": 2 * 1024**3, "pids": 128,
                   "timeout_seconds": 60, "disk_bytes": 64 * 1024**2},
    })
    assert result["exit_code"] == 0 and not result["timed_out"] and not result["cancelled"], json.dumps(result)
    probe = json.loads((workspace / "docs/isolation.json").read_text())
    assert probe == {
        "effective_capabilities": "0000000000000000", "engine_socket": False,
        "forbidden_env": [], "network_reachable": False,
        "projects_visible": False, "service_account": False, "uid": os.geteuid(),
    }
    assert set(result["changed_paths"]) == {
        "docs/binary.bin", "docs/isolation.json", "docs/link", "docs/original.txt", "docs/renamed.txt",
        "docs/executable.sh",
    }
    assert (workspace / "docs/link").is_symlink()
    assert os.access(workspace / "docs/executable.sh", os.X_OK)
    assert result["candidate_manifest"]["docs/link"]["mode"] == "120000"
    assert result["candidate_manifest"]["docs/executable.sh"]["mode"] == "100755"
    assert result["workspace_filesystem"]["capacity_bytes"] <= 64 * 1024**2 + 4096

    cas = tmp_path / "cas"
    cas.mkdir(mode=0o700)
    execution = Execution(2033, envelope.attempt_id, envelope.command_id, 0, False)
    publication = publish({
        "artifact_root": str(cas), "action_id": 2033, "attempt_id": envelope.attempt_id,
        "worktree": str(workspace), "source_commit": source,
        "candidate_commit": result["candidate_commit"],
        "execution_envelope": json.loads(json.dumps(envelope.as_dict())),
        "execution": asdict(execution), "verification_outcome": "passed",
        "verification_evidence": {"runner": "oci", "isolation": probe,
                                  "changed_paths": result["changed_paths"]},
        "redacted_log": result["log"],
    }, allow_unsafe_test_root=True)
    recovered = recover_publication(cas, 2033, envelope.attempt_id, allow_unsafe_test_root=True)
    assert recovered == publication
    store = FilesystemCAS(cas, allow_unsafe_test_root=True)
    released_records = {"execution_envelope", "candidate", "execution", "verification", "publication"}
    for record_name, record_ref in publication["records"].items():
        record = store.get(record_ref)
        if record_name in released_records:
            require_compatible(json.loads(record))


def test_real_rootless_runner_disk_enospc_yields_no_candidate(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir(mode=0o700)
    git(workspace, "init", "-q")
    git(workspace, "config", "user.email", "runner@example.invalid")
    git(workspace, "config", "user.name", "Runner")
    (workspace / "README.md").write_text("source\n")
    git(workspace, "add", ".")
    git(workspace, "commit", "-qm", "source")
    source = git(workspace, "rev-parse", "HEAD")
    envelope = ExecutionEnvelope(EXECUTION_ENVELOPE_V1, 2033, "oci-enospc", source,
                                 "fixture:oci-enospc", ("docs/**",))
    result = oci_execute({
        "engine": ENGINE, "image": IMAGE, "envelope": envelope.as_dict(),
        "seccomp_sha256": SECCOMP_SHA256,
        "registered_command_id": envelope.command_id, "deterministic": True,
        "command": ["dd", "if=/dev/zero", "of=payload", "bs=1M", "count=64", "conv=fsync"],
        "workspace": str(workspace), "network": "none", "uid": os.geteuid(),
        "environment": {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"},
        "limits": {"cpus": 1, "memory_bytes": 256 * 1024**2, "pids": 32,
                   "timeout_seconds": 60, "disk_bytes": 32 * 1024**2},
    })
    assert result["exit_code"] != 0
    assert result["candidate_bundle_sha256"] is None
    assert result["candidate_commit"] == source
    assert git(workspace, "rev-parse", "HEAD") == source


def test_real_wrapper_never_runs_worker_git_as_root_and_reaps_setsid_descendants(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir(mode=0o700)
    git(workspace, "init", "-q")
    git(workspace, "config", "user.email", "runner@example.invalid")
    git(workspace, "config", "user.name", "Runner")
    (workspace / "README.md").write_text("source\n")
    git(workspace, "add", ".")
    git(workspace, "commit", "-qm", "source")
    source = git(workspace, "rev-parse", "HEAD")
    envelope = ExecutionEnvelope(EXECUTION_ENVELOPE_V1, 2033, "oci-hostile-config", source,
                                 "fixture:oci-hostile-config", ("docs/**", ".gitattributes"))
    script = r'''
import os, pathlib, time
root = pathlib.Path("docs"); root.mkdir()
evil = root / "evil"
evil.write_text("#!/bin/sh\ntouch /control/privileged-marker\n")
evil.chmod(0o755)
pathlib.Path(".gitattributes").write_text("docs/proof.txt filter=evil\n")
(root / "proof.txt").write_text("proof\n")
os.system("git add . && git -c user.name=Runner -c user.email=runner@example.invalid commit -qm hostile")
os.system("git config core.fsmonitor docs/evil; git config core.hooksPath docs; git config filter.evil.clean docs/evil")
if os.fork() == 0:
    os.setsid()
    if os.fork() == 0:
        time.sleep(30); pathlib.Path("/control/descendant-survived").touch(); os._exit(0)
    os._exit(0)
'''
    started = time.monotonic()
    result = oci_execute({
        "engine": ENGINE, "image": IMAGE, "envelope": envelope.as_dict(),
        "seccomp_sha256": SECCOMP_SHA256,
        "registered_command_id": envelope.command_id, "deterministic": True,
        "command": ["python", "-c", script], "workspace": str(workspace),
        "network": "none", "uid": os.geteuid(),
        "environment": {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"},
        "limits": {"cpus": 1, "memory_bytes": 256 * 1024**2, "pids": 32,
                   "timeout_seconds": 60, "disk_bytes": 64 * 1024**2},
    })
    assert result["exit_code"] == 0
    assert time.monotonic() - started < 10
    assert result["workspace_filesystem"]["descendants_reaped"] is True
    assert set(result["changed_paths"]) == {".gitattributes", "docs/evil", "docs/proof.txt"}
