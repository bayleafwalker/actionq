from __future__ import annotations

import json
import hashlib
import os
import signal
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from actionq_contracts import EXECUTION_ENVELOPE_V1
from actionq_runner.oci import OciPolicyError, oci_execute, oci_preflight


INFO = {
    "security": {"rootless": True, "seccomp": True},
    "cgroup": {"version": 2, "controllers": ["cpu", "memory", "pids", "io"]},
}
IMAGE = "registry.example/actionq-worker@sha256:" + "a" * 64


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def packet(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    git(workspace, "init", "-q")
    git(workspace, "config", "user.email", "runner@example.invalid")
    git(workspace, "config", "user.name", "Runner")
    (workspace / "README.md").write_text("source\n")
    git(workspace, "add", ".")
    git(workspace, "commit", "-qm", "source")
    source = git(workspace, "rev-parse", "HEAD")
    engine = tmp_path / "engine/bin/rootless-engine"
    profile = tmp_path / "engine/share/containers/seccomp.json"
    engine.parent.mkdir(parents=True)
    engine.write_text("fake\n")
    profile.parent.mkdir(parents=True)
    profile.write_text("{}\n")
    return {
        "engine": str(engine), "image": IMAGE,
        "seccomp_sha256": hashlib.sha256(b"{}\n").hexdigest(),
        "envelope": {
            "contract_id": EXECUTION_ENVELOPE_V1, "action_id": 2033,
            "attempt_id": "attempt-one", "source_commit": source,
            "command_id": "registered:offline-proof", "allowed_paths": ["docs/**"],
        },
        "registered_command_id": "registered:offline-proof", "deterministic": True,
        "command": ["python", "-c", "print('ok')"], "workspace": str(workspace),
        "network": "none", "uid": os.geteuid(), "environment": {"LANG": "C.UTF-8"},
        "limits": {"cpus": 1, "memory_bytes": 256 * 1024**2, "pids": 64, "timeout_seconds": 30},
    }


class Engine:
    def __init__(self):
        self.calls: list[list[str]] = []
        self.kwargs: list[dict] = []
        self.inspect = None

    def __call__(self, argv, **_kwargs):
        self.calls.append(list(argv))
        self.kwargs.append(dict(_kwargs))
        if argv[1] == "info":
            return SimpleNamespace(returncode=0, stdout=json.dumps(INFO), stderr="")
        if argv[1] == "create":
            def option(name):
                return argv[argv.index(name) + 1]
            volume = option("--volume")
            source = volume.split(":", 1)[0]
            tmpfs_values = [argv[index + 1] for index, item in enumerate(argv) if item == "--tmpfs"]
            security_values = [argv[index + 1] for index, item in enumerate(argv)
                               if item == "--security-opt"]
            self.inspect = [{
                "ImageDigest": "sha256:" + "a" * 64,
                "EffectiveCaps": ["CAP_CHOWN", "CAP_SETUID", "CAP_SETGID", "CAP_DAC_OVERRIDE", "CAP_KILL"],
                "BoundingCaps": ["CAP_CHOWN", "CAP_SETUID", "CAP_SETGID", "CAP_DAC_OVERRIDE", "CAP_KILL"],
                "Config": {"User": option("--user"), "Cmd": argv[argv.index(IMAGE) + 1:]},
                "Mounts": [{"Source": source, "Destination": "/input/source.bundle", "RW": False,
                            "Propagation": "rprivate", "Options": ["nosuid", "nodev", "noexec", "rbind"]}],
                "HostConfig": {
                    "NetworkMode": "none", "ReadonlyRootfs": True, "Privileged": False,
                    "PidMode": "private", "IpcMode": "private",
                    "CapAdd": [], "CapDrop": ["ALL"],
                    "SecurityOpt": security_values,
                    "Memory": int(option("--memory")),
                    "MemorySwap": int(option("--memory-swap")),
                    "NanoCpus": int(float(option("--cpus")) * 1_000_000_000),
                    "PidsLimit": int(option("--pids-limit")), "Devices": [],
                    "Annotations": {"io.podman.annotations.userns": "keep-id:uid=0,gid=0"},
                    "Tmpfs": {value.split(":", 1)[0]: value.split(":", 1)[1]
                              for value in tmpfs_values},
                },
            }]
        if argv[1] == "inspect":
            return SimpleNamespace(returncode=0, stdout=json.dumps(self.inspect), stderr="")
        if argv[1] == "exec":
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        if argv[1] == "cp":
            if argv[2].endswith("runner-metadata.json"):
                Path(argv[3]).write_text(json.dumps({"capacity_bytes": 32 * 1024**2,
                                                      "workspace_mode": "0o700", "command_exit": 0,
                                                      "descendants_reaped": True}))
            else:
                source = Path(self.inspect[0]["Mounts"][0]["Source"])
                Path(argv[3]).write_bytes(source.read_bytes())
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")


class Process:
    def __init__(self, argv, *, mutate=None, timeout_count=0, **_kwargs):
        self.argv = list(argv)
        self.returncode = 0
        self.mutate = mutate
        self.mutated = False
        self.timeout_count = timeout_count
        self.communications = 0

    def communicate(self, _input=None, timeout=None):
        self.communications += 1
        if self.communications <= self.timeout_count:
            raise subprocess.TimeoutExpired(self.argv, timeout)
        if self.mutate is not None and not self.mutated:
            self.mutate()
            self.mutated = True
        self.returncode = 0
        return b"worker ok\n", None

    def poll(self):
        return None


def factory(*, mutate=None, timeout_count=0):
    made = []
    kwargs_seen = []

    def create(argv, **kwargs):
        kwargs_seen.append(dict(kwargs))
        process = Process(argv, mutate=mutate, timeout_count=timeout_count, **kwargs)
        made.append(process)
        return process

    create.made = made
    create.kwargs_seen = kwargs_seen
    return create


def test_preflight_requires_rootless_seccomp_and_all_cgroup_v2_controllers():
    engine = Engine()
    assert oci_preflight("rootless-engine", run=engine) == {
        "engine": "rootless-engine", "rootless": True, "seccomp": True,
        "cgroup_version": 2, "cgroup_controllers": ["cpu", "io", "memory", "pids"],
    }
    assert engine.calls == [["rootless-engine", "info", "--format", "json"]]

    for changed, match in [
        ({"security": {"rootless": False, "seccomp": True}, "cgroup": INFO["cgroup"]}, "rootless"),
        ({"security": {"rootless": True, "seccomp": False}, "cgroup": INFO["cgroup"]}, "seccomp"),
        ({"security": INFO["security"], "cgroup": {"version": 1, "controllers": ["cpu", "memory", "pids"]}}, "cgroup v2"),
        ({"security": INFO["security"], "cgroup": {"version": 2, "controllers": ["cpu", "memory"]}}, "pids"),
    ]:
        def run(_argv, **_kwargs):
            return SimpleNamespace(returncode=0, stdout=json.dumps(changed), stderr="")
        with pytest.raises(OciPolicyError, match=match):
            oci_preflight("rootless-engine", run=run)

    def podman_run(_argv, **_kwargs):
        return SimpleNamespace(returncode=0, stdout=json.dumps({
            "host": {
                "security": {"rootless": True, "seccompEnabled": True},
                "cgroupVersion": "v2", "cgroupControllers": ["cpu", "memory", "pids"],
            }
        }), stderr="")
    assert oci_preflight("podman", run=podman_run)["cgroup_version"] == 2


@pytest.mark.parametrize("field,value,match", [
    ("image", "worker:latest", "immutable"),
    ("network", "bridge", "network"),
    ("uid", 0, "nonroot"),
    ("uid", 65534 if os.geteuid() != 65534 else 1000, "supervisor UID"),
    ("uid", "1000", "nonroot"),
    ("deterministic", False, "deterministic"),
    ("registered_command_id", "registered:other", "bound"),
])
def test_packet_rejects_each_identity_or_isolation_weakening(packet, field, value, match):
    packet[field] = value
    with pytest.raises(OciPolicyError, match=match):
        oci_execute(packet, run=Engine(), popen=factory())


@pytest.mark.parametrize("field,value", [
    ("cpus", 2.01), ("memory_bytes", 2 * 1024**3 + 1),
    ("pids", 129), ("timeout_seconds", 1801),
])
def test_packet_rejects_each_resource_limit_weakening(packet, field, value):
    packet["limits"][field] = value
    with pytest.raises(OciPolicyError, match=field):
        oci_execute(packet, run=Engine(), popen=factory())


@pytest.mark.parametrize("field,value", [
    ("privileged", True), ("host_pid", True), ("host_ipc", True),
    ("mounts", ["/:/host"]), ("devices", ["/dev/kvm"]),
    ("socket", "/var/run/docker.sock"),
])
def test_packet_has_no_surface_for_privilege_mount_device_or_socket(packet, field, value):
    packet[field] = value
    with pytest.raises(OciPolicyError, match="extra"):
        oci_execute(packet, run=Engine(), popen=factory())


@pytest.mark.parametrize("environment", [
    {"ACTIONQ_URL": "postgresql://db"}, {"PROVIDER_TOKEN": "value"},
    {"SAFE": "https://user:password@example.invalid"},
    {"SAFE": "Bearer abcdefghijklmnopqrstuvwxyz"},
])
def test_packet_rejects_authority_and_secret_environment(packet, environment):
    packet["environment"] = environment
    with pytest.raises(OciPolicyError, match="credential"):
        oci_execute(packet, run=Engine(), popen=factory())


def test_exact_create_argv_contains_every_frozen_control_and_no_secret(packet):
    engine = Engine()
    processes = factory()
    result = oci_execute(packet, run=engine, popen=processes)
    create = next(call for call in engine.calls if call[1] == "create")
    assert create == [
        packet["engine"], "create", "--name", create[3],
        "--pull", "never",
        "--network", "none", "--read-only", "--userns", "keep-id:uid=0,gid=0",
        "--user", "0:0",
        "--cap-drop", "ALL", "--cap-add", "CHOWN", "--cap-add", "SETUID",
        "--cap-add", "SETGID", "--cap-add", "DAC_OVERRIDE",
        "--cap-add", "KILL",
        "--security-opt", "no-new-privileges",
        "--security-opt", create[create.index("--security-opt", create.index("--security-opt") + 1) + 1],
        "--pids-limit", "64", "--cpus", "1", "--memory", str(256 * 1024**2),
        "--memory-swap", str(256 * 1024**2), "--pid", "private", "--ipc", "private",
        "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m",
        "--tmpfs", "/run:rw,noexec,nosuid,nodev,size=16m",
        "--tmpfs", "/control:rw,noexec,nosuid,nodev,size=1m,mode=0700",
        "--tmpfs", f"/workspace:rw,nosuid,nodev,size={10 * 1024**3},mode=0700",
        "--volume", create[create.index("--volume") + 1],
        "--workdir", "/workspace", "--env", "LANG=C.UTF-8", IMAGE,
        "python", "/usr/local/lib/actionq-oci-wrapper.py", "--source", "/input/source.bundle",
        "--source-commit", packet["envelope"]["source_commit"], "--candidate",
        "/workspace/candidate.bundle", "--worker-uid", str(os.geteuid()),
        "--", "python", "-c", "print('ok')",
    ]
    assert processes.made[0].argv == [
        packet["engine"], "start", "--attach", create[3],
    ]
    assert engine.calls[-1] == [packet["engine"], "rm", "--force", create[3]]
    candidate_cp = next(index for index, call in enumerate(engine.calls)
                        if call[1] == "cp" and call[2].endswith("candidate.bundle"))
    orderly_stop = next(index for index, call in enumerate(engine.calls)
                        if call[1:4] == ["kill", "--signal", "TERM"])
    removal = max(index for index, call in enumerate(engine.calls) if call[1] == "rm")
    assert candidate_cp < orderly_stop < removal
    flattened = "\0".join(item for call in engine.calls for item in call)
    assert not any(secret in flattened.upper() for secret in ("ACTIONQ_URL", "TOKEN", "PASSWORD", "DOCKER.SOCK"))
    assert result["candidate_commit"] == packet["envelope"]["source_commit"]
    assert result["changed_paths"] == [] and result["exit_code"] == 0


def test_stale_container_is_removed_before_create(packet):
    engine = Engine()
    oci_execute(packet, run=engine, popen=factory())
    stale_rm = next(index for index, call in enumerate(engine.calls) if call[1] == "rm")
    create = next(index for index, call in enumerate(engine.calls) if call[1] == "create")
    assert stale_rm < create


def test_sigterm_during_create_is_fenced_and_container_is_removed(packet):
    class SignalEngine(Engine):
        def __call__(self, argv, **kwargs):
            result = super().__call__(argv, **kwargs)
            if argv[1] == "create":
                os.kill(os.getpid(), signal.SIGTERM)
            return result

    engine = SignalEngine()
    result = oci_execute(packet, run=engine, popen=factory())
    assert result["cancelled"] is True
    assert not any(call[1] == "start" for call in engine.calls)
    assert engine.calls[-1][1:3] == ["rm", "--force"]


@pytest.mark.parametrize("weakening", [
    "network", "rootfs", "privileged", "memory", "pids", "image", "mount", "tmpfs", "userns", "wrapper", "seccomp",
])
def test_engine_inspect_weakening_fails_before_worker_start(packet, weakening):
    class WeakEngine(Engine):
        def __call__(self, argv, **kwargs):
            result = super().__call__(argv, **kwargs)
            if argv[1] == "inspect":
                value = self.inspect[0]
                host = value["HostConfig"]
                if weakening == "network": host["NetworkMode"] = "bridge"
                elif weakening == "rootfs": host["ReadonlyRootfs"] = False
                elif weakening == "privileged": host["Privileged"] = True
                elif weakening == "memory": host["Memory"] = 0
                elif weakening == "pids": host["PidsLimit"] = 0
                elif weakening == "image": value["ImageDigest"] = "sha256:" + "b" * 64
                elif weakening == "mount": value["Mounts"].append(dict(value["Mounts"][0]))
                elif weakening == "tmpfs": host["Tmpfs"].pop("/run")
                elif weakening == "userns": host["Annotations"].clear()
                elif weakening == "wrapper": value["Config"]["Cmd"] = ["sh"]
                elif weakening == "seccomp": host["SecurityOpt"][-1] = "seccomp=unconfined"
                return SimpleNamespace(returncode=0, stdout=json.dumps(self.inspect), stderr="")
            return result

    engine = WeakEngine()
    processes = factory()
    with pytest.raises(OciPolicyError, match="weakened"):
        oci_execute(packet, run=engine, popen=processes)
    assert not processes.made
    assert engine.calls[-1][1:3] == ["rm", "--force"]
    assert all("env" not in kwargs for kwargs in engine.kwargs)
    assert all("env" not in kwargs for kwargs in processes.kwargs_seen)


def test_workspace_requires_exact_clean_remoteless_source(packet):
    workspace = Path(packet["workspace"])
    git(workspace, "remote", "add", "origin", "https://example.invalid/repo.git")
    with pytest.raises(OciPolicyError, match="remotes"):
        oci_execute(packet, run=Engine(), popen=factory())
    git(workspace, "remote", "remove", "origin")
    (workspace / "dirty").write_text("x")
    with pytest.raises(OciPolicyError, match="clean"):
        oci_execute(packet, run=Engine(), popen=factory())
    (workspace / "dirty").unlink()
    packet["envelope"]["source_commit"] = "b" * 40
    with pytest.raises(OciPolicyError, match="exact source"):
        oci_execute(packet, run=Engine(), popen=factory())


def test_source_is_only_a_read_only_bundle_and_host_workspace_is_unchanged(packet):
    workspace = Path(packet["workspace"])
    before = git(workspace, "status", "--porcelain=v1")
    engine = Engine()
    result = oci_execute(packet, run=engine, popen=factory())
    volume = next(call for call in engine.calls if call[1] == "create")
    value = volume[volume.index("--volume") + 1]
    assert value.endswith(":/input/source.bundle:ro,rprivate,nosuid,nodev,noexec")
    assert ":/workspace:rw" not in value
    assert git(workspace, "status", "--porcelain=v1") == before
    assert result["candidate_bundle_sha256"]


def test_disk_bound_is_configurable_and_capped(packet):
    packet["limits"]["disk_bytes"] = 32 * 1024**2
    engine = Engine()
    result = oci_execute(packet, run=engine, popen=factory())
    create = next(call for call in engine.calls if call[1] == "create")
    assert "/workspace:rw,nosuid,nodev,size=33554432,mode=0700" in create
    assert result["workspace_disk_bytes"] == 32 * 1024**2
    packet["limits"]["disk_bytes"] = 10 * 1024**3 + 1
    with pytest.raises(OciPolicyError, match="disk_bytes"):
        oci_execute(packet, run=Engine(), popen=factory())


def test_cancel_before_start_never_executes_untrusted_container(packet):
    cancel = threading.Event()
    cancel.set()
    engine = Engine()
    processes = factory(timeout_count=1)
    result = oci_execute(packet, run=engine, popen=processes, cancel_event=cancel)
    operations = [call[1:4] for call in engine.calls]
    assert ["kill", "--signal", "TERM"] not in operations
    assert ["kill", "--signal", "KILL"] not in operations
    assert engine.calls[-1][1:3] == ["rm", "--force"]
    assert processes.made == []
    assert result["cancelled"] is True and result["exit_code"] == 130


def test_timeout_sends_term_then_kill_and_waits(packet):
    packet["limits"]["timeout_seconds"] = 0.000001
    class NotReadyEngine(Engine):
        def __call__(self, argv, **kwargs):
            result = super().__call__(argv, **kwargs)
            if argv[1] == "exec":
                return SimpleNamespace(returncode=1, stdout=b"", stderr=b"")
            return result
    engine = NotReadyEngine()
    processes = factory(timeout_count=1)
    result = oci_execute(packet, run=engine, popen=processes)
    operations = [call[1:4] for call in engine.calls]
    assert ["kill", "--signal", "TERM"] in operations
    assert ["kill", "--signal", "KILL"] in operations
    assert processes.made[0].communications == 2
    assert result["timed_out"] is True and result["exit_code"] == 124
