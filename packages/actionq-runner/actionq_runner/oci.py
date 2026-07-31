"""Rootless OCI implementation of the released portable runner envelope."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import signal
import shutil
import stat
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from actionq_contracts import EXECUTION_ENVELOPE_V1, require_compatible

_IMAGE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")
_ENGINE = re.compile(r"(?:/[A-Za-z0-9._+-]+)+|[A-Za-z0-9._+-]+\Z")
_BLOCKED_ENV = (
    "ACTIONQ", "SPRINT", "KUBE", "SSH", "GIT", "AWS", "TOKEN", "SECRET",
    "PASSWORD", "CREDENTIAL", "AUTH", "DATABASE", "PROVIDER", "DOCKER", "CONTAINER",
)
_SECRET_VALUE = re.compile(
    r"-----BEGIN .*PRIVATE KEY-----|(?:postgres(?:ql)?|https?)://[^\s/@:]+:[^\s/@]+@|"
    r"\bBearer\s+\S+|(?:TOKEN|PASSWORD|SECRET|PRIVATE_KEY)\s*=",
    re.I,
)
_MAX_LOG = 1024 * 1024
_DEFAULT_DISK = 10 * 1024**3
_MAX_DISK = 10 * 1024**3
_GIT_CONFIG = ["-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false"]


def _git_env() -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
    }


class Completed(Protocol):
    returncode: int
    stdout: str | bytes | None
    stderr: str | bytes | None


Run = Callable[..., Completed]
PopenFactory = Callable[..., subprocess.Popen[bytes]]


class OciPolicyError(ValueError):
    """The requested execution weakens the frozen OCI boundary."""


def _engine_argv(engine: str | os.PathLike[str]) -> str:
    value = os.fspath(engine)
    if not value or not _ENGINE.fullmatch(value) or Path(value).name in {".", ".."}:
        raise OciPolicyError("engine must be one explicit executable without arguments")
    return value


def _json_output(completed: Completed) -> Mapping[str, Any]:
    if completed.returncode:
        detail = completed.stderr or completed.stdout or "engine info failed"
        raise OciPolicyError(f"rootless OCI engine preflight failed: {detail!s}")
    raw = completed.stdout.decode() if isinstance(completed.stdout, bytes) else completed.stdout
    try:
        value = json.loads(raw or "")
    except (TypeError, json.JSONDecodeError) as exc:
        raise OciPolicyError("rootless OCI engine info was not JSON") from exc
    if not isinstance(value, Mapping):
        raise OciPolicyError("rootless OCI engine info must be an object")
    return value


def _engine_security(info: Mapping[str, Any]) -> tuple[bool, bool]:
    host = info.get("host")
    if isinstance(host, Mapping):
        security = host.get("security")
        if isinstance(security, Mapping):
            return security.get("rootless") is True, security.get("seccompEnabled") is True
    security = info.get("security")
    if isinstance(security, Mapping):
        return security.get("rootless") is True, security.get("seccomp") is True
    options = info.get("SecurityOptions")
    if not isinstance(options, list):
        return False, False
    normalized = {str(item).lower() for item in options}
    return (
        any("rootless" in item for item in normalized),
        any("seccomp" in item for item in normalized),
    )


def _cgroup_info(info: Mapping[str, Any]) -> tuple[str, set[str]]:
    host = info.get("host")
    if isinstance(host, Mapping):
        version = str(host.get("cgroupVersion", ""))
        controllers = host.get("cgroupControllers")
        if isinstance(controllers, list):
            return version, {str(value) for value in controllers}
    cgroup = info.get("cgroup")
    if isinstance(cgroup, Mapping):
        version = str(cgroup.get("version", ""))
        controllers = cgroup.get("controllers")
    else:
        version = str(info.get("CgroupVersion", ""))
        controllers = info.get("CgroupControllers")
    if not isinstance(controllers, list):
        return version, set()
    return version, {str(value) for value in controllers}


def oci_preflight(engine: str | os.PathLike[str], *, run: Run = subprocess.run) -> dict[str, Any]:
    """Fail closed unless the explicit engine proves the frozen host boundary."""
    executable = _engine_argv(engine)
    completed = run(
        [executable, "info", "--format", "json"], capture_output=True, text=True,
        check=False, stdin=subprocess.DEVNULL,
    )
    info = _json_output(completed)
    rootless, seccomp = _engine_security(info)
    version, controllers = _cgroup_info(info)
    missing = {"cpu", "memory", "pids"} - controllers
    if not rootless:
        raise OciPolicyError("OCI engine must prove security.rootless=true")
    if not seccomp:
        raise OciPolicyError("OCI engine must prove seccomp support")
    if version not in {"2", "v2"} or missing:
        raise OciPolicyError(
            f"OCI engine requires cgroup v2 cpu/memory/pids controllers; missing={sorted(missing)}"
        )
    return {
        "engine": executable, "rootless": True, "seccomp": True,
        "cgroup_version": 2, "cgroup_controllers": sorted(controllers),
    }


def _git(workspace: Path, *args: str) -> bytes:
    completed = subprocess.run(
        ["git", *_GIT_CONFIG, "-C", os.fspath(workspace), *args], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=False, env=_git_env(),
    )
    if completed.returncode:
        raise OciPolicyError(f"Git workspace check failed: {' '.join(args)}")
    return completed.stdout


def _changed_paths(workspace: Path, source: str, candidate: str) -> tuple[str, ...]:
    raw = _git(workspace, "diff", "--name-status", "-z", "--find-renames", source, candidate)
    fields = raw.split(b"\0")
    paths: set[str] = set()
    index = 0
    while index < len(fields):
        status = fields[index]
        index += 1
        if not status:
            continue
        count = 2 if status[:1] in {b"R", b"C"} else 1
        for _ in range(count):
            if index >= len(fields) or not fields[index]:
                raise OciPolicyError("Git returned a malformed changed-path record")
            path = fields[index].decode("utf-8", "surrogateescape")
            index += 1
            if path.startswith("/") or ".." in Path(path).parts:
                raise OciPolicyError("Git returned an unsafe changed path")
            paths.add(path)
    return tuple(sorted(paths))


def _validate_workspace(workspace: Path, source: str) -> None:
    if not workspace.is_absolute() or not workspace.is_dir() or workspace.is_symlink():
        raise OciPolicyError("workspace must be one existing absolute real directory")
    if _git(workspace, "rev-parse", "HEAD").decode().strip() != source:
        raise OciPolicyError("workspace does not match the exact source commit")
    if _git(workspace, "remote").strip():
        raise OciPolicyError("workspace Git remotes must be removed before OCI execution")
    if _git(workspace, "status", "--porcelain=v1", "--untracked-files=all").strip():
        raise OciPolicyError("workspace must be clean before OCI execution")


def _positive_number(value: Any, field: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OciPolicyError(f"{field} must be numeric")
    number = float(value)
    if not 0 < number <= maximum:
        raise OciPolicyError(f"{field} exceeds the frozen limit")
    return number


def _validate_packet(packet: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "engine", "image", "envelope", "registered_command_id", "deterministic", "command",
        "workspace", "network", "uid", "environment", "limits", "seccomp_sha256",
    }
    if set(packet) != required:
        raise OciPolicyError(
            f"invalid OCI packet fields: missing={sorted(required - set(packet))}, extra={sorted(set(packet) - required)}"
        )
    engine = _engine_argv(str(packet["engine"]))
    image = str(packet["image"])
    if not _IMAGE.fullmatch(image):
        raise OciPolicyError("OCI image must be an immutable name@sha256 digest")
    seccomp_sha256 = str(packet["seccomp_sha256"])
    if not re.fullmatch(r"[0-9a-f]{64}", seccomp_sha256):
        raise OciPolicyError("seccomp_sha256 must freeze one lowercase SHA-256 digest")
    envelope = dict(packet["envelope"])
    if require_compatible(envelope) != EXECUTION_ENVELOPE_V1:
        raise OciPolicyError("OCI runner requires execution-envelope/v1")
    if envelope["source_commit"] == "unavailable":
        raise OciPolicyError("OCI runner requires an exact source commit")
    if packet["registered_command_id"] != envelope["command_id"]:
        raise OciPolicyError("command is not bound to the released envelope")
    if packet["deterministic"] is not True:
        raise OciPolicyError("OCI pilot accepts only a registered deterministic command")
    command = packet["command"]
    if not isinstance(command, list) or not command or not all(isinstance(item, str) and item for item in command):
        raise OciPolicyError("command must be a non-empty argv list")
    if any(Path(item).name in {"actionq-runner", "actionq-daemon", "actionctl", "sprintctl", "docker", "podman"}
           for item in command):
        raise OciPolicyError("nested runner, authority, or container engine command is forbidden")
    if packet["network"] != "none":
        raise OciPolicyError("OCI pilot network must be exactly none")
    uid = packet["uid"]
    if (isinstance(uid, bool) or not isinstance(uid, int) or uid <= 0
            or uid != os.geteuid()):
        raise OciPolicyError("OCI keep-id worker UID must equal the numeric nonroot supervisor UID")
    environment = packet["environment"]
    if not isinstance(environment, Mapping) or not all(
        isinstance(key, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)
        and isinstance(value, str) for key, value in environment.items()
    ):
        raise OciPolicyError("OCI environment must be a string mapping")
    forbidden = sorted(
        key for key, value in environment.items()
        if any(marker in key.upper() for marker in _BLOCKED_ENV) or _SECRET_VALUE.search(value)
    )
    if forbidden:
        raise OciPolicyError(f"OCI environment contains authority or credential material: {forbidden}")
    limits = packet["limits"]
    if not isinstance(limits, Mapping) or not set(limits) <= {
        "cpus", "memory_bytes", "pids", "timeout_seconds", "disk_bytes",
    } or not {"cpus", "memory_bytes", "pids", "timeout_seconds"} <= set(limits):
        raise OciPolicyError("OCI limits must declare cpu, memory, pids, timeout, and optional disk")
    cpus = _positive_number(limits["cpus"], "cpus", 2)
    memory = limits["memory_bytes"]
    pids = limits["pids"]
    timeout = _positive_number(limits["timeout_seconds"], "timeout_seconds", 1800)
    if isinstance(memory, bool) or not isinstance(memory, int) or not 0 < memory <= 2 * 1024**3:
        raise OciPolicyError("memory_bytes exceeds the frozen limit")
    if isinstance(pids, bool) or not isinstance(pids, int) or not 0 < pids <= 128:
        raise OciPolicyError("pids exceeds the frozen limit")
    disk = limits.get("disk_bytes", _DEFAULT_DISK)
    if isinstance(disk, bool) or not isinstance(disk, int) or not 1024**2 <= disk <= _MAX_DISK:
        raise OciPolicyError("disk_bytes exceeds the frozen limit")
    supplied_workspace = Path(str(packet["workspace"]))
    if not supplied_workspace.is_absolute() or supplied_workspace.is_symlink():
        raise OciPolicyError("workspace must be one explicit absolute real directory")
    workspace = supplied_workspace.resolve(strict=True)
    return {
        "engine": engine, "image": image, "envelope": envelope, "command": list(command),
        "workspace": workspace, "uid": uid, "environment": dict(environment),
        "cpus": cpus, "memory": memory, "pids": pids, "timeout": timeout, "disk": disk,
        "seccomp_sha256": seccomp_sha256,
    }


def _cpu_arg(value: float) -> str:
    return str(int(value)) if value.is_integer() else format(value, ".12g")


def _create_argv(config: Mapping[str, Any], name: str, source_bundle: Path, seccomp_profile: Path) -> list[str]:
    argv = [
        str(config["engine"]), "create", "--name", name,
        "--pull", "never",
        "--network", "none", "--read-only", "--userns", "keep-id:uid=0,gid=0",
        "--user", "0:0",
        "--cap-drop", "ALL", "--cap-add", "CHOWN", "--cap-add", "SETUID",
        "--cap-add", "SETGID", "--cap-add", "DAC_OVERRIDE",
        "--cap-add", "KILL",
        "--security-opt", "no-new-privileges",
        "--security-opt", f"seccomp={seccomp_profile}",
        "--pids-limit", str(config["pids"]), "--cpus", _cpu_arg(float(config["cpus"])),
        "--memory", str(config["memory"]), "--memory-swap", str(config["memory"]),
        "--pid", "private", "--ipc", "private",
        "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m",
        "--tmpfs", "/run:rw,noexec,nosuid,nodev,size=16m",
        "--tmpfs", "/control:rw,noexec,nosuid,nodev,size=1m,mode=0700",
        "--tmpfs", f"/workspace:rw,nosuid,nodev,size={config['disk']},mode=0700",
        "--volume", f"{source_bundle}:/input/source.bundle:ro,rprivate,nosuid,nodev,noexec",
        "--workdir", "/workspace",
    ]
    for key in sorted(config["environment"]):
        argv.extend(("--env", f"{key}={config['environment'][key]}"))
    return [*argv, str(config["image"]), "python", "/usr/local/lib/actionq-oci-wrapper.py",
            "--source", "/input/source.bundle", "--source-commit",
            str(config["envelope"]["source_commit"]), "--candidate",
            "/workspace/candidate.bundle", "--worker-uid", str(config["uid"]),
            "--", *config["command"]]


def _engine_call(run: Run, argv: Sequence[str]) -> Completed:
    return run(list(argv), capture_output=True, check=False, stdin=subprocess.DEVNULL)


def _verify_created_container(config: Mapping[str, Any], name: str, source_bundle: Path,
                              seccomp_profile: Path, run: Run) -> None:
    """Read back the engine decision before executing untrusted bytes."""
    completed = _engine_call(run, [config["engine"], "inspect", name])
    if completed.returncode:
        raise OciPolicyError("OCI engine could not inspect the created container")
    raw = completed.stdout.decode() if isinstance(completed.stdout, bytes) else completed.stdout
    try:
        values = json.loads(raw or "")
        value = values[0] if isinstance(values, list) and len(values) == 1 else None
    except json.JSONDecodeError as exc:
        raise OciPolicyError("OCI inspect result was not JSON") from exc
    if not isinstance(value, Mapping):
        raise OciPolicyError("OCI inspect result must contain exactly one container")
    host, container = value.get("HostConfig"), value.get("Config")
    mounts = value.get("Mounts")
    if not isinstance(host, Mapping) or not isinstance(container, Mapping) or not isinstance(mounts, list):
        raise OciPolicyError("OCI inspect result is missing security fields")
    expected_digest = str(config["image"]).rsplit("@", 1)[1]
    required = {
        "image": value.get("ImageDigest") == expected_digest,
        "user": container.get("User") == "0:0",
        "wrapper": container.get("Cmd") == [
            "python", "/usr/local/lib/actionq-oci-wrapper.py", "--source", "/input/source.bundle",
            "--source-commit", str(config["envelope"]["source_commit"]), "--candidate",
            "/workspace/candidate.bundle", "--worker-uid", str(config["uid"]),
            "--", *config["command"],
        ],
        "network": host.get("NetworkMode") == "none",
        "rootfs": host.get("ReadonlyRootfs") is True,
        "privileged": host.get("Privileged") is False,
        "pid": host.get("PidMode") == "private",
        "ipc": host.get("IpcMode") == "private",
        "cap_add": host.get("CapAdd") in ([], None)
        and set(value.get("EffectiveCaps") or [])
        == {"CAP_CHOWN", "CAP_SETUID", "CAP_SETGID", "CAP_DAC_OVERRIDE", "CAP_KILL"}
        and set(value.get("BoundingCaps") or [])
        == {"CAP_CHOWN", "CAP_SETUID", "CAP_SETGID", "CAP_DAC_OVERRIDE", "CAP_KILL"},
        "cap_drop": isinstance(host.get("CapDrop"), list) and bool(host.get("CapDrop")),
        "no_new_privileges": "no-new-privileges" in (host.get("SecurityOpt") or []),
        "seccomp": f"seccomp={seccomp_profile}" in (host.get("SecurityOpt") or [])
        and "seccomp=unconfined" not in (host.get("SecurityOpt") or []),
        "memory": host.get("Memory") == config["memory"],
        "memory_swap": host.get("MemorySwap") == config["memory"],
        "cpus": host.get("NanoCpus") == int(float(config["cpus"]) * 1_000_000_000),
        "pids": host.get("PidsLimit") == config["pids"],
        "devices": host.get("Devices") in ([], None),
        "userns": (host.get("Annotations") or {}).get("io.podman.annotations.userns")
        == "keep-id:uid=0,gid=0",
    }
    mount = mounts[0] if len(mounts) == 1 and isinstance(mounts[0], Mapping) else {}
    options = set(mount.get("Options") or [])
    required["workspace_mount"] = (
        len(mounts) == 1 and mount.get("Source") == os.fspath(source_bundle)
        and mount.get("Destination") == "/input/source.bundle" and mount.get("RW") is False
        and mount.get("Propagation") == "rprivate"
        and {"nosuid", "nodev", "noexec"} <= options
    )
    tmpfs = host.get("Tmpfs") or {}
    required["tmpfs"] = (
        set(tmpfs) == {"/tmp", "/run", "/control", "/workspace"}
        and all(flag in str(tmpfs["/workspace"]) for flag in
                ("nosuid", "nodev", f"size={config['disk']}", "mode=0700"))
        and all(all(flag in str(tmpfs[path]) for flag in ("nosuid", "nodev", "noexec"))
                for path in ("/tmp", "/run", "/control"))
    )
    failed = sorted(key for key, accepted in required.items() if not accepted)
    if failed:
        raise OciPolicyError(f"OCI engine weakened the created container: {failed}")


def _redact_log(value: bytes) -> str:
    text = value[:_MAX_LOG].decode("utf-8", "replace")
    lines = []
    for line in text.splitlines(keepends=True):
        lines.append("[REDACTED]\n" if _SECRET_VALUE.search(line) else line)
    return "".join(lines)


def _stage_root(override: Path | None) -> Path:
    if override is not None:
        root = override
    else:
        state = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
        root = state / "actionq/oci-staging/v1"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise OciPolicyError("OCI staging root must be a real directory")
    return root


def _create_source_bundle(workspace: Path, source: str, destination: Path) -> None:
    completed = subprocess.run(
        ["git", *_GIT_CONFIG, "-C", os.fspath(workspace), "bundle", "create", os.fspath(destination), "HEAD"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        env=_git_env(),
    )
    if completed.returncode:
        raise OciPolicyError("could not create the exact source Git bundle")
    os.chmod(destination, 0o600)
    if _git(workspace, "rev-parse", "HEAD").decode().strip() != source:
        raise OciPolicyError("source changed while its bundle was created")


def _stage_seccomp_profile(engine: str, stage: Path, expected_sha256: str) -> Path:
    source = Path(engine).parent.parent / "share/containers/seccomp.json"
    try:
        metadata = source.lstat()
    except OSError as exc:
        raise OciPolicyError(f"OCI engine seccomp profile is unavailable: {source}") from exc
    if source.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise OciPolicyError("OCI engine seccomp profile must be a real regular file")
    destination = stage / "seccomp.json"
    shutil.copyfile(source, destination, follow_symlinks=False)
    if hashlib.sha256(destination.read_bytes()).hexdigest() != expected_sha256:
        raise OciPolicyError("OCI seccomp profile does not match its frozen digest")
    os.chmod(destination, 0o400)
    return destination


def _validate_candidate_bundle(stage: Path, source: str, envelope: Mapping[str, Any]) -> tuple[str, tuple[str, ...], dict[str, Any], str]:
    bundle = stage / "candidate.bundle"
    metadata = bundle.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise OciPolicyError("candidate bundle must be a mode-0600 regular file")
    verify = subprocess.run(["git", *_GIT_CONFIG, "bundle", "verify", os.fspath(bundle)],
                            capture_output=True, check=False, env=_git_env())
    if verify.returncode:
        raise OciPolicyError("candidate Git bundle failed verification")
    checkout = stage / "validate"
    cloned = subprocess.run(["git", *_GIT_CONFIG, "clone", "--quiet", "--no-checkout",
                             os.fspath(bundle), os.fspath(checkout)],
                            capture_output=True, check=False, env=_git_env())
    if cloned.returncode:
        raise OciPolicyError("candidate Git bundle could not be cloned")
    _git(checkout, "remote", "remove", "origin")
    if subprocess.run(["git", *_GIT_CONFIG, "-C", os.fspath(checkout), "fsck", "--full", "--strict"],
                      capture_output=True, check=False, env=_git_env()).returncode:
        raise OciPolicyError("candidate repository failed strict fsck")
    candidate = _git(checkout, "rev-parse", "HEAD").decode().strip()
    if subprocess.run(["git", *_GIT_CONFIG, "-C", os.fspath(checkout), "merge-base", "--is-ancestor", source, candidate],
                      capture_output=True, check=False, env=_git_env()).returncode:
        raise OciPolicyError("OCI candidate does not descend from the exact source")
    changed = _changed_paths(checkout, source, candidate)
    disallowed = sorted(path for path in changed if not any(
        fnmatch.fnmatchcase(path, pattern) for pattern in envelope["allowed_paths"]
    ))
    if disallowed:
        raise OciPolicyError(f"OCI candidate changed paths outside the frozen allowlist: {disallowed}")
    manifest: dict[str, Any] = {}
    for path in changed:
        line = _git(checkout, "ls-tree", candidate, "--", path).decode().strip()
        if not line:
            manifest[path] = None
            continue
        head, _tab, _name = line.partition("\t")
        mode, kind, oid = head.split()
        manifest[path] = {"mode": mode, "type": kind, "object": oid}
    digest = hashlib.sha256(bundle.read_bytes()).hexdigest()
    return candidate, changed, manifest, digest


def _promote_verified_candidate(workspace: Path, bundle: Path, candidate: str) -> None:
    """Import only the already verified object graph into the publisher worktree."""
    fetched = subprocess.run(
        ["git", *_GIT_CONFIG, "-C", os.fspath(workspace), "fetch", "--no-tags", os.fspath(bundle), "HEAD"],
        capture_output=True, check=False, env=_git_env(),
    )
    if fetched.returncode:
        raise OciPolicyError("verified candidate could not be imported into the host worktree")
    if _git(workspace, "rev-parse", "FETCH_HEAD").decode().strip() != candidate:
        raise OciPolicyError("candidate import selected an unexpected commit")
    checked = subprocess.run(
        ["git", *_GIT_CONFIG, "-C", os.fspath(workspace), "checkout", "--detach", "--force", candidate],
        capture_output=True, check=False, env=_git_env(),
    )
    if checked.returncode:
        raise OciPolicyError("verified candidate could not be checked out for publication")
    if _git(workspace, "remote").strip() or _git(
        workspace, "status", "--porcelain=v1", "--untracked-files=all"
    ).strip():
        raise OciPolicyError("promoted candidate worktree was not clean and remoteless")


def oci_execute(
    packet: Mapping[str, Any], *, run: Run = subprocess.run,
    popen: PopenFactory = subprocess.Popen, cancel_event: threading.Event | None = None,
    staging_root: Path | None = None,
) -> dict[str, Any]:
    """Execute one deterministic envelope in a disposable rootless container."""
    config = _validate_packet(packet)
    oci_preflight(config["engine"], run=run)
    envelope = config["envelope"]
    workspace: Path = config["workspace"]
    source = str(envelope["source_commit"])
    _validate_workspace(workspace, source)
    name = "actionq-" + hashlib.sha256(
        f"{envelope['action_id']}\0{envelope['attempt_id']}".encode()
    ).hexdigest()[:20]
    stage = Path(tempfile.mkdtemp(prefix=f"{envelope['action_id']}-", dir=_stage_root(staging_root)))
    os.chmod(stage, 0o700)
    source_bundle = stage / "source.bundle"
    _create_source_bundle(workspace, source, source_bundle)
    seccomp_profile = _stage_seccomp_profile(
        config["engine"], stage, config["seccomp_sha256"],
    )
    created = False
    process: subprocess.Popen[bytes] | None = None
    timed_out = False
    cancelled = False
    output = b""
    previous_handler: Any = None
    signal_cancel = threading.Event()

    def request_cancel(_signum: int, _frame: Any) -> None:
        signal_cancel.set()

    candidate = source
    changed: tuple[str, ...] = ()
    manifest: dict[str, Any] = {}
    bundle_digest: str | None = None
    workspace_filesystem: dict[str, Any] | None = None
    sealed = False
    try:
        if threading.current_thread() is threading.main_thread():
            previous_handler = signal.signal(signal.SIGTERM, request_cancel)
        # A deterministic identity makes an uncatchable supervisor SIGKILL
        # recoverable: the next execution removes any prior container before
        # it can create or start another one with the same attempt identity.
        _engine_call(run, [config["engine"], "rm", "--force", name])
        create = _engine_call(run, _create_argv(config, name, source_bundle, seccomp_profile))
        if create.returncode:
            raise RuntimeError(f"OCI container create failed: {create.stderr!s}")
        created = True
        _verify_created_container(config, name, source_bundle, seccomp_profile, run)
        if signal_cancel.is_set() or (cancel_event is not None and cancel_event.is_set()):
            cancelled = True
        else:
            process = popen(
                [config["engine"], "start", "--attach", name], stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
            )
        deadline = time.monotonic() + float(config["timeout"])
        while process is not None:
            if signal_cancel.is_set() or (cancel_event is not None and cancel_event.is_set()):
                cancelled = True
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            readiness = _engine_call(run, [config["engine"], "exec", name, "test", "-f", "/control/sealed"])
            if readiness.returncode == 0:
                sealed = True
                break
            if process.poll() is not None:
                output, _ = process.communicate()
                raise OciPolicyError(
                    "OCI wrapper exited before sealing its result: " + _redact_log(output or b"")
                )
            time.sleep(min(0.1, remaining))
        if (timed_out or cancelled) and process is not None:
            term = _engine_call(run, [config["engine"], "kill", "--signal", "TERM", name])
            try:
                if term.returncode:
                    raise subprocess.TimeoutExpired([config["engine"], "start", "--attach", name], 0)
                output, _ = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                killed = _engine_call(run, [config["engine"], "kill", "--signal", "KILL", name])
                if killed.returncode:
                    raise RuntimeError("OCI engine failed to kill the cancelled container")
                try:
                    output, _ = process.communicate(timeout=10)
                except subprocess.TimeoutExpired as exc:
                    raise RuntimeError("OCI container did not die after SIGKILL") from exc
        exit_code = 124 if timed_out else 130 if cancelled else 125
        if sealed:
            metadata_path = stage / "runner-metadata.json"
            metadata_copy = _engine_call(run, [config["engine"], "cp", f"{name}:/control/runner-metadata.json",
                                               os.fspath(metadata_path)])
            if metadata_copy.returncode or metadata_path.is_symlink() or not metadata_path.is_file():
                raise OciPolicyError("OCI worker did not expose trusted filesystem metadata")
            try:
                workspace_filesystem = json.loads(metadata_path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise OciPolicyError("OCI workspace metadata was invalid") from exc
            if not isinstance(workspace_filesystem, dict) or workspace_filesystem.get("workspace_mode") != "0o700":
                raise OciPolicyError("OCI workspace did not retain mode 0700")
            capacity = workspace_filesystem.get("capacity_bytes")
            if not isinstance(capacity, int) or capacity <= 0 or capacity > config["disk"] + 4096:
                raise OciPolicyError("OCI workspace exceeded its disk bound")
            command_exit = workspace_filesystem.get("command_exit")
            if not isinstance(command_exit, int):
                raise OciPolicyError("OCI wrapper omitted the command exit status")
            if workspace_filesystem.get("descendants_reaped") is not True:
                raise OciPolicyError("OCI wrapper did not prove descendant cleanup")
            exit_code = command_exit
            if exit_code == 0:
                copied = _engine_call(run, [config["engine"], "cp", f"{name}:/workspace/candidate.bundle",
                                            os.fspath(stage / "candidate.bundle")])
                if copied.returncode:
                    raise OciPolicyError("OCI worker did not expose a candidate bundle")
                candidate_path = stage / "candidate.bundle"
                if candidate_path.is_symlink() or not candidate_path.is_file():
                    raise OciPolicyError("OCI candidate copy was not a regular file")
                os.chmod(candidate_path, 0o600)
                candidate, changed, manifest, bundle_digest = _validate_candidate_bundle(stage, source, envelope)
                _promote_verified_candidate(workspace, candidate_path, candidate)
            stopped = _engine_call(run, [config["engine"], "kill", "--signal", "TERM", name])
            if stopped.returncode:
                raise RuntimeError("OCI engine failed to stop the sealed wrapper")
            try:
                output, _ = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                killed = _engine_call(run, [config["engine"], "kill", "--signal", "KILL", name])
                if killed.returncode:
                    raise RuntimeError("OCI engine failed to kill the sealed wrapper")
                output, _ = process.communicate(timeout=10)
    finally:
        if previous_handler is not None:
            signal.signal(signal.SIGTERM, previous_handler)
        if created:
            _engine_call(run, [config["engine"], "rm", "--force", name])
        shutil.rmtree(stage, ignore_errors=True)
    _validate_workspace(workspace, candidate if exit_code == 0 else source)
    return {
        "contract_id": "oci-execution-result/v1", "action_id": envelope["action_id"],
        "attempt_id": envelope["attempt_id"], "command_id": envelope["command_id"],
        "source_commit": source, "candidate_commit": candidate, "changed_paths": list(changed),
        "exit_code": exit_code, "timed_out": timed_out, "cancelled": cancelled,
        "log": _redact_log(output or b""), "candidate_manifest": manifest,
        "candidate_bundle_sha256": bundle_digest, "workspace_disk_bytes": config["disk"],
        "workspace_filesystem": workspace_filesystem,
        "seccomp_sha256": config["seccomp_sha256"],
    }


__all__ = ["OciPolicyError", "oci_execute", "oci_preflight"]
