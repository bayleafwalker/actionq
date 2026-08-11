"""Secret-free child supervisor for the released portable execution envelope."""
from __future__ import annotations

import json
import fnmatch
import hashlib
import os
import signal
import subprocess
import time
import threading
import ctypes
from pathlib import Path
from typing import Any

from actionq_contracts import require_compatible

from .staging import quarantine, receive, seal, staging_dir


_child: subprocess.Popen[bytes] | None = None
_grace_seconds = 30.0


def _changed_paths(worktree: Path) -> set[str]:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"], cwd=worktree,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if completed.returncode:
        raise ValueError("runner workspace must be a readable Git worktree")
    paths: set[str] = set()
    records = completed.stdout.split(b"\0")
    index = 0
    while index < len(records):
        record = records[index]
        index += 1
        if not record:
            continue
        status, raw_path = record[:2], record[3:]
        paths.add(raw_path.decode("utf-8", "surrogateescape"))
        if b"R" in status or b"C" in status:
            if index < len(records) and records[index]:
                paths.add(records[index].decode("utf-8", "surrogateescape"))
                index += 1
    return paths


def _workspace_fingerprint(worktree: Path) -> str:
    """Capture the whole worktree so a no-tools phase cannot mutate it.

    Git status alone omits ignored files and collapses an untracked directory
    to one path.  Hashing the filesystem (excluding only Git's metadata)
    closes both gaps while keeping the fingerprint itself secret-free.
    """
    worktree = worktree.resolve()
    digest = hashlib.sha256()
    for directory, dirnames, filenames in os.walk(worktree, topdown=True, followlinks=False):
        directory_path = Path(directory)
        # Only the owning repository's metadata is outside the workspace
        # projection.  A nested, ordinary ``.git`` directory is workspace
        # content and must not be an invisible mutation channel.
        if directory_path == worktree:
            dirnames[:] = sorted(name for name in dirnames if name != ".git")
            filenames = [name for name in filenames if name != ".git"]
        else:
            dirnames[:] = sorted(dirnames)
        entries = sorted(dirnames + filenames)
        for name in entries:
            candidate = directory_path / name
            relative = candidate.relative_to(worktree).as_posix().encode("utf-8", "surrogateescape")
            stat_result = candidate.lstat()
            digest.update(relative)
            digest.update(b"\0")
            digest.update(str(stat_result.st_mode).encode("ascii"))
            digest.update(b"\0")
            if candidate.is_symlink():
                digest.update(os.readlink(candidate).encode("utf-8", "surrogateescape"))
            elif candidate.is_file():
                with candidate.open("rb") as stream:
                    while chunk := stream.read(1024 * 1024):
                        digest.update(chunk)
            digest.update(b"\0")
    return digest.hexdigest()


def _validate_command(command: list[str], command_id: str, *, contained_worker: bool) -> None:
    forbidden = {"actionq-runner", "actionq-daemon", "actionctl", "sprintctl"}
    if any(Path(part).name in forbidden for part in command):
        raise ValueError("nested authority or runner command is forbidden")
    runner = command_id.rsplit(":", 1)[-1]
    executable = Path(command[0]).name
    if contained_worker and runner in {"harness", "scope-iterate", "finalizer"} and executable != "sudo":
        raise ValueError("registered contained harness command must enter through sudo")
    if runner in {"fake", "fake-commit"} and not executable.startswith("python"):
        raise ValueError("registered fake command must use the Python test runner")


def _redact(data: bytes) -> bytes:
    # Runner input is already credential-stripped. Redact common assignment
    # forms before any bytes enter the recovery spool.
    lines = []
    for line in data.splitlines(keepends=True):
        lowered = line.lower()
        if any(marker in lowered for marker in (
            b"database_url=", b"pgpassword=", b"github_token=", b"aws_secret_access_key=",
            b"authorization: bearer", b"private key", b"claim_receipt=",
        )):
            lines.append(b"[REDACTED]\n")
        else:
            lines.append(line)
    return b"".join(lines)


def _stop(_signum, _frame) -> None:
    if _child is not None and _child.poll() is None:
        # Worker inherits the runner's process group. Ignore our own forwarded
        # TERM while delivering it to every descendant in the kill domain.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.killpg(os.getpgrp(), signal.SIGTERM)
        try:
            _child.wait(timeout=_grace_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgrp(), signal.SIGKILL)
            _child.wait()


def _process_group_members(process_group: int) -> list[int]:
    members: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            pid = int(entry.name)
            if pid != os.getpid() and os.getpgid(pid) == process_group:
                members.append(pid)
        except (OSError, ValueError):
            continue
    return members


def _stop_descendants_after_exit() -> None:
    """Ensure a direct worker exit leaves no process running into finalization."""
    # Production launches the runner with ``start_new_session=True``.  A
    # direct library/CLI invocation can share its caller's process group; in
    # that mode killing the group would also kill the supervisor.
    if os.getpgrp() != os.getpid():
        return
    process_group = os.getpgrp()
    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(0.01)
    members = _process_group_members(process_group)
    deadline = time.monotonic() + _grace_seconds
    while time.monotonic() < deadline and _process_group_members(process_group):
        time.sleep(0.01)
    for pid in _process_group_members(process_group):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    # Linux reparents orphaned descendants to a registered subreaper even if
    # they escaped through setsid(2).  Reap that new-session escape class too.
    while True:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        if pid <= 0:
            break


def _become_subreaper() -> None:
    """Contain descendants that create a new session before their parent exits."""
    if os.name != "posix" or not Path("/proc").exists():
        raise RuntimeError("portable runner requires Linux descendant containment")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _direct_children() -> list[int]:
    children: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            status = (entry / "status").read_text(encoding="utf-8")
            parent_line = next(line for line in status.splitlines() if line.startswith("PPid:"))
            if int(parent_line.split()[1]) == os.getpid():
                children.append(int(entry.name))
        except (OSError, StopIteration, ValueError):
            continue
    return children


def _stop_reparented_descendants() -> None:
    deadline = time.monotonic() + _grace_seconds
    while True:
        children = [pid for pid in _direct_children() if _child is None or pid != _child.pid]
        if not children:
            return
        for pid in children:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        if time.monotonic() >= deadline:
            for pid in children:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            for pid in children:
                try:
                    os.waitpid(pid, 0)
                except (ChildProcessError, ProcessLookupError):
                    pass
            return
        time.sleep(0.01)


def execute(packet: dict[str, Any]) -> int:
    global _child, _grace_seconds
    envelope = dict(packet["envelope"])
    require_compatible(envelope)
    attempt = staging_dir(int(envelope["action_id"]), str(envelope["attempt_id"]))
    command = packet["command"]
    if not isinstance(command, list) or not command or not all(isinstance(part, str) for part in command):
        raise ValueError("runner command must be a non-empty string list")
    if packet.get("registered_command_id") != envelope["command_id"]:
        raise ValueError("runner command is not bound to the frozen command id")
    _validate_command(
        command, str(envelope["command_id"]),
        contained_worker=bool(packet.get("contained_worker", True)),
    )
    cwd_value = packet.get("cwd")
    worktree = Path(str(cwd_value)).resolve() if cwd_value else None
    if worktree is not None and packet.get("projection_success_path"):
        for managed_key in ("progress_path", "projection_success_path", "output_path"):
            managed_value = packet.get(managed_key)
            if not managed_value:
                continue
            try:
                Path(str(managed_value)).resolve().relative_to(worktree)
            except ValueError:
                continue
            raise ValueError("lifecycle controller projections must be outside the worktree")
    before: set[str] = set()
    before_fingerprint: str | None = None
    if envelope["source_commit"] != "unavailable":
        if worktree is None:
            raise ValueError("source-bound execution requires an exact workspace")
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=worktree, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if completed.returncode or completed.stdout.strip() != envelope["source_commit"]:
            raise ValueError("runner workspace does not match the frozen source commit")
        before = _changed_paths(worktree)
        if str(envelope["command_id"]).endswith(":finalizer"):
            before_fingerprint = _workspace_fingerprint(worktree)
    environment = packet.get("environment") or {}
    if not isinstance(environment, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in environment.items()):
        raise ValueError("runner environment must be a string mapping")
    blocked = ("ACTIONQ", "SPRINT", "KUBE", "SSH_", "GIT_", "AWS_", "TOKEN", "SECRET", "PASSWORD")
    forbidden_keys = sorted(
        key for key in environment if any(marker in key.upper() for marker in blocked)
    )
    if forbidden_keys:
        raise ValueError(f"runner environment contains authority credentials: {forbidden_keys}")
    _grace_seconds = float(packet.get("grace_seconds", 30.0))
    if not 0 <= _grace_seconds <= 30:
        raise ValueError("runner grace must be between zero and 30 seconds")
    signal.signal(signal.SIGTERM, _stop)
    _become_subreaper()
    _child = subprocess.Popen(
        command, cwd=packet.get("cwd") or None, env=environment,
        stdin=subprocess.PIPE if packet.get("stdin") is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=False,
    )
    def reap_after_direct_exit() -> None:
        assert _child is not None
        _child.wait()
        _stop_descendants_after_exit()
        _stop_reparented_descendants()

    reaper = threading.Thread(target=reap_after_direct_exit, name="actionq-runner-reaper", daemon=True)
    reaper.start()
    if packet.get("stdin") is not None:
        assert _child.stdin is not None
        _child.stdin.write(str(packet["stdin"]).encode())
        _child.stdin.close()
    projection_path = packet.get("progress_path")
    projection_stream = None
    if projection_path:
        target = Path(str(projection_path))
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        projection_stream = os.fdopen(descriptor, "wb")
    chunks: list[bytes] = []
    pending = b""
    assert _child.stdout is not None
    try:
        while chunk := _child.stdout.readline():
            chunks.append(chunk)
            if projection_stream is not None:
                pending += chunk
                lines = pending.splitlines(keepends=True)
                pending = b""
                if lines and not lines[-1].endswith((b"\n", b"\r")):
                    pending = lines.pop()
                for line in lines:
                    projection_stream.write(_redact(line))
                projection_stream.flush()
        if projection_stream is not None and pending:
            projection_stream.write(_redact(pending))
            projection_stream.flush()
    finally:
        if projection_stream is not None:
            projection_stream.close()
    stdout = b"".join(chunks)
    reaper.join(timeout=max(0.1, _grace_seconds + 1.0))
    if worktree is not None and envelope["source_commit"] != "unavailable":
        if str(envelope["command_id"]).endswith(":finalizer"):
            if before_fingerprint != _workspace_fingerprint(worktree):
                raise ValueError("no-tools finalizer changed the workspace")
        changed = _changed_paths(worktree) - before
        for managed_key in ("progress_path", "projection_success_path", "output_path"):
            managed_value = packet.get(managed_key)
            if not managed_value:
                continue
            try:
                managed = Path(str(managed_value)).resolve().relative_to(worktree).as_posix()
            except ValueError:
                continue
            changed.discard(managed)
        disallowed = sorted(
            path for path in changed
            if not any(fnmatch.fnmatchcase(path, pattern) for pattern in envelope["allowed_paths"])
        )
        if disallowed:
            raise ValueError(f"runner changed paths outside the frozen allowlist: {disallowed}")
    incoming = receive(attempt, "execution.log", stdout or b"", redact=_redact)
    quarantine(attempt, incoming.name)
    sealed = seal(attempt, incoming.name, _redact(stdout or b""))
    output_path = packet.get("output_path")
    if output_path:
        target = Path(str(output_path))
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        target.write_bytes(sealed.read_bytes())
        target.chmod(0o600)
    # The marker is the last projection operation.  Its presence therefore
    # proves fingerprint, spool sealing, and requested output projection all
    # succeeded; a step-finish event alone is never reported as success.
    success_path = packet.get("projection_success_path")
    if success_path:
        target = Path(str(success_path))
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        target.write_text(json.dumps({
            "action_id": int(envelope["action_id"]),
            "attempt_id": str(envelope["attempt_id"]),
            "command_id": str(envelope["command_id"]),
            "source_commit": str(envelope["source_commit"]),
        }, sort_keys=True), encoding="utf-8")
        target.chmod(0o600)
    return int(_child.returncode or 0)
