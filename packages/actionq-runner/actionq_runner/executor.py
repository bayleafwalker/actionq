"""Secret-free child supervisor for the released portable execution envelope."""
from __future__ import annotations

import json
import fnmatch
import os
import signal
import subprocess
from pathlib import Path
from typing import Any

from actionq_contracts import require_compatible

from .staging import quarantine, receive, seal, staging_dir


_child: subprocess.Popen[bytes] | None = None
_grace_seconds = 30.0


def _changed_paths(worktree: Path) -> set[str]:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z"], cwd=worktree,
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


def _validate_command(command: list[str], command_id: str, *, contained_worker: bool) -> None:
    forbidden = {"actionq-runner", "actionq-daemon", "actionctl", "sprintctl"}
    if any(Path(part).name in forbidden for part in command):
        raise ValueError("nested authority or runner command is forbidden")
    runner = command_id.rsplit(":", 1)[-1]
    executable = Path(command[0]).name
    if contained_worker and runner in {"harness", "scope-iterate"} and executable != "sudo":
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
    before: set[str] = set()
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
    _child = subprocess.Popen(
        command, cwd=packet.get("cwd") or None, env=environment,
        stdin=subprocess.PIPE if packet.get("stdin") is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=False,
    )
    stdout, _ = _child.communicate(
        str(packet["stdin"]).encode() if packet.get("stdin") is not None else None
    )
    if worktree is not None and envelope["source_commit"] != "unavailable":
        changed = _changed_paths(worktree) - before
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
    return int(_child.returncode or 0)
