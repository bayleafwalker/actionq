"""Secret-free child supervisor for the released portable execution envelope."""
from __future__ import annotations

import json
import os
import signal
import subprocess
from pathlib import Path
from typing import Any

from actionq_contracts import require_compatible

from .staging import quarantine, receive, seal, staging_dir


_child: subprocess.Popen[bytes] | None = None
_grace_seconds = 30.0


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
        os.killpg(_child.pid, signal.SIGTERM)
        try:
            _child.wait(timeout=_grace_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(_child.pid, signal.SIGKILL)
            _child.wait()


def execute(packet: dict[str, Any]) -> int:
    global _child, _grace_seconds
    envelope = dict(packet["envelope"])
    require_compatible(envelope)
    attempt = staging_dir(int(envelope["action_id"]), str(envelope["attempt_id"]))
    command = packet["command"]
    if not isinstance(command, list) or not command or not all(isinstance(part, str) for part in command):
        raise ValueError("runner command must be a non-empty string list")
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
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
    )
    stdout, _ = _child.communicate(
        str(packet["stdin"]).encode() if packet.get("stdin") is not None else None
    )
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
