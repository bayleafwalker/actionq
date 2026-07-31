"""Private supervisor-owned recovery spool for pre-#2032 runner records."""
from __future__ import annotations

import json
import os
import re
import shutil
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_FORBIDDEN = re.compile(
    rb"(?i)(postgres(?:ql)?://|authorization:\s*bearer|private[_ -]?key|"
    rb"claim[_ -]?receipt(?![_ -]?digest)|ssh_auth_sock|kubeconfig|git_askpass|provider[_ -]?token)"
)
ACTIVE_RETENTION_SECONDS = 7 * 24 * 60 * 60
SEALED_RETENTION_SECONDS = 72 * 60 * 60


@dataclass(frozen=True)
class AttemptSpool:
    root: Path
    action_id: int
    claim_incarnation: str


def _component(value: str) -> str:
    if not _NAME.fullmatch(value):
        raise ValueError("invalid runner staging path component")
    return value


def _mkdir_secure(path: Path) -> None:
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ValueError("runner staging path must be a real directory")
    if info.st_uid != os.geteuid():
        raise PermissionError("runner staging path is not supervisor-owned")
    os.chmod(path, 0o700)


def staging_dir(action_id: int, claim_incarnation: str) -> AttemptSpool:
    if action_id <= 0:
        raise ValueError("action_id must be positive")
    incarnation = _component(claim_incarnation)
    state_home = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
    current = state_home
    _mkdir_secure(current)
    for component in ("actionq", "runner-staging", "v1", str(action_id), incarnation):
        current = current / component
        _mkdir_secure(current)
    for phase in ("incoming", "quarantine", "sealed"):
        _mkdir_secure(current / phase)
    return AttemptSpool(current, action_id, incarnation)


def open_staging(action_id: int, claim_incarnation: str) -> AttemptSpool:
    if action_id <= 0:
        raise ValueError("action_id must be positive")
    incarnation = _component(claim_incarnation)
    state_home = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
    root = state_home / "actionq" / "runner-staging" / "v1" / str(action_id) / incarnation
    spool = AttemptSpool(root, action_id, incarnation)
    for phase in ("incoming", "quarantine", "sealed"):
        fd = _phase_fd(spool, phase)
        os.close(fd)
    return spool


def _phase_fd(spool: AttemptSpool, phase: str) -> int:
    _component(phase)
    expected = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / (
        f"actionq/runner-staging/v1/{spool.action_id}/{spool.claim_incarnation}/{phase}"
    )
    if spool.root / phase != expected:
        raise ValueError("spool is outside the canonical runner staging root")
    return os.open(expected, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)


def _atomic_write(spool: AttemptSpool, phase: str, name: str, data: bytes) -> Path:
    _component(name)
    directory_fd = _phase_fd(spool, phase)
    temporary = f".{name}.{os.getpid()}.tmp"
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory_fd)
        try:
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short runner staging write")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.rename(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)
    finally:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.close(directory_fd)
    return spool.root / phase / name


def receive(
    spool: AttemptSpool, name: str, worker_bytes: bytes,
    *, redact: Callable[[bytes], bytes],
) -> Path:
    """Redact in memory, then receive only credential-screened transient bytes."""
    redacted = redact(worker_bytes)
    if not isinstance(redacted, bytes):
        raise TypeError("runner staging redactor must return bytes")
    if _FORBIDDEN.search(redacted):
        raise ValueError("incoming runner record contains forbidden credential material")
    return _atomic_write(spool, "incoming", name, redacted)


def quarantine(spool: AttemptSpool, name: str) -> Path:
    """Move a closed incoming file out of the worker-visible phase."""
    _component(name)
    source_fd = _phase_fd(spool, "incoming")
    target_fd = _phase_fd(spool, "quarantine")
    try:
        os.rename(name, name, src_dir_fd=source_fd, dst_dir_fd=target_fd)
        os.fsync(source_fd); os.fsync(target_fd)
    finally:
        os.close(source_fd); os.close(target_fd)
    return spool.root / "quarantine" / name


def seal(spool: AttemptSpool, name: str, normalized_redacted_data: bytes) -> Path:
    """Seal only supervisor-normalized, credential-free bytes from quarantine."""
    quarantined = spool.root / "quarantine" / _component(name)
    if quarantined.is_symlink() or not quarantined.is_file():
        raise ValueError("worker record must be quarantined before sealing")
    if _FORBIDDEN.search(normalized_redacted_data):
        raise ValueError("sealed runner record contains forbidden credential material")
    target = _atomic_write(spool, "sealed", name, normalized_redacted_data)
    quarantined.unlink()
    directory_fd = _phase_fd(spool, "quarantine")
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return target


def mark_reconciled(spool: AttemptSpool, *, terminal: bool) -> None:
    _atomic_write(spool, "sealed", "recovery-state.json", json.dumps(
        {"reconciled": True, "terminal": terminal}, sort_keys=True, separators=(",", ":")
    ).encode())


def collect(spool: AttemptSpool, *, now: float | None = None) -> bool:
    """Collect only reconciled canonical attempt roots after their retention."""
    for phase in ("incoming", "quarantine", "sealed"):
        fd = _phase_fd(spool, phase)
        os.close(fd)
    root_info = spool.root.lstat()
    if not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.geteuid():
        raise PermissionError("spool root is not a supervisor-owned directory")
    state_path = spool.root / "sealed" / "recovery-state.json"
    if state_path.is_symlink() or not state_path.is_file():
        return False
    state = json.loads(state_path.read_text())
    if state != {"reconciled": True, "terminal": bool(state.get("terminal"))}:
        return False
    now = time.time() if now is None else now
    limit = SEALED_RETENTION_SECONDS if state["terminal"] else ACTIVE_RETENTION_SECONDS
    if now - state_path.stat().st_mtime < limit:
        return False
    parent = spool.root.parent
    shutil.rmtree(spool.root)
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    return True
