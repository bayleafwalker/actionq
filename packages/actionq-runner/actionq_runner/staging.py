"""Private, noncanonical supervisor staging for pre-#2032 runner records."""
from __future__ import annotations

import os
import re
import shutil
import stat
import time
from pathlib import Path

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
ACTIVE_RETENTION_SECONDS = 7 * 24 * 60 * 60
SEALED_RETENTION_SECONDS = 72 * 60 * 60


def _component(value: str) -> str:
    if not _NAME.fullmatch(value):
        raise ValueError("invalid runner staging path component")
    return value


def _mkdir_secure(path: Path) -> None:
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ValueError("runner staging path must be a real directory")
    os.chmod(path, 0o700)


def staging_dir(action_id: int, claim_incarnation: str) -> Path:
    if action_id <= 0:
        raise ValueError("action_id must be positive")
    root = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
    path = root / "actionq" / "runner-staging" / "v1" / str(action_id) / _component(claim_incarnation)
    current = root
    for component in ("actionq", "runner-staging", "v1", str(action_id), claim_incarnation):
        current = current / component
        _mkdir_secure(current)
    for phase in ("incoming", "quarantine", "sealed"):
        _mkdir_secure(path / phase)
    return path


def _atomic_write(directory: Path, name: str, data: bytes) -> Path:
    _component(name)
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
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
    return directory / name


def quarantine(path: Path, name: str, data: bytes) -> Path:
    """Accept worker bytes into quarantine; callers must redact before sealing."""
    return _atomic_write(path / "quarantine", name, data)


def seal(path: Path, name: str, normalized_redacted_data: bytes) -> Path:
    return _atomic_write(path / "sealed", name, normalized_redacted_data)


def collect(path: Path, *, reconciled: bool, terminal: bool, now: float | None = None) -> bool:
    """Delete only reconciled attempts whose bounded retention has elapsed."""
    if not reconciled:
        return False
    now = time.time() if now is None else now
    age = now - path.stat().st_mtime
    limit = SEALED_RETENTION_SECONDS if terminal else ACTIVE_RETENTION_SECONDS
    if age < limit:
        return False
    shutil.rmtree(path)
    return True
