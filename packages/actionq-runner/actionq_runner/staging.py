"""Private, noncanonical supervisor staging for pre-publication runner records."""
from __future__ import annotations

import os
from pathlib import Path


def staging_dir(action_id: int, claim_incarnation: str) -> Path:
    root = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "actionq/runner-staging/v1"
    path = root / str(action_id) / claim_incarnation
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink():
        raise ValueError("runner staging path must not be a symlink")
    return path


def seal(path: Path, name: str, data: bytes) -> Path:
    target = path / "sealed" / name
    target.parent.mkdir(mode=0o700, exist_ok=True)
    if target.is_symlink() or "/" in name:
        raise ValueError("invalid runner staging name")
    temporary = target.with_suffix(target.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, target)
    return target
