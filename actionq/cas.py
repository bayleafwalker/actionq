"""Server-owned immutable artifact storage used by ActionQ authority paths."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat
import tempfile
from typing import Union


PathLike = Union[Path, str]


def artifact_ref(value: bytes) -> str:
    return "artifact:sha256:" + hashlib.sha256(value).hexdigest()


def fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class _DaemonCAS:
    """Owner-controlled CAS for privacy-minimal terminal result bytes.

    ``allow_unsafe_test_root`` is intentionally an explicit constructor-only
    escape hatch for tests.  Production callers always use the durable-root
    checks below; callers never receive a packet-provided root.
    """

    def __init__(self, root: PathLike, *, allow_unsafe_test_root: bool = False):
        supplied_root = Path(root)
        if not supplied_root.is_absolute():
            raise ValueError("artifact_root must be absolute")
        if supplied_root.is_symlink():
            raise PermissionError("artifact_root may not be a symlink")
        self.root = supplied_root.resolve()
        if not self.root.exists():
            raise ValueError("artifact_root must be explicitly provisioned")
        if not allow_unsafe_test_root:
            unsafe = [
                Path("/tmp"), Path("/var/tmp"), Path.home() / ".cache",
                Path.home() / ".local/state/actionq", Path("/projects/dev/_artifacts"),
            ]
            for variable in ("XDG_CACHE_HOME", "XDG_RUNTIME_DIR"):
                if os.environ.get(variable):
                    unsafe.append(Path(os.environ[variable]).resolve())
            if any(self.root == base or base in self.root.parents for base in unsafe):
                raise ValueError("artifact_root may not be temporary, cache, runtime, or runner staging storage")
            if any((ancestor / ".git").exists() for ancestor in (self.root, *self.root.parents)):
                raise ValueError("artifact_root may not be inside a Git checkout")
        self.objects = self.root / "objects" / "sha256"
        self._ensure_directory(self.root)
        for directory in (self.root / "objects", self.objects):
            existed = directory.exists()
            self._ensure_directory(directory)
            if not existed:
                fsync_directory(directory.parent)

    @staticmethod
    def _ensure_directory(path: Path) -> None:
        path.mkdir(mode=0o700, parents=False, exist_ok=True)
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise PermissionError(f"CAS path is not a real directory: {path}")
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise PermissionError(f"CAS directory must be owner-only: {path}")

    def _path(self, reference: str) -> Path:
        prefix = "artifact:sha256:"
        digest = reference.removeprefix(prefix)
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("invalid artifact reference")
        return self.objects / digest[:2] / digest[2:]

    def put(self, value: bytes) -> str:
        if not isinstance(value, bytes):
            raise TypeError("CAS accepts exact bytes")
        reference = artifact_ref(value)
        target = self._path(reference)
        if not target.parent.exists():
            target.parent.mkdir(mode=0o700, exist_ok=True)
            self._ensure_directory(target.parent)
            fsync_directory(self.objects)
        else:
            self._ensure_directory(target.parent)
        if target.exists():
            if self.get(reference) != value:
                raise RuntimeError(f"CAS collision or corruption at {reference}")
            return reference
        fd, temporary_name = tempfile.mkstemp(prefix=".incoming-", dir=target.parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb", closefd=True) as stream:
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, target, follow_symlinks=False)
            except FileExistsError:
                pass
            fsync_directory(target.parent)
            if self.get(reference) != value:
                raise RuntimeError(f"CAS collision or corruption at {reference}")
            return reference
        finally:
            temporary.unlink(missing_ok=True)

    def get(self, reference: str) -> bytes:
        path = self._path(reference)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise RuntimeError(f"CAS object is not regular: {reference}")
            chunks: list[bytes] = []
            while chunk := os.read(fd, 1024 * 1024):
                chunks.append(chunk)
        finally:
            os.close(fd)
        value = b"".join(chunks)
        if artifact_ref(value) != reference:
            raise RuntimeError(f"CAS object is corrupt: {reference}")
        return value
