"""Fail-closed immutable candidate artifact primitives.

This module deliberately has no queue, Git remote, merge, approval, or
disposition authority.  It resolves exact bytes from an injected CAS and
persists a create-only local recovery journal for a candidate operation.
"""
from __future__ import annotations

import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any, Mapping

from actionq_contracts import canonical_bytes, require_compatible

from .publisher import ArtifactStore, artifact_ref, _fsync_dir


JOURNAL_CONTRACT = "candidate-recovery-journal/v1"


def resolve_exact_contract(
    store: ArtifactStore, reference: str, expected_contract: str,
) -> dict[str, Any]:
    """Resolve one CAS value only when its locator and canonical contract agree."""
    raw = store.get(reference)
    if artifact_ref(raw) != reference:
        raise RuntimeError("CAS resolver returned bytes with a different digest")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("artifact is not UTF-8 canonical JSON") from exc
    if not isinstance(value, dict) or canonical_bytes(value) != raw:
        raise ValueError("artifact is not exact canonical JSON")
    try:
        contract_id = require_compatible(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("artifact contains an invalid contract") from exc
    if contract_id != expected_contract:
        raise ValueError(f"artifact contract must be {expected_contract}")
    return value


class CandidateRecoveryJournal:
    """Namespaced create-only recovery state outside the artifact authority.

    The journal contains only sanitized operation intent and artifact locators.
    It is purposefully separate from the #2032 publication journal so neither
    implementation can reinterpret the other one's recovery records.
    """

    def __init__(self, root: Path | str, *, namespace: str, operation_id: str):
        supplied = Path(root)
        if not supplied.is_absolute() or supplied.is_symlink() or not supplied.is_dir():
            raise ValueError("candidate recovery root must be an existing absolute directory")
        if not namespace or "/" in namespace or not operation_id or "/" in operation_id:
            raise ValueError("candidate recovery namespace and operation id must be path-safe")
        if namespace in {".", ".."} or operation_id in {".", ".."}:
            raise ValueError("candidate recovery namespace and operation id must be path-safe")
        self.root = supplied.resolve()
        self.directory = self.root / "candidate-recovery" / namespace / operation_id
        current = self.root
        for part in ("candidate-recovery", namespace, operation_id):
            current = current / part
            if not current.exists():
                current.mkdir(mode=0o700)
                _fsync_dir(current.parent)
            info = current.lstat()
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_mode & 0o077:
                raise PermissionError("candidate recovery journal must be owner-only")

    def read(self, stage: str) -> dict[str, Any] | None:
        if not stage or "/" in stage:
            raise ValueError("candidate recovery stage is unsafe")
        path = self.directory / f"{stage}.json"
        if not path.exists():
            return None
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise PermissionError("candidate recovery record must be owner-only")
            raw = os.read(fd, 4 * 1024 * 1024)
            if os.read(fd, 1):
                raise RuntimeError("candidate recovery record is too large")
        finally:
            os.close(fd)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("candidate recovery record is corrupt") from exc
        if not isinstance(value, dict) or canonical_bytes(value) != raw:
            raise RuntimeError("candidate recovery record is noncanonical")
        if value.get("contract_id") != JOURNAL_CONTRACT or value.get("stage") != stage:
            raise RuntimeError("candidate recovery record has an invalid identity")
        return value

    def write(self, stage: str, value: Mapping[str, Any]) -> dict[str, Any]:
        if not stage or "/" in stage:
            raise ValueError("candidate recovery stage is unsafe")
        payload = {"contract_id": JOURNAL_CONTRACT, "stage": stage, **dict(value)}
        raw = canonical_bytes(payload)
        path = self.directory / f"{stage}.json"
        current = self.read(stage)
        if current is not None:
            if current != payload:
                raise RuntimeError(f"candidate recovery conflict at {stage}")
            return current
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{stage}-", dir=self.directory)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path, follow_symlinks=False)
            except FileExistsError:
                pass
            _fsync_dir(self.directory)
            persisted = self.read(stage)
            if persisted != payload:
                raise RuntimeError(f"candidate recovery conflict at {stage}")
            return persisted
        finally:
            temporary.unlink(missing_ok=True)
