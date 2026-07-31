"""Immutable candidate publication for the trusted ActionQ runner supervisor.

The filesystem backend is deliberately small: callers address bytes, while the
backend owns create-only placement and durability.  The publication journal is
private operational state, not an artifact and not an ActionQ authority record.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Protocol

from actionq_contracts import (
    CANDIDATE_V1,
    EXECUTION_ENVELOPE_V1,
    EXECUTION_V1,
    PUBLICATION_V1,
    VERIFICATION_V1,
    Candidate,
    Publication,
    Verification,
    canonical_bytes,
    require_compatible,
)

ARTIFACT_PREFIX = "artifact:sha256:"
JOURNAL_CONTRACT = "actionq-publication-journal/v1"
MANIFEST_CONTRACT = "changed-path-manifest/v1"
MAX_REDACTED_LOG_BYTES = 1024 * 1024
REDACTED_LOG_RETENTION_DAYS = 30
_TRUNCATED = b"\n[actionq-runner: redacted log truncated]\n"
_OID = re.compile(r"[0-9a-f]{40,64}\Z")
_DIGEST = re.compile(r"artifact:sha256:([0-9a-f]{64})\Z")
_FORBIDDEN_KEYS = re.compile(
    r"(^|_)(claim_?receipt|raw_?receipt|password|passwd|secret|token|credential|"
    r"database_?url|db_?url|private_?key|ssh_?agent|auth|authorization|cookie)(_|$)", re.I
)
_FORBIDDEN_TEXT = (
    re.compile(r"-----BEGIN (?:OPENSSH |RSA |EC )?PRIVATE KEY-----"),
    re.compile(r"postgres(?:ql)?://[^\s/@:]+:[^\s/@]+@", re.I),
    re.compile(r"(?:gh[oprsu]_|github_pat_)[A-Za-z0-9_]{12,}"),
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}", re.I),
    re.compile(r"https?://[^\s/@:]+:[^\s/@]+@", re.I),
    re.compile(r"(?:TOKEN|PASSWORD|PASSWD|SECRET|PRIVATE_KEY)\s*=\s*\S+", re.I),
)


class ArtifactStore(Protocol):
    """Backend-neutral immutable byte store."""

    def put(self, value: bytes) -> str: ...
    def get(self, reference: str) -> bytes: ...


def artifact_ref(value: bytes) -> str:
    return f"{ARTIFACT_PREFIX}{hashlib.sha256(value).hexdigest()}"


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class FilesystemCAS:
    """Atomic, create-only filesystem CAS with verification on every access."""

    def __init__(self, root: Path | str, *, allow_unsafe_test_root: bool = False):
        supplied_root = Path(root)
        if not supplied_root.is_absolute():
            raise ValueError("artifact_root must be absolute")
        if supplied_root.is_symlink():
            raise PermissionError("artifact_root may not be a symlink")
        self.root = supplied_root.resolve()
        if not self.root.exists():
            raise ValueError("artifact_root must be explicitly provisioned")
        if not allow_unsafe_test_root:
            unsafe = [Path("/tmp"), Path("/var/tmp")]
            for variable in ("XDG_CACHE_HOME", "XDG_RUNTIME_DIR"):
                if os.environ.get(variable):
                    unsafe.append(Path(os.environ[variable]).resolve())
            unsafe.extend((Path.home() / ".cache", Path.home() / ".local/state/actionq"))
            unsafe.append(Path("/projects/dev/_artifacts"))
            if any(self.root == base or base in self.root.parents for base in unsafe):
                raise ValueError("artifact_root may not be temporary, cache, runtime, or runner staging storage")
            if any((ancestor / ".git").exists() for ancestor in (self.root, *self.root.parents)):
                raise ValueError("artifact_root may not be inside a Git checkout")
        self.objects = self.root / "objects" / "sha256"
        self._ensure_directory(self.root, 0o700)
        for directory in (self.root / "objects", self.objects):
            existed = directory.exists()
            self._ensure_directory(directory, 0o700)
            if not existed:
                _fsync_dir(directory.parent)

    @staticmethod
    def _ensure_directory(path: Path, mode: int) -> None:
        path.mkdir(mode=mode, parents=False, exist_ok=True)
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise PermissionError(f"CAS path is not a real directory: {path}")
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise PermissionError(f"CAS directory must be owner-only: {path}")

    def _path(self, reference: str) -> Path:
        match = _DIGEST.fullmatch(reference)
        if not match:
            raise ValueError("invalid artifact reference")
        digest = match.group(1)
        return self.objects / digest[:2] / digest[2:]

    def put(self, value: bytes) -> str:
        if not isinstance(value, bytes):
            raise TypeError("CAS accepts exact bytes")
        reference = artifact_ref(value)
        target = self._path(reference)
        if not target.parent.exists():
            target.parent.mkdir(mode=0o700, exist_ok=True)
            self._ensure_directory(target.parent, 0o700)
            _fsync_dir(self.objects)
        else:
            self._ensure_directory(target.parent, 0o700)
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
            _fsync_dir(target.parent)
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


def _reject_forbidden(value: Any, path: str = "packet") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            if _FORBIDDEN_KEYS.search(key):
                raise ValueError(f"forbidden credential field: {path}.{key}")
            _reject_forbidden(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_forbidden(item, f"{path}[{index}]")
    elif isinstance(value, str):
        if any(pattern.search(value) for pattern in _FORBIDDEN_TEXT):
            raise ValueError(f"forbidden credential material in {path}")


def _run_git(worktree: Path, *args: str, text: bool = True) -> str | bytes:
    result = subprocess.run(
        ["git", "-C", os.fspath(worktree), *args], check=False, capture_output=True,
        text=text, env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C"},
    )
    if result.returncode:
        stderr = result.stderr if text else result.stderr.decode("utf-8", "replace")
        raise ValueError(f"git {' '.join(args)} failed: {stderr.strip()}")
    return result.stdout


def _exact_oid(value: Any, field: str) -> str:
    oid = str(value)
    if not _OID.fullmatch(oid):
        raise ValueError(f"{field} must be a full lowercase Git object id")
    return oid


def _changed_paths(worktree: Path, source: str, candidate: str) -> tuple[str, ...]:
    raw = _run_git(worktree, "diff", "--name-only", "-z", "--find-renames", source, candidate, text=False)
    assert isinstance(raw, bytes)
    paths = tuple(sorted(part.decode("utf-8", "surrogateescape") for part in raw.split(b"\0") if part))
    for path in paths:
        if path.startswith("/") or ".." in Path(path).parts or "\0" in path:
            raise ValueError("Git produced an unsafe changed path")
    return paths


def _record(value: Any, contract_id: str) -> bytes:
    record = asdict(value) if hasattr(value, "__dataclass_fields__") else dict(value)
    if require_compatible(record) != contract_id:
        raise ValueError(f"expected {contract_id}")
    return canonical_bytes(record)


class _Journal:
    def __init__(self, root: Path, action_id: int, attempt_id: str):
        if not attempt_id or "/" in attempt_id or "\0" in attempt_id or attempt_id in {".", ".."}:
            raise ValueError("attempt_id is unsafe")
        self.directory = root / "journals" / str(action_id) / attempt_id
        current = root
        for component in ("journals", str(action_id), attempt_id):
            current = current / component
            if not current.exists():
                current.mkdir(mode=0o700)
                _fsync_dir(current.parent)
            info = current.lstat()
            if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
                    or info.st_uid != os.geteuid() or info.st_mode & 0o077):
                raise PermissionError("publication journal must be owner-only")

    def read(self, stage: str) -> dict[str, Any] | None:
        path = self.directory / f"{stage}.json"
        if not path.exists():
            return None
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise PermissionError("publication journal event must be owner-only")
            raw = os.read(fd, 4 * 1024 * 1024)
            if os.read(fd, 1):
                raise RuntimeError("journal event is too large")
        finally:
            os.close(fd)
        value = json.loads(raw)
        if canonical_bytes(value) != raw or value.get("contract_id") != JOURNAL_CONTRACT:
            raise RuntimeError("publication journal is corrupt")
        return value

    def write(self, stage: str, value: dict[str, Any]) -> None:
        payload = {"contract_id": JOURNAL_CONTRACT, "stage": stage, **value}
        raw = canonical_bytes(payload)
        path = self.directory / f"{stage}.json"
        if path.exists():
            if self.read(stage) != payload:
                raise RuntimeError(f"publication journal conflict at {stage}")
            return
        fd, temporary_name = tempfile.mkstemp(prefix=f".{stage}-", dir=self.directory)
        temporary = Path(temporary_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path, follow_symlinks=False)
            except FileExistsError:
                pass
            _fsync_dir(self.directory)
            if self.read(stage) != payload:
                raise RuntimeError(f"publication journal conflict at {stage}")
        finally:
            temporary.unlink(missing_ok=True)

def _make_bundle(worktree: Path, temporary: Path) -> bytes:
    result = subprocess.run(
        # HEAD is a named ref, which bundle create requires. The caller has
        # already proved that HEAD is exactly the requested candidate OID.
        ["git", "-C", os.fspath(worktree), "bundle", "create", os.fspath(temporary), "HEAD"],
        capture_output=True, text=True, env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C"},
    )
    if result.returncode:
        raise ValueError(f"git bundle create failed: {result.stderr.strip()}")
    verify = subprocess.run(
        ["git", "-C", os.fspath(worktree), "bundle", "verify", os.fspath(temporary)],
        capture_output=True, text=True, env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C"},
    )
    if verify.returncode:
        raise ValueError(f"git bundle verify failed: {verify.stderr.strip()}")
    return temporary.read_bytes()


def _preflight(packet: Mapping[str, Any], *, allow_unsafe_test_root: bool) -> tuple[Path, dict[str, Any]]:
    """Return a sanitized, normalized recovery intent without writing anything."""
    _reject_forbidden(packet)  # No filesystem mutation may precede this check.
    required = {
        "artifact_root", "action_id", "attempt_id", "worktree", "source_commit", "candidate_commit",
        "execution_envelope", "execution", "verification_outcome", "verification_evidence", "redacted_log",
    }
    missing, extra = required - set(packet), set(packet) - required
    if missing or extra:
        raise ValueError(f"invalid publication packet: missing={sorted(missing)}, extra={sorted(extra)}")
    action_id = packet["action_id"]
    attempt_id = packet["attempt_id"]
    if not isinstance(action_id, int) or action_id <= 0 or not isinstance(attempt_id, str) or not attempt_id:
        raise ValueError("action_id and attempt_id must identify an attempt")
    worktree = Path(str(packet["worktree"])).resolve(strict=True)
    if not worktree.is_dir():
        raise ValueError("worktree must be a directory")
    source = _exact_oid(packet["source_commit"], "source_commit")
    candidate = _exact_oid(packet["candidate_commit"], "candidate_commit")
    envelope = dict(packet["execution_envelope"])
    execution = dict(packet["execution"])
    require_compatible(envelope)
    require_compatible(execution)
    if envelope["contract_id"] != EXECUTION_ENVELOPE_V1 or execution["contract_id"] != EXECUTION_V1:
        raise ValueError("packet requires released execution-envelope/v1 and execution/v1 records")
    for record in (envelope, execution):
        if record["action_id"] != action_id or record["attempt_id"] != attempt_id:
            raise ValueError("record attempt identity mismatch")
    if envelope["source_commit"] != source or execution["command_id"] != envelope["command_id"]:
        raise ValueError("execution record does not match its envelope")
    outcome = str(packet["verification_outcome"])
    if outcome not in {"passed", "failed", "skipped"}:
        raise ValueError("verification outcome is invalid")
    evidence_bytes = canonical_bytes(packet["verification_evidence"])
    log = packet["redacted_log"]
    if not isinstance(log, str):
        raise ValueError("redacted_log must be text")
    log_bytes = log.encode("utf-8")
    if len(log_bytes) > MAX_REDACTED_LOG_BYTES:
        prefix = log_bytes[:MAX_REDACTED_LOG_BYTES - len(_TRUNCATED)].decode("utf-8", "ignore")
        log = prefix + _TRUNCATED.decode("ascii")

    if _run_git(worktree, "status", "--porcelain=v1", "--untracked-files=all") != "":
        raise ValueError("candidate worktree must be clean")
    if _run_git(worktree, "rev-parse", "HEAD").strip() != candidate:
        raise ValueError("candidate commit is promotion-stale")
    if _run_git(worktree, "cat-file", "-t", source).strip() != "commit":
        raise ValueError("source commit is unavailable")
    if _run_git(worktree, "cat-file", "-t", candidate).strip() != "commit":
        raise ValueError("candidate commit is unavailable")
    ancestor = subprocess.run(["git", "-C", os.fspath(worktree), "merge-base", "--is-ancestor", source, candidate])
    if ancestor.returncode != 0:
        raise ValueError("source commit is not an ancestor of candidate")
    source_tree = str(_run_git(worktree, "rev-parse", f"{source}^{{tree}}")).strip()
    candidate_tree = str(_run_git(worktree, "rev-parse", f"{candidate}^{{tree}}")).strip()
    paths = _changed_paths(worktree, source, candidate)

    supplied_artifact_root = Path(str(packet["artifact_root"]))
    resolved_artifact_root = supplied_artifact_root.resolve()
    if (resolved_artifact_root == worktree or worktree in resolved_artifact_root.parents
            or resolved_artifact_root in worktree.parents):
        raise ValueError("artifact_root may not be in or contain the worktree")
    artifact_root = _existing_root(
        supplied_artifact_root, allow_unsafe_test_root=allow_unsafe_test_root,
    )
    return artifact_root, {
        "action_id": action_id, "attempt_id": attempt_id, "worktree": os.fspath(worktree),
        "source_commit": source, "candidate_commit": candidate, "source_tree": source_tree,
        "candidate_tree": candidate_tree, "changed_paths": list(paths),
        "execution_envelope": envelope, "execution": execution,
        "verification_outcome": outcome, "verification_evidence": packet["verification_evidence"],
        "redacted_log": log,
    }


def _intent_payload(event: Mapping[str, Any]) -> dict[str, Any]:
    if event.get("contract_id") != JOURNAL_CONTRACT or event.get("stage") != "intent":
        raise RuntimeError("publication intent is invalid")
    intent = {key: value for key, value in event.items() if key not in {"contract_id", "stage"}}
    expected = {
        "action_id", "attempt_id", "worktree", "source_commit", "candidate_commit", "source_tree",
        "candidate_tree", "changed_paths", "execution_envelope", "execution",
        "verification_outcome", "verification_evidence", "redacted_log",
    }
    if set(intent) != expected:
        raise RuntimeError("publication intent fields are invalid")
    _reject_forbidden(intent, "intent")
    action_id, attempt_id = intent["action_id"], intent["attempt_id"]
    if not isinstance(action_id, int) or action_id <= 0 or not isinstance(attempt_id, str) or not attempt_id:
        raise RuntimeError("publication intent attempt identity is invalid")
    for field in ("source_commit", "candidate_commit", "source_tree", "candidate_tree"):
        try:
            _exact_oid(intent[field], field)
        except ValueError as exc:
            raise RuntimeError("publication intent Git identity is invalid") from exc
    paths = intent["changed_paths"]
    if (not isinstance(paths, list) or paths != sorted(paths) or len(paths) != len(set(paths))
            or any(not isinstance(path, str) or path.startswith("/") or ".." in Path(path).parts for path in paths)):
        raise RuntimeError("publication intent changed paths are invalid")
    envelope, execution = intent["execution_envelope"], intent["execution"]
    try:
        if require_compatible(envelope) != EXECUTION_ENVELOPE_V1 or require_compatible(execution) != EXECUTION_V1:
            raise ValueError("wrong execution contract")
    except (TypeError, ValueError) as exc:
        raise RuntimeError("publication intent execution contracts are invalid") from exc
    if (envelope["action_id"] != action_id or execution["action_id"] != action_id
            or envelope["attempt_id"] != attempt_id or execution["attempt_id"] != attempt_id
            or envelope["source_commit"] != intent["source_commit"]
            or envelope["command_id"] != execution["command_id"]):
        raise RuntimeError("publication intent execution binding is invalid")
    if intent["verification_outcome"] not in {"passed", "failed", "skipped"}:
        raise RuntimeError("publication intent verification outcome is invalid")
    if not isinstance(intent["redacted_log"], str):
        raise RuntimeError("publication intent redacted log is invalid")
    canonical_bytes(intent["verification_evidence"])
    return intent


def _continue_publication(
    artifact_root: Path, journal: _Journal, intent: Mapping[str, Any], *,
    fault_after: str | None, allow_unsafe_test_root: bool,
) -> dict[str, Any]:
    """Continue solely from durable sanitized intent and stage events."""
    action_id, attempt_id = int(intent["action_id"]), str(intent["attempt_id"])
    source, candidate = str(intent["source_commit"]), str(intent["candidate_commit"])
    source_tree, candidate_tree = str(intent["source_tree"]), str(intent["candidate_tree"])
    paths = tuple(str(path) for path in intent["changed_paths"])
    envelope, execution = dict(intent["execution_envelope"]), dict(intent["execution"])
    outcome = str(intent["verification_outcome"])
    evidence_bytes = canonical_bytes(intent["verification_evidence"])
    log_bytes = str(intent["redacted_log"]).encode("utf-8")
    store = FilesystemCAS(artifact_root, allow_unsafe_test_root=allow_unsafe_test_root)
    prior_bundle = journal.read("bundle")
    if prior_bundle:
        expected_bundle = {
            "action_id": action_id, "attempt_id": attempt_id, "source_commit": source,
            "candidate_commit": candidate, "source_tree": source_tree, "candidate_tree": candidate_tree,
        }
        if any(prior_bundle.get(key) != value for key, value in expected_bundle.items()):
            raise RuntimeError("journal belongs to a different candidate")
        bundle_ref = str(prior_bundle["bundle_ref"])
        store.get(bundle_ref)
    else:
        worktree = Path(str(intent["worktree"])).resolve(strict=True)
        if _run_git(worktree, "status", "--porcelain=v1", "--untracked-files=all") != "":
            raise ValueError("candidate worktree must be clean")
        if _run_git(worktree, "rev-parse", "HEAD").strip() != candidate:
            raise ValueError("candidate commit is promotion-stale")
        descriptor, temporary_name = tempfile.mkstemp(prefix=".bundle-", dir=store.root)
        os.close(descriptor)
        temporary = Path(temporary_name)
        temporary.unlink()
        try:
            bundle_bytes = _make_bundle(worktree, temporary)
        finally:
            temporary.unlink(missing_ok=True)
        bundle_ref = store.put(bundle_bytes)
        journal.write("bundle", {
            "action_id": action_id, "attempt_id": attempt_id, "source_commit": source,
            "candidate_commit": candidate, "source_tree": source_tree, "candidate_tree": candidate_tree,
            "bundle_ref": bundle_ref,
        })
    if fault_after == "bundle":
        raise RuntimeError("injected failure after bundle publication")

    manifest = {
        "contract_id": MANIFEST_CONTRACT, "action_id": action_id, "attempt_id": attempt_id,
        "source_commit": source, "source_tree": source_tree, "candidate_commit": candidate,
        "candidate_tree": candidate_tree, "changed_paths": list(paths),
    }
    candidate_record = Candidate(action_id, attempt_id, source, paths)
    candidate_bytes = _record(candidate_record, CANDIDATE_V1)
    candidate_digest = f"sha256:{hashlib.sha256(candidate_bytes).hexdigest()}"
    evidence_digest = f"sha256:{hashlib.sha256(evidence_bytes).hexdigest()}"
    verification_record = Verification(
        action_id, attempt_id, execution["command_id"], outcome, evidence_digest,
    )
    verification_bytes = _record(verification_record, VERIFICATION_V1)
    verification_digest = f"sha256:{hashlib.sha256(verification_bytes).hexdigest()}"
    publication_record = Publication(
        action_id, attempt_id, candidate_digest, verification_digest,
        "verified" if outcome == "passed" else "rejected",
    )
    objects = {
        "bundle": bundle_ref,
        "manifest": store.put(canonical_bytes(manifest)),
        "execution_envelope": store.put(_record(envelope, EXECUTION_ENVELOPE_V1)),
        "candidate": store.put(candidate_bytes),
        "execution": store.put(_record(execution, EXECUTION_V1)),
        "verification": store.put(verification_bytes),
        "publication": store.put(_record(publication_record, PUBLICATION_V1)),
        "verification_evidence": store.put(evidence_bytes),
        "redacted_log": store.put(log_bytes),
    }
    state = {
        "action_id": action_id, "attempt_id": attempt_id, "source_commit": source,
        "candidate_commit": candidate, "source_tree": source_tree, "candidate_tree": candidate_tree,
        "records": objects, "redacted_log_retention_days": REDACTED_LOG_RETENTION_DAYS,
    }
    receipt = {
        "contract_id": "publication-receipt/v1", **state,
    }
    journal_ref = store.put(canonical_bytes(receipt))
    state["journal_ref"] = journal_ref
    journal.write("objects", state)
    if fault_after == "objects":
        raise RuntimeError("injected failure after all object publication")
    for reference in objects.values():
        store.get(reference)
    store.get(journal_ref)
    journal.write("complete", state)
    return state


def publish(
    packet: Mapping[str, Any], *, fault_after: str | None = None, allow_unsafe_test_root: bool = False,
) -> dict[str, Any]:
    """Validate, persist sanitized intent, and immutably publish one candidate."""
    artifact_root, intent = _preflight(packet, allow_unsafe_test_root=allow_unsafe_test_root)
    journal = _Journal(artifact_root, int(intent["action_id"]), str(intent["attempt_id"]))
    # Intent is the first durable publication write. Exact create-only replay
    # prevents an attempt identity from being rebound to a different packet.
    journal.write("intent", intent)
    persisted = journal.read("intent")
    assert persisted is not None
    return _continue_publication(
        artifact_root, journal, _intent_payload(persisted), fault_after=fault_after,
        allow_unsafe_test_root=allow_unsafe_test_root,
    )


def _existing_root(root: Path | str, *, allow_unsafe_test_root: bool = False) -> Path:
    path = Path(root)
    if not path.is_absolute() or not path.is_dir() or path.is_symlink():
        raise ValueError("artifact_root must be a provisioned existing absolute directory")
    info = path.stat()
    if info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise PermissionError("artifact_root must be owner-only")
    if not allow_unsafe_test_root:
        unsafe = [
            Path("/tmp"), Path("/var/tmp"), Path("/projects/dev/_artifacts"),
            Path.home() / ".cache", Path.home() / ".local/state/actionq",
        ]
        for variable in ("XDG_CACHE_HOME", "XDG_RUNTIME_DIR"):
            if os.environ.get(variable):
                unsafe.append(Path(os.environ[variable]).resolve())
        if any(path == base or base in path.parents for base in unsafe):
            raise ValueError("artifact_root may not be temporary, cache, runtime, or runner staging storage")
        if any((ancestor / ".git").exists() for ancestor in (path, *path.parents)):
            raise ValueError("artifact_root may not be inside a Git checkout")
    return path


def list_publications(
    artifact_root: Path | str, action_id: int, *, allow_unsafe_test_root: bool = False,
) -> list[dict[str, Any]]:
    """List complete publications and safe summaries of interrupted attempts."""
    if not isinstance(action_id, int) or action_id <= 0:
        raise ValueError("action_id must be positive")
    root = _existing_root(artifact_root, allow_unsafe_test_root=allow_unsafe_test_root)
    action_root = root / "journals" / str(action_id)
    if not action_root.exists():
        return []
    publications: list[tuple[int, dict[str, Any]]] = []
    for attempt in action_root.iterdir():
        if attempt.is_symlink() or not attempt.is_dir():
            raise RuntimeError("unsafe publication journal entry")
        journal = _Journal(root, action_id, attempt.name)
        intent_event = journal.read("intent")
        if intent_event is None:
            raise RuntimeError("publication journal is missing its intent")
        intent = _intent_payload(intent_event)
        complete = journal.read("complete")
        if complete is not None:
            state = {key: value for key, value in complete.items() if key not in {"contract_id", "stage"}}
            publications.append(((attempt / "complete.json").stat().st_mtime_ns, state))
            continue
        latest_stage = "intent"
        summary: dict[str, Any] = {
            "action_id": action_id, "attempt_id": attempt.name, "status": "incomplete",
            "latest_stage": latest_stage, "source_commit": intent["source_commit"],
            "candidate_commit": intent["candidate_commit"], "source_tree": intent["source_tree"],
            "candidate_tree": intent["candidate_tree"],
        }
        latest_path = attempt / "intent.json"
        bundle = journal.read("bundle")
        if bundle is not None:
            latest_stage, latest_path = "bundle", attempt / "bundle.json"
            summary.update({"latest_stage": latest_stage, "bundle_ref": bundle["bundle_ref"]})
        objects = journal.read("objects")
        if objects is not None:
            latest_stage, latest_path = "objects", attempt / "objects.json"
            summary.update({
                "latest_stage": latest_stage, "journal_ref": objects["journal_ref"],
                "records": objects["records"],
            })
        publications.append((latest_path.stat().st_mtime_ns, summary))
    return [value for _, value in sorted(publications, key=lambda item: (-item[0], item[1]["attempt_id"]))]


def resume_publication(
    artifact_root: Path | str, action_id: int, attempt_id: str, *,
    fault_after: str | None = None, allow_unsafe_test_root: bool = False,
) -> dict[str, Any]:
    """Resume an interrupted publication using only its durable intent."""
    if not isinstance(action_id, int) or action_id <= 0:
        raise ValueError("action_id must be positive")
    root = _existing_root(artifact_root, allow_unsafe_test_root=allow_unsafe_test_root)
    directory = root / "journals" / str(action_id) / attempt_id
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("publication intent does not exist")
    journal = _Journal(root, action_id, attempt_id)
    event = journal.read("intent")
    if event is None:
        raise ValueError("publication intent does not exist")
    intent = _intent_payload(event)
    if intent["action_id"] != action_id or intent["attempt_id"] != attempt_id:
        raise RuntimeError("publication intent identity mismatch")
    return _continue_publication(
        root, journal, intent, fault_after=fault_after,
        allow_unsafe_test_root=allow_unsafe_test_root,
    )


def recover_publication(
    artifact_root: Path | str, action_id: int, attempt_id: str | None = None, *,
    allow_unsafe_test_root: bool = False,
) -> dict[str, Any] | None:
    """Recover a complete publication and verify every referenced CAS byte."""
    values = [
        value for value in list_publications(
            artifact_root, action_id, allow_unsafe_test_root=allow_unsafe_test_root,
        ) if value.get("status") != "incomplete"
    ]
    if attempt_id is not None:
        values = [value for value in values if value["attempt_id"] == attempt_id]
    if not values:
        return None
    value = values[0]
    store = FilesystemCAS(artifact_root, allow_unsafe_test_root=allow_unsafe_test_root)
    for reference in value["records"].values():
        store.get(reference)
    receipt = json.loads(store.get(value["journal_ref"]))
    if receipt != {"contract_id": "publication-receipt/v1", **{k: v for k, v in value.items() if k != "journal_ref"}}:
        raise RuntimeError("publication receipt does not match journal")
    return value


def acknowledge_settlement(
    packet: Mapping[str, Any], *, allow_unsafe_test_root: bool = False,
) -> dict[str, Any]:
    """Append the one idempotent ActionQ settlement acknowledgement."""
    _reject_forbidden(packet)
    required = {
        "artifact_root", "action_id", "attempt_id", "journal_ref", "terminal_status", "result_ref",
        "authoritative_decision",
    }
    if set(packet) != required:
        raise ValueError("invalid settlement acknowledgement packet")
    action_id, attempt_id = packet["action_id"], packet["attempt_id"]
    publication = recover_publication(
        packet["artifact_root"], action_id, str(attempt_id),
        allow_unsafe_test_root=allow_unsafe_test_root,
    )
    if publication is None or publication["journal_ref"] != packet["journal_ref"]:
        raise ValueError("settlement acknowledgement does not bind a complete publication")
    status = packet["terminal_status"]
    if status != "completed":
        raise ValueError("publication settlement acknowledgement requires completed status")
    result_ref = str(packet["result_ref"])
    if result_ref != packet["journal_ref"]:
        raise ValueError("completed ActionQ result_ref must equal the publication journal_ref")
    decision = packet["authoritative_decision"]
    if not isinstance(decision, Mapping) or set(decision) != {
        "action_id", "status", "result_ref", "completed_at",
    }:
        raise ValueError("authoritative_decision fields are ambiguous")
    if (decision["action_id"] != action_id or decision["status"] != "completed"
            or decision["result_ref"] != result_ref
            or not isinstance(decision["completed_at"], str) or not decision["completed_at"]):
        raise ValueError("authoritative_decision does not match completed publication settlement")
    value = {
        "contract_id": JOURNAL_CONTRACT, "stage": "settlement", "action_id": action_id,
        "attempt_id": attempt_id, "journal_ref": packet["journal_ref"],
        "terminal_status": status, "result_ref": result_ref,
        "authoritative_decision": dict(decision),
    }
    journal = _Journal(Path(str(packet["artifact_root"])), action_id, str(attempt_id))
    journal.write("settlement", {key: item for key, item in value.items() if key not in {"contract_id", "stage"}})
    return value


def query_settlement(
    artifact_root: Path | str, action_id: int, attempt_id: str, *, allow_unsafe_test_root: bool = False,
) -> dict[str, Any] | None:
    if not isinstance(action_id, int) or action_id <= 0:
        raise ValueError("action_id must be positive")
    root = _existing_root(artifact_root, allow_unsafe_test_root=allow_unsafe_test_root)
    attempt = root / "journals" / str(action_id) / attempt_id
    if not attempt.exists():
        return None
    return _Journal(root, action_id, attempt_id).read("settlement")


__all__ = [
    "ArtifactStore", "FilesystemCAS", "MAX_REDACTED_LOG_BYTES", "acknowledge_settlement", "artifact_ref",
    "list_publications", "publish", "query_settlement", "recover_publication", "resume_publication",
]
