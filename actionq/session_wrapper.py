"""Harness-neutral Tier-0 session wrapper.

Wraps a manual or dispatched session -- any harness, or none at all -- and
emits a validated ``session-capsule/v1`` artifact once at session end
(clean or crash-inferred). This is "mechanical exhaust": recorded with no
agent cooperation required, so it works identically for a human typing
commands in a terminal and for a daemon-dispatched harness child process.

Normative contract:
  /projects/dev/agentops/docs/dispatch/session-mechanization-contracts.md
Normative schema:
  /projects/dev/agentops/templates/dispatch/session-mechanization/session-capsule.schema.json

Ownership split (per the contract doc): actionq owns this wrapper
*mechanism* and the session lifecycle *state* it observes; agentops owns
the capsule *contract* above. This module mirrors the semantic validation
rules from agentops' dependency-free validator
(``templates/dispatch/scripts/validate_session_mechanization_artifacts.py``)
rather than importing or shelling out to it, so actionq does not take a
runtime dependency on another repo's checkout being present. Keep
``_validate_capsule`` below in sync with that script if the contract
changes; ``verification/`` or the caller's own CI can additionally run the
external validator against emitted capsules for cross-repo parity.

Non-scope (per work item #1114): raw transcript retention, sprint state
mutation, and a scribectl dependency. This module never writes sprintctl or
scribectl state and never persists prompt/transcript content -- only an
opt-in prompt digest sidecar (see ``prompt_digest_sidecar_path``), which is
not itself a capsule field since the schema is ``additionalProperties:
false`` and does not define one.

Failure posture: recording fails OPEN. Any error while computing git state,
building the capsule payload, validating it, or writing it to disk is
caught, reported through the ``on_recorder_error`` callback (default:
stderr), and never re-raised. The wrapped command's own exit code always
propagates untouched.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

SCHEMA_VERSION = "session-capsule/v1"

DEFAULT_CAPSULE_DIR = Path("~/.local/state/actionq/session-wrapper/capsules")
DEFAULT_MARKER_DIR = Path("~/.local/state/actionq/session-wrapper/markers")

_TARGET_RANKS = {"explicit", "path-scope", "doc-link", "candidate", "repo-level"}
_VERIFICATION_RESULTS = {"pass", "fail", "error", "skipped"}
_END_KINDS = {"clean-end", "end-inferred"}
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}([0-9a-f]{24})?$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SHORTSTAT_RE = re.compile(
    r"(?:(\d+) files? changed)?(?:,?\s*(\d+) insertions?\(\+\))?(?:,?\s*(\d+) deletions?\(-\))?"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _default_error_reporter(message: str, exc: BaseException | None = None) -> None:
    detail = f": {exc}" if exc is not None else ""
    print(f"actionq.session_wrapper: {message}{detail}", file=sys.stderr)


def _pid_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class SessionWrapperError(RuntimeError):
    """Raised only for caller-facing setup mistakes, never during recording."""


@dataclass(frozen=True)
class SessionTarget:
    rank: str
    ref: str

    def __post_init__(self) -> None:
        if self.rank not in _TARGET_RANKS:
            raise SessionWrapperError(f"target.rank {self.rank!r} is not a recognized rank")
        if not self.ref or not self.ref.strip():
            raise SessionWrapperError("target.ref must be a non-empty string")


@dataclass(frozen=True)
class SessionClaim:
    claim_id: str
    work_item_id: str
    claim_type: str
    acquired_automatically: bool = False


@dataclass(frozen=True)
class SessionIdentity:
    """Caller-supplied identity/context inputs for a wrapped session."""

    repo_project: str
    actor: str
    harness: str = "manual"
    model_name: str | None = None
    model_version: str | None = None
    repo_id: str | None = None
    target: SessionTarget | None = None
    claim: SessionClaim | None = None
    runtime_session_id: str | None = None
    starting_watermark_ingest_offset: int | str = 0
    starting_watermark_age_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.claim is not None and self.claim.acquired_automatically:
            if self.target is None or self.target.rank != "explicit":
                raise SessionWrapperError(
                    "claim.acquired_automatically requires target.rank == explicit"
                )


@dataclass(frozen=True)
class VerificationOutcome:
    command: str
    result: str

    def __post_init__(self) -> None:
        if self.result not in _VERIFICATION_RESULTS:
            raise SessionWrapperError(f"verification result {self.result!r} is not recognized")


def run_verification_command(command: Sequence[str] | str, *, cwd: Path | None = None) -> VerificationOutcome:
    """Run a verification command and classify its outcome.

    A non-zero, well-formed exit is ``fail``; a command that could not be
    started at all (missing binary, OS error) is ``error``.
    """
    display = command if isinstance(command, str) else " ".join(command)
    try:
        completed = subprocess.run(
            command, cwd=cwd, shell=isinstance(command, str), text=True, capture_output=True, check=False
        )
    except OSError:
        return VerificationOutcome(command=display, result="error")
    return VerificationOutcome(command=display, result="pass" if completed.returncode == 0 else "fail")


@dataclass
class _Marker:
    """Crash-recovery marker written before wrapped work begins.

    Mirrors ``actionq.daemon.SessionRecord``'s stale-state pattern: durable
    state is written before the wrapped process starts and cleared only
    after a capsule (clean-end or end-inferred) has been recorded, so a
    wrapper process that itself dies mid-session leaves enough evidence for
    a later invocation to close the session out honestly.
    """

    session_id: str
    capsule_id: str
    origin_stream_id: str
    repo_path: str
    base_commit: str
    branch: str
    worktree: str
    started_at: str
    pid: int
    identity: dict[str, Any]


def _marker_path(marker_dir: Path, session_id: str) -> Path:
    safe = session_id.replace("/", "_")
    return marker_dir / f"{safe}.json"


def _write_marker(marker_dir: Path, marker: _Marker) -> Path:
    marker_dir.mkdir(parents=True, exist_ok=True)
    path = _marker_path(marker_dir, marker.session_id)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(asdict(marker), sort_keys=True), encoding="utf-8")
    tmp.replace(path)
    return path


def _read_marker(path: Path) -> _Marker | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return _Marker(**payload)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _clear_marker(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _git(repo_path: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_path), *args], text=True, capture_output=True, check=False
    )
    if completed.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout.strip()


def _parse_shortstat(text: str) -> dict[str, int]:
    match = _SHORTSTAT_RE.search(text)
    if not match:
        return {"files_changed": 0, "insertions": 0, "deletions": 0}
    files, insertions, deletions = match.groups()
    return {
        "files_changed": int(files or 0),
        "insertions": int(insertions or 0),
        "deletions": int(deletions or 0),
    }


def _git_state_at_start(repo_path: Path) -> tuple[str, str]:
    """Return ``(base_commit, branch)`` at session start."""
    base_commit = _git(repo_path, "rev-parse", "HEAD")
    try:
        branch = _git(repo_path, "rev-parse", "--abbrev-ref", "HEAD")
    except RuntimeError:
        branch = ""
    return base_commit, branch


def _git_status_entries(repo_path: Path) -> list[tuple[str, str]]:
    """Parse ``git status --porcelain`` into ``(status_code, path)`` pairs.

    Rename entries ("R  old -> new") are reduced to their new path. Exotic
    quoted filenames are handled best-effort by stripping surrounding
    quotes; this mirrors the common case well enough for Tier-0 mechanical
    exhaust without pulling in a full porcelain-v2/-z parser.
    """
    output = _git(repo_path, "status", "--porcelain")
    entries: list[tuple[str, str]] = []
    for line in output.splitlines():
        if not line:
            continue
        code, rest = line[:2], line[3:]
        if " -> " in rest:
            rest = rest.split(" -> ", 1)[1]
        entries.append((code, rest.strip().strip('"')))
    return entries


def _new_file_patch(repo_path: Path, rel_path: str) -> str:
    """A synthetic unified diff for an untracked file, via ``--no-index``.

    ``git diff --no-index`` exits 1 when the two sides differ (the normal
    case here, since /dev/null is always "empty"), which is not an error.
    """
    completed = subprocess.run(
        ["git", "-C", str(repo_path), "diff", "--no-index", "--", "/dev/null", rel_path],
        text=True, capture_output=True, check=False,
    )
    return completed.stdout


def _git_state_at_end(repo_path: Path, base_commit: str) -> dict[str, Any]:
    head_commit = _git(repo_path, "rev-parse", "HEAD")
    try:
        branch = _git(repo_path, "rev-parse", "--abbrev-ref", "HEAD")
    except RuntimeError:
        branch = ""
    worktree = _git(repo_path, "rev-parse", "--show-toplevel")

    if head_commit == base_commit:
        commits: list[str] = []
    else:
        log = _git(repo_path, "log", "--format=%H", f"{base_commit}..{head_commit}")
        commits = [line for line in log.splitlines() if line]

    status_entries = _git_status_entries(repo_path)
    dirty = bool(status_entries)
    untracked_paths = sorted({path for code, path in status_entries if code == "??"})

    # Cumulative session diff for *tracked* content: base_commit vs. current
    # working tree, so this covers both committed-during-session and
    # still-uncommitted changes to files git already knows about.
    shortstat = _git(repo_path, "diff", base_commit, "--shortstat")
    tracked_stat = _parse_shortstat(shortstat)
    names = _git(repo_path, "diff", base_commit, "--name-only")
    tracked_touched = {line for line in names.splitlines() if line}

    # `git diff` never reports untracked files, but they still make the
    # worktree dirty and still belong in the session's evidence -- fold
    # them in explicitly rather than silently under-reporting.
    untracked_line_counts: dict[str, int] = {}
    for rel_path in untracked_paths:
        try:
            text = (repo_path / rel_path).read_text(encoding="utf-8", errors="replace")
        except (OSError, UnicodeDecodeError):
            text = ""
        untracked_line_counts[rel_path] = len(text.splitlines()) if text else 0

    diff_stat = {
        "files_changed": tracked_stat["files_changed"] + len(untracked_paths),
        "insertions": tracked_stat["insertions"] + sum(untracked_line_counts.values()),
        "deletions": tracked_stat["deletions"],
    }
    touched_paths = sorted(tracked_touched | set(untracked_paths))

    patch_digest = None
    if dirty:
        # Schema rule: sha256 of the uncommitted dirty-state patch. Tracked
        # staged+unstaged changes come from `git diff HEAD`; untracked new
        # files are folded in via a synthetic --no-index diff each, in
        # deterministic path order, so the digest is reproducible.
        tracked_patch = subprocess.run(
            ["git", "-C", str(repo_path), "diff", "HEAD"], text=True, capture_output=True, check=False
        ).stdout
        untracked_patches = "".join(_new_file_patch(repo_path, path) for path in untracked_paths)
        combined = tracked_patch + untracked_patches
        patch_digest = hashlib.sha256(combined.encode("utf-8", errors="replace")).hexdigest()

    return {
        "base_commit": base_commit,
        "head_commit": head_commit,
        "commits": commits,
        "branch": branch,
        "worktree": worktree,
        "dirty": dirty,
        "patch_digest": patch_digest,
        "diff_stat": diff_stat,
        "touched_paths": touched_paths,
    }


def _validate_capsule(payload: dict[str, Any]) -> None:
    """Mirror the cross-field rules in agentops'
    ``validate_session_mechanization_artifacts.py`` for session-capsule/v1.
    Raises ``ValueError`` on the first violation found.
    """
    required = (
        "schema_version", "capsule_id", "origin_stream_id", "runtime_session_id", "repo",
        "harness", "model", "actor", "target", "claim", "starting_watermark",
        "started_at", "ended_at", "end", "git", "verification", "privacy",
    )
    missing = [field_name for field_name in required if field_name not in payload]
    if missing:
        raise ValueError(f"capsule missing fields: {', '.join(missing)}")
    if payload["schema_version"] != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION!r}")
    if not _UUID_RE.fullmatch(payload["capsule_id"]):
        raise ValueError("capsule_id must be a lowercase UUID")
    if not _UUID_RE.fullmatch(payload["origin_stream_id"]):
        raise ValueError("origin_stream_id must be a lowercase UUID")
    if not payload["runtime_session_id"] or not str(payload["runtime_session_id"]).strip():
        raise ValueError("runtime_session_id must be non-blank")
    if not payload.get("repo", {}).get("project"):
        raise ValueError("repo.project must be non-blank")
    if not payload["harness"] or not str(payload["harness"]).strip():
        raise ValueError("harness must be non-blank")
    if not payload["actor"] or not str(payload["actor"]).strip():
        raise ValueError("actor must be non-blank")

    model = payload["model"]
    if model is not None and (not isinstance(model, dict) or not model.get("name")):
        raise ValueError("model must be null or an object with a non-empty name")

    target = payload["target"]
    claim = payload["claim"]
    if target is not None:
        if target.get("rank") not in _TARGET_RANKS:
            raise ValueError(f"target.rank {target.get('rank')!r} is not recognized")
        if not target.get("ref"):
            raise ValueError("target.ref must be non-blank")
    if claim is not None:
        for key in ("claim_id", "work_item_id", "claim_type", "acquired_automatically"):
            if key not in claim:
                raise ValueError(f"claim missing {key}")
        if claim["acquired_automatically"] and (target is None or target.get("rank") != "explicit"):
            raise ValueError("claim.acquired_automatically requires target.rank == explicit")

    watermark = payload["starting_watermark"]
    for key in ("ingest_offset", "age_seconds"):
        if key not in watermark:
            raise ValueError(f"starting_watermark missing {key}")

    end = payload["end"]
    if end.get("kind") not in _END_KINDS:
        raise ValueError("end.kind must be clean-end or end-inferred")

    git = payload["git"]
    for key in ("base_commit", "head_commit", "commits", "branch", "worktree", "dirty", "diff_stat", "touched_paths"):
        if key not in git:
            raise ValueError(f"git missing {key}")
    if not _GIT_SHA_RE.fullmatch(git["base_commit"]) or not _GIT_SHA_RE.fullmatch(git["head_commit"]):
        raise ValueError("git.base_commit and git.head_commit must be full git object ids")
    for sha in git["commits"]:
        if not _GIT_SHA_RE.fullmatch(sha):
            raise ValueError("git.commits entries must be full git object ids")
    patch_digest = git.get("patch_digest")
    if patch_digest is not None and not _SHA256_RE.fullmatch(patch_digest):
        raise ValueError("git.patch_digest must be null or a lowercase sha256")
    if git["dirty"] and not patch_digest:
        raise ValueError("git.patch_digest is required when git.dirty is true")
    for key in ("files_changed", "insertions", "deletions"):
        if key not in git["diff_stat"]:
            raise ValueError(f"git.diff_stat missing {key}")

    for entry in payload["verification"]:
        if entry.get("result") not in _VERIFICATION_RESULTS:
            raise ValueError(f"verification result {entry.get('result')!r} is not recognized")

    privacy = payload["privacy"]
    if "raw_transcript_captured" not in privacy:
        raise ValueError("privacy.raw_transcript_captured is required")
    if not privacy["raw_transcript_captured"] and privacy.get("raw_transcript_ref"):
        raise ValueError("raw_transcript_ref must be null when raw_transcript_captured is false")


def compute_prompt_digest(prompt: str | bytes | None) -> str | None:
    """sha256 hex digest of a prompt, or ``None`` if no prompt was supplied.

    Not a capsule field (the schema is ``additionalProperties: false`` and
    defines no such field); callers that want it recorded should persist
    the returned digest as a sidecar (see ``prompt_digest_sidecar_path``)
    alongside the capsule, never the raw prompt content itself.
    """
    if prompt is None:
        return None
    data = prompt.encode("utf-8") if isinstance(prompt, str) else prompt
    return hashlib.sha256(data).hexdigest()


def prompt_digest_sidecar_path(capsule_dir: Path, capsule_id: str) -> Path:
    return capsule_dir / f"{capsule_id}.prompt-digest.txt"


def _build_capsule(
    *,
    capsule_id: str,
    origin_stream_id: str,
    runtime_session_id: str,
    identity: SessionIdentity,
    started_at: str,
    ended_at: str,
    end_kind: str,
    end_reason: str | None,
    exit_code: int | None,
    git_state: dict[str, Any],
    verification: Sequence[VerificationOutcome],
) -> dict[str, Any]:
    model = None
    if identity.model_name:
        model = {"name": identity.model_name}
        if identity.model_version:
            model["version"] = identity.model_version

    repo: dict[str, Any] = {"project": identity.repo_project}
    if identity.repo_id:
        repo["repo_id"] = identity.repo_id

    target = None
    if identity.target is not None:
        target = {"rank": identity.target.rank, "ref": identity.target.ref}

    claim = None
    if identity.claim is not None:
        claim = {
            "claim_id": identity.claim.claim_id,
            "work_item_id": identity.claim.work_item_id,
            "claim_type": identity.claim.claim_type,
            "acquired_automatically": identity.claim.acquired_automatically,
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "capsule_id": capsule_id,
        "origin_stream_id": origin_stream_id,
        "runtime_session_id": runtime_session_id,
        "repo": repo,
        "harness": identity.harness,
        "model": model,
        "actor": identity.actor,
        "target": target,
        "claim": claim,
        "starting_watermark": {
            "ingest_offset": identity.starting_watermark_ingest_offset,
            "age_seconds": identity.starting_watermark_age_seconds,
        },
        "started_at": started_at,
        "ended_at": ended_at,
        "end": {"kind": end_kind, "reason": end_reason, "exit_code": exit_code},
        "git": {
            "base_commit": git_state["base_commit"],
            "head_commit": git_state["head_commit"],
            "commits": git_state["commits"],
            "branch": git_state["branch"],
            "worktree": git_state["worktree"],
            "dirty": git_state["dirty"],
            "patch_digest": git_state["patch_digest"],
            "diff_stat": git_state["diff_stat"],
            "touched_paths": git_state["touched_paths"],
        },
        "verification": [
            {"command": item.command, "result": item.result, "evidence_ref": None} for item in verification
        ],
        "privacy": {"raw_transcript_captured": False, "raw_transcript_ref": None, "retention_days": None},
    }


class SessionWrapper:
    """Wraps one manual-or-dispatched session and records its Tier-0 capsule.

    Usage (dispatched / arbitrary command)::

        wrapper = SessionWrapper(identity, repo_path=Path("/projects/dev/foo"))
        exit_code = wrapper.run(["claude", "-p", "do the thing"])

    Usage (manual, wrapping an existing block of work)::

        wrapper = SessionWrapper(identity, repo_path=Path("/projects/dev/foo"))
        wrapper.start()
        try:
            ...  # human or scripted work happens here
            outcome = 0
        finally:
            wrapper.finish(exit_code=outcome, verification=[...])

    Every public entry point fails open: a recording error is reported
    through ``on_recorder_error`` and swallowed. ``run()`` and ``finish()``
    always return normally with the wrapped work's own outcome.
    """

    def __init__(
        self,
        identity: SessionIdentity,
        *,
        repo_path: Path,
        capsule_dir: Path | None = None,
        marker_dir: Path | None = None,
        on_recorder_error: Callable[[str, BaseException | None], None] = _default_error_reporter,
    ):
        self.identity = identity
        self.repo_path = Path(repo_path)
        self.capsule_dir = Path(capsule_dir or DEFAULT_CAPSULE_DIR).expanduser()
        self.marker_dir = Path(marker_dir or DEFAULT_MARKER_DIR).expanduser()
        self.on_recorder_error = on_recorder_error

        self.session_id = identity.runtime_session_id or f"aqs:{uuid.uuid4()}"
        self.capsule_id = str(uuid.uuid4())
        # No durable outbox exists yet for Tier-0 wrapper output (out of
        # scope for #1114); each capsule mints its own stream identity.
        self.origin_stream_id = str(uuid.uuid4())

        self._marker_path: Path | None = None
        self._base_commit: str | None = None
        self._branch: str | None = None
        self._started_at: str | None = None
        self._finished = False
        self.last_capsule_path: Path | None = None
        self.last_capsule: dict[str, Any] | None = None

    # -- lifecycle -----------------------------------------------------

    def start(self) -> None:
        """Record session start and write a crash-recovery marker.

        Recovers any stale marker from a previous crashed wrapper process
        in this marker directory before starting a new session, so crash
        evidence is never silently lost by piling up markers.
        """
        recover_stale_markers(self.marker_dir, self.capsule_dir, on_recorder_error=self.on_recorder_error)

        self._base_commit, self._branch = _git_state_at_start(self.repo_path)
        self._started_at = _now()

        try:
            marker = _Marker(
                session_id=self.session_id,
                capsule_id=self.capsule_id,
                origin_stream_id=self.origin_stream_id,
                repo_path=str(self.repo_path),
                base_commit=self._base_commit,
                branch=self._branch,
                worktree=str(self.repo_path),
                started_at=self._started_at,
                pid=os.getpid(),
                identity=_identity_to_marker_dict(self.identity),
            )
            self._marker_path = _write_marker(self.marker_dir, marker)
        except Exception as exc:  # fail open: a marker write failure must not block manual work
            self.on_recorder_error("failed to write session start marker", exc)
            self._marker_path = None

    def finish(
        self,
        *,
        exit_code: int | None,
        end_kind: str = "clean-end",
        reason: str | None = None,
        verification: Sequence[VerificationOutcome] = (),
    ) -> Path | None:
        """Build, validate, and write the capsule. Never raises."""
        if self._finished:
            return self.last_capsule_path
        self._finished = True
        try:
            return self._record(exit_code=exit_code, end_kind=end_kind, reason=reason, verification=verification)
        except Exception as exc:  # the whole point: recording must fail open
            self.on_recorder_error("failed to record session capsule", exc)
            return None
        finally:
            if self._marker_path is not None:
                _clear_marker(self._marker_path)

    def _record(
        self,
        *,
        exit_code: int | None,
        end_kind: str,
        reason: str | None,
        verification: Sequence[VerificationOutcome],
    ) -> Path | None:
        assert self._base_commit is not None and self._started_at is not None, "start() was not called"
        git_state = _git_state_at_end(self.repo_path, self._base_commit)
        capsule = _build_capsule(
            capsule_id=self.capsule_id,
            origin_stream_id=self.origin_stream_id,
            runtime_session_id=self.session_id,
            identity=self.identity,
            started_at=self._started_at,
            ended_at=_now(),
            end_kind=end_kind,
            end_reason=reason,
            exit_code=exit_code,
            git_state=git_state,
            verification=verification,
        )
        _validate_capsule(capsule)
        self.capsule_dir.mkdir(parents=True, exist_ok=True)
        path = self.capsule_dir / f"{self.capsule_id}.json"
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(capsule, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
        self.last_capsule_path = path
        self.last_capsule = capsule
        return path

    def run(
        self,
        argv: Sequence[str],
        *,
        verification: Sequence[str | Sequence[str]] = (),
        cwd: Path | None = None,
    ) -> int:
        """Spawn ``argv`` as the wrapped process, wait for it, then record.

        Returns the child's exit code (or a synthesized negative signal
        code) regardless of whether capsule recording succeeds.
        """
        self.start()
        exit_code: int
        end_kind = "clean-end"
        reason: str | None = "wrapped command exited"
        try:
            completed = subprocess.run(list(argv), cwd=cwd or self.repo_path)
            exit_code = completed.returncode
        except KeyboardInterrupt:
            exit_code = -signal.SIGINT
            reason = "interrupted"
            raise
        except Exception as exc:
            exit_code = -1
            end_kind = "end-inferred"
            reason = f"failed to launch wrapped command: {exc}"
            self.finish(exit_code=exit_code, end_kind=end_kind, reason=reason)
            raise
        else:
            outcomes = [
                item if isinstance(item, VerificationOutcome) else run_verification_command(item, cwd=cwd or self.repo_path)
                for item in verification
            ]
            self.finish(exit_code=exit_code, end_kind=end_kind, reason=reason, verification=outcomes)
        return exit_code


def _identity_to_marker_dict(identity: SessionIdentity) -> dict[str, Any]:
    payload = asdict(identity)
    return payload


def _identity_from_marker_dict(payload: dict[str, Any]) -> SessionIdentity:
    target = payload.get("target")
    claim = payload.get("claim")
    return SessionIdentity(
        repo_project=payload["repo_project"],
        actor=payload["actor"],
        harness=payload.get("harness", "manual"),
        model_name=payload.get("model_name"),
        model_version=payload.get("model_version"),
        repo_id=payload.get("repo_id"),
        target=SessionTarget(**target) if target else None,
        claim=SessionClaim(**claim) if claim else None,
        runtime_session_id=payload.get("runtime_session_id"),
        starting_watermark_ingest_offset=payload.get("starting_watermark_ingest_offset", 0),
        starting_watermark_age_seconds=payload.get("starting_watermark_age_seconds", 0.0),
    )


def recover_stale_markers(
    marker_dir: Path,
    capsule_dir: Path,
    *,
    on_recorder_error: Callable[[str, BaseException | None], None] = _default_error_reporter,
) -> list[Path]:
    """Emit an ``end-inferred`` capsule for each marker whose pid is dead.

    Mirrors ``actionq.daemon.Daemon.recover_stale_state``: a wrapper process
    that died before calling ``finish()`` leaves its marker behind. The next
    wrapper invocation (or an explicit maintenance call) sweeps markers
    whose pid is no longer alive into a best-effort ``end-inferred``
    capsule built from whatever git state survives, then clears the marker.
    A marker whose pid is still alive is left untouched -- it may belong to
    a concurrently running session.

    Failures are reported and swallowed per marker; one bad marker does not
    block recovery of the others, and recovery never blocks the caller's
    own session start.
    """
    recorded: list[Path] = []
    if not marker_dir.exists():
        return recorded
    for marker_path in sorted(marker_dir.glob("*.json")):
        marker = _read_marker(marker_path)
        if marker is None:
            # Malformed marker: nothing recoverable, but do not let it pile
            # up forever either.
            _clear_marker(marker_path)
            continue
        if _pid_alive(marker.pid):
            # Still running (or owned by a live pid we can't distinguish
            # from a genuinely active session) -- leave it for its own
            # wrapper to finish() normally.
            continue
        try:
            identity = _identity_from_marker_dict(marker.identity)
            repo_path = Path(marker.repo_path)
            try:
                git_state = _git_state_at_end(repo_path, marker.base_commit)
            except Exception:
                # Repo may be gone entirely; fall back to base==head so the
                # capsule can still be built and the crash is not silently
                # dropped.
                git_state = {
                    "base_commit": marker.base_commit,
                    "head_commit": marker.base_commit,
                    "commits": [],
                    "branch": marker.branch,
                    "worktree": marker.worktree,
                    "dirty": False,
                    "patch_digest": None,
                    "diff_stat": {"files_changed": 0, "insertions": 0, "deletions": 0},
                    "touched_paths": [],
                }
            capsule = _build_capsule(
                capsule_id=marker.capsule_id,
                origin_stream_id=marker.origin_stream_id,
                runtime_session_id=marker.session_id,
                identity=identity,
                started_at=marker.started_at,
                ended_at=_now(),
                end_kind="end-inferred",
                end_reason="wrapper-process-crash-recovery",
                exit_code=None,
                git_state=git_state,
                verification=(),
            )
            _validate_capsule(capsule)
            capsule_dir.mkdir(parents=True, exist_ok=True)
            path = capsule_dir / f"{marker.capsule_id}.json"
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(capsule, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(path)
            recorded.append(path)
        except Exception as exc:
            on_recorder_error(f"failed to recover stale session marker {marker_path}", exc)
        finally:
            _clear_marker(marker_path)
    return recorded


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="actionq-session-wrap",
        description="Wrap a manual or dispatched command and record a session-capsule/v1 artifact.",
    )
    parser.add_argument("--repo", required=True, type=Path, help="Git worktree the session runs in")
    parser.add_argument("--project", required=True, help="repo.project value, e.g. an sprintctl repo name")
    parser.add_argument("--actor", required=True)
    parser.add_argument("--harness", default="manual")
    parser.add_argument("--model-name", default=None)
    parser.add_argument("--model-version", default=None)
    parser.add_argument("--target-rank", default=None, choices=sorted(_TARGET_RANKS))
    parser.add_argument("--target-ref", default=None)
    parser.add_argument("--runtime-session-id", default=None)
    parser.add_argument("--capsule-dir", type=Path, default=None)
    parser.add_argument("--marker-dir", type=Path, default=None)
    parser.add_argument("--verify", action="append", default=[], help="Shell command to run after the wrapped command exits")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    if not args.command:
        parser.error("no command given to wrap (pass it after --)")
    command = args.command[1:] if args.command[0] == "--" else args.command

    target = None
    if args.target_rank and args.target_ref:
        target = SessionTarget(rank=args.target_rank, ref=args.target_ref)

    identity = SessionIdentity(
        repo_project=args.project,
        actor=args.actor,
        harness=args.harness,
        model_name=args.model_name,
        model_version=args.model_version,
        target=target,
        runtime_session_id=args.runtime_session_id,
    )
    wrapper = SessionWrapper(identity, repo_path=args.repo, capsule_dir=args.capsule_dir, marker_dir=args.marker_dir)
    return wrapper.run(command, verification=list(args.verify))


if __name__ == "__main__":
    raise SystemExit(main())
