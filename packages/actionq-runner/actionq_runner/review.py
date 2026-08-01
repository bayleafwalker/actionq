"""Bounded Auditctl v2 observation publisher for immutable review results."""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from typing import Any, Sequence


_ARTIFACT = re.compile(r"artifact:sha256:[0-9a-f]{64}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40,64}\Z")
_BASE_FIELDS = frozenset({
    "action_id", "attempt_id", "plan_ref", "subject_kind", "publication_ref",
    "verification_result_ref", "review_result_artifact_ref", "topology", "findings_digest",
    "review_outcome",
})
_IDENTITY_FIELDS = (
    "action_id", "attempt_id", "plan_ref", "subject_kind", "publication_ref",
    "verification_result_ref", "review_result_artifact_ref",
)


@dataclass(frozen=True)
class ReviewObservation:
    actor: str
    reviewed_commit: str
    metadata: dict[str, Any]
    work_item_id: int | None = None
    sprint_id: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.actor, str) or not self.actor:
            raise ValueError("review actor is required")
        if not isinstance(self.reviewed_commit, str) or not _OID.fullmatch(self.reviewed_commit):
            raise ValueError("reviewed_commit must be a full lowercase Git object id")
        _validate_metadata(self.metadata)
        for value, label in ((self.work_item_id, "work_item_id"), (self.sprint_id, "sprint_id")):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
                raise ValueError(f"{label} must be a positive integer when present")

    @property
    def refs(self) -> list[str]:
        values = [f"sha:{self.reviewed_commit}"]
        if self.work_item_id is not None:
            values.append(f"wi:{self.work_item_id}")
        if self.sprint_id is not None:
            values.append(f"sprint:{self.sprint_id}")
        return values


def _validate_metadata(metadata: dict[str, Any]) -> None:
    if not isinstance(metadata, dict):
        raise ValueError("review metadata must be an object")
    fields = _BASE_FIELDS | ({"runtime_session_id"} if "runtime_session_id" in metadata else set())
    if set(metadata) != fields:
        raise ValueError(f"review metadata fields must be exact: {sorted(fields)}")
    for name in ("action_id", "attempt_id", "topology"):
        if not isinstance(metadata[name], str) or not metadata[name]:
            raise ValueError(f"review metadata {name} must be a non-empty string")
    if metadata["subject_kind"] not in {"candidate", "integration"}:
        raise ValueError("review metadata subject_kind is invalid")
    if metadata["review_outcome"] not in {"no-findings", "findings-recorded"}:
        raise ValueError("review metadata review_outcome is invalid")
    for name in ("plan_ref", "publication_ref", "verification_result_ref", "review_result_artifact_ref"):
        if not isinstance(metadata[name], str) or not _ARTIFACT.fullmatch(metadata[name]):
            raise ValueError(f"review metadata {name} must be an immutable artifact ref")
    if not isinstance(metadata["findings_digest"], str) or not _DIGEST.fullmatch(metadata["findings_digest"]):
        raise ValueError("review metadata findings_digest must be canonical")
    if "runtime_session_id" in metadata and (not isinstance(metadata["runtime_session_id"], str) or not metadata["runtime_session_id"]):
        raise ValueError("review metadata runtime_session_id must be a non-empty string")
    forbidden = ("approval", "acceptance", "merge", "release", "prompt", "transcript", "receipt", "credential", "secret", "token", "raw_log")
    if any(any(term in key.lower() for term in forbidden) for key in metadata):
        raise ValueError("review metadata contains forbidden authority material")


def _matching(event: dict[str, Any], observation: ReviewObservation) -> bool:
    metadata = event.get("metadata")
    return (
        event.get("type") == "candidate.reviewed" and event.get("source") == "actionq-review"
        and isinstance(metadata, dict)
        and all(metadata.get(field) == observation.metadata[field] for field in _IDENTITY_FIELDS)
    )


def reconcile_review(
    auditctl_bin: str, observation: ReviewObservation, *, timeout_seconds: float = 10.0,
) -> str:
    """Return published, absent, conflict, or inconclusive using bounded CLI reads."""
    if not 0 < timeout_seconds <= 10:
        raise ValueError("review reconciliation timeout must be between zero and ten seconds")
    try:
        completed = subprocess.run(
            [auditctl_bin, "list", "--type", "candidate.reviewed", "--source", "actionq-review",
             "--limit", "1000", "--json"],
            capture_output=True, text=True, timeout=timeout_seconds, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "inconclusive"
    if completed.returncode:
        return "inconclusive"
    try:
        events = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return "inconclusive"
    if not isinstance(events, list):
        return "inconclusive"
    for event in events:
        if not isinstance(event, dict):
            return "inconclusive"
        if _matching(event, observation):
            immutable = ("publication_ref", "verification_result_ref", "review_result_artifact_ref")
            if any(event["metadata"].get(name) != observation.metadata[name] for name in immutable):
                return "conflict"
            return "published"
        metadata = event.get("metadata")
        if isinstance(metadata, dict) and metadata.get("action_id") == observation.metadata["action_id"] and metadata.get("attempt_id") == observation.metadata["attempt_id"]:
            return "conflict"
    return "inconclusive" if len(events) >= 1000 else "absent"


def publish_review(
    auditctl_bin: str, observation: ReviewObservation, *, timeout_seconds: float = 10.0,
) -> str:
    """Publish once after reconciliation; never blindly retries ``auditctl add``."""
    state = reconcile_review(auditctl_bin, observation, timeout_seconds=timeout_seconds)
    if state != "absent":
        return state
    args: list[str] = [
        auditctl_bin, "add", "--type", "candidate.reviewed", "--source", "actionq-review",
        "--actor", observation.actor, "--summary", "Independent candidate review recorded",
    ]
    for reference in observation.refs:
        args.extend(("--ref", reference))
    args.extend(("--metadata", json.dumps(observation.metadata, sort_keys=True, separators=(",", ":")), "--json"))
    try:
        completed = subprocess.run(args, capture_output=True, text=True, timeout=timeout_seconds, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return reconcile_review(auditctl_bin, observation, timeout_seconds=timeout_seconds)
    if completed.returncode == 0:
        return "published"
    return reconcile_review(auditctl_bin, observation, timeout_seconds=timeout_seconds)
