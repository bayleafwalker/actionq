"""Bounded Auditctl v2 observation publisher for immutable review results."""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from actionq_contracts import (
    ACTION_CREATION_REQUEST_V1,
    CANDIDATE_REVIEW_RESULT_V1,
    CANDIDATE_REVIEW_SPEC_V1,
    CANDIDATE_VERIFICATION_RESULT_V1,
    PUBLICATION_V1,
    artifact_digest,
    canonical_bytes,
    require_compatible,
)

from .candidates import resolve_exact_contract
from .publisher import ArtifactStore, artifact_ref


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
_FINDING_FIELDS = frozenset({"id", "category", "summary"})


def _require_not_cancelled(cancelled: Callable[[], bool] | None) -> None:
    if cancelled is not None and cancelled():
        raise RuntimeError("candidate review was cancelled")


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


def publish_review_result(
    store: ArtifactStore, *, spec_ref: str, findings: Sequence[Mapping[str, str]],
    cancelled: Callable[[], bool] | None = None,
) -> str:
    """Publish a bounded immutable review result before any Auditctl observation.

    Findings are deliberately short identifiers/categories/summaries only: raw
    logs, prompts, credentials, approval language, and review transcripts have
    no place in the candidate protocol artifact.
    """
    _require_not_cancelled(cancelled)
    spec = resolve_exact_contract(store, spec_ref, CANDIDATE_REVIEW_SPEC_V1)
    verification = resolve_exact_contract(
        store, spec["verification_result_ref"], CANDIDATE_VERIFICATION_RESULT_V1,
    )
    if verification["outcome"] != "passed":
        raise RuntimeError("candidate review requires an exact passed verification result")
    if (verification["candidate_ref"], verification["candidate_digest"]) != (
        spec["candidate_ref"], spec["candidate_digest"],
    ):
        raise RuntimeError("review candidate does not match its verification result")
    if (verification["publication_ref"], verification["publication_digest"]) != (
        spec["publication_ref"], spec["publication_digest"],
    ):
        raise RuntimeError("review publication does not match its verification eligibility binding")
    publication = resolve_exact_contract(store, spec["publication_ref"], PUBLICATION_V1)
    if publication["terminal_status"] != "verified":
        raise RuntimeError("review publication is not eligible")
    normalized: list[dict[str, str]] = []
    for finding in findings:
        if not isinstance(finding, Mapping) or set(finding) != _FINDING_FIELDS:
            raise ValueError("review findings must have exact id, category, and summary fields")
        copied = dict(finding)
        if any(not isinstance(copied[field], str) or not copied[field] for field in _FINDING_FIELDS):
            raise ValueError("review finding fields must be non-empty strings")
        if any(term in " ".join(copied.values()).lower() for term in ("approval", "merge", "release", "token", "secret", "credential", "prompt", "transcript")):
            raise ValueError("review finding contains forbidden protocol material")
        normalized.append(copied)
    findings_raw = canonical_bytes({"contract_id": "candidate-review-findings/v1", "findings": normalized})
    _require_not_cancelled(cancelled)
    findings_ref = store.put(findings_raw)
    if artifact_ref(findings_raw) != findings_ref or store.get(findings_ref) != findings_raw:
        raise RuntimeError("CAS failed to preserve review findings bytes")
    result = {
        "contract_id": CANDIDATE_REVIEW_RESULT_V1,
        "spec_ref": spec_ref,
        "spec_digest": artifact_digest(spec_ref),
        "outcome": "findings-recorded" if normalized else "no-findings",
        "findings_ref": findings_ref,
        "findings_digest": artifact_digest(findings_ref),
    }
    require_compatible(result)
    raw = canonical_bytes(result)
    _require_not_cancelled(cancelled)
    result_ref = store.put(raw)
    if artifact_ref(raw) != result_ref or store.get(result_ref) != raw:
        raise RuntimeError("CAS failed to preserve review result bytes")
    return result_ref


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


def _publish_observation(
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


def publish_review(
    auditctl_bin: str,
    *,
    store: ArtifactStore,
    request: dict[str, Any],
    action_id: int,
    attempt_id: str,
    actor: str,
    review_spec_ref: str,
    review_result_ref: str,
    runtime_session_id: str | None = None,
    timeout_seconds: float = 10.0,
) -> str:
    """Derive one Auditctl v2 observation only from frozen review artifacts.

    The caller may provide runtime identity fields, but every candidate,
    publication, outcome, and immutable reference is reconstructed from the
    compiler request and exact CAS records before the bounded subprocess call.
    """
    require_compatible(request)
    if request.get("contract_id") != ACTION_CREATION_REQUEST_V1 or request.get("role") != "candidate-review":
        raise ValueError("review observation requires a candidate-review creation request")
    spec = resolve_exact_contract(store, review_spec_ref, CANDIDATE_REVIEW_SPEC_V1)
    result = resolve_exact_contract(store, review_result_ref, CANDIDATE_REVIEW_RESULT_V1)
    if request.get("spec_ref") != review_spec_ref or request.get("spec_digest") != artifact_digest(review_spec_ref):
        raise ValueError("review request does not bind the exact review spec")
    if result["spec_ref"] != review_spec_ref or result["spec_digest"] != artifact_digest(review_spec_ref):
        raise ValueError("review result does not bind the exact review spec")
    findings = store.get(result["findings_ref"])
    if artifact_ref(findings) != result["findings_ref"]:
        raise RuntimeError("review findings locator does not match its bytes")
    metadata = {
        "action_id": str(action_id), "attempt_id": attempt_id,
        "plan_ref": request["plan_ref"], "subject_kind": spec["subject_kind"],
        "publication_ref": spec["publication_ref"],
        "verification_result_ref": spec["verification_result_ref"],
        "review_result_artifact_ref": review_result_ref,
        "topology": request["topology"], "findings_digest": result["findings_digest"],
        "review_outcome": result["outcome"],
    }
    if runtime_session_id is not None:
        metadata["runtime_session_id"] = runtime_session_id
    return _publish_observation(
        auditctl_bin,
        ReviewObservation(actor=actor, reviewed_commit=spec["reviewed_commit"], metadata=metadata),
        timeout_seconds=timeout_seconds,
    )
