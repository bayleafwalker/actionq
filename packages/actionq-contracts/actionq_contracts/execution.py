from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

EXECUTION_ENVELOPE_V1 = "execution-envelope/v1"
CLAIM_V1 = "claim/v1"
CANDIDATE_V1 = "candidate/v1"
EXECUTION_V1 = "execution/v1"
VERIFICATION_V1 = "verification/v1"
PUBLICATION_V1 = "publication/v1"
RUNNER_AUTH_V1 = "runner-auth/v1"
ACTION_CREATION_REQUEST_V1 = "action-creation-request/v1"
VERIFICATION_PROFILE_V1 = "verification-profile/v1"
CANDIDATE_VERIFICATION_SPEC_V1 = "candidate-verification-spec/v1"
CANDIDATE_VERIFICATION_RESULT_V1 = "candidate-verification-result/v1"
CANDIDATE_INTEGRATION_SPEC_V1 = "candidate-integration-spec/v1"
CANDIDATE_INTEGRATION_RESULT_V1 = "candidate-integration-result/v1"
CANDIDATE_REVIEW_SPEC_V1 = "candidate-review-spec/v1"
CANDIDATE_REVIEW_RESULT_V1 = "candidate-review-result/v1"
SUPPORTED_CONTRACT_IDS = frozenset({
    EXECUTION_ENVELOPE_V1, CLAIM_V1, CANDIDATE_V1, EXECUTION_V1,
    VERIFICATION_V1, PUBLICATION_V1, RUNNER_AUTH_V1,
    ACTION_CREATION_REQUEST_V1, VERIFICATION_PROFILE_V1,
    CANDIDATE_VERIFICATION_SPEC_V1, CANDIDATE_VERIFICATION_RESULT_V1,
    CANDIDATE_INTEGRATION_SPEC_V1, CANDIDATE_INTEGRATION_RESULT_V1,
    CANDIDATE_REVIEW_SPEC_V1, CANDIDATE_REVIEW_RESULT_V1,
})

_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_ARTIFACT = re.compile(r"artifact:sha256:[0-9a-f]{64}\Z")
_GIT_OID = re.compile(r"[0-9a-f]{40,64}\Z")
_FORBIDDEN_FIELD = re.compile(
    r"(?:claim_?(?:token|receipt)|receipt|credential|secret|password|token|"
    r"local_?path|worktree|remote|prompt|transcript|raw_?log|approval|"
    r"merge|push|release|disposition)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ContractRecord:
    contract_id: str
    action_id: int
    attempt_id: str

    def __post_init__(self) -> None:
        if self.contract_id not in SUPPORTED_CONTRACT_IDS:
            raise ValueError(f"unsupported contract id: {self.contract_id}")
        if self.action_id <= 0 or not self.attempt_id:
            raise ValueError("action_id and attempt_id must identify an attempt")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionEnvelope(ContractRecord):
    source_commit: str
    command_id: str
    allowed_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.contract_id != EXECUTION_ENVELOPE_V1:
            raise ValueError("ExecutionEnvelope requires execution-envelope/v1")
        if not self.source_commit or not self.command_id:
            raise ValueError("source_commit and command_id are required")


@dataclass(frozen=True)
class Claim:
    action_id: int
    attempt_id: str
    worker_id: str
    receipt_digest: str
    lease_deadline: str
    contract_id: str = CLAIM_V1


@dataclass(frozen=True)
class Candidate:
    action_id: int
    attempt_id: str
    source_commit: str
    changed_paths: tuple[str, ...]
    contract_id: str = CANDIDATE_V1


@dataclass(frozen=True)
class Execution:
    action_id: int
    attempt_id: str
    command_id: str
    exit_code: int
    timed_out: bool
    contract_id: str = EXECUTION_V1


@dataclass(frozen=True)
class Verification:
    action_id: int
    attempt_id: str
    command_id: str
    outcome: str
    evidence_digest: str
    contract_id: str = VERIFICATION_V1


@dataclass(frozen=True)
class Publication:
    action_id: int
    attempt_id: str
    candidate_digest: str
    verification_digest: str
    terminal_status: str
    contract_id: str = PUBLICATION_V1


@dataclass(frozen=True)
class RunnerAuth:
    runner_id: str
    operation: str
    resource: str
    request_id: str
    issued_at: str
    expires_at: str
    contract_id: str = RUNNER_AUTH_V1

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_REQUIRED_FIELDS = {
    CLAIM_V1: frozenset(Claim.__dataclass_fields__),
    CANDIDATE_V1: frozenset(Candidate.__dataclass_fields__),
    EXECUTION_V1: frozenset(Execution.__dataclass_fields__),
    VERIFICATION_V1: frozenset(Verification.__dataclass_fields__),
    PUBLICATION_V1: frozenset(Publication.__dataclass_fields__),
    RUNNER_AUTH_V1: frozenset(RunnerAuth.__dataclass_fields__),
    EXECUTION_ENVELOPE_V1: frozenset(ExecutionEnvelope.__dataclass_fields__),
}

# These records deliberately use plain mappings rather than dataclasses.  They
# cross independently versioned compiler, queue, runner, and reviewer
# packages, so their exact key set is the compatibility boundary.
_IMMUTABLE_V1_FIELDS: dict[str, frozenset[str]] = {
    ACTION_CREATION_REQUEST_V1: frozenset({
        "contract_id", "plan_ref", "topology", "role", "subject", "spec_ref",
        "spec_digest", "input_set_digest",
    }),
    VERIFICATION_PROFILE_V1: frozenset({
        "contract_id", "profile_id", "command_id", "registry_ref", "registry_digest",
    }),
    CANDIDATE_VERIFICATION_SPEC_V1: frozenset({
        "contract_id", "candidate_ref", "candidate_digest",
        "profile_ref", "profile_digest",
    }),
    CANDIDATE_VERIFICATION_RESULT_V1: frozenset({
        "contract_id", "spec_ref", "spec_digest", "candidate_ref", "candidate_digest",
        "outcome", "evidence_ref", "evidence_digest",
    }),
    CANDIDATE_INTEGRATION_SPEC_V1: frozenset({
        "contract_id", "topology", "base_commit",
        "member_result_refs", "input_set_digest",
    }),
    CANDIDATE_INTEGRATION_RESULT_V1: frozenset({
        "contract_id", "spec_ref", "spec_digest", "outcome", "candidate_ref",
        "candidate_digest",
    }),
    CANDIDATE_REVIEW_SPEC_V1: frozenset({
        "contract_id", "candidate_ref", "candidate_digest",
        "verification_result_ref", "verification_result_digest",
    }),
    CANDIDATE_REVIEW_RESULT_V1: frozenset({
        "contract_id", "spec_ref", "spec_digest", "outcome", "findings_ref", "findings_digest",
    }),
}


def _json_value(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, float):
        raise ValueError("floats are not canonical contract values")
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("contract object keys must be strings")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def canonical_bytes(value: Any) -> bytes:
    """Return deterministic contract bytes; raw floats are deliberately rejected."""
    return json.dumps(
        _json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sha256_digest(value: Any) -> str:
    return f"sha256:{hashlib.sha256(canonical_bytes(value)).hexdigest()}"


def require_compatible(value: dict[str, Any]) -> str:
    contract_id = value.get("contract_id")
    if contract_id not in SUPPORTED_CONTRACT_IDS:
        raise ValueError(f"unsupported contract id: {contract_id!r}")
    fields = _REQUIRED_FIELDS.get(str(contract_id), _IMMUTABLE_V1_FIELDS.get(str(contract_id)))
    assert fields is not None
    missing = fields - set(value)
    extra = set(value) - fields
    if missing or extra:
        raise ValueError(f"invalid {contract_id} fields: missing={sorted(missing)}, extra={sorted(extra)}")
    if contract_id in _IMMUTABLE_V1_FIELDS:
        _require_immutable_v1(value, str(contract_id))
        return str(contract_id)
    if contract_id != RUNNER_AUTH_V1:
        if not isinstance(value.get("action_id"), int) or int(value["action_id"]) <= 0:
            raise ValueError("action_id must be a positive integer")
        if not isinstance(value.get("attempt_id"), str) or not value["attempt_id"]:
            raise ValueError("attempt_id is required")
    elif not all(isinstance(value.get(field), str) and value[field]
                 for field in ("runner_id", "operation", "resource", "request_id", "issued_at", "expires_at")):
        raise ValueError("runner authentication fields must be non-empty strings")
    for field in ("receipt_digest", "evidence_digest", "candidate_digest", "verification_digest"):
        if field in value and not _SHA256.fullmatch(str(value[field])):
            raise ValueError(f"{field} must be a canonical sha256 digest")
    if "source_commit" in value and value["source_commit"] != "unavailable" and not re.fullmatch(
        r"[0-9a-f]{7,64}", str(value["source_commit"])
    ):
        raise ValueError("source_commit must be a lowercase Git object id")
    if "allowed_paths" in value:
        for path in value["allowed_paths"]:
            if not isinstance(path, str) or path.startswith("/") or ".." in path.split("/"):
                raise ValueError("allowed paths must be relative and traversal-free")
    if contract_id == VERIFICATION_V1 and value["outcome"] not in {"passed", "failed", "skipped"}:
        raise ValueError("verification outcome is invalid")
    return str(contract_id)


def artifact_digest(reference: str) -> str:
    """Return the digest bound by an immutable artifact locator."""
    if not isinstance(reference, str) or not _ARTIFACT.fullmatch(reference):
        raise ValueError("artifact reference must be artifact:sha256:<hex>")
    return "sha256:" + reference.removeprefix("artifact:sha256:")


def _exact_string(value: Any, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")


def _artifact_binding(value: dict[str, Any], ref: str, digest: str) -> None:
    if not isinstance(value[ref], str) or not _ARTIFACT.fullmatch(value[ref]):
        raise ValueError(f"{ref} must be an immutable artifact locator")
    if not isinstance(value[digest], str) or not _SHA256.fullmatch(value[digest]):
        raise ValueError(f"{digest} must be a canonical sha256 digest")
    if artifact_digest(value[ref]) != value[digest]:
        raise ValueError(f"{ref} and {digest} must bind the same bytes")


def _deny_forbidden(value: Any, path: str = "contract") -> None:
    if isinstance(value, float):
        raise ValueError("floats are not canonical contract values")
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            if _FORBIDDEN_FIELD.search(key):
                raise ValueError(f"forbidden field: {path}.{key}")
            _deny_forbidden(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _deny_forbidden(child, f"{path}[{index}]")


def _require_immutable_v1(value: dict[str, Any], contract_id: str) -> None:
    _deny_forbidden(value)
    for name in ("plan_ref", "spec_ref", "request_ref", "profile_ref", "candidate_ref",
                 "evidence_ref", "verification_result_ref", "findings_ref"):
        if name in value:
            # conflict results deliberately have no candidate artifact.
            if value[name] is not None and not _ARTIFACT.fullmatch(str(value[name])):
                raise ValueError(f"{name} must be an immutable artifact locator")
    for name in ("spec_digest", "request_digest", "profile_digest", "candidate_digest",
                 "evidence_digest", "verification_result_digest", "findings_digest", "registry_digest",
                 "input_set_digest"):
        if name in value:
            if value[name] is not None and not _SHA256.fullmatch(str(value[name])):
                raise ValueError(f"{name} must be a canonical sha256 digest")
    for ref, digest in (
        ("spec_ref", "spec_digest"), ("request_ref", "request_digest"),
        ("profile_ref", "profile_digest"), ("candidate_ref", "candidate_digest"),
        ("evidence_ref", "evidence_digest"),
        ("verification_result_ref", "verification_result_digest"),
        ("findings_ref", "findings_digest"), ("registry_ref", "registry_digest"),
    ):
        if ref in value and value[ref] is not None:
            _artifact_binding(value, ref, digest)
    if contract_id == ACTION_CREATION_REQUEST_V1:
        _artifact_binding(value, "spec_ref", "spec_digest")
        if not isinstance(value["plan_ref"], str) or not _ARTIFACT.fullmatch(value["plan_ref"]):
            raise ValueError("plan_ref must be an immutable artifact locator")
        if value["topology"] not in {"independent", "stacked", "wave-integrated"}:
            raise ValueError("topology is invalid")
        if value["role"] not in {"candidate-verification", "candidate-integration", "candidate-review"}:
            raise ValueError("role is invalid")
        _exact_string(value["subject"], "subject")
    elif contract_id == VERIFICATION_PROFILE_V1:
        _artifact_binding(value, "registry_ref", "registry_digest")
        for field in ("profile_id", "command_id"):
            _exact_string(value[field], field)
    elif contract_id == CANDIDATE_INTEGRATION_SPEC_V1:
        if value["topology"] not in {"stacked", "wave-integrated"}:
            raise ValueError("integration topology is invalid")
        if not isinstance(value["base_commit"], str) or not _GIT_OID.fullmatch(value["base_commit"]):
            raise ValueError("base_commit must be a full lowercase Git object id")
        refs = value["member_result_refs"]
        # This list is the compiler's frozen #2034 ordinal order.  Sorting by
        # digest would silently change a wave's integration topology.
        if not isinstance(refs, list) or not refs or len(refs) != len(set(refs)):
            raise ValueError("member_result_refs must be a non-empty unique ordinal list")
        if any(not isinstance(ref, str) or not _ARTIFACT.fullmatch(ref) for ref in refs):
            raise ValueError("member_result_refs must be immutable artifact locators")
    elif contract_id == CANDIDATE_VERIFICATION_RESULT_V1:
        if value["outcome"] not in {"passed", "candidate-failed"}:
            raise ValueError("verification outcome is invalid")
    elif contract_id == CANDIDATE_INTEGRATION_RESULT_V1:
        if value["outcome"] not in {"integrated", "conflict"}:
            raise ValueError("integration outcome is invalid")
        if value["outcome"] == "conflict" and (value["candidate_ref"] is not None or value["candidate_digest"] is not None):
            raise ValueError("conflict must not publish an integration candidate")
        if value["outcome"] == "integrated":
            _artifact_binding(value, "candidate_ref", "candidate_digest")
    elif contract_id == CANDIDATE_REVIEW_RESULT_V1:
        if value["outcome"] not in {"no-findings", "findings-recorded"}:
            raise ValueError("review outcome is invalid")
