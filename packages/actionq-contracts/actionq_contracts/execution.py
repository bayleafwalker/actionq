from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

EXECUTION_ENVELOPE_V1 = "execution-envelope/v1"
CLAIM_V1 = "claim/v1"
CANDIDATE_V1 = "candidate/v1"
EXECUTION_V1 = "execution/v1"
VERIFICATION_V1 = "verification/v1"
PUBLICATION_V1 = "publication/v1"
SUPPORTED_CONTRACT_IDS = frozenset({
    EXECUTION_ENVELOPE_V1, CLAIM_V1, CANDIDATE_V1, EXECUTION_V1,
    VERIFICATION_V1, PUBLICATION_V1,
})


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


_REQUIRED_FIELDS = {
    CLAIM_V1: frozenset(Claim.__dataclass_fields__),
    CANDIDATE_V1: frozenset(Candidate.__dataclass_fields__),
    EXECUTION_V1: frozenset(Execution.__dataclass_fields__),
    VERIFICATION_V1: frozenset(Verification.__dataclass_fields__),
    PUBLICATION_V1: frozenset(Publication.__dataclass_fields__),
    EXECUTION_ENVELOPE_V1: frozenset(ExecutionEnvelope.__dataclass_fields__),
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
    missing = _REQUIRED_FIELDS[str(contract_id)] - set(value)
    extra = set(value) - _REQUIRED_FIELDS[str(contract_id)]
    if missing or extra:
        raise ValueError(f"invalid {contract_id} fields: missing={sorted(missing)}, extra={sorted(extra)}")
    if not isinstance(value.get("action_id"), int) or int(value["action_id"]) <= 0:
        raise ValueError("action_id must be a positive integer")
    if not isinstance(value.get("attempt_id"), str) or not value["attempt_id"]:
        raise ValueError("attempt_id is required")
    return str(contract_id)
