from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
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


def canonical_bytes(value: Any) -> bytes:
    """Return deterministic contract bytes; raw floats are deliberately rejected."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sha256_digest(value: Any) -> str:
    return f"sha256:{hashlib.sha256(canonical_bytes(value)).hexdigest()}"


def require_compatible(value: dict[str, Any]) -> str:
    contract_id = value.get("contract_id")
    if contract_id not in SUPPORTED_CONTRACT_IDS:
        raise ValueError(f"unsupported contract id: {contract_id!r}")
    return str(contract_id)
