"""Verification boundary for signed runner-control requests."""
from __future__ import annotations

import base64
import json
import os
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from actionq_contracts import RUNNER_AUTH_V1, canonical_bytes, require_compatible

from . import db


@dataclass(frozen=True)
class VerifiedRunner:
    runner_id: str
    request_id: str
    operation: str


def _registry() -> dict[str, str]:
    path = os.environ.get("ACTIONQ_RUNNER_IDENTITY_REGISTRY")
    if not path:
        raise db.ActionQError("runner identity registry is not configured")
    registry_path = Path(path)
    if registry_path.is_symlink() or not registry_path.is_file():
        raise db.ActionQError("runner identity registry is not a regular file")
    info = registry_path.stat()
    if info.st_uid not in {0, os.geteuid()} or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise db.ActionQError("runner identity registry must be owner-controlled and not group/world writable")
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in payload.items()):
        raise db.ActionQError("runner identity registry must map runner ids to Ed25519 public keys")
    return payload


def verify_runner_proof(proof: dict[str, Any], *, operation: str, resource: str) -> VerifiedRunner:
    signature_text = proof.get("signature")
    signed = {key: value for key, value in proof.items() if key != "signature"}
    require_compatible(signed)
    if (signed["contract_id"] != RUNNER_AUTH_V1 or signed["operation"] != operation
            or signed["resource"] != resource):
        raise db.ActionQError("runner proof operation is not valid for this command")
    now = datetime.now(timezone.utc)
    try:
        issued = datetime.fromisoformat(str(signed["issued_at"]).replace("Z", "+00:00"))
        expires = datetime.fromisoformat(str(signed["expires_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise db.ActionQError("runner proof timestamps are invalid") from exc
    if issued > now or expires <= now or (expires - issued).total_seconds() > 120:
        raise db.ActionQError("runner proof is outside its validity window")
    public_text = _registry().get(str(signed["runner_id"]))
    if public_text is None:
        raise db.ActionQError("runner identity is not registered")
    try:
        public_key = Ed25519PublicKey.from_public_bytes(base64.b64decode(public_text, validate=True))
        public_key.verify(base64.b64decode(str(signature_text), validate=True), canonical_bytes(signed))
    except Exception as exc:
        raise db.ActionQError("runner proof signature is invalid") from exc
    return VerifiedRunner(str(signed["runner_id"]), str(signed["request_id"]), operation)
