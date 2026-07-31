"""Daemon-only Ed25519 runner identity; private keys never enter worker state."""
from __future__ import annotations

import base64
import json
import os
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from actionq_contracts import RUNNER_AUTH_V1, RunnerAuth, canonical_bytes


def sign_runner_request(private_key_path: Path, *, runner_id: str, operation: str,
                        resource: str, request_id: str) -> dict[str, str]:
    info = private_key_path.lstat()
    if private_key_path.is_symlink() or not private_key_path.is_file() or info.st_uid != os.geteuid():
        raise PermissionError("runner private key must be a supervisor-owned regular file")
    if info.st_mode & 0o077:
        raise PermissionError("runner private key must be mode 0600")
    key = serialization.load_pem_private_key(private_key_path.read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("runner private key must be Ed25519")
    now = datetime.now(timezone.utc)
    record = RunnerAuth(
        runner_id=runner_id, operation=operation, resource=resource, request_id=request_id,
        issued_at=now.isoformat().replace("+00:00", "Z"),
        expires_at=(now + timedelta(seconds=60)).isoformat().replace("+00:00", "Z"),
    )
    signed = record.as_dict()
    return {**signed, "signature": base64.b64encode(key.sign(canonical_bytes(signed))).decode("ascii")}


def verify_runner_request(proof: dict[str, str], *, operation: str, resource: str) -> str:
    registry_value = os.environ.get("ACTIONQ_RUNNER_IDENTITY_REGISTRY")
    if not registry_value:
        raise PermissionError("runner identity registry is not configured")
    registry_path = Path(registry_value)
    info = registry_path.lstat()
    if (registry_path.is_symlink() or not registry_path.is_file()
            or info.st_uid not in {0, os.geteuid()}
            or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)):
        raise PermissionError("runner identity registry must be owner-controlled")
    runner_id = str(proof.get("runner_id") or "")
    if proof.get("operation") != operation or proof.get("resource") != resource:
        raise PermissionError("runner proof does not authorize this operation and resource")
    expires_at = datetime.fromisoformat(str(proof.get("expires_at", "")).replace("Z", "+00:00"))
    issued_at = datetime.fromisoformat(str(proof.get("issued_at", "")).replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    if not issued_at <= now <= expires_at or expires_at - issued_at > timedelta(seconds=60):
        raise PermissionError("runner proof is expired or invalid")
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    encoded_key = registry.get(runner_id)
    if not isinstance(encoded_key, str):
        raise PermissionError("runner identity is not registered")
    signature = base64.b64decode(str(proof.get("signature") or ""), validate=True)
    signed = {key: value for key, value in proof.items() if key != "signature"}
    Ed25519PublicKey.from_public_bytes(base64.b64decode(encoded_key, validate=True)).verify(
        signature, canonical_bytes(signed)
    )
    return runner_id
