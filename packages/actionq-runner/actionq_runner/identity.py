"""Daemon-only Ed25519 runner identity; private keys never enter worker state."""
from __future__ import annotations

import base64
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

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
