from __future__ import annotations

import hashlib
import json
from typing import Any

EXECUTION_ENVELOPE_V1 = "execution-envelope/v1"


def canonical_bytes(value: Any) -> bytes:
    """Return deterministic contract bytes; raw floats are deliberately rejected."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sha256_digest(value: Any) -> str:
    return f"sha256:{hashlib.sha256(canonical_bytes(value)).hexdigest()}"
