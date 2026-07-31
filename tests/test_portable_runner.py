from __future__ import annotations

import importlib
import json
import os
import stat
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "packages/actionq-contracts"))
sys.path.insert(0, str(ROOT / "packages/actionq-runner"))

from actionq_contracts import EXECUTION_ENVELOPE_V1, ExecutionEnvelope, canonical_bytes, require_compatible
from actionq_runner.staging import collect, quarantine, seal, staging_dir


def test_contract_canonicalization_and_explicit_compatibility():
    envelope = ExecutionEnvelope(
        contract_id=EXECUTION_ENVELOPE_V1, action_id=2031, attempt_id="claim-1",
        source_commit="abc123", command_id="pytest", allowed_paths=("actionq/",),
    )
    assert canonical_bytes(envelope.as_dict()) == canonical_bytes(json.loads(canonical_bytes(envelope.as_dict())))
    assert require_compatible(envelope.as_dict()) == EXECUTION_ENVELOPE_V1
    with pytest.raises(ValueError, match="unsupported"):
        require_compatible({"contract_id": "execution-envelope/v2"})


def test_server_and_runner_distributions_have_no_cross_imports():
    server_sources = "\n".join(path.read_text() for path in (ROOT / "actionq").rglob("*.py"))
    runner_sources = "\n".join(path.read_text() for path in (ROOT / "packages/actionq-runner").rglob("*.py"))
    assert "actionq_runner" not in server_sources
    assert "from actionq " not in runner_sources
    assert "import actionq" not in runner_sources


def test_staging_is_private_atomic_and_rejects_traversal(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    attempt = staging_dir(2031, "claim-1")
    assert stat.S_IMODE(attempt.stat().st_mode) == 0o700
    incoming = quarantine(attempt, "result.json", b"worker bytes")
    sealed = seal(attempt, "result.json", b'{"redacted":true}')
    assert incoming.read_bytes() == b"worker bytes"
    assert sealed.read_bytes() == b'{"redacted":true}'
    assert stat.S_IMODE(sealed.stat().st_mode) == 0o600
    assert not list(sealed.parent.glob("*.tmp"))
    with pytest.raises(ValueError):
        staging_dir(2031, "../escape")
    with pytest.raises(ValueError):
        seal(attempt, "../escape", b"x")


def test_gc_skips_unreconciled_and_obeys_terminal_retention(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    attempt = staging_dir(2031, "claim-2")
    old = attempt.stat().st_mtime + 8 * 24 * 60 * 60
    assert collect(attempt, reconciled=False, terminal=True, now=old) is False
    assert attempt.exists()
    assert collect(attempt, reconciled=True, terminal=True, now=old) is True
    assert not attempt.exists()
