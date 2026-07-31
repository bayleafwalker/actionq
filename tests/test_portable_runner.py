from __future__ import annotations

import importlib
import json
import os
import stat
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "packages/actionq-contracts"))
sys.path.insert(0, str(ROOT / "packages/actionq-runner"))

from actionq_contracts import (
    CLAIM_V1, EXECUTION_ENVELOPE_V1, Claim, ExecutionEnvelope, canonical_bytes,
    require_compatible,
)
from actionq_runner.staging import collect, mark_reconciled, quarantine, receive, seal, staging_dir


def test_contract_canonicalization_and_explicit_compatibility():
    envelope = ExecutionEnvelope(
        contract_id=EXECUTION_ENVELOPE_V1, action_id=2031, attempt_id="claim-1",
        source_commit="abc123", command_id="pytest", allowed_paths=("actionq/",),
    )
    assert canonical_bytes(envelope.as_dict()) == canonical_bytes(json.loads(canonical_bytes(envelope.as_dict())))
    assert require_compatible(envelope.as_dict()) == EXECUTION_ENVELOPE_V1
    with pytest.raises(ValueError, match="unsupported"):
        require_compatible({"contract_id": "execution-envelope/v2"})
    claim = Claim(2031, "claim-1", "runner:devbox", "sha256:abc", "2026-08-01T00:00:00Z")
    assert require_compatible(claim.__dict__) == CLAIM_V1
    with pytest.raises(ValueError, match="floats"):
        canonical_bytes({"contract_id": CLAIM_V1, "ratio": 1.0})


def test_server_and_runner_distributions_have_no_cross_imports():
    server_sources = "\n".join(path.read_text() for path in (ROOT / "actionq").rglob("*.py"))
    runner_sources = "\n".join(path.read_text() for path in (ROOT / "packages/actionq-runner").rglob("*.py"))
    assert "actionq_runner" not in server_sources
    assert "from actionq " not in runner_sources
    assert "import actionq" not in runner_sources
    root_metadata = tomllib.loads((ROOT / "pyproject.toml").read_text())
    runner_metadata = tomllib.loads((ROOT / "packages/actionq-runner/pyproject.toml").read_text())
    assert "actionq-runner" not in " ".join(root_metadata["project"]["dependencies"])
    assert runner_metadata["project"]["dependencies"] == ["actionq-contracts==0.1.0"]


def test_staging_is_private_atomic_and_rejects_traversal(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    attempt = staging_dir(2031, "claim-1")
    assert stat.S_IMODE(attempt.root.stat().st_mode) == 0o700
    incoming = receive(attempt, "result.json", b"worker secret bytes", redact=lambda _: b"worker bytes")
    assert incoming.read_bytes() == b"worker bytes"
    quarantined = quarantine(attempt, "result.json")
    assert quarantined.read_bytes() == b"worker bytes"
    sealed = seal(attempt, "result.json", b'{"redacted":true}')
    assert sealed.read_bytes() == b'{"redacted":true}'
    assert not quarantined.exists()
    assert stat.S_IMODE(sealed.stat().st_mode) == 0o600
    assert not list(sealed.parent.glob("*.tmp"))
    with pytest.raises(ValueError):
        staging_dir(2031, "../escape")
    with pytest.raises(ValueError):
        seal(attempt, "../escape", b"x")
    receive(attempt, "secret.txt", b"raw", redact=lambda value: value)
    quarantine(attempt, "secret.txt")
    with pytest.raises(ValueError, match="credential"):
        seal(attempt, "secret.txt", b"postgresql://secret")
    with pytest.raises(ValueError, match="incoming"):
        receive(attempt, "blocked.txt", b"raw", redact=lambda _: b"Authorization: Bearer secret")


def test_gc_skips_unreconciled_and_obeys_terminal_retention(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    attempt = staging_dir(2031, "claim-2")
    old = attempt.root.stat().st_mtime + 8 * 24 * 60 * 60
    assert collect(attempt, now=old) is False
    assert attempt.root.exists()
    mark_reconciled(attempt, terminal=True)
    retained_until = (attempt.root / "sealed/recovery-state.json").stat().st_mtime + 8 * 24 * 60 * 60
    assert collect(attempt, now=retained_until) is True
    assert not attempt.root.exists()
