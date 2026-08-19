"""Two ACP backends, one sealed envelope, one acceptance path.

Skipped unless ACP_FUNGIBILITY=1. Costs real inference on both backends.

The assertion is deliberately *not* capability parity. Backends differ, and forcing them
to look alike would mean emulating features an agent does not have. What must hold is that
the same sealed envelope executes correctly through the same adapter on both, and that the
differences remain visible in the assurance map rather than being averaged away.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from actionq_runner.acp import AcpExecutionAdapter
from actionq_runner.execution_contract import (
    Assurance,
    ContextPolicy,
    ExecutionEnvelope,
    ExecutionInvariants,
    PermissionDecision,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("ACP_FUNGIBILITY") != "1",
    reason="set ACP_FUNGIBILITY=1; this runs real inference on two backends",
)

TASK = "Create a file named result.txt whose only content is the word OK. Then stop."

BACKENDS = {
    "opencode": {
        "command": ["opencode", "acp"],
        "model": os.environ.get("ACP_OPENCODE_MODEL", "local3090/worker-fast"),
        "mode": "build",
    },
    "codex": {
        "command": ["npx", "-y", "@agentclientprotocol/codex-acp@1.4.0"],
        "model": os.environ.get("ACP_CODEX_MODEL", "gpt-5.6-sol[low]"),
        "mode": "agent",
    },
}


def _repo(tmp_path: Path, name: str) -> Path:
    root = tmp_path / name
    root.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    (root / "README.md").write_text("sealed task workspace\n")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"],
        cwd=root, check=True,
    )
    return root


def _run(name: str, spec: dict, root: Path) -> dict:
    envelope = ExecutionEnvelope(
        execution_id="E-fungibility-1",
        model=spec["model"],
        instruction=TASK,
        invariants=ExecutionInvariants(
            root=str(root), revision="HEAD",
            permitted_paths=("result.txt",),
            acceptance_target="result.txt",
        ),
        context_policy=ContextPolicy(hot_material=(TASK,)),
        mode=spec["mode"],
    )
    with AcpExecutionAdapter(
        spec["command"], agent=name,
        permission_resolver=lambda _r: PermissionDecision.ALLOW,
    ) as adapter:
        adapter.open(envelope)
        outcome = adapter.prompt(timeout=600.0)
        report = adapter.verify_completion()
        return {
            "stop_reason": outcome.stop_reason,
            "accepted": report.ok,
            "assurance": adapter.assurance(),
            "faults": adapter.protocol_faults,
            "artifact": (root / "result.txt").read_text().strip()
            if (root / "result.txt").exists() else None,
        }


@pytest.fixture(scope="module")
def results(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("fungibility")
    out = {}
    for name, spec in BACKENDS.items():
        if shutil.which(spec["command"][0]) is None:
            pytest.skip(f"{name} backend unavailable")
        out[name] = _run(name, spec, _repo(tmp, name))
    return out


def test_both_backends_execute_the_sealed_envelope(results: dict) -> None:
    for name, result in results.items():
        assert result["stop_reason"] == "end_turn", name
        assert not result["faults"], f"{name} produced protocol faults: {result['faults']}"


def test_both_reach_the_same_acceptance_outcome(results: dict) -> None:
    for name, result in results.items():
        assert result["accepted"], f"{name} failed acceptance"
        assert result["artifact"] == "OK", f"{name} produced {result['artifact']!r}"


def test_assurance_differs_and_stays_visible(results: dict) -> None:
    """Fungibility is not parity. The difference must survive, not be averaged away."""
    opencode = results["opencode"]["assurance"]
    codex = results["codex"]["assurance"]
    assert opencode["root"] is Assurance.VERIFIED
    assert codex["root"] is Assurance.UNVERIFIABLE
    assert opencode != codex, "distinct backends reported identical assurance"


def test_a_capability_gap_does_not_block_acceptance(results: dict) -> None:
    """Codex cannot attest its root, yet the execution is still valid.

    Capability absence changes assurance; it does not fail the run and it does not select
    a vendor-specific code path.
    """
    assert results["codex"]["assurance"]["root"] is Assurance.UNVERIFIABLE
    assert results["codex"]["accepted"]
