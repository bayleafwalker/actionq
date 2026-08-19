"""Live conformance: the adapter against a real ACP harness.

Skipped unless ACP_LIVE=1 and the harness is installed. The fake agent proves the
adapter's logic; only this proves the protocol assumptions still hold against a harness
that ships on its own schedule.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from actionq_runner.acp import AcpExecutionAdapter, BoundaryViolation, ModelBindingError
from actionq_runner.execution_contract import (
    BindingStatus,
    EventKind,
    ExecutionEnvelope,
    ExecutionInvariants,
)

LIVE = os.environ.get("ACP_LIVE") == "1"
AGENT = os.environ.get("ACP_LIVE_COMMAND", "opencode acp").split()
MODEL = os.environ.get("ACP_LIVE_MODEL", "local3090/worker-fast")

pytestmark = pytest.mark.skipif(
    not LIVE or shutil.which(AGENT[0]) is None,
    reason="set ACP_LIVE=1 with an installed ACP harness",
)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "README.md").write_text("hello\n")
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"],
        cwd=tmp_path, check=True,
    )
    return tmp_path


def _envelope(repo: Path, **kw) -> ExecutionEnvelope:
    args = dict(
        execution_id="E-live",
        model=MODEL,
        instruction="Reply with exactly the word OK and nothing else.",
        invariants=ExecutionInvariants(root=str(repo), revision="HEAD"),
        mode="plan",
    )
    args.update(kw)
    return ExecutionEnvelope(**args)


def test_live_handshake_reports_v1(repo: Path) -> None:
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        a.open(_envelope(repo))
        caps = a.capabilities
        assert caps.protocol_version == 1
        assert caps.agent_name


def test_live_binding_strength_is_reported_honestly(repo: Path) -> None:
    """OpenCode 1.18.18 offers no model read-back, so the binding is ASSERTED, not VERIFIED.

    If a future release adds one, this flips to VERIFIED and the assertion below fails --
    which is the point. The adapter must not quietly keep claiming the weaker status once
    the stronger one becomes available.
    """
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        a.open(_envelope(repo))
        binding = a.model_binding
        assert binding.requested == MODEL
        assert binding.status in (BindingStatus.ASSERTED, BindingStatus.VERIFIED)
        if binding.status is BindingStatus.ASSERTED:
            assert a._observed_model() is None, "read-back appeared; re-verify the codec"


def test_live_strict_mode_refuses_an_unverifiable_harness(repo: Path) -> None:
    """A caller that demands proof of the backend cannot get it from this harness."""
    with AcpExecutionAdapter(AGENT, agent="opencode", require_verified_model=True) as a:
        with pytest.raises(ModelBindingError, match="not verified"):
            a.open(_envelope(repo))


def test_live_unknown_model_is_refused_before_any_work(repo: Path) -> None:
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        with pytest.raises(ModelBindingError):
            a.open(_envelope(repo, model="nonexistent/definitely-not-a-model"))


def test_live_prompt_round_trip(repo: Path) -> None:
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        a.open(_envelope(repo))
        outcome = a.prompt(timeout=300.0)
        assert outcome.stop_reason
        kinds = [e.kind for e in a.events()]
        assert EventKind.RUNTIME_STARTED in kinds
        assert EventKind.RUNTIME_STOPPED in kinds
        unmapped = [e.payload for e in a.events() if e.kind is EventKind.PROTOCOL_UNMAPPED]
        assert not unmapped, f"harness sent traffic the codec does not map: {unmapped}"


def test_live_session_reports_the_root_we_named(repo: Path) -> None:
    """The one piece of execution identity ACP can read back."""
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        a.open(_envelope(repo))
        root_reports = [r for r in a.boundary_reports if r.phase == "session_root"]
        assert root_reports and root_reports[0].ok
        assert root_reports[0].checks[0].observed == str(repo)


def test_live_hostile_pwd_does_not_move_the_effective_root(repo: Path, monkeypatch) -> None:
    """Regression guard for the `opencode run` project-root bug on the ACP path.

    Measured 2026-08-19: an explicit session/new cwd wins over an inherited PWD. If a
    future release regresses that, the root read-back is what notices.
    """
    monkeypatch.setenv("PWD", str(repo.parent))
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        a.open(_envelope(repo))
        root_reports = [r for r in a.boundary_reports if r.phase == "session_root"]
    assert root_reports[0].ok


def test_live_wrong_revision_never_launches_the_agent(repo: Path) -> None:
    from actionq_runner.execution_contract import ExecutionInvariants

    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        with pytest.raises(BoundaryViolation, match="revision.pinned"):
            a.open(_envelope(repo, invariants=ExecutionInvariants(
                root=str(repo), revision="0" * 40)))


def test_live_acp_usage_is_not_a_measure_of_context_size(repo: Path) -> None:
    """Pins the finding that rules ACP usage out of budget accounting.

    Measured 2026-08-19: identical requests reported 516, then 4, 4, 4 input tokens while
    the inference server reported a steady 22 with cached_tokens rising 0 -> 18. The
    figure tracks prefix-cache state, not request size. If a future release makes it
    stable, this test fails and the accounting can be revisited -- which is the point.
    """
    seen = []
    for _ in range(3):
        with AcpExecutionAdapter(AGENT, agent="opencode") as a:
            a.open(_envelope(repo))
            seen.append(a.prompt(timeout=300.0).usage.get("inputTokens"))
    assert len(set(seen)) > 1, (
        f"ACP inputTokens was stable across identical requests ({seen}); "
        "re-examine whether it can now be trusted for accounting"
    )


def test_live_harness_usage_is_labelled_unverifiable(repo: Path) -> None:
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        a.open(_envelope(repo))
        a.prompt(timeout=300.0)
        assert a.assurance()["harness_usage"].value == "unverifiable"


def test_live_root_is_verified_while_model_is_only_asserted(repo: Path) -> None:
    """The granularity that motivated per-property assurance."""
    with AcpExecutionAdapter(AGENT, agent="opencode") as a:
        a.open(_envelope(repo))
        levels = a.assurance()
    assert levels["root"].value == "verified"
    assert levels["model"].value == "asserted"
