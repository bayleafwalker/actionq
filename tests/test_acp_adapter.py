"""Adapter tests against a scriptable fake agent.

The interesting cases are the dishonest ones: an agent that reports success while leaving
the session bound to its own default is the failure this adapter exists to prevent.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from actionq_runner.acp import AcpError, AcpExecutionAdapter, ModelBindingError
from actionq_runner.execution_contract import (
    BindingStatus,
    ContextAddress,
    ContextPolicy,
    ContextTier,
    EventKind,
    ExecutionEnvelope,
    ExecutionInvariants,
    PermissionDecision,
)

FAKE = [sys.executable, str(Path(__file__).parent / "fake_acp_agent.py")]


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "README.md").write_text("hello\n")
    return tmp_path


def envelope(repo: Path, **overrides) -> ExecutionEnvelope:
    defaults = dict(
        execution_id="E-123",
        model="local3090/worker-fast",
        instruction="say OK",
        invariants=ExecutionInvariants(root=str(repo), revision="HEAD"),
        mode="plan",
    )
    defaults.update(overrides)
    return ExecutionEnvelope(**defaults)


def adapter(behaviour: str = "good", **kwargs) -> AcpExecutionAdapter:
    import os

    os.environ["ACP_FAKE_BEHAVIOUR"] = behaviour
    return AcpExecutionAdapter(FAKE, agent="fake", **kwargs)


# -- envelope invariants ---------------------------------------------------


def test_envelope_requires_an_explicit_model(repo: Path) -> None:
    """A harness default is not a choice. The invalid state is unrepresentable."""
    with pytest.raises(ValueError, match="must name a model"):
        envelope(repo, model="")


def test_invariants_require_a_pinned_revision(repo: Path) -> None:
    with pytest.raises(ValueError, match="pinned revision"):
        ExecutionInvariants(root=str(repo), revision="")


def test_context_address_is_a_reference_not_a_payload() -> None:
    with pytest.raises(ValueError):
        ContextAddress(tier=ContextTier.WARM, provider="", address="doc://x")


def test_context_policy_partitions_by_tier() -> None:
    policy = ContextPolicy(addresses=(
        ContextAddress(ContextTier.WARM, "git", "a.py"),
        ContextAddress(ContextTier.COLD, "docs", "spec.md"),
    ))
    assert len(policy.addresses_for(ContextTier.WARM)) == 1
    assert len(policy.addresses_for(ContextTier.COLD)) == 1
    assert policy.addresses_for(ContextTier.HOT) == ()


# -- model binding ---------------------------------------------------------


def test_open_binds_and_verifies_the_model(repo: Path) -> None:
    with adapter() as a:
        handle = a.open(envelope(repo))
        assert handle.protocol == "acp"
        assert handle.external_session_id == "ses_fake_0001"
        assert a.model_binding.status is BindingStatus.VERIFIED
        assert a.model_binding.observed == "local3090/worker-fast"


def test_binding_without_readback_is_asserted_not_verified(repo: Path) -> None:
    """An agent that cannot report its model yields a weaker claim, stated as such."""
    with adapter("no_status") as a:
        a.open(envelope(repo))
        assert a.model_binding.status is BindingStatus.ASSERTED
        assert not a.model_binding.verified
        assert "read it back" in a.model_binding.detail


def test_unverified_binding_is_fatal_when_strictness_is_demanded(repo: Path) -> None:
    with adapter("no_status", require_verified_model=True) as a:
        with pytest.raises(ModelBindingError, match="not verified"):
            a.open(envelope(repo))


def test_binding_strength_is_emitted_as_telemetry(repo: Path) -> None:
    """The weak case must be visible in the event stream, not just on the object."""
    with adapter("no_status") as a:
        a.open(envelope(repo))
        binding = [e for e in a.events() if e.kind is EventKind.MODEL_BINDING]
    assert binding and binding[0].payload["status"] == "asserted"


def test_outcome_carries_the_binding(repo: Path) -> None:
    with adapter() as a:
        a.open(envelope(repo))
        outcome = a.prompt()
    assert outcome.model_binding.requested == "local3090/worker-fast"


def test_agent_that_lies_about_binding_is_refused(repo: Path) -> None:
    """set_model returned ok; the session stayed on the harness default."""
    with adapter("lying_set_model") as a:
        with pytest.raises(ModelBindingError, match="reports model"):
            a.open(envelope(repo))


def test_agent_without_set_model_is_refused(repo: Path) -> None:
    with adapter("no_set_model") as a:
        with pytest.raises(ModelBindingError, match="cannot bind a model"):
            a.open(envelope(repo))


def test_unverifiable_binding_falls_back_to_offered_options(repo: Path) -> None:
    """No read-back: accept a model the agent offered, refuse one it did not."""
    with adapter("no_status") as a:
        with pytest.raises(ModelBindingError, match="does not offer model"):
            a.open(envelope(repo, model="nonexistent/model"))


def test_protocol_version_mismatch_is_refused(repo: Path) -> None:
    with adapter("wrong_version") as a:
        with pytest.raises(AcpError, match="negotiated ACP v2"):
            a.open(envelope(repo))


# -- telemetry -------------------------------------------------------------


def test_prompt_normalizes_the_update_stream(repo: Path) -> None:
    with adapter() as a:
        a.open(envelope(repo))
        outcome = a.prompt()
        assert outcome.stop_reason == "end_turn"
        assert outcome.usage["totalTokens"] == 12
        kinds = [e.kind for e in a.events()]

    assert EventKind.RUNTIME_STARTED in kinds
    assert EventKind.OUTPUT_DELTA in kinds
    assert EventKind.TOOL_STARTED in kinds
    assert EventKind.RUNTIME_STOPPED in kinds
    # Unrecognized traffic surfaces rather than vanishing.
    assert EventKind.PROTOCOL_UNMAPPED in kinds


def test_output_delta_carries_the_text(repo: Path) -> None:
    with adapter() as a:
        a.open(envelope(repo))
        a.prompt()
        deltas = [e for e in a.events() if e.kind is EventKind.OUTPUT_DELTA]
    assert "".join(e.payload["text"] for e in deltas) == "hello"


# -- permissions -----------------------------------------------------------


def test_permission_defaults_to_deny(repo: Path) -> None:
    """An adapter with no policy wired in has no authority to grant."""
    with adapter("asks_permission") as a:
        a.open(envelope(repo))
        a.prompt()
        resolved = [e for e in a.events() if e.kind is EventKind.POLICY_RESOLVED]
    assert resolved and resolved[0].payload["decision"] == "deny"


def test_permission_resolver_supplies_authority(repo: Path) -> None:
    seen: list[dict] = []

    def allow(request):
        seen.append(request)
        return PermissionDecision.ALLOW

    with adapter("asks_permission", permission_resolver=allow) as a:
        a.open(envelope(repo))
        a.prompt()
        resolved = [e for e in a.events() if e.kind is EventKind.POLICY_RESOLVED]
    assert seen, "resolver was consulted"
    assert resolved[0].payload["decision"] == "allow"


def test_failing_resolver_denies(repo: Path) -> None:
    def broken(_request):
        raise RuntimeError("policy backend down")

    with adapter("asks_permission", permission_resolver=broken) as a:
        a.open(envelope(repo))
        a.prompt()
        resolved = [e for e in a.events() if e.kind is EventKind.POLICY_RESOLVED]
    assert resolved[0].payload["decision"] == "deny"


# -- process hygiene -------------------------------------------------------


def test_close_is_idempotent_and_reopen_is_refused(repo: Path) -> None:
    a = adapter()
    a.open(envelope(repo))
    a.close()
    a.close()
    with adapter() as b:
        b.open(envelope(repo))
        with pytest.raises(AcpError, match="already open"):
            b.open(envelope(repo))
