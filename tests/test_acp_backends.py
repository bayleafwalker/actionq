"""The backend registry, and the guard that keeps a model on its own backend.

Evidence A4 is the reason this file exists: an execution dispatched without an
explicit model silently bound a hosted default -- wrong backend, real cost,
plausible output, no error. The envelope closed that by requiring a model. These
tests cover the neighbouring hole, where the model is present and well-formed but
belongs to a different backend.
"""
from __future__ import annotations

import pytest

from actionq_runner.acp import (
    REGISTRY,
    AcpBackend,
    Billing,
    BindingChannel,
    binding_is_trustworthy,
    check_model_allowed,
)
from actionq_runner.acp.v1 import ModelBindingError


def test_registry_declares_the_backends_phase_3_exercised() -> None:
    assert {"opencode", "codex", "claude-code"} <= set(REGISTRY)


def test_local_and_hosted_are_distinguished() -> None:
    assert REGISTRY["opencode"].billing is Billing.LOCAL
    # Both remote agents cost money; a cost gate keys on this, never on the name.
    assert REGISTRY["codex"].billing is Billing.HOSTED
    assert REGISTRY["claude-code"].billing is Billing.HOSTED


def test_the_local_profile_is_refused_on_a_hosted_backend() -> None:
    """The concrete A4-adjacent mistake: local3090/worker-fast sent to Claude."""
    with pytest.raises(ModelBindingError) as excinfo:
        check_model_allowed(REGISTRY["claude-code"], "local3090/worker-fast")
    message = str(excinfo.value)
    assert "local3090/worker-fast" in message
    assert "claude-code" in message


def test_each_backend_accepts_its_own_namespace() -> None:
    check_model_allowed(REGISTRY["opencode"], "local3090/worker-fast")
    check_model_allowed(REGISTRY["codex"], "gpt-5.6-sol[low]")
    check_model_allowed(REGISTRY["claude-code"], "sonnet")


def test_cross_dispatch_is_refused_in_every_direction() -> None:
    wrong = [
        ("opencode", "sonnet"),
        ("opencode", "gpt-5.6-sol[low]"),
        ("codex", "local3090/worker-fast"),
        ("claude-code", "gpt-5.6-sol[low]"),
    ]
    for backend, model in wrong:
        with pytest.raises(ModelBindingError):
            check_model_allowed(REGISTRY[backend], model)


def test_unconstrained_backend_accepts_anything_but_must_be_chosen_explicitly() -> None:
    """An empty namespace tuple is permitted, but it is a decision, not a default."""
    anything = AcpBackend(
        name="anything", command=("true",), billing=Billing.LOCAL
    )
    check_model_allowed(anything, "whatever/model")
    # None of the real backends are unconstrained.
    assert all(b.model_namespaces for b in REGISTRY.values())


def test_registry_entries_are_version_pinned_when_fetched_per_run() -> None:
    """An unpinned agent silently changes what an execution measured."""
    for backend in REGISTRY.values():
        if backend.command[0] == "npx":
            assert "@" in backend.command[-1].lstrip("@"), backend.name


def test_binding_channel_records_what_was_measured() -> None:
    assert REGISTRY["opencode"].binding_channel is BindingChannel.PROTOCOL
    assert REGISTRY["codex"].binding_channel is BindingChannel.PROTOCOL
    # Finding A13: set_model returns success for literally any id and there is no
    # read-back, so its success means nothing.
    assert REGISTRY["claude-code"].binding_channel is BindingChannel.UNVALIDATED


def test_an_unvalidated_binding_is_never_trustworthy() -> None:
    assert binding_is_trustworthy(REGISTRY["opencode"])
    assert binding_is_trustworthy(REGISTRY["codex"])
    assert not binding_is_trustworthy(REGISTRY["claude-code"])


def test_the_probe_evidence_still_says_set_model_validates_nothing() -> None:
    """Bind the UNVALIDATED verdict to the capture that justified it.

    If a future agent version starts validating, this fails and the registry
    should be re-measured rather than left pessimistic out of habit.
    """
    import json
    from pathlib import Path as _P

    probe = (
        _P(__file__).parents[1]
        / "docs/evidence/acp-agents/claude-code-acp-0.16.2.set-model-probe.json"
    )
    if not probe.is_file():
        pytest.skip(f"probe evidence not present: {probe}")
    data = json.loads(probe.read_text())
    advertised = set(data["advertised"])
    accepted_unadvertised = [
        v["modelId"] for v in data["probes"].values()
        if v["accepted"] and v["modelId"] not in advertised
    ]
    assert accepted_unadvertised, "probe no longer shows unadvertised ids being accepted"


def test_backends_are_frozen() -> None:
    with pytest.raises(Exception):
        REGISTRY["opencode"].billing = Billing.HOSTED  # type: ignore[misc]


def test_claude_model_ids_match_the_captured_evidence() -> None:
    """The registry's Claude model ids must be the ones the agent advertises.

    A first pass guessed a "claude-" prefix and was wrong: the agent advertises
    `default`, `sonnet`, `haiku` -- selectors rather than model names. Bind the
    registry to the capture so the guess cannot creep back in, and so a future
    agent version that changes its vocabulary fails here rather than at dispatch.
    """
    import json
    from pathlib import Path

    capture = (
        Path(__file__).parents[1]
        / "docs/evidence/acp-agents/claude-code-acp-0.16.2.session-new.json"
    )
    if not capture.is_file():
        pytest.skip(f"capture not present: {capture}")
    advertised = {
        m["modelId"]
        for m in json.loads(capture.read_text())["session_new"]["result"]["models"][
            "availableModels"
        ]
    }
    assert set(REGISTRY["claude-code"].model_namespaces) == advertised
