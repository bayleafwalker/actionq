"""Capability divergence across real ACP v1 implementations.

Fungibility must not mean "both agents pretend to identical capability". It means Vuoro
executes correctly across both, and the differences stay *visible* so policy can act on
them. These tests run against `initialize` responses captured from three shipping agents.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from actionq_runner.acp.v1 import PROTOCOL_VERSION, AgentCapabilities

EVIDENCE = Path(__file__).resolve().parents[1] / "docs" / "evidence" / "acp-agents"
AGENTS = sorted(EVIDENCE.glob("*.initialize.json"))


def _caps(path: Path) -> AgentCapabilities:
    return AgentCapabilities.from_initialize(json.loads(path.read_text()))


def test_evidence_covers_more_than_one_implementation() -> None:
    assert len(AGENTS) >= 3, "capability comparison needs several real agents"


@pytest.mark.parametrize("path", AGENTS, ids=lambda p: p.name)
def test_every_agent_negotiates_v1(path: Path) -> None:
    assert _caps(path).protocol_version == PROTOCOL_VERSION


@pytest.mark.parametrize("path", AGENTS, ids=lambda p: p.name)
def test_capabilities_parse_without_agent_specific_handling(path: Path) -> None:
    """One codec reads all three. No per-vendor branch in the parser."""
    caps = _caps(path)
    assert caps.agent_name and caps.agent_version
    assert isinstance(caps.session_capabilities, frozenset)


def test_session_capabilities_genuinely_diverge() -> None:
    """The observed spread that motivates per-property assurance."""
    by_agent = {p.name.split(".initialize")[0]: _caps(p).session_capabilities for p in AGENTS}

    opencode = next(v for k, v in by_agent.items() if k.startswith("opencode"))
    claude = next(v for k, v in by_agent.items() if k.startswith("claude-code"))
    gemini = next(v for k, v in by_agent.items() if k.startswith("gemini"))

    # OpenCode is the only one advertising session close.
    assert "close" in opencode
    assert "close" not in claude

    # Gemini advertises no session capabilities at all.
    assert gemini == frozenset()

    # So no two of the three agree.
    assert len({frozenset(v) for v in by_agent.values()}) == 3


def test_root_readback_is_not_universally_available() -> None:
    """`session/list` carried the root verification. One agent does not advertise it.

    That is exactly why assurance is per-property: the same adapter yields VERIFIED root
    on one backend and UNVERIFIABLE on another, and must say so rather than average them.
    """
    by_agent = {p.name.split(".initialize")[0]: _caps(p) for p in AGENTS}
    with_list = {k for k, v in by_agent.items() if "list" in v.session_capabilities}
    assert with_list, "at least one agent supports session listing"
    assert with_list != set(by_agent), "at least one agent does not"


def test_prompt_capabilities_diverge() -> None:
    caps = {p.name.split(".initialize")[0]: _caps(p).prompt_capabilities for p in AGENTS}
    gemini = next(v for k, v in caps.items() if k.startswith("gemini"))
    opencode = next(v for k, v in caps.items() if k.startswith("opencode"))
    assert "audio" in gemini and "audio" not in opencode


@pytest.mark.parametrize("path", AGENTS, ids=lambda p: p.name)
def test_every_agent_declares_an_auth_method(path: Path) -> None:
    assert _caps(path).auth_methods
