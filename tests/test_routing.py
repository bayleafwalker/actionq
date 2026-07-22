from __future__ import annotations

import json
from pathlib import Path

import pytest

from actionq.routing import (
    HarnessRoute, RoutingContext, RoutingError, RoutingRequest,
    resolve_routing, same_provider_fallback,
)


def _policy(tmp_path: Path) -> Path:
    path = tmp_path / "routing.json"
    path.write_text(json.dumps({
        "schema_version": 1,
        "caller_harness_providers": {"claude": "anthropic", "codex": "codex", "kimi": "kimi"},
        "aliases": {
            "fast-build": {
                "anthropic": {"model": "claude-haiku", "verified": True},
                "codex": {
                    "model": "gpt-spark", "fallback": "gpt-luna", "verified": True,
                    "transport": "chatgpt", "surfaces": ["codex-cli"],
                },
            },
            "codex-only": {"codex": {"model": "gpt-sol", "verified": True}},
        },
    }), encoding="utf-8")
    return path


def _context(tmp_path: Path, caller: str | None, *, transport="chatgpt", surface="codex-cli") -> RoutingContext:
    return RoutingContext(
        policy_path=_policy(tmp_path), default_harness="caller", trusted_caller_harness=caller,
        caller_transport=transport, caller_surface=surface,
        harnesses={"claude": HarnessRoute("claude"), "codex": HarnessRoute("codex")},
    )


def test_claude_and_codex_caller_branches(tmp_path):
    request = RoutingRequest("fast-build", project_harness="caller")
    claude = resolve_routing(request, _context(tmp_path, "claude"))
    codex = resolve_routing(request, _context(tmp_path, "codex"))
    assert (claude.harness, claude.provider, claude.model) == ("claude", "anthropic", "claude-haiku")
    assert (codex.harness, codex.provider, codex.model) == ("codex", "codex", "gpt-spark")


def test_explicit_then_class_then_project_precedence(tmp_path):
    context = _context(tmp_path, "codex")
    result = resolve_routing(
        RoutingRequest("fast-build", action_harness="claude", action_class_harness="codex", project_harness="codex"),
        context,
    )
    assert result.harness == "claude"
    assert result.routing_source == "action-explicit"


def test_model_precedence_is_explicit_then_class_then_project(tmp_path):
    context = _context(tmp_path, "codex")
    result = resolve_routing(
        RoutingRequest(
            "frontier-default",
            project_harness="caller",
            action_model="fast-build",
            action_class_model="codex-only",
            project_model="codex-only",
        ),
        context,
    )
    assert result.requested_selector == "fast-build"
    assert result.model == "gpt-spark"


def test_explicit_harness_uses_its_configured_transport_not_caller_transport(tmp_path):
    context = RoutingContext(
        policy_path=_policy(tmp_path),
        default_harness="caller",
        trusted_caller_harness="claude",
        caller_transport="anthropic-cli",
        caller_surface="claude-code",
        harnesses={
            "claude": HarnessRoute("claude"),
            "codex": HarnessRoute("codex", transport="chatgpt", surface="codex-cli"),
        },
    )
    result = resolve_routing(
        RoutingRequest("fast-build", action_harness="codex"),
        context,
    )
    assert result.model == "gpt-spark"
    assert result.fallback_reason is None


def test_missing_caller_identity_with_multiple_harnesses_fails(tmp_path):
    with pytest.raises(RoutingError, match="trusted_caller_harness"):
        resolve_routing(RoutingRequest("fast-build", project_harness="caller"), _context(tmp_path, None))


def test_kimi_and_cross_provider_fallback_fail_closed(tmp_path):
    with pytest.raises(RoutingError, match="no verified provider branch"):
        resolve_routing(RoutingRequest("fast-build", project_harness="caller"), _context(tmp_path, "kimi"))
    with pytest.raises(RoutingError, match="no verified provider branch"):
        resolve_routing(RoutingRequest("codex-only", project_harness="caller"), _context(tmp_path, "claude"))


def test_spark_transport_falls_back_to_luna_without_changing_provider(tmp_path):
    result = resolve_routing(
        RoutingRequest("fast-build", project_harness="caller"),
        _context(tmp_path, "codex", transport="api", surface="api-worker"),
    )
    assert (result.harness, result.provider, result.model) == ("codex", "codex", "gpt-luna")
    assert result.fallback_reason == "transport-incompatible"


def test_spark_limit_handoff_route_is_luna_on_codex(tmp_path):
    primary = resolve_routing(
        RoutingRequest("fast-build", project_harness="caller"), _context(tmp_path, "codex")
    )
    fallback = same_provider_fallback(primary, reason="confirmed Spark usage limit")
    assert (fallback.harness, fallback.provider, fallback.model) == ("codex", "codex", "gpt-luna")
    assert fallback.routing_source == "same-provider-fallback"
