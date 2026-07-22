"""Trusted caller-mode harness and logical-model routing.

The policy is loaded from agentops' canonical JSON artifact.  This module
contains mechanics only: it does not duplicate model identifiers.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


class RoutingError(ValueError):
    pass


@dataclass(frozen=True)
class HarnessRoute:
    name: str
    bin: str | None = None
    provider: str | None = None
    transport: str | None = None
    surface: str | None = None


@dataclass(frozen=True)
class RoutingContext:
    policy_path: Path | None = None
    default_harness: str | None = None
    trusted_caller_harness: str | None = None
    caller_provider: str | None = None
    caller_transport: str | None = None
    caller_surface: str | None = None
    harnesses: Mapping[str, HarnessRoute] | None = None


@dataclass(frozen=True)
class RoutingRequest:
    model_selector: str
    action_harness: str | None = None
    action_class_harness: str | None = None
    project_harness: str | None = None
    action_model: str | None = None
    action_class_model: str | None = None
    project_model: str | None = None


@dataclass(frozen=True)
class RoutingResult:
    requested_selector: str
    caller_harness: str | None
    harness: str
    provider: str | None
    model: str
    transport: str | None
    surface: str | None
    routing_source: str
    fallback_model: str | None = None
    fallback_reason: str | None = None

    def provenance(self) -> dict[str, str | None]:
        return {
            "requested_selector": self.requested_selector,
            "trusted_caller_harness": self.caller_harness,
            "resolved_harness": self.harness,
            "resolved_provider": self.provider,
            "resolved_model": self.model,
            "transport": self.transport,
            "surface": self.surface,
            "routing_source": self.routing_source,
            "fallback_model": self.fallback_model,
            "fallback_reason": self.fallback_reason,
        }


def load_policy(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RoutingError(f"cannot load model routing policy {path}: {exc}") from exc
    if policy.get("schema_version") != 1:
        raise RoutingError(f"unsupported model routing policy schema in {path}")
    return policy


def _concrete(value: str | None) -> bool:
    return bool(value) and value != "caller"


def _select_harness(request: RoutingRequest, context: RoutingContext) -> tuple[str, str]:
    if _concrete(request.action_harness):
        return str(request.action_harness), "action-explicit"
    if _concrete(request.action_class_harness):
        return str(request.action_class_harness), "action-class-override"
    if _concrete(request.project_harness):
        return str(request.project_harness), "project-default"
    caller_requested = "caller" in (
        request.action_harness,
        request.action_class_harness,
        request.project_harness,
        context.default_harness,
    )
    harnesses = context.harnesses or {}
    if caller_requested:
        if context.trusted_caller_harness:
            return context.trusted_caller_harness, "caller-inheritance"
        if len(harnesses) != 1:
            raise RoutingError(
                "caller routing requires trusted_caller_harness when multiple harnesses are configured"
            )
    if len(harnesses) == 1:
        return next(iter(harnesses)), "single-configured-harness"
    raise RoutingError("cannot resolve harness from explicit, class, project, caller, or single-harness policy")


def _provider(harness: str, context: RoutingContext, policy: dict[str, Any] | None) -> str | None:
    harness_route = (context.harnesses or {}).get(harness)
    if harness_route and harness_route.provider:
        return harness_route.provider
    if harness == context.trusted_caller_harness and context.caller_provider:
        return context.caller_provider
    if policy:
        return policy.get("caller_harness_providers", {}).get(harness)
    return None


def resolve_routing(request: RoutingRequest, context: RoutingContext) -> RoutingResult:
    policy = load_policy(context.policy_path)
    harness, source = _select_harness(request, context)
    if harness == "caller":
        raise RoutingError("caller is a selector, not an executable harness")
    selector = next(
        (
            value
            for value in (
                request.action_model,
                request.action_class_model,
                request.project_model,
                request.model_selector,
            )
            if value
        ),
        "",
    )
    if not selector:
        raise RoutingError(f"no model selector configured for harness {harness!r}")
    harness_route = (context.harnesses or {}).get(harness)
    provider = _provider(harness, context, policy)
    if not policy or selector not in policy.get("aliases", {}):
        return RoutingResult(
            selector, context.trusted_caller_harness, harness, provider, selector,
            harness_route.transport if harness_route else None,
            harness_route.surface if harness_route else None, source,
        )
    if not provider:
        raise RoutingError(f"harness {harness!r} requires an explicit provider mapping for alias {selector!r}")
    branch = policy["aliases"][selector].get(provider)
    if not branch or not branch.get("verified"):
        raise RoutingError(f"alias {selector!r} has no verified provider branch for {provider!r}")
    required_transport = branch.get("transport")
    surfaces = branch.get("surfaces") or []
    actual_transport = (
        harness_route.transport
        if harness_route and harness_route.transport
        else context.caller_transport
    )
    actual_surface = (
        harness_route.surface
        if harness_route and harness_route.surface
        else context.caller_surface
    )
    incompatible = (
        (required_transport and actual_transport != required_transport)
        or (surfaces and actual_surface not in surfaces)
    )
    if incompatible:
        fallback = branch.get("fallback")
        if not fallback:
            raise RoutingError(
                f"alias {selector!r} transport/surface is incompatible and no same-provider fallback exists"
            )
        return RoutingResult(
            selector, context.trusted_caller_harness, harness, provider, fallback,
            actual_transport, actual_surface, source,
            fallback_reason="transport-incompatible",
        )
    return RoutingResult(
        selector, context.trusted_caller_harness, harness, provider, branch["model"],
        required_transport or actual_transport, actual_surface, source,
        fallback_model=branch.get("fallback"),
    )


def same_provider_fallback(result: RoutingResult, *, reason: str) -> RoutingResult:
    if not result.fallback_model:
        raise RoutingError(
            f"no same-provider fallback is declared for {result.provider or result.harness!r}; "
            "cross-provider fallback is never implicit"
        )
    return RoutingResult(
        requested_selector=result.requested_selector,
        caller_harness=result.caller_harness,
        harness=result.harness,
        provider=result.provider,
        model=result.fallback_model,
        transport=result.transport,
        surface=result.surface,
        routing_source="same-provider-fallback",
        fallback_reason=reason,
    )
