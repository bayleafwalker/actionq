"""Context-policy enforcement.

Protocol-neutral. Two separate jobs:

*Before dispatch* — reject an envelope that has already violated the policy: WARM/COLD
content embedded as payload rather than referenced as an address, or HOT material over its
budget. This is deterministic and runs against our own tokenizer, on content we hold.

*During execution* — account for what retrieval promoted into view, so promotion is
recorded rather than merely permitted.

The invariant is not a token maximum. It is:

    Bulky context outside HOT must remain addressable rather than silently becoming
    model-visible content.

That survives HOT being re-derived at 10 240 or 16 384. A bare ``assert tokens <= 12288``
would have turned a measured parameter into the architecture.

Why enforce rather than advise: eager injection made accepted work 4.7x slower at
identical acceptance and produced no error at any layer (local-inference F15/F15b).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from .execution_boundary import BoundaryCheck, BoundaryReport
from .execution_contract import (
    Assurance,
    ContextAddress,
    ContextPolicy,
    ContextTier,
    ContextUsage,
)


class TokenCounter(Protocol):
    """Counts tokens for the model the execution is bound to.

    Pluggable because the count must come from the *execution's own* tokenizer. A
    character heuristic would make the budget a different quantity per model, which is
    the ambiguity this module exists to remove.
    """

    def __call__(self, text: str) -> int: ...


def approximate_counter(text: str) -> int:
    """Deliberately crude fallback, ~4 chars per token.

    Only for when no real tokenizer is reachable. It is not accurate and callers are told
    so through the returned assurance level, never silently.
    """
    return (len(text) + 3) // 4


# Content that is bulky enough that embedding it is the failure we are guarding against.
# Below this, an inline string is a label or a short instruction, not injected evidence.
EMBEDDED_CONTENT_TOKENS = 512


@dataclass(frozen=True)
class ContextPlan:
    """What an execution is authorised to hold, once checked."""

    usage: ContextUsage
    hot_assurance: Assurance
    warm_addresses: tuple[ContextAddress, ...] = ()
    cold_addresses: tuple[ContextAddress, ...] = ()


def verify_context_policy(
    policy: ContextPolicy,
    *,
    count_tokens: TokenCounter | None = None,
) -> tuple[BoundaryReport, ContextPlan]:
    """Check an envelope's context allocation before anything is dispatched."""
    counter = count_tokens or approximate_counter
    assurance = Assurance.VERIFIED if count_tokens is not None else Assurance.ASSERTED

    checks: list[BoundaryCheck] = []

    hot_tokens = sum(counter(material) for material in policy.hot_material)
    checks.append(BoundaryCheck(
        name="context.hot_within_budget",
        passed=hot_tokens <= policy.hot_budget,
        expected=f"<= {policy.hot_budget} tokens",
        observed=f"{hot_tokens} tokens",
        detail=None if count_tokens is not None else
        "counted with the approximate fallback; not the execution's tokenizer",
    ))

    # The load-bearing check. An address whose 'address' field carries a wall of content
    # is a payload wearing a reference's clothes, and recreates the slow arm.
    embedded = [
        a for a in policy.addresses
        if a.tier in (ContextTier.WARM, ContextTier.COLD)
        and counter(a.address) > EMBEDDED_CONTENT_TOKENS
    ]
    checks.append(BoundaryCheck(
        name="context.tiers_remain_addressable",
        passed=not embedded,
        expected="WARM/COLD carried as addresses",
        observed=(
            "all addressable" if not embedded
            else f"{len(embedded)} embedded as content: {[a.provider for a in embedded]}"
        ),
        detail=None if not embedded else
        "embedding bulky evidence recreates the eager-injection arm (4.7x slower, no error)",
    ))

    checks.append(BoundaryCheck(
        name="context.promotion_bounded",
        passed=policy.promotion_allowed or policy.promotion_budget == 0,
        expected="promotion disallowed implies no promotion budget",
        observed=f"allowed={policy.promotion_allowed} budget={policy.promotion_budget}",
    ))

    report = BoundaryReport(phase="context_policy", checks=tuple(checks))
    plan = ContextPlan(
        usage=ContextUsage(hot_tokens=hot_tokens),
        hot_assurance=assurance,
        warm_addresses=policy.addresses_for(ContextTier.WARM),
        cold_addresses=policy.addresses_for(ContextTier.COLD),
    )
    return report, plan


def account_promotion(
    plan: ContextPlan,
    policy: ContextPolicy,
    promoted_tokens: int,
) -> tuple[BoundaryReport, ContextPlan]:
    """Record what retrieval promoted into view, and check it against the policy."""
    checks: list[BoundaryCheck] = []

    checks.append(BoundaryCheck(
        name="context.promotion_permitted",
        passed=policy.promotion_allowed or promoted_tokens == 0,
        expected="no promotion" if not policy.promotion_allowed else "promotion allowed",
        observed=f"{promoted_tokens} tokens promoted",
    ))

    if policy.promotion_budget:
        checks.append(BoundaryCheck(
            name="context.promotion_within_budget",
            passed=promoted_tokens <= policy.promotion_budget,
            expected=f"<= {policy.promotion_budget} tokens",
            observed=f"{promoted_tokens} tokens",
        ))

    usage = ContextUsage(
        hot_tokens=plan.usage.hot_tokens,
        promoted_tokens=promoted_tokens,
        harness_tokens=plan.usage.harness_tokens,
        harness_assurance=plan.usage.harness_assurance,
    )
    updated = ContextPlan(
        usage=usage,
        hot_assurance=plan.hot_assurance,
        warm_addresses=plan.warm_addresses,
        cold_addresses=plan.cold_addresses,
    )
    return BoundaryReport(phase="context_promotion", checks=tuple(checks)), updated


def llama_cpp_counter(endpoint: str, *, timeout: float = 30.0) -> Callable[[str], int]:
    """A real tokenizer, via a llama.cpp `/tokenize` endpoint.

    vLLM speaks a different request dialect (`prompt` rather than `content`) at the same
    path, so the engine is part of the counter's identity, not a detail it can hide.
    """
    import json
    import urllib.request

    def count(text: str) -> int:
        request = urllib.request.Request(
            endpoint,
            data=json.dumps({"content": text}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return len(json.load(response)["tokens"])

    return count
