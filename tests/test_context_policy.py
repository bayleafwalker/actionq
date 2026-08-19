"""Context-policy tests.

The failure being guarded is silent: eager injection makes accepted work several times
slower and nothing errors. So the assertions are about *representation* -- whether bulky
evidence stayed addressable -- not only about token arithmetic.
"""
from __future__ import annotations

import pytest

from actionq_runner.context_policy import (
    EMBEDDED_CONTENT_TOKENS,
    account_promotion,
    approximate_counter,
    verify_context_policy,
)
from actionq_runner.execution_contract import (
    Assurance,
    ContextAddress,
    ContextPolicy,
    ContextTier,
    ContextUsage,
)


def _address(tier: ContextTier, address: str) -> ContextAddress:
    return ContextAddress(tier=tier, provider="git", address=address)


# -- the load-bearing invariant --------------------------------------------


def test_addressable_warm_and_cold_pass() -> None:
    policy = ContextPolicy(addresses=(
        _address(ContextTier.WARM, "src/a.py"),
        _address(ContextTier.COLD, "docs/spec.md"),
    ))
    report, plan = verify_context_policy(policy)
    assert report.ok
    assert len(plan.warm_addresses) == 1 and len(plan.cold_addresses) == 1


def test_content_embedded_as_an_address_is_refused() -> None:
    """A payload wearing a reference's clothes. This recreates the slow arm."""
    wall_of_text = "x" * (EMBEDDED_CONTENT_TOKENS * 4 + 400)
    policy = ContextPolicy(addresses=(_address(ContextTier.COLD, wall_of_text),))
    report, _ = verify_context_policy(policy)
    assert not report.ok
    assert report.violations[0].name == "context.tiers_remain_addressable"


def test_hot_material_may_be_inline() -> None:
    """HOT is the tier that is *supposed* to be inlined; only WARM/COLD must stay refs."""
    policy = ContextPolicy(hot_material=("a short task brief",))
    report, _ = verify_context_policy(policy)
    assert report.ok


# -- budget is a parameter, not the architecture ---------------------------


def test_hot_over_budget_is_refused() -> None:
    policy = ContextPolicy(hot_material=("word " * 5000,), hot_budget=100)
    report, _ = verify_context_policy(policy)
    assert not report.ok
    assert report.violations[0].name == "context.hot_within_budget"


def test_budget_is_configurable_not_hardcoded() -> None:
    """Re-deriving HOT must not require touching enforcement code."""
    material = ("word " * 400,)
    tight = ContextPolicy(hot_material=material, hot_budget=10)
    roomy = ContextPolicy(hot_material=material, hot_budget=100_000)
    assert not verify_context_policy(tight)[0].ok
    assert verify_context_policy(roomy)[0].ok


def test_default_budget_matches_the_measured_value() -> None:
    assert ContextPolicy().hot_budget == 12288


# -- tokenizer provenance --------------------------------------------------


def test_fallback_counter_is_labelled_as_approximate() -> None:
    """An approximate count must never be reported as a verified one."""
    report, plan = verify_context_policy(ContextPolicy(hot_material=("hello",)))
    assert plan.hot_assurance is Assurance.ASSERTED
    check = [c for c in report.checks if c.name == "context.hot_within_budget"][0]
    assert "approximate" in check.detail


def test_real_counter_yields_verified_assurance() -> None:
    report, plan = verify_context_policy(
        ContextPolicy(hot_material=("hello",)), count_tokens=lambda text: len(text.split())
    )
    assert plan.hot_assurance is Assurance.VERIFIED
    check = [c for c in report.checks if c.name == "context.hot_within_budget"][0]
    assert check.detail is None


def test_counter_choice_changes_the_measured_quantity() -> None:
    material = ("one two three four five six seven eight",)
    by_words = verify_context_policy(
        ContextPolicy(hot_material=material), count_tokens=lambda t: len(t.split())
    )[1]
    by_chars = verify_context_policy(ContextPolicy(hot_material=material))[1]
    assert by_words.usage.hot_tokens != by_chars.usage.hot_tokens


# -- promotion -------------------------------------------------------------


def test_promotion_is_recorded_not_merely_permitted() -> None:
    policy = ContextPolicy(promotion_allowed=True)
    _, plan = verify_context_policy(policy)
    report, updated = account_promotion(plan, policy, promoted_tokens=900)
    assert report.ok
    assert updated.usage.promoted_tokens == 900


def test_promotion_when_disallowed_is_a_violation() -> None:
    policy = ContextPolicy(promotion_allowed=False)
    _, plan = verify_context_policy(policy)
    report, _ = account_promotion(plan, policy, promoted_tokens=1)
    assert not report.ok
    assert report.violations[0].name == "context.promotion_permitted"


def test_promotion_over_its_budget_is_a_violation() -> None:
    policy = ContextPolicy(promotion_allowed=True, promotion_budget=100)
    _, plan = verify_context_policy(policy)
    report, _ = account_promotion(plan, policy, promoted_tokens=101)
    assert not report.ok
    assert report.violations[0].name == "context.promotion_within_budget"


# -- usage accounting ------------------------------------------------------


def test_task_and_harness_tokens_are_separate_quantities() -> None:
    """Conflating them makes the budget ambiguous and hides a growing preamble."""
    usage = ContextUsage(hot_tokens=2411, promoted_tokens=0, harness_tokens=6897)
    assert usage.task_tokens == 2411
    assert usage.total_tokens == 9308


def test_unknown_harness_share_yields_no_false_total() -> None:
    usage = ContextUsage(hot_tokens=2411)
    assert usage.task_tokens == 2411
    assert usage.total_tokens is None
    assert usage.harness_assurance is Assurance.UNVERIFIABLE
