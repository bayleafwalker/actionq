import pytest

from actionq.harness_profiles import validate_harness_profile
from actionq.routing import RoutingError, RoutingResult


def _routing() -> RoutingResult:
    return RoutingResult(
        requested_selector="", caller_harness=None, harness="opencode",
        provider="opencode-go", model="opencode-go/deepseek-v4-flash",
        transport=None, surface=None, routing_source="test",
    )


def test_provider_profile_fails_closed_until_qualified():
    with pytest.raises(RoutingError, match="not qualified"):
        validate_harness_profile("opencode-nixpkgs-devbox-1.18.4", _routing())


def test_unknown_provider_profile_is_rejected():
    with pytest.raises(RoutingError, match="unknown harness_profile"):
        validate_harness_profile("opencode-anywhere-9.9.9", _routing())


def test_provider_profile_is_bound_to_its_harness():
    routing = RoutingResult(
        requested_selector="", caller_harness=None, harness="codex", provider="codex",
        model="gpt-test", transport=None, surface=None, routing_source="test",
    )
    with pytest.raises(RoutingError, match="incompatible"):
        validate_harness_profile("opencode-nixpkgs-devbox-1.18.4", routing)
