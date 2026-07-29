"""Known provider-harness implementation profiles.

Profiles identify tested command semantics and deployment provenance.  They
are deliberately finite: accepting an arbitrary version string would turn a
host-package drift into an unreviewed execution-policy change.
"""

from __future__ import annotations

from dataclasses import dataclass

from .routing import RoutingError, RoutingResult


@dataclass(frozen=True)
class HarnessProfile:
    profile_id: str
    harness: str
    provider: str
    semantic_adapter: str
    qualification_state: str
    worker_user: str


KNOWN_PROFILES = {
    "opencode-nixpkgs-devbox-1.18.4": HarnessProfile(
        profile_id="opencode-nixpkgs-devbox-1.18.4",
        harness="opencode",
        provider="opencode-go",
        semantic_adapter="opencode-noninteractive/v1",
        qualification_state="preflight_observed",
        worker_user="agentworker",
    ),
}


def validate_harness_profile(
    profile_id: str | None,
    routing: RoutingResult,
    *,
    worker_user: str | None,
) -> HarnessProfile:
    if not profile_id:
        raise RoutingError("provider-backed scope-iterate requires a harness_profile")
    profile = KNOWN_PROFILES.get(profile_id)
    if profile is None:
        raise RoutingError(f"unknown harness_profile {profile_id!r}")
    if routing.harness != profile.harness or routing.provider != profile.provider:
        raise RoutingError(
            f"harness_profile {profile_id!r} is incompatible with "
            f"{routing.harness}/{routing.provider}"
        )
    if worker_user != profile.worker_user:
        raise RoutingError(
            f"harness_profile {profile_id!r} requires worker_user "
            f"{profile.worker_user!r}"
        )
    if profile.qualification_state != "qualified":
        raise RoutingError(
            f"harness_profile {profile_id!r} is not qualified "
            f"({profile.qualification_state})"
        )
    return profile
