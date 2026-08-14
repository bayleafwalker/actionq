"""Composed daemon runtime implementation.

The public compatibility surface remains :mod:`actionq.daemon`; this module
composes lifecycle, runner, claim-client, and audit boundaries.
"""

from __future__ import annotations

from .daemon_audit import DaemonAuditMixin
from .daemon_claim import DaemonClaimMixin
from .daemon_lifecycle import DaemonLifecycleMixin
from .daemon_runner import DaemonRunnerMixin


class Daemon(
    DaemonLifecycleMixin,
    DaemonRunnerMixin,
    DaemonClaimMixin,
    DaemonAuditMixin,
):
    """ActionQ execution lifecycle composed from focused daemon services."""

    pass
