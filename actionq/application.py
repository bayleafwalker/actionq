"""Compatibility application surface composed from focused ActionQ services."""

from __future__ import annotations

from .application_core import (
    ACTION_RESOURCE_NOT_FOUND, AuthenticatedActionResourcePrincipal, ActionQCore,
    InvocationProvenance,
)
from .application_enqueue import EnqueueService
from .application_groups import GroupService
from .application_claim import ClaimService
from .application_completion import CompletionService
from .application_outbox import OutboxService
from .application_dispatch import DispatchService


class ActionQApplication(
    DispatchService, OutboxService, CompletionService, ClaimService,
    GroupService, EnqueueService, ActionQCore,
):
    """One stable application boundary over focused operation services."""

    pass


__all__ = [
    "ActionQApplication", "InvocationProvenance",
    "AuthenticatedActionResourcePrincipal", "ACTION_RESOURCE_NOT_FOUND",
]
