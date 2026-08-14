"""Structural guardrails for the ActionQ application service boundary."""

from actionq.application import ActionQApplication
from actionq.application_claim import ClaimService
from actionq.application_completion import CompletionService
from actionq.application_core import ActionQCore
from actionq.application_dispatch import DispatchService
from actionq.application_enqueue import EnqueueService
from actionq.application_groups import GroupService
from actionq.application_outbox import OutboxService


def test_application_is_composed_from_operation_services():
    assert ActionQApplication.__mro__[1:8] == (
        DispatchService,
        OutboxService,
        CompletionService,
        ClaimService,
        GroupService,
        EnqueueService,
        ActionQCore,
    )


def test_application_methods_live_in_their_service_modules():
    assert ActionQApplication.enqueue.__module__ == EnqueueService.__module__
    assert ActionQApplication.claim.__module__ == ClaimService.__module__
    assert ActionQApplication.complete.__module__ == CompletionService.__module__
    assert ActionQApplication.list_execution_groups.__module__ == GroupService.__module__
    assert ActionQApplication.emit_event.__module__ == OutboxService.__module__
    assert ActionQApplication.dispatch.__module__ == DispatchService.__module__
