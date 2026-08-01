"""Bounded cross-action cancellation histories for the candidate pipeline."""
from __future__ import annotations

from dataclasses import dataclass, replace
from itertools import product


@dataclass(frozen=True)
class Pipeline:
    verification: str = "pending"
    integration: str = "pending"
    review: str = "pending"


def _step(state: Pipeline, operation: str) -> Pipeline:
    role, event = operation.split(":", 1)
    current = getattr(state, role)
    if event == "claim" and current == "pending":
        if role == "integration" and state.verification != "completed":
            return state
        if role == "review" and state.integration != "completed":
            return state
        return replace(state, **{role: "claimed"})
    if event == "cancel" and current in {"pending", "claimed"}:
        return replace(state, **{role: "cancelled"})
    if event == "publish" and current == "claimed":
        return replace(state, **{role: "completed"})
    return state


def test_cancelled_action_never_publishes_or_unlocks_a_downstream_action():
    operations = tuple(
        f"{role}:{event}"
        for role in ("verification", "integration", "review")
        for event in ("claim", "cancel", "publish")
    )
    for history in product(operations, repeat=3):
        state = Pipeline()
        cancelled: set[str] = set()
        for operation in history:
            role, event = operation.split(":", 1)
            before = state
            state = _step(state, operation)
            if event == "cancel" and getattr(state, role) == "cancelled":
                cancelled.add(role)
            assert all(getattr(state, item) != "completed" for item in cancelled)
            if before.verification != "completed":
                assert state.integration != "claimed" and state.review != "claimed"
            if before.integration != "completed":
                assert state.review != "claimed"
