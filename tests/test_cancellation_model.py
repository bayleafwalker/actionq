"""Bounded Depth-3 state model for cancellation/settlement histories."""
from __future__ import annotations

from itertools import product


def step(state: tuple[str, bool], operation: str) -> tuple[str, bool]:
    status, stopped = state
    if operation == "complete" and status == "claimed":
        return "completed", stopped
    if operation == "cancel" and status == "claimed":
        return "cancelling", stopped
    if operation == "ack" and status == "cancelling" and stopped:
        return "cancelled", True
    if operation == "death" and status == "cancelling":
        return status, True
    if operation == "timeout" and status == "cancelling":
        return "cancelled", stopped
    # Duplicate cancellation, sweep, renewal, late settlement, and late ack
    # are rejected/ignored and cannot move the projection.
    return state


def test_bounded_depth_three_cancellation_histories():
    operations = ("complete", "cancel", "ack", "death", "timeout", "sweep", "renew")
    for history in product(operations, repeat=3):
        state = ("claimed", False)
        terminal_seen = None
        for operation in history:
            state = step(state, operation)
            if state[0] in ("completed", "cancelled"):
                terminal_seen = terminal_seen or state[0]
                assert state[0] == terminal_seen
            if state[0] == "cancelling":
                assert step(state, "complete") == state
                assert step(state, "sweep") == state
