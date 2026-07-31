"""Bounded Depth-3 model of receipt-, identity-, and request-fenced cancellation."""
from __future__ import annotations

from dataclasses import dataclass, replace
from itertools import product


@dataclass(frozen=True)
class State:
    status: str = "claimed"
    receipt_live: bool = True
    request_id: str | None = None
    expected_auth: str = "auth:one"
    process_state: str = "running"
    terminal_kind: str | None = None


def step(state: State, operation: str) -> State:
    if operation == "complete" and state.status == "claimed" and state.receipt_live:
        return replace(state, status="completed", receipt_live=False, terminal_kind="completed")
    if operation == "cancel" and state.status == "claimed":
        return replace(state, status="cancelling", receipt_live=False, request_id="cancel:one")
    if operation == "death" and state.status == "cancelling":
        return replace(state, process_state="stopped")
    if operation == "ack" and state.status == "cancelling" and state.process_state == "stopped":
        return replace(state, status="cancelled", terminal_kind="stop-acknowledged")
    if operation == "ack-wrong-auth" or operation == "ack-wrong-request":
        return state
    if operation == "timeout" and state.status == "cancelling":
        return replace(state, status="cancelled", process_state="unknown",
                       terminal_kind="stop-unacknowledged-timeout")
    # Duplicate cancel, sweep, renewal, late settlement, and ack response retry
    # cannot create a second terminal transition.
    return state


def test_bounded_depth_three_cancellation_histories():
    operations = (
        "complete", "cancel", "cancel", "ack", "ack-wrong-auth", "ack-wrong-request",
        "death", "timeout", "sweep", "renew",
    )
    for history in product(operations, repeat=3):
        state = State()
        terminal_seen = None
        for operation in history:
            state = step(state, operation)
            if state.status in ("completed", "cancelled"):
                terminal_seen = terminal_seen or state.terminal_kind
                assert state.terminal_kind == terminal_seen
                assert state.receipt_live is False
            if state.status == "cancelling":
                assert step(state, "complete") == state
                assert step(state, "sweep") == state
                assert step(state, "renew") == state
                assert step(state, "ack-wrong-auth") == state
                assert step(state, "ack-wrong-request") == state
            if state.terminal_kind == "stop-unacknowledged-timeout":
                assert state.process_state == "unknown"
