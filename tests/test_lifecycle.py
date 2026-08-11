from __future__ import annotations

import json

import pytest

from actionq.lifecycle import (
    BoundedLifecycleProfile,
    LifecycleValidationError,
    assert_no_tool_events,
    finalization_declaration,
    parse_json_events,
    session_id_from_events,
)


def _event(value: dict) -> dict:
    return {"type": "message.updated", "properties": {"sessionID": "ses_1", **value}}


def test_profile_is_fixed_and_opt_in():
    profile = BoundedLifecycleProfile()
    assert profile.validate().work_timeout_seconds == 1200
    with pytest.raises(LifecycleValidationError, match="20 minutes"):
        BoundedLifecycleProfile(work_timeout_seconds=1201).validate()
    with pytest.raises(LifecycleValidationError, match="no tools"):
        BoundedLifecycleProfile(finalizer_tools=("read",)).validate()


def test_session_identity_and_json_events_are_stable():
    events = parse_json_events(json.dumps(_event({"info": {"role": "assistant"}})))
    assert session_id_from_events(events) == "ses_1"
    with pytest.raises(LifecycleValidationError, match="identity changed"):
        session_id_from_events(events + [_event({"sessionID": "ses_2"})])
    with pytest.raises(LifecycleValidationError, match="not JSON"):
        parse_json_events("worker finished")


def test_finalizer_requires_one_bound_declaration_and_no_tools():
    declaration = {
        "contract_id": "dispatch-finalization/v1", "action_id": 7,
        "attempt_id": "aqs:7", "session_id": "ses_1",
        "terminal_status": "completed", "summary": "verified",
    }
    event = _event({"declaration": declaration})
    event["type"] = "dispatch.finalization"
    assert finalization_declaration(
        [event], action_id=7, attempt_id="aqs:7", session_id="ses_1",
    ) == declaration
    tool = _event({"part": {"type": "tool"}})
    with pytest.raises(LifecycleValidationError, match="tool event"):
        assert_no_tool_events([tool])
    with pytest.raises(LifecycleValidationError, match="exactly one"):
        finalization_declaration(
            [_event({"info": {"role": "assistant"}})],
            action_id=7, attempt_id="aqs:7", session_id="ses_1",
        )
