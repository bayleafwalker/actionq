from __future__ import annotations

import json

import pytest

from actionq.lifecycle import (
    BoundedLifecycleProfile,
    LifecycleValidationError,
    assert_no_tool_events,
    completion_indication,
    finalization_declaration,
    parse_json_events,
    session_id_from_events,
)


def _event(value: dict, *, event_type: str = "text", timestamp: int = 1) -> dict:
    return {
        "type": event_type, "sessionID": value.pop("sessionID", "ses_1"),
        "timestamp": timestamp, "part": {"type": "text", **value},
    }


def test_profile_is_fixed_and_opt_in():
    profile = BoundedLifecycleProfile()
    assert profile.validate().work_timeout_seconds == 1200
    with pytest.raises(LifecycleValidationError, match="20 minutes"):
        BoundedLifecycleProfile(work_timeout_seconds=1201).validate()
    with pytest.raises(LifecycleValidationError, match="no tools"):
        BoundedLifecycleProfile(finalizer_tools=("read",)).validate()


def test_session_identity_and_json_events_are_stable():
    events = parse_json_events(json.dumps(_event({"text": "working"})))
    assert session_id_from_events(events) == "ses_1"
    with pytest.raises(LifecycleValidationError, match="identity changed"):
        session_id_from_events(events + [_event({"sessionID": "ses_2"})])
    with pytest.raises(LifecycleValidationError, match="not JSON"):
        parse_json_events("worker finished")
    failed = _event({"status": "failed"})
    with pytest.raises(LifecycleValidationError, match="reports an error"):
        parse_json_events(json.dumps(failed))


def test_finalizer_requires_one_bound_declaration_and_no_tools():
    declaration = {
        "contract_id": "dispatch-finalization/v1", "action_id": 7,
        "attempt_id": "aqs:7", "session_id": "ses_1",
        "terminal_status": "completed", "summary": "verified",
    }
    event = _event({"text": json.dumps(declaration)})
    assert finalization_declaration(
        [event], action_id=7, attempt_id="aqs:7", session_id="ses_1",
    ) == declaration
    tool = _event({"type": "tool"})
    with pytest.raises(LifecycleValidationError, match="tool event"):
        assert_no_tool_events([tool])
    with pytest.raises(LifecycleValidationError, match="exactly one"):
        finalization_declaration(
            [_event({"text": "not a declaration"})],
            action_id=7, attempt_id="aqs:7", session_id="ses_1",
        )


def test_completion_indication_requires_bound_step_finish():
    unfinished = _event({"text": "working"})
    assert completion_indication([unfinished]) is None
    finished = _event({"type": "step-finish"}, event_type="step_finish", timestamp=2)
    assert completion_indication([unfinished, finished]) == "ses_1"
