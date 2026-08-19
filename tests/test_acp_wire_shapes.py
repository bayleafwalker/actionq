"""Replay captured OpenCode wire traffic through the adapter's normalization.

Two layers, deliberately separate:

* ``docs/evidence/opencode-1.18.18/*.raw.jsonl`` answers *what did OpenCode actually do?*
  It is preserved verbatim and is never asserted against field-by-field.
* The assertions below answer *what contract does ActionQ depend on?* They are semantic:
  identity is retained, lifecycle is coherent, completion is observable.

Asserting OpenCode's exact transcript would quietly turn a protocol adapter into an
OpenCode-1.18.18 transcript parser. ACP leaves most update fields optional, so a second
agent may legitimately send far less.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from actionq_runner.acp.telemetry import normalize_inbound
from actionq_runner.execution_contract import EventKind, ToolCall, ToolStatus

EVIDENCE = Path(__file__).resolve().parents[1] / "docs" / "evidence" / "opencode-1.18.18"
CAPTURES = sorted(EVIDENCE.glob("*.raw.jsonl"))


def _messages(path: Path) -> list[dict]:
    out = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("dir") == "recv" and isinstance(entry.get("msg"), dict):
            out.append(entry["msg"])
    return out


def _replay(path: Path) -> tuple[dict[str, ToolCall], list, str | None]:
    calls: dict[str, ToolCall] = {}
    events = []
    stop_reason = None
    for message in _messages(path):
        result = message.get("result")
        if isinstance(result, dict) and "stopReason" in result:
            stop_reason = result["stopReason"]
        if message.get("method") != "session/update":
            continue
        for event in normalize_inbound(message):
            events.append(event)
            if event.kind in (EventKind.TOOL_STARTED, EventKind.TOOL_UPDATED):
                cid = event.payload["tool_call_id"]
                call = calls.setdefault(cid, ToolCall(tool_call_id=cid))
                call.apply(
                    kind=event.payload.get("tool_kind"),
                    title=event.payload.get("title"),
                    status=event.payload.get("status"),
                )
    return calls, events, stop_reason


def test_evidence_is_present() -> None:
    assert CAPTURES, "captured wire evidence is missing"


@pytest.mark.parametrize("path", CAPTURES, ids=lambda p: p.name)
def test_no_captured_traffic_is_unmapped(path: Path) -> None:
    """The codec must recognise everything a real harness sent."""
    _, events, _ = _replay(path)
    unmapped = [e.payload for e in events if e.kind is EventKind.PROTOCOL_UNMAPPED]
    assert not unmapped, f"{path.name} contains traffic the codec does not map: {unmapped}"


@pytest.mark.parametrize("path", CAPTURES, ids=lambda p: p.name)
def test_tool_identity_is_retained_across_updates(path: Path) -> None:
    calls, events, _ = _replay(path)
    seen = {e.payload["tool_call_id"] for e in events
            if e.kind in (EventKind.TOOL_STARTED, EventKind.TOOL_UPDATED)}
    assert seen == set(calls), "every update resolved to a tracked call"
    assert all(cid for cid in seen), "no update arrived without identity"


@pytest.mark.parametrize("path", CAPTURES, ids=lambda p: p.name)
def test_every_tool_reaches_an_observable_terminal_state(path: Path) -> None:
    calls, _, _ = _replay(path)
    assert calls, f"{path.name} recorded no tool calls"
    unfinished = [c.tool_call_id for c in calls.values() if not c.terminal]
    assert not unfinished, f"tool calls never reached a terminal status: {unfinished}"


@pytest.mark.parametrize("path", CAPTURES, ids=lambda p: p.name)
def test_lifecycle_is_internally_coherent(path: Path) -> None:
    """Starts pending, ends terminal, and nothing follows the terminal status."""
    calls, _, _ = _replay(path)
    for call in calls.values():
        history = call.status_history
        assert history, f"{call.tool_call_id} reported no status at all"
        assert history[0] is ToolStatus.PENDING
        assert history[-1].terminal
        assert not any(s.terminal for s in history[:-1]), "status changed after terminal"


def test_title_is_not_identity() -> None:
    """Observed: OpenCode sends 'read' then 'data.txt' for the same call.

    Correlating on title would silently mis-attribute work to the wrong invocation.
    """
    calls, _, _ = _replay(EVIDENCE / "read.raw.jsonl")
    call = next(iter(calls.values()))
    titles = [t for t in (call.title,) if t]
    raw_titles = []
    for message in _messages(EVIDENCE / "read.raw.jsonl"):
        if message.get("method") == "session/update":
            update = message["params"]["update"]
            if update.get("toolCallId") == call.tool_call_id and update.get("title"):
                raw_titles.append(update["title"])
    assert len(set(raw_titles)) > 1, "expected the observed title mutation"
    assert titles, "the tracked call still carries a title"


def test_observed_tool_kinds() -> None:
    """Recorded, not asserted as exhaustive -- ACP does not fix the set."""
    kinds = set()
    for path in CAPTURES:
        calls, _, _ = _replay(path)
        kinds |= {c.kind for c in calls.values() if c.kind}
    assert {"read", "edit", "execute"} <= kinds, f"missing an induced kind: {kinds}"


def test_rejected_permission_fails_the_tool_not_the_turn() -> None:
    """A refused tool ends `failed` while the prompt still completes normally.

    Worth pinning: a harness that instead aborted the turn would change how ActionQ must
    interpret a policy denial.
    """
    calls, _, stop_reason = _replay(EVIDENCE / "permission-reject.raw.jsonl")
    assert stop_reason == "end_turn"
    assert any(c.status is ToolStatus.FAILED for c in calls.values())


def test_cancelled_turn_reports_cancelled_stop_reason() -> None:
    calls, _, stop_reason = _replay(EVIDENCE / "permission-cancel.raw.jsonl")
    assert stop_reason == "cancelled"
    assert all(c.terminal for c in calls.values()), "tools still resolved after cancellation"
