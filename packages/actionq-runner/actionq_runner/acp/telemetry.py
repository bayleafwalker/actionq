"""ACP wire traffic -> normalized Vuoro execution telemetry.

A deliberate subset, not a mirror. Unmapped traffic becomes PROTOCOL_UNMAPPED rather than
being dropped: an agent that starts sending something new should be visible, not silent.
"""
from __future__ import annotations

from typing import Any, Iterable

from ..execution_contract import EventKind, ExecutionEvent

# session/update carries a discriminated union keyed by `sessionUpdate`.
_UPDATE_KINDS: dict[str, EventKind] = {
    "agent_message_chunk": EventKind.OUTPUT_DELTA,
    "agent_thought_chunk": EventKind.REASONING_DELTA,
    "user_message_chunk": EventKind.OUTPUT_DELTA,
    "tool_call": EventKind.TOOL_STARTED,
    "tool_call_update": EventKind.TOOL_UPDATED,
    "usage_update": EventKind.USAGE_REPORTED,
    "plan": EventKind.EXECUTION_OUTPUT,
    "available_commands_update": EventKind.EXECUTION_OUTPUT,
    "current_mode_update": EventKind.EXECUTION_OUTPUT,
}


def normalize_inbound(message: dict[str, Any]) -> Iterable[ExecutionEvent]:
    method = message.get("method")
    params = message.get("params") or {}

    if method != "session/update":
        return [
            ExecutionEvent(
                kind=EventKind.PROTOCOL_UNMAPPED,
                payload={"method": method},
                raw=message,
            )
        ]

    update = params.get("update") or {}
    discriminator = update.get("sessionUpdate")
    kind = _UPDATE_KINDS.get(discriminator)
    if kind is None:
        return [
            ExecutionEvent(
                kind=EventKind.PROTOCOL_UNMAPPED,
                payload={"method": method, "sessionUpdate": discriminator},
                raw=message,
            )
        ]

    payload: dict[str, Any] = {"session_id": params.get("sessionId")}
    if kind in (EventKind.OUTPUT_DELTA, EventKind.REASONING_DELTA):
        payload["text"] = _content_text(update.get("content"))
    elif kind in (EventKind.TOOL_STARTED, EventKind.TOOL_UPDATED):
        payload.update(
            {
                "tool_call_id": update.get("toolCallId"),
                "title": update.get("title"),
                "status": update.get("status"),
                "tool_kind": update.get("kind"),
            }
        )
    elif kind is EventKind.USAGE_REPORTED:
        payload["usage"] = {k: v for k, v in update.items() if k != "sessionUpdate"}
    else:
        payload["update"] = {k: v for k, v in update.items() if k != "sessionUpdate"}

    return [ExecutionEvent(kind=kind, payload=payload, raw=message)]


def _content_text(content: Any) -> str:
    if isinstance(content, dict):
        return content.get("text", "") if content.get("type") == "text" else ""
    if isinstance(content, list):
        return "".join(_content_text(part) for part in content)
    if isinstance(content, str):
        return content
    return ""
