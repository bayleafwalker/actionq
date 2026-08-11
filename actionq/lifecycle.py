"""Bounded, controller-owned dispatch lifecycle mechanics.

The profile in this module is deliberately opt-in.  Existing action classes
continue to use the historical one-child lifecycle; a configured profile must
prove all of the invariants needed by the OpenCode continuation path before a
child is started.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Iterable


FINALIZER_AGENT = "ao-finalizer"
DEFAULT_WORK_SECONDS = 20 * 60
DEFAULT_FINALIZATION_SECONDS = 2 * 60
DEFAULT_TOTAL_SECONDS = 25 * 60
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

NO_TOOLS_OPENCODE_CONFIG = json.dumps({
    "agent": {
        FINALIZER_AGENT: {
            "mode": "primary",
            "permission": {
                "*": "deny", "read": "deny", "glob": "deny", "grep": "deny",
                "list": "deny", "todowrite": "deny", "todoread": "deny",
                "edit": "deny", "write": "deny", "patch": "deny", "bash": "deny",
                "task": "deny", "external_directory": "deny", "webfetch": "deny",
                "websearch": "deny",
            },
        }
    }
}, sort_keys=True, separators=(",", ":"))


class LifecycleValidationError(ValueError):
    """An untrusted lifecycle profile, event stream, or declaration."""


@dataclass(frozen=True)
class BoundedLifecycleProfile:
    """The only lifecycle profile currently qualified by ActionQ.

    The ceilings are fixed by the governing plan.  Keeping them in a typed
    profile makes the phase budget visible in events and prevents a config
    typo from silently widening the worker's authority or spend.
    """

    profile_id: str = "opencode-verified"
    enabled: bool = True
    harness: str = "opencode"
    work_timeout_seconds: float = DEFAULT_WORK_SECONDS
    finalization_timeout_seconds: float = DEFAULT_FINALIZATION_SECONDS
    total_timeout_seconds: float = DEFAULT_TOTAL_SECONDS
    finalizer_agent: str = FINALIZER_AGENT
    finalizer_tools: tuple[str, ...] = ()

    def validate(self) -> "BoundedLifecycleProfile":
        if not self.enabled:
            raise LifecycleValidationError("bounded lifecycle profile is disabled")
        if self.harness != "opencode":
            raise LifecycleValidationError("bounded lifecycle requires the qualified OpenCode harness")
        if self.finalizer_agent != FINALIZER_AGENT:
            raise LifecycleValidationError("bounded lifecycle requires the ao-finalizer agent")
        if self.finalizer_tools:
            raise LifecycleValidationError("bounded lifecycle finalizer must have no tools")
        if self.work_timeout_seconds != DEFAULT_WORK_SECONDS:
            raise LifecycleValidationError("bounded lifecycle work ceiling must be 20 minutes")
        if self.finalization_timeout_seconds != DEFAULT_FINALIZATION_SECONDS:
            raise LifecycleValidationError("bounded lifecycle finalization ceiling must be 2 minutes")
        if self.total_timeout_seconds != DEFAULT_TOTAL_SECONDS:
            raise LifecycleValidationError("bounded lifecycle total ceiling must be 25 minutes")
        if self.total_timeout_seconds < self.work_timeout_seconds:
            raise LifecycleValidationError("total ceiling must contain the work ceiling")
        return self

    @classmethod
    def from_config(cls, profile_id: str, raw: dict[str, Any]) -> "BoundedLifecycleProfile":
        if not isinstance(raw, dict):
            raise LifecycleValidationError(f"lifecycle profile {profile_id!r} must be an object")
        tools = raw.get("finalizer_tools", raw.get("tools", ()))
        if not isinstance(tools, (list, tuple)):
            raise LifecycleValidationError("lifecycle finalizer tools must be a list")
        return cls(
            profile_id=profile_id,
            enabled=bool(raw.get("enabled", True)),
            harness=str(raw.get("harness", "opencode")),
            work_timeout_seconds=float(raw.get("work_timeout_seconds", DEFAULT_WORK_SECONDS)),
            finalization_timeout_seconds=float(raw.get("finalization_timeout_seconds", DEFAULT_FINALIZATION_SECONDS)),
            total_timeout_seconds=float(raw.get("total_timeout_seconds", DEFAULT_TOTAL_SECONDS)),
            finalizer_agent=str(raw.get("finalizer_agent", FINALIZER_AGENT)),
            finalizer_tools=tuple(str(value) for value in tools),
        ).validate()


def parse_json_events(text: str) -> list[dict[str, Any]]:
    """Parse a bounded OpenCode JSON event stream without accepting prose."""
    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise LifecycleValidationError(f"OpenCode event line {line_number} is not JSON") from exc
        if not isinstance(value, dict) or not isinstance(value.get("type"), str):
            raise LifecycleValidationError(f"OpenCode event line {line_number} is not an event object")
        if _error_event(value):
            raise LifecycleValidationError(f"OpenCode event line {line_number} reports an error")
        events.append(value)
    if not events:
        raise LifecycleValidationError("OpenCode produced no JSON events")
    return events


def _error_event(event: dict[str, Any]) -> bool:
    event_type = str(event.get("type", "")).lower().replace("_", "-")
    if event_type == "error" or event_type.endswith(".error") or event_type.endswith("-error"):
        return True
    properties = event.get("properties")
    if not isinstance(properties, dict):
        return False
    if properties.get("error") or properties.get("errors"):
        return True
    for value in _walk_objects(properties):
        for key in ("error", "errors"):
            if value.get(key):
                return True
        status = value.get("status")
        if isinstance(status, dict) and str(status.get("type", "")).lower() in {"error", "failed", "failure"}:
            return True
    return False


def _session_ids(events: Iterable[dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    for event in events:
        properties = event.get("properties")
        if not isinstance(properties, dict):
            raise LifecycleValidationError("OpenCode event has no properties envelope")
        session_id = properties.get("sessionID")
        if not isinstance(session_id, str) or not SESSION_ID_RE.fullmatch(session_id):
            raise LifecycleValidationError("OpenCode event has no valid properties.sessionID")
        result.add(session_id)
    return result


def session_id_from_events(events: Iterable[dict[str, Any]], *, expected: str | None = None) -> str:
    ids = _session_ids(events)
    if len(ids) != 1:
        raise LifecycleValidationError("OpenCode session identity changed during a lifecycle phase")
    session_id = next(iter(ids))
    if expected is not None and session_id != expected:
        raise LifecycleValidationError("OpenCode finalizer did not continue the work session")
    return session_id


def _tool_event(event: dict[str, Any]) -> bool:
    event_type = str(event.get("type", "")).lower()
    properties = event.get("properties")
    if not isinstance(properties, dict):
        return False
    part = properties.get("part")
    info = properties.get("info")
    values = [event_type, str(properties.get("role", "")).lower()]
    if isinstance(part, dict):
        values.append(str(part.get("type", "")).lower())
    if isinstance(info, dict):
        values.append(str(info.get("role", "")).lower())
    return any(value == "tool" or value.startswith("tool-") for value in values)


def assert_no_tool_events(events: Iterable[dict[str, Any]]) -> None:
    if any(_tool_event(event) for event in events):
        raise LifecycleValidationError("OpenCode finalizer emitted a tool event")


def _walk_objects(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_objects(child)


def finalization_declaration(
    events: Iterable[dict[str, Any]], *, action_id: int, attempt_id: str, session_id: str,
) -> dict[str, Any]:
    """Extract and validate the finalizer's explicit terminal declaration.

    The declaration is intentionally small and non-authoritative.  The
    controller still creates the immutable dispatch result and asks ActionQ to
    settle it.  A JSON event, exit code, or prose without this declaration can
    never authorize success.
    """
    event_list = list(events)
    session_id_from_events(event_list, expected=session_id)
    assert_no_tool_events(event_list)
    candidates: list[dict[str, Any]] = []
    for event in event_list:
        if event.get("type") == "dispatch.finalization" and isinstance(event.get("declaration"), dict):
            candidates.append(event["declaration"])
            continue
        if isinstance(event.get("finalization"), dict):
            candidates.append(event["finalization"])
            continue
        for value in _walk_objects(event):
            if value.get("contract_id") == "dispatch-finalization/v1":
                candidates.append(value)
    if len(candidates) != 1:
        raise LifecycleValidationError("finalizer must emit exactly one dispatch-finalization/v1 declaration")
    declaration = candidates[0]
    allowed = {"contract_id", "action_id", "attempt_id", "session_id", "terminal_status", "summary"}
    if set(declaration) != allowed:
        raise LifecycleValidationError("finalizer declaration has an unexpected or missing field")
    if declaration["contract_id"] != "dispatch-finalization/v1":
        raise LifecycleValidationError("unsupported finalizer declaration contract")
    if declaration["action_id"] != action_id or declaration["attempt_id"] != attempt_id:
        raise LifecycleValidationError("finalizer declaration is not bound to the live action attempt")
    if declaration["session_id"] != session_id:
        raise LifecycleValidationError("finalizer declaration is not bound to the OpenCode session")
    if declaration["terminal_status"] not in {"completed", "no_change", "blocked", "failed", "budget_exhausted"}:
        raise LifecycleValidationError("finalizer declaration has an unsupported terminal status")
    if not isinstance(declaration["summary"], str) or not declaration["summary"].strip():
        raise LifecycleValidationError("finalizer declaration requires a non-empty summary")
    return declaration
