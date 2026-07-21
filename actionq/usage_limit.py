"""Usage-limit pause/resume detection and handoff (work item #976).

Owner: actionq daemon and supported harness adapters. This module
recognizes *confirmed* harness usage-limit signals from already-captured
process output -- never by scraping a provider's private usage/billing
API (explicit non-scope) -- and produces a bounded, durable handoff
reference the daemon can point a `session.paused` event at.

Minimum viable mechanism (matches
``docs/plans/actionq-server-daemon-workstream-c-plan.md``'s "Pause/resume
answer"): no supported harness has a proven native pause/resume contract,
so a detected usage-limit signal is "checkpoint-and-fail," not a real
process suspend. The daemon fails the action, but records `session.paused`
with a handoff reference *before* the failure so an operator or a later
action can see this was a recoverable pause, not an ordinary failure, and
can re-dispatch from the handoff. Resume means re-dispatch: a new session
emits `session.resumed` referencing the old session id, never process
continuation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

_MAX_EVIDENCE_CHARS = 4000

# Confirmed-signal allowlist: only phrases each harness's own CLI is
# documented or observed to print for rate/usage limiting. A nonzero exit
# with none of these phrases present is an ordinary failure, not a pause --
# classification never guesses from exit code alone.
_SIGNALS: dict[str, tuple[str, ...]] = {
    "claude": (
        "rate limit",
        "rate_limit",
        "usage limit",
        "quota exceeded",
        "429 too many requests",
        "please try again later",
    ),
    "codex": (
        "rate limit",
        "rate_limit_exceeded",
        "usage limit",
        "429 too many requests",
        "too many requests",
    ),
    "opencode": ("rate limit", "usage limit", "429 too many requests", "quota exceeded"),
    "codestral": ("rate limit", "usage limit", "429 too many requests", "quota exceeded"),
}


@dataclass(frozen=True)
class UsageLimitSignal:
    detected: bool
    reason: str | None = None
    evidence: str | None = None


def _bounded_tail(text: str, limit: int = _MAX_EVIDENCE_CHARS) -> str:
    return text[-limit:] if len(text) > limit else text


def classify_usage_limit(
    harness: str | None,
    *,
    exit_code: int,
    output: str,
) -> UsageLimitSignal:
    """Classify one harness invocation's outcome as a confirmed usage-limit
    signal or an ordinary result.

    A zero exit code is never a usage-limit signal. A nonzero exit code is
    only classified as one when the captured output contains a known,
    harness-specific confirmed phrase; everything else is an ordinary
    failure so operators are never told a plain bug is a recoverable pause.
    """
    if exit_code == 0:
        return UsageLimitSignal(detected=False)
    phrases = _SIGNALS.get(harness or "", ())
    haystack = output.lower()
    for phrase in phrases:
        if phrase in haystack:
            return UsageLimitSignal(
                detected=True,
                reason=f"confirmed usage-limit signal matched: {phrase!r}",
                evidence=_bounded_tail(output),
            )
    return UsageLimitSignal(detected=False)


def write_handoff(
    handoff_dir: Path,
    *,
    session_id: str,
    action_id: int | None,
    action_type: str | None,
    harness: str | None,
    model: str | None,
    reason: str,
    evidence: str | None,
) -> Path:
    """Write a bounded, durable handoff/checkpoint markdown file.

    Bounded: evidence is already tail-truncated by ``classify_usage_limit``;
    this function does not further embed unbounded transcript content.
    Durable: written atomically (temp file + rename) under ``handoff_dir``,
    which survives the daemon process and is the re-dispatch reference an
    operator or a later action points at.
    """
    handoff_dir.mkdir(parents=True, exist_ok=True)
    safe_session_id = session_id.replace("/", "_").replace(":", "_")
    path = handoff_dir / f"{safe_session_id}.md"
    recorded_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    lines = [
        "# actionq usage-limit handoff",
        "",
        f"- session_id: {session_id}",
        f"- action_id: {action_id}",
        f"- action_type: {action_type}",
        f"- harness: {harness}",
        f"- model: {model or ''}",
        f"- recorded_at: {recorded_at}",
        f"- reason: {reason}",
        "",
        "## evidence (bounded tail of captured output)",
        "",
        "```",
        evidence or "(no evidence captured)",
        "```",
        "",
        "Resume means re-dispatch, not process continuation: start a new",
        "action referencing this handoff and emit `session.resumed` with",
        "`resumed_from_session_id` set to the session id above.",
        "",
    ]
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(lines), encoding="utf-8")
    tmp.replace(path)
    return path
