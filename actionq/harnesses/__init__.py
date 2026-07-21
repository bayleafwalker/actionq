"""Multi-harness runner adapters (work item #970).

Owner: actionq harness layer. This package defines one adapter interface
(``HarnessAdapter`` in ``actionq.harnesses.base``) and implements it
incrementally per harness:

- ``claude``   -- ported from the proven invocation in
  ``actionq-dispatch/actionq_dispatcher/worker.py::ClaudeWorker``.
- ``codex``    -- noninteractive ``codex exec`` invocation.
- ``opencode`` / ``codestral`` -- noninteractive ``opencode run`` invocation.
- ``copilot-cli`` -- deliberately unsupported. ``gh copilot`` has no proven
  noninteractive auth path in this environment; ``get_adapter`` raises
  ``HarnessUnsupportedError`` for it rather than silently falling back to
  another harness or guessing at an interactive flow.

Non-scope (per work item #970): scraping private usage APIs, silent
fallback to a different harness when the requested one is unavailable, and
wiring these adapters into a worktree/prompt orchestration -- the daemon
does not yet own worktree creation or prompt construction (see
``docs/plans/actionq-server-daemon-workstream-c-plan.md``, steps 7-9), so
this package only defines and tests the adapters themselves.
"""
from __future__ import annotations

from .base import HarnessAdapter, HarnessInvocation, HarnessResult, HarnessUnsupportedError
from .claude import ClaudeAdapter
from .codex import CodexAdapter
from .copilot import CopilotAdapter
from .opencode import OpenCodeAdapter

_ADAPTERS: dict[str, type[HarnessAdapter]] = {
    "claude": ClaudeAdapter,
    "codex": CodexAdapter,
    "opencode": OpenCodeAdapter,
    "codestral": OpenCodeAdapter,
    "copilot-cli": CopilotAdapter,
}


def supported_harnesses() -> list[str]:
    """Harness names with a real, invocable adapter (excludes ``copilot-cli``)."""
    return sorted(name for name, cls in _ADAPTERS.items() if cls is not CopilotAdapter)


def get_adapter(name: str, **kwargs) -> HarnessAdapter:
    """Resolve a harness name to a constructed adapter.

    Raises ``HarnessUnsupportedError`` for names with no proven noninteractive
    adapter (currently only ``copilot-cli``) and ``KeyError`` for names this
    module does not know about at all -- these are deliberately different
    exceptions so callers cannot silently treat "not yet proven" the same as
    "not routable."
    """
    try:
        adapter_cls = _ADAPTERS[name]
    except KeyError as exc:
        raise KeyError(f"no harness adapter registered for {name!r}") from exc
    if adapter_cls is CopilotAdapter:
        raise HarnessUnsupportedError(
            f"harness {name!r} has no proven noninteractive auth path in this environment"
        )
    return adapter_cls(**kwargs)


__all__ = [
    "HarnessAdapter",
    "HarnessInvocation",
    "HarnessResult",
    "HarnessUnsupportedError",
    "ClaudeAdapter",
    "CodexAdapter",
    "OpenCodeAdapter",
    "CopilotAdapter",
    "get_adapter",
    "supported_harnesses",
]
