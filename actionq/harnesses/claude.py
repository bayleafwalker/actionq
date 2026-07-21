"""Claude harness adapter (work item #970).

Invocation is ported from the proven ``ClaudeWorker`` in
``actionq-dispatch/actionq_dispatcher/worker.py`` (ownership: that repo is
the compatibility surface; this module is the equivalent for the
``actionq``-owned daemon). Behavior is preserved deliberately -- flags,
prompt-on-stdin, and ACL tool args -- rather than reinvented.
"""
from __future__ import annotations

from .base import HarnessAdapter, HarnessInvocation


class ClaudeAdapter(HarnessAdapter):
    name = "claude"

    def __init__(
        self,
        bin_path: str | None = None,
        *,
        allowed_tools: tuple[str, ...] = (),
        disallowed_tools: tuple[str, ...] = (),
    ):
        super().__init__(bin_path or "claude")
        self.allowed_tools = allowed_tools
        self.disallowed_tools = disallowed_tools

    def build_command(self, invocation: HarnessInvocation) -> list[str]:
        command = [
            self.bin_path,
            "-p",
            "--output-format",
            "json",
            "--no-session-persistence",
            "--add-dir",
            str(invocation.worktree),
        ]
        if invocation.model:
            command.extend(["--model", invocation.model])
        if self.allowed_tools:
            command.extend(["--allowedTools", ",".join(self.allowed_tools)])
        if self.disallowed_tools:
            command.extend(["--disallowedTools", ",".join(self.disallowed_tools)])
        return command
