"""OpenCode / Codestral harness adapter (work item #970).

Command shape follows the published OpenCode CLI's noninteractive
convention: ``opencode run <message> --model <provider/model>`` prints the
agent's final response and exits, taking the prompt as a positional
argument rather than on stdin.

Verification caveat (residual risk, call out explicitly to the operator
before trusting this adapter for a real dispatch): the ``opencode`` binary
installed in this sandbox resolves to a bundled Bun runtime shim rather
than a working OpenCode CLI (``opencode --help`` prints Bun's own help, not
OpenCode's), so this adapter's command shape could not be confirmed
against a locally *running* CLI the way the Claude adapter's shape was
ported from proven code and the Codex adapter's shape was confirmed
against ``codex exec --help`` in this same environment. Do not treat an
OpenCode/Codestral dispatch as smoke-verified until one real disposable
invocation against a genuinely working ``opencode`` install has been run
and its receipt recorded.
"""
from __future__ import annotations

from .base import HarnessAdapter, HarnessInvocation


class OpenCodeAdapter(HarnessAdapter):
    name = "opencode"

    def __init__(self, bin_path: str | None = None):
        super().__init__(bin_path or "opencode")

    def build_command(self, invocation: HarnessInvocation) -> list[str]:
        command = [self.bin_path, "run"]
        if invocation.model:
            command.extend(["--model", invocation.model])
        command.append(invocation.prompt)
        return command

    def stdin_text(self, invocation: HarnessInvocation) -> str | None:
        # The prompt travels as a positional argument for this adapter, not
        # on stdin -- see the verification caveat in the module docstring.
        return None
