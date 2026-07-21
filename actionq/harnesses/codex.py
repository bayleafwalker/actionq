"""Codex harness adapter (work item #970).

Command shape verified against the installed ``codex`` CLI's own
``--help`` output in this environment (``codex exec --help``): ``codex exec``
is the documented noninteractive subcommand; passing ``-`` as the prompt
argument makes it read instructions from stdin (also the default when no
prompt argument is given), matching the stdin-prompt convention already
used by the Claude adapter. ``--skip-git-repo-check`` lets it run against a
disposable worktree that might not itself be the invocation cwd's git root;
``--sandbox`` defaults to the safer ``workspace-write`` rather than
``danger-full-access``.
"""
from __future__ import annotations

from .base import HarnessAdapter, HarnessInvocation

_SANDBOX_MODES = ("read-only", "workspace-write", "danger-full-access")


class CodexAdapter(HarnessAdapter):
    name = "codex"

    def __init__(self, bin_path: str | None = None, *, sandbox: str = "workspace-write"):
        super().__init__(bin_path or "codex")
        if sandbox not in _SANDBOX_MODES:
            raise ValueError(f"sandbox must be one of {_SANDBOX_MODES}, got {sandbox!r}")
        self.sandbox = sandbox

    def build_command(self, invocation: HarnessInvocation) -> list[str]:
        command = [
            self.bin_path,
            "exec",
            "--skip-git-repo-check",
            "--json",
            "--sandbox",
            self.sandbox,
            "-C",
            str(invocation.worktree),
        ]
        if invocation.model:
            command.extend(["--model", invocation.model])
        # Explicit "-" (rather than relying on the no-argument default) so
        # the command is unambiguous even if a future codex version changes
        # its no-argument behavior.
        command.append("-")
        return command
