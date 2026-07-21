"""Common harness adapter interface (work item #970).

An adapter's only job is to turn a ``HarnessInvocation`` into a concrete,
noninteractive subprocess command and run it with a bounded timeout,
returning captured output and a plain classification of what happened. It
never inspects the output for meaning (usage-limit detection lives in
``actionq.usage_limit``, work item #976) and never retries or falls back to
a different harness on its own.
"""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence


class HarnessUnsupportedError(RuntimeError):
    """Raised for a harness with no proven noninteractive invocation path."""


@dataclass(frozen=True)
class HarnessInvocation:
    """Everything an adapter needs to build and run one noninteractive turn."""

    prompt: str
    worktree: Path
    model: str | None = None
    timeout_seconds: float = 1800.0
    extra_env: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class HarnessResult:
    """The raw, uninterpreted outcome of one adapter invocation."""

    command: Sequence[str]
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool


class HarnessAdapter:
    """Base class for a noninteractive harness runner adapter."""

    name: str = "base"

    def __init__(self, bin_path: str | None = None):
        self.bin_path = bin_path or self.name

    def build_command(self, invocation: HarnessInvocation) -> list[str]:
        """Return the argv for one noninteractive invocation."""
        raise NotImplementedError

    def stdin_text(self, invocation: HarnessInvocation) -> str | None:
        """Text piped to the child's stdin, or ``None`` for no stdin at all."""
        return invocation.prompt

    def build_env(self, invocation: HarnessInvocation) -> dict[str, str]:
        env = dict(os.environ)
        env.update(invocation.extra_env)
        return env

    def invoke(self, invocation: HarnessInvocation) -> HarnessResult:
        """Run the adapter's command synchronously with a bounded timeout.

        Never raises for a well-formed command that simply fails or times
        out -- that is ordinary adapter output, classified by the caller.
        Only an ``OSError`` starting the process (missing binary, permission
        denied) propagates, since that is a caller/config mistake rather
        than harness output.
        """
        command = self.build_command(invocation)
        env = self.build_env(invocation)
        stdin_text = self.stdin_text(invocation)
        try:
            completed = subprocess.run(
                command,
                cwd=invocation.worktree,
                env=env,
                input=stdin_text,
                text=True,
                capture_output=True,
                timeout=invocation.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            return HarnessResult(
                command=command,
                exit_code=-1,
                stdout=exc.stdout or "",
                stderr=exc.stderr or "",
                timed_out=True,
            )
        return HarnessResult(
            command=command,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            timed_out=False,
        )
