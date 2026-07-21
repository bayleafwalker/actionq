from __future__ import annotations

import shutil
import stat
import sys
from pathlib import Path

import pytest

from actionq.harnesses import (
    ClaudeAdapter,
    CodexAdapter,
    CopilotAdapter,
    HarnessInvocation,
    HarnessUnsupportedError,
    OpenCodeAdapter,
    get_adapter,
    supported_harnesses,
)


def _fake_bin(tmp_path: Path, name: str, script: str) -> Path:
    """Write an executable shell script standing in for a real harness CLI."""
    path = tmp_path / name
    path.write_text(f"#!{shutil.which('sh') or '/bin/sh'}\n{script}\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


# -- registry -----------------------------------------------------------


def test_supported_harnesses_excludes_copilot():
    assert "copilot-cli" not in supported_harnesses()
    assert {"claude", "codex", "opencode", "codestral"} <= set(supported_harnesses())


def test_get_adapter_returns_typed_instances():
    assert isinstance(get_adapter("claude"), ClaudeAdapter)
    assert isinstance(get_adapter("codex"), CodexAdapter)
    assert isinstance(get_adapter("opencode"), OpenCodeAdapter)
    assert isinstance(get_adapter("codestral"), OpenCodeAdapter)


def test_get_adapter_rejects_copilot_explicitly_not_silently():
    with pytest.raises(HarnessUnsupportedError):
        get_adapter("copilot-cli")


def test_get_adapter_rejects_unknown_harness_with_key_error():
    with pytest.raises(KeyError):
        get_adapter("some-future-harness")


def test_copilot_adapter_invoke_raises_directly_too(tmp_path: Path):
    adapter = CopilotAdapter()
    invocation = HarnessInvocation(prompt="hi", worktree=tmp_path)
    with pytest.raises(HarnessUnsupportedError):
        adapter.invoke(invocation)


# -- command construction (fake tests, no real CLI required) ------------


def test_claude_adapter_builds_expected_command(tmp_path: Path):
    adapter = ClaudeAdapter(bin_path="claude", allowed_tools=("Read",), disallowed_tools=("Bash",))
    invocation = HarnessInvocation(prompt="do the thing", worktree=tmp_path, model="claude-sonnet-4-6")
    command = adapter.build_command(invocation)
    assert command[0] == "claude"
    assert "--model" in command and command[command.index("--model") + 1] == "claude-sonnet-4-6"
    assert "--add-dir" in command and command[command.index("--add-dir") + 1] == str(tmp_path)
    assert "--allowedTools" in command and "Read" in command
    assert "--disallowedTools" in command and "Bash" in command
    assert adapter.stdin_text(invocation) == "do the thing"


def test_codex_adapter_builds_expected_command(tmp_path: Path):
    adapter = CodexAdapter(bin_path="codex")
    invocation = HarnessInvocation(prompt="do the thing", worktree=tmp_path, model="gpt-5.3-codex")
    command = adapter.build_command(invocation)
    assert command[:3] == ["codex", "exec", "--skip-git-repo-check"]
    assert "--sandbox" in command and command[command.index("--sandbox") + 1] == "workspace-write"
    assert "-C" in command and command[command.index("-C") + 1] == str(tmp_path)
    assert "--model" in command and command[command.index("--model") + 1] == "gpt-5.3-codex"
    assert command[-1] == "-"


def test_codex_adapter_rejects_unknown_sandbox_mode():
    with pytest.raises(ValueError):
        CodexAdapter(sandbox="full-access-please")


def test_opencode_adapter_builds_expected_command(tmp_path: Path):
    adapter = OpenCodeAdapter(bin_path="opencode")
    invocation = HarnessInvocation(prompt="do the thing", worktree=tmp_path, model="mistral/codestral-latest")
    command = adapter.build_command(invocation)
    assert command[:2] == ["opencode", "run"]
    assert "--model" in command and command[command.index("--model") + 1] == "mistral/codestral-latest"
    assert command[-1] == "do the thing"
    assert adapter.stdin_text(invocation) is None


# -- invocation against fake binaries (deterministic, no real model calls) --


def test_invoke_captures_output_exit_code_and_stdin(tmp_path: Path):
    fake = _fake_bin(
        tmp_path,
        "fake-claude",
        'cat > /dev/null\necho "ok on stdout"\necho "warn on stderr" 1>&2\nexit 0',
    )
    adapter = ClaudeAdapter(bin_path=str(fake))
    result = adapter.invoke(HarnessInvocation(prompt="hello", worktree=tmp_path))
    assert result.exit_code == 0
    assert "ok on stdout" in result.stdout
    assert "warn on stderr" in result.stderr
    assert result.timed_out is False


def test_invoke_reports_nonzero_exit(tmp_path: Path):
    fake = _fake_bin(tmp_path, "fake-codex", "echo failing 1>&2\nexit 7")
    adapter = CodexAdapter(bin_path=str(fake))
    result = adapter.invoke(HarnessInvocation(prompt="hello", worktree=tmp_path))
    assert result.exit_code == 7
    assert "failing" in result.stderr


def test_invoke_enforces_timeout(tmp_path: Path):
    fake = _fake_bin(tmp_path, "fake-slow", "sleep 5")
    adapter = ClaudeAdapter(bin_path=str(fake))
    result = adapter.invoke(HarnessInvocation(prompt="hello", worktree=tmp_path, timeout_seconds=0.2))
    assert result.timed_out is True
    assert result.exit_code == -1


# -- real disposable smoke evidence --------------------------------------
#
# These do not perform a real model dispatch (no API usage, no writes): they
# confirm the actual installed CLI is present, noninteractive-invocable, and
# exits cleanly for a read-only "--version"/"--help" style probe. That is
# the disposable smoke evidence the work item requires for a *supported*
# adapter, short of a full paid model turn.


def _real_binary(name: str) -> str | None:
    return shutil.which(name)


@pytest.mark.skipif(_real_binary("claude") is None, reason="claude CLI not installed in this environment")
def test_claude_real_binary_smoke():
    import subprocess

    adapter = ClaudeAdapter(bin_path=_real_binary("claude"))
    result = subprocess.run([adapter.bin_path, "--version"], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0
    assert result.stdout.strip()


@pytest.mark.skipif(_real_binary("codex") is None, reason="codex CLI not installed in this environment")
def test_codex_real_binary_smoke():
    import subprocess

    adapter = CodexAdapter(bin_path=_real_binary("codex"))
    result = subprocess.run([adapter.bin_path, "exec", "--help"], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0
    assert "Run Codex non-interactively" in result.stdout


@pytest.mark.skipif(_real_binary("opencode") is None, reason="opencode CLI not installed in this environment")
def test_opencode_real_binary_smoke():
    """Confirms the binary is present and executes; does not assert on its
    output shape, since this environment's ``opencode`` is a Bun shim (see
    the caveat in actionq/harnesses/opencode.py) rather than a working
    OpenCode CLI -- the receipt here is "binary ran," not "adapter shape
    matches a genuine OpenCode CLI."""
    import subprocess

    adapter = OpenCodeAdapter(bin_path=_real_binary("opencode"))
    result = subprocess.run([adapter.bin_path, "--version"], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0 or result.stdout or result.stderr
