from __future__ import annotations

import stat
import subprocess
from pathlib import Path

import pytest

from actionq.scope_iterate import (
    FakeCommitWorker, InvariantError, PathACL, PolicyError,
    ScopeIterateKernel, ScopeIteratePolicy, ScopeIterateRequest, ToolACL,
    load_policy, validate_changed_paths,
)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args), cwd=repo, check=True, text=True, capture_output=True
    ).stdout.strip()


def repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.invalid")
    (repo / "README.md").write_text("# demo\n")
    git(repo, "add", "README.md")
    git(repo, "commit", "-m", "initial")
    return repo


def policy(tmp_path: Path, *, fake_path: str = "docs/smoke.md") -> ScopeIteratePolicy:
    prompt = tmp_path / "prompt.md"
    prompt.write_text(
        "{action_json}\n{sprint_item_json}\n{working_dir}\n{branch_name}\n"
        "{allowed_scope}\n{test_command}\n"
    )
    return ScopeIteratePolicy(
        worktree_root=(tmp_path / "worktrees").resolve(),
        prompt_template=prompt.resolve(),
        path_acl=PathACL(
            ("**",),
            (".git/**", ".sprintctl/**", "secrets/**", "**/.env", "**/.env.*"),
        ),
        tool_acl=ToolACL(),
        test_command=("python3", "-c", "pass"),
        fake_commit_path=fake_path,
    )


def request(repo: Path) -> ScopeIterateRequest:
    return ScopeIterateRequest(
        action_id=11, project="demo", repository=repo.resolve(),
        action={"id": 11, "action_type": "scope-iterate"},
        sprint_item={"item": {"id": 42, "status": "pending"}},
    )


def test_fake_kernel_prepares_isolated_worktree_and_passes_all_post_invariants(tmp_path):
    repo = repository(tmp_path)
    kernel = ScopeIterateKernel()
    prepared = kernel.prepare(request(repo), policy(tmp_path))

    assert git(prepared.worktree, "remote") == ""
    assert (prepared.worktree / ".git").is_dir()
    for path in (prepared.worktree, *prepared.worktree.rglob("*")):
        if not path.is_symlink():
            assert path.stat().st_mode & stat.S_IWGRP

    result = kernel.run(prepared, FakeCommitWorker())

    assert result.branch == "agent/scope-iterate/11"
    assert result.changed_paths == ("docs/smoke.md",)
    assert result.worktree.parent == (tmp_path / "worktrees" / "demo").resolve()
    assert "scope-iterate" in prepared.prompt
    assert result.head_sha != result.base_sha
    assert git(repo, "status", "--porcelain") == ""


def test_prepare_refuses_existing_branch_before_mutating_worktree(tmp_path):
    repo = repository(tmp_path)
    git(repo, "branch", "agent/scope-iterate/11")

    with pytest.raises(InvariantError, match="branch-absent"):
        ScopeIterateKernel().prepare(request(repo), policy(tmp_path))

    assert not (tmp_path / "worktrees").exists()


def test_verify_rejects_denied_committed_path(tmp_path):
    repo = repository(tmp_path)
    kernel = ScopeIterateKernel()
    prepared = kernel.prepare(
        request(repo), policy(tmp_path, fake_path=".sprintctl/state")
    )

    with pytest.raises(InvariantError, match="diff-scope-respected"):
        kernel.run(prepared, FakeCommitWorker())


def test_verify_rejects_dirty_worktree_before_tests(tmp_path):
    repo = repository(tmp_path)
    kernel = ScopeIterateKernel()
    prepared = kernel.prepare(request(repo), policy(tmp_path))
    assert FakeCommitWorker().run(prepared) == 0
    (prepared.worktree / "uncommitted").write_text("dirty")

    with pytest.raises(InvariantError, match="worktree-clean"):
        kernel.verify(prepared)


def test_path_acl_rejects_escape_and_requires_allow_match(tmp_path):
    acl = PathACL(
        ("src/**",),
        (
            ".git/**", ".sprintctl/**", "secrets/**", "**/.env", "**/.env.*",
            "src/private/**",
        ),
    )
    with pytest.raises(PolicyError, match="safe worktree-relative"):
        validate_changed_paths(["../outside"], worktree=tmp_path, acl=acl)
    with pytest.raises(InvariantError, match="outside ACL allowlist"):
        validate_changed_paths(["docs/readme.md"], worktree=tmp_path, acl=acl)
    with pytest.raises(InvariantError, match="denied by ACL"):
        validate_changed_paths(["src/private/key"], worktree=tmp_path, acl=acl)


def test_policy_requires_network_none_and_rejects_unknown_config(tmp_path):
    with pytest.raises(PolicyError, match="network policy"):
        ToolACL(network="full")
    with pytest.raises(PolicyError, match="unknown scope-iterate"):
        load_policy({"unexpected": True}, config_dir=tmp_path)


def test_load_policy_requires_explicit_production_boundaries(tmp_path):
    prompt = tmp_path / "prompt.md"
    prompt.write_text("{working_dir}")
    parsed = load_policy({
        "worktree_root": "state/worktrees",
        "prompt_template": "prompt.md",
        "path_allow": ["**"],
        "path_deny": [".git/**", ".sprintctl/**", "secrets/**", "**/.env", "**/.env.*"],
        "allowed_tools": ["read", "edit", "grep", "bash"],
        "bash_allow": ["git status", "git diff", "git add", "git commit", "pytest"],
        "bash_deny": [
            "git push", "git merge", "git reset", "ssh", "scp", "curl",
            "wget", "rm", "kubectl", "flux", "talosctl",
        ],
        "network": "none",
        "test_command": ["pytest", "-q"],
    }, config_dir=tmp_path)

    assert parsed.worktree_root == (tmp_path / "state/worktrees").resolve()
    assert parsed.test_command == ("pytest", "-q")


def test_invalid_prompt_is_rejected_but_artifact_is_preserved(tmp_path):
    repo = repository(tmp_path)
    configured = policy(tmp_path)
    configured.prompt_template.write_text("{unknown_placeholder}")

    with pytest.raises(PolicyError, match="invalid prompt"):
        ScopeIterateKernel().prepare(request(repo), configured)

    assert (tmp_path / "worktrees" / "demo" / "11").is_dir()
