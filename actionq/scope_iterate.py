"""Production-safe execution kernel for ``scope-iterate`` actions.

This module deliberately has no queue, sprint, or daemon dependencies.  The
coordinator owns claims and settlement; this kernel owns the isolated Git
artifact and the invariants that make it safe to hand to a worker.
"""
from __future__ import annotations

import fnmatch
import grp
import json
import os
import re
import shlex
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


class ScopeIterateError(RuntimeError):
    """Base error for a rejected scope-iterate execution."""


class PolicyError(ScopeIterateError):
    """The configured policy is unsafe or malformed."""


class InvariantError(ScopeIterateError):
    """A mandatory pre- or post-execution invariant failed."""

    def __init__(self, invariant: str, detail: str) -> None:
        super().__init__(f"{invariant}: {detail}")
        self.invariant = invariant
        self.detail = detail


@dataclass(frozen=True)
class PathACL:
    allow: tuple[str, ...]
    deny: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.allow:
            raise PolicyError("path ACL requires at least one allow pattern")
        required = {".git/**", ".sprintctl/**", "secrets/**", "**/.env", "**/.env.*"}
        missing = required - set(self.deny)
        if missing:
            raise PolicyError(f"path ACL is missing mandatory deny patterns: {sorted(missing)}")
        for pattern in (*self.allow, *self.deny):
            if not pattern or "\x00" in pattern:
                raise PolicyError("path ACL patterns must be non-empty text")
            candidate = Path(pattern)
            if candidate.is_absolute() or ".." in candidate.parts:
                raise PolicyError(f"path ACL pattern must be worktree-relative: {pattern}")


@dataclass(frozen=True)
class ToolACL:
    allowed_tools: tuple[str, ...] = ("read", "edit", "grep", "bash")
    bash_allow: tuple[str, ...] = (
        "git status", "git diff", "git add", "git commit", "pytest",
        "python", "python3", "uv run", "ruff",
    )
    bash_deny: tuple[str, ...] = (
        "git push", "git merge", "git reset", "ssh", "scp", "curl",
        "wget", "rm", "kubectl", "flux", "talosctl",
    )
    network: str = "none"

    def __post_init__(self) -> None:
        if self.network != "none":
            raise PolicyError("scope-iterate network policy must be 'none'")
        if not self.allowed_tools or not self.bash_allow or not self.bash_deny:
            raise PolicyError("tool and command ACLs must be explicit and non-empty")
        required = {
            "git push", "git merge", "git reset", "ssh", "scp", "curl",
            "wget", "rm", "kubectl", "flux", "talosctl",
        }
        missing = required - set(self.bash_deny)
        if missing:
            raise PolicyError(f"command ACL is missing mandatory denies: {sorted(missing)}")


@dataclass(frozen=True)
class ScopeIteratePolicy:
    worktree_root: Path
    prompt_template: Path
    path_acl: PathACL
    tool_acl: ToolACL = ToolACL()
    test_command: tuple[str, ...] = ("pytest",)
    branch_prefix: str = "agent/scope-iterate"
    fake_commit_path: str = "docs/actionq-dispatch-smoke.md"
    shared_group: str | None = None

    def __post_init__(self) -> None:
        if not self.worktree_root.is_absolute():
            raise PolicyError("worktree_root must be absolute")
        if not self.prompt_template.is_absolute():
            raise PolicyError("prompt_template must be absolute")
        if not self.test_command or any(not part for part in self.test_command):
            raise PolicyError("test_command must be a non-empty argv sequence")
        _validate_relative_path(self.fake_commit_path, field="fake_commit_path")
        if not self.branch_prefix or self.branch_prefix.startswith(("/", "-")):
            raise PolicyError("branch_prefix must be a safe relative Git ref prefix")
        if self.shared_group is not None and not re.fullmatch(
            r"[a-z_][a-z0-9_-]*[$]?", self.shared_group
        ):
            raise PolicyError("shared_group must be a safe local group name")


@dataclass(frozen=True)
class ScopeIterateRequest:
    action_id: int
    project: str
    repository: Path
    action: Mapping[str, Any]
    sprint_item: Mapping[str, Any]
    base_ref: str = "HEAD"

    def __post_init__(self) -> None:
        if self.action_id <= 0:
            raise PolicyError("action_id must be positive")
        if not self.project or self.project in {".", ".."} or "/" in self.project:
            raise PolicyError("project must be one safe path component")
        if not self.repository.is_absolute():
            raise PolicyError("repository must be absolute")
        if not self.base_ref or self.base_ref.startswith("-"):
            raise PolicyError("base_ref must be a non-option Git revision")


@dataclass(frozen=True)
class PreparedScopeIterate:
    request: ScopeIterateRequest
    policy: ScopeIteratePolicy
    worktree: Path
    branch: str
    base_sha: str
    prompt: str


@dataclass(frozen=True)
class VerificationResult:
    worktree: Path
    branch: str
    base_sha: str
    head_sha: str
    changed_paths: tuple[str, ...]

    @property
    def result_ref(self) -> str:
        return f"branch={self.branch} worktree={self.worktree} commit={self.head_sha}"


class Worker(Protocol):
    def run(self, prepared: PreparedScopeIterate) -> int: ...


def load_policy(raw: Mapping[str, Any], *, config_dir: Path) -> ScopeIteratePolicy:
    """Parse a strict policy mapping. Unknown keys are rejected."""
    allowed = {
        "worktree_root", "prompt_template", "path_allow", "path_deny",
        "allowed_tools", "bash_allow", "bash_deny", "network",
        "test_command", "branch_prefix", "fake_commit_path", "shared_group",
    }
    unknown = set(raw) - allowed
    if unknown:
        raise PolicyError(f"unknown scope-iterate policy keys: {sorted(unknown)}")

    def required(name: str) -> Any:
        if name not in raw:
            raise PolicyError(f"missing scope-iterate policy key: {name}")
        return raw[name]

    def path_value(name: str) -> Path:
        value = Path(os.path.expanduser(os.path.expandvars(str(required(name)))))
        return value if value.is_absolute() else (config_dir / value).resolve()

    def string_list(name: str) -> tuple[str, ...]:
        value = required(name)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise PolicyError(f"{name} must be a list of strings")
        return tuple(value)

    test = required("test_command")
    if isinstance(test, str):
        test_argv = tuple(shlex.split(test))
    elif isinstance(test, list) and all(isinstance(item, str) for item in test):
        test_argv = tuple(test)
    else:
        raise PolicyError("test_command must be a string or list of strings")
    return ScopeIteratePolicy(
        worktree_root=path_value("worktree_root"),
        prompt_template=path_value("prompt_template"),
        path_acl=PathACL(
            allow=string_list("path_allow"),
            deny=string_list("path_deny"),
        ),
        tool_acl=ToolACL(
            allowed_tools=string_list("allowed_tools"),
            bash_allow=string_list("bash_allow"),
            bash_deny=string_list("bash_deny"),
            network=str(required("network")),
        ),
        test_command=test_argv,
        branch_prefix=str(raw.get("branch_prefix", "agent/scope-iterate")),
        fake_commit_path=str(raw.get("fake_commit_path", "docs/actionq-dispatch-smoke.md")),
        shared_group=(str(raw["shared_group"]) if raw.get("shared_group") is not None else None),
    )


def validate_changed_paths(
    changed_paths: Sequence[str], *, worktree: Path, acl: PathACL
) -> None:
    root = worktree.resolve()
    for raw in changed_paths:
        rel = _validate_relative_path(raw, field="changed path")
        absolute = (root / rel).resolve(strict=False)
        try:
            absolute.relative_to(root)
        except ValueError as exc:
            raise InvariantError("diff-scope-respected", f"path escapes worktree: {raw}") from exc
        normalized = rel.as_posix()
        if any(fnmatch.fnmatch(normalized, pattern) for pattern in acl.deny):
            raise InvariantError("diff-scope-respected", f"path denied by ACL: {raw}")
        if not any(fnmatch.fnmatch(normalized, pattern) for pattern in acl.allow):
            raise InvariantError("diff-scope-respected", f"path outside ACL allowlist: {raw}")


class ScopeIterateKernel:
    """Prepare and verify an isolated artifact without lifecycle side effects."""

    def prepare(
        self, request: ScopeIterateRequest, policy: ScopeIteratePolicy
    ) -> PreparedScopeIterate:
        repo = request.repository.resolve()
        self._require_git_repo(repo)
        branch = f"{policy.branch_prefix}/{request.action_id}"
        if self._run(repo, ("git", "check-ref-format", "--branch", branch), check=False).returncode:
            raise InvariantError("branch-name-valid", f"invalid branch: {branch}")
        if self._run(
            repo, ("git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"),
            check=False,
        ).returncode == 0:
            raise InvariantError("branch-absent", f"branch already exists: {branch}")

        base_sha = self._stdout(repo, ("git", "rev-parse", "--verify", f"{request.base_ref}^{{commit}}"))
        worktree_root = policy.worktree_root.resolve()
        worktree = (worktree_root / request.project / str(request.action_id)).resolve()
        try:
            worktree.relative_to(worktree_root)
        except ValueError as exc:
            raise PolicyError("derived worktree path escapes worktree_root") from exc
        if worktree.exists() and any(worktree.iterdir()):
            raise InvariantError("worktree-absent", f"non-empty path exists: {worktree}")
        worktree.parent.mkdir(parents=True, exist_ok=True)
        # A linked worktree keeps its Git metadata under the coordinator's
        # checkout, which the contained worker identity must not be able to
        # traverse or mutate.  Use a standalone, no-hardlink clone so the
        # disposable artifact owns both its files and Git metadata.
        self._run(
            repo,
            ("git", "clone", "--no-hardlinks", "--quiet", str(repo), str(worktree)),
        )
        self._run(
            worktree,
            ("git", "switch", "--quiet", "--create", branch, base_sha),
        )
        self._run(worktree, ("git", "remote", "remove", "origin"))
        self._run(worktree, ("git", "config", "core.fileMode", "false"))
        self._share_with_group(worktree, policy.shared_group)

        try:
            prompt = self._render_prompt(request, policy, worktree, branch)
        except Exception:
            # Preserve a successfully-created worktree as evidence.
            raise
        return PreparedScopeIterate(request, policy, worktree, branch, base_sha, prompt)

    @staticmethod
    def _share_with_group(worktree: Path, shared_group: str | None) -> None:
        """Make the disposable clone writable by its inherited shared group."""
        shared_gid: int | None = None
        if shared_group is not None:
            try:
                shared_gid = grp.getgrnam(shared_group).gr_gid
            except KeyError as exc:
                raise PolicyError(f"shared_group does not exist: {shared_group}") from exc
            if shared_gid not in os.getgroups():
                raise PolicyError(
                    f"coordinator is not a member of shared_group: {shared_group}"
                )
        for path in (worktree, *worktree.rglob("*")):
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                continue
            if shared_gid is not None:
                os.chown(path, -1, shared_gid)
            path.chmod(stat.S_IMODE(mode) | stat.S_IWGRP)

    def verify(self, prepared: PreparedScopeIterate) -> VerificationResult:
        worktree = prepared.worktree
        branch = self._stdout(worktree, ("git", "branch", "--show-current"))
        if branch != prepared.branch:
            raise InvariantError("branch-current", f"expected {prepared.branch}, got {branch}")
        ahead = int(self._stdout(
            worktree, ("git", "rev-list", "--count", f"{prepared.base_sha}..HEAD")
        ))
        if ahead <= 0:
            raise InvariantError("branch-ahead-of-base", "branch has no commits ahead of base")
        changed = tuple(filter(None, self._stdout(
            worktree, ("git", "diff", "--name-only", f"{prepared.base_sha}..HEAD")
        ).splitlines()))
        if not changed:
            raise InvariantError("diff-nonempty", "no committed diff")
        dirty = self._stdout(worktree, ("git", "status", "--porcelain"))
        if dirty:
            raise InvariantError("worktree-clean", f"uncommitted changes: {dirty}")
        validate_changed_paths(changed, worktree=worktree, acl=prepared.policy.path_acl)
        result = self._run(worktree, prepared.policy.test_command, check=False)
        if result.returncode:
            detail = (result.stderr or result.stdout or "validation command failed").strip()
            raise InvariantError("tests-pass", detail)
        head = self._stdout(worktree, ("git", "rev-parse", "HEAD"))
        return VerificationResult(
            worktree, prepared.branch, prepared.base_sha, head, changed
        )

    def run(self, prepared: PreparedScopeIterate, worker: Worker) -> VerificationResult:
        exit_code = worker.run(prepared)
        if exit_code != 0:
            raise InvariantError("worker-success", f"worker exited with code {exit_code}")
        return self.verify(prepared)

    def _render_prompt(
        self, request: ScopeIterateRequest, policy: ScopeIteratePolicy,
        worktree: Path, branch: str,
    ) -> str:
        template = policy.prompt_template.read_text(encoding="utf-8")
        values = {
            "action_json": json.dumps(request.action, indent=2, default=str),
            "sprint_item_json": json.dumps(request.sprint_item, indent=2, default=str),
            "working_dir": str(worktree),
            "branch_name": branch,
            "allowed_scope": json.dumps({
                "path_allowlist": policy.path_acl.allow,
                "path_denylist": policy.path_acl.deny,
                "bash_allowlist": policy.tool_acl.bash_allow,
                "bash_denylist": policy.tool_acl.bash_deny,
                "network": policy.tool_acl.network,
            }, indent=2),
            "test_command": shlex.join(policy.test_command),
        }
        try:
            return template.format(**values)
        except (KeyError, ValueError) as exc:
            raise PolicyError(f"invalid prompt template: {exc}") from exc

    @staticmethod
    def _require_git_repo(repo: Path) -> None:
        if not repo.is_dir():
            raise InvariantError("project-exists", f"repository is not a directory: {repo}")
        result = subprocess.run(
            ("git", "rev-parse", "--is-inside-work-tree"), cwd=repo,
            text=True, capture_output=True, check=False,
        )
        if result.returncode or result.stdout.strip() != "true":
            raise InvariantError("project-is-git-repository", str(repo))

    @staticmethod
    def _run(
        cwd: Path, argv: Sequence[str], *, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            tuple(argv), cwd=cwd, text=True, capture_output=True, check=False,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
        if check and result.returncode:
            raise InvariantError(
                "command-success",
                f"{shlex.join(argv)}: {(result.stderr or result.stdout).strip()}",
            )
        return result

    def _stdout(self, cwd: Path, argv: Sequence[str]) -> str:
        return self._run(cwd, argv).stdout.strip()


class FakeCommitWorker:
    """Disposable smoke worker; never invokes a model or network service."""

    def run(self, prepared: PreparedScopeIterate) -> int:
        rel = _validate_relative_path(
            prepared.policy.fake_commit_path, field="fake_commit_path"
        )
        target = prepared.worktree / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            "# actionq scope-iterate smoke\n\n"
            "Created by the disposable ActionQ fake kernel.\n",
            encoding="utf-8",
        )
        commands = (
            ("git", "add", "--", rel.as_posix()),
            (
                "git", "-c", "user.name=actionq smoke",
                "-c", "user.email=actionq-smoke@example.invalid",
                "commit", "-m", "actionq scope-iterate smoke",
            ),
        )
        for command in commands:
            result = ScopeIterateKernel._run(prepared.worktree, command, check=False)
            if result.returncode:
                return result.returncode
        return 0


def _validate_relative_path(value: str, *, field: str) -> Path:
    if not value or "\x00" in value:
        raise PolicyError(f"{field} must be non-empty text")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path == Path("."):
        raise PolicyError(f"{field} must be a safe worktree-relative path: {value}")
    return path
