"""Shared git-evidence collection for crash/inferred-end recovery.

Extracted from ``actionq.session_wrapper`` (work item #1114) so the daemon's
own stale-session recovery (work item #1115) can collect the same
"surviving commits/worktree" evidence for a crashed daemon-dispatched
session that the Tier-0 wrapper already collects for a crashed wrapped
session, instead of duplicating (and risking drifting) the git plumbing.

Every function here is read-only against the target repo: recovery must
never mutate a worktree it is only trying to describe.
"""
from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path
from typing import Any

_SHORTSTAT_RE = re.compile(
    r"(?:(\d+) files? changed)?(?:,?\s*(\d+) insertions?\(\+\))?(?:,?\s*(\d+) deletions?\(-\))?"
)


def _git(repo_path: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_path), *args], text=True, capture_output=True, check=False
    )
    if completed.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout.strip()


def git_state_at_start(repo_path: Path) -> tuple[str, str]:
    """Return ``(base_commit, branch)`` at session start."""
    base_commit = _git(repo_path, "rev-parse", "HEAD")
    try:
        branch = _git(repo_path, "rev-parse", "--abbrev-ref", "HEAD")
    except RuntimeError:
        branch = ""
    return base_commit, branch


def _parse_shortstat(text: str) -> dict[str, int]:
    match = _SHORTSTAT_RE.search(text)
    if not match:
        return {"files_changed": 0, "insertions": 0, "deletions": 0}
    files, insertions, deletions = match.groups()
    return {
        "files_changed": int(files or 0),
        "insertions": int(insertions or 0),
        "deletions": int(deletions or 0),
    }


def _git_status_entries(repo_path: Path) -> list[tuple[str, str]]:
    output = _git(repo_path, "status", "--porcelain")
    entries: list[tuple[str, str]] = []
    for line in output.splitlines():
        if not line:
            continue
        code, rest = line[:2], line[3:]
        if " -> " in rest:
            rest = rest.split(" -> ", 1)[1]
        entries.append((code, rest.strip().strip('"')))
    return entries


def _new_file_patch(repo_path: Path, rel_path: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_path), "diff", "--no-index", "--", "/dev/null", rel_path],
        text=True, capture_output=True, check=False,
    )
    return completed.stdout


def collect_git_evidence(repo_path: Path, base_commit: str) -> dict[str, Any]:
    """Collect commits/worktree/dirty-state evidence between ``base_commit``
    and the repo's current state.

    Raises if ``repo_path`` is not a usable git worktree at all (missing,
    not a repo, etc.) -- callers own the "worktree unavailable" fallback so
    a bounded, honest evidence gap is recorded rather than a crash.
    """
    head_commit = _git(repo_path, "rev-parse", "HEAD")
    try:
        branch = _git(repo_path, "rev-parse", "--abbrev-ref", "HEAD")
    except RuntimeError:
        branch = ""
    worktree = _git(repo_path, "rev-parse", "--show-toplevel")

    if head_commit == base_commit:
        commits: list[str] = []
    else:
        log = _git(repo_path, "log", "--format=%H", f"{base_commit}..{head_commit}")
        commits = [line for line in log.splitlines() if line]

    status_entries = _git_status_entries(repo_path)
    dirty = bool(status_entries)
    untracked_paths = sorted({path for code, path in status_entries if code == "??"})

    shortstat = _git(repo_path, "diff", base_commit, "--shortstat")
    tracked_stat = _parse_shortstat(shortstat)
    names = _git(repo_path, "diff", base_commit, "--name-only")
    tracked_touched = {line for line in names.splitlines() if line}

    untracked_line_counts: dict[str, int] = {}
    for rel_path in untracked_paths:
        try:
            text = (repo_path / rel_path).read_text(encoding="utf-8", errors="replace")
        except (OSError, UnicodeDecodeError):
            text = ""
        untracked_line_counts[rel_path] = len(text.splitlines()) if text else 0

    diff_stat = {
        "files_changed": tracked_stat["files_changed"] + len(untracked_paths),
        "insertions": tracked_stat["insertions"] + sum(untracked_line_counts.values()),
        "deletions": tracked_stat["deletions"],
    }
    touched_paths = sorted(tracked_touched | set(untracked_paths))

    patch_digest = None
    if dirty:
        tracked_patch = subprocess.run(
            ["git", "-C", str(repo_path), "diff", "HEAD"], text=True, capture_output=True, check=False
        ).stdout
        untracked_patches = "".join(_new_file_patch(repo_path, path) for path in untracked_paths)
        combined = tracked_patch + untracked_patches
        patch_digest = hashlib.sha256(combined.encode("utf-8", errors="replace")).hexdigest()

    return {
        "base_commit": base_commit,
        "head_commit": head_commit,
        "commits": commits,
        "branch": branch,
        "worktree": worktree,
        "dirty": dirty,
        "patch_digest": patch_digest,
        "diff_stat": diff_stat,
        "touched_paths": touched_paths,
    }


def collect_git_evidence_bounded(repo_path: Path, base_commit: str) -> dict[str, Any]:
    """``collect_git_evidence`` with a bounded fallback when the worktree is
    unavailable (deleted, not a git repo, etc.): base==head, no commits, no
    dirty state, so recovery still produces a valid, honest evidence record
    instead of failing the whole recovery attempt."""
    try:
        return collect_git_evidence(repo_path, base_commit)
    except Exception:
        return {
            "base_commit": base_commit,
            "head_commit": base_commit,
            "commits": [],
            "branch": "",
            "worktree": str(repo_path),
            "dirty": False,
            "patch_digest": None,
            "diff_stat": {"files_changed": 0, "insertions": 0, "deletions": 0},
            "touched_paths": [],
            "evidence_unavailable": True,
        }
