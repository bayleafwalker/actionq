from __future__ import annotations

import subprocess
from pathlib import Path

from actionq.git_evidence import collect_git_evidence, collect_git_evidence_bounded, git_state_at_start


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)


def _make_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-q", "-m", "initial commit")
    return repo


def test_git_state_at_start_returns_head_and_branch(tmp_path: Path):
    repo = _make_repo(tmp_path)
    base_commit, branch = git_state_at_start(repo)
    assert len(base_commit) == 40
    assert branch in {"main", "master"}


def test_collect_git_evidence_reports_clean_state_when_nothing_changed(tmp_path: Path):
    repo = _make_repo(tmp_path)
    base_commit, _branch = git_state_at_start(repo)
    evidence = collect_git_evidence(repo, base_commit)
    assert evidence["base_commit"] == evidence["head_commit"] == base_commit
    assert evidence["commits"] == []
    assert evidence["dirty"] is False
    assert evidence["patch_digest"] is None


def test_collect_git_evidence_reports_surviving_commit(tmp_path: Path):
    repo = _make_repo(tmp_path)
    base_commit, _branch = git_state_at_start(repo)
    (repo / "new.txt").write_text("work done before crash\n", encoding="utf-8")
    _git(repo, "add", "new.txt")
    _git(repo, "commit", "-q", "-m", "surviving commit")

    evidence = collect_git_evidence(repo, base_commit)
    assert len(evidence["commits"]) == 1
    assert evidence["head_commit"] != base_commit
    assert "new.txt" in evidence["touched_paths"]


def test_collect_git_evidence_reports_dirty_untracked_state(tmp_path: Path):
    repo = _make_repo(tmp_path)
    base_commit, _branch = git_state_at_start(repo)
    (repo / "scratch.txt").write_text("uncommitted\n", encoding="utf-8")

    evidence = collect_git_evidence(repo, base_commit)
    assert evidence["dirty"] is True
    assert evidence["patch_digest"] is not None
    assert "scratch.txt" in evidence["touched_paths"]


def test_collect_git_evidence_raises_for_unusable_repo(tmp_path: Path):
    missing = tmp_path / "does-not-exist"
    import pytest

    with pytest.raises(RuntimeError):
        collect_git_evidence(missing, "0" * 40)


def test_collect_git_evidence_bounded_degrades_gracefully_when_worktree_unavailable(tmp_path: Path):
    missing = tmp_path / "does-not-exist"
    evidence = collect_git_evidence_bounded(missing, "0" * 40)
    assert evidence["commits"] == []
    assert evidence["dirty"] is False
    assert evidence["evidence_unavailable"] is True
    assert evidence["base_commit"] == evidence["head_commit"] == "0" * 40


def test_collect_git_evidence_bounded_matches_unbounded_for_a_healthy_repo(tmp_path: Path):
    repo = _make_repo(tmp_path)
    base_commit, _branch = git_state_at_start(repo)
    bounded = collect_git_evidence_bounded(repo, base_commit)
    direct = collect_git_evidence(repo, base_commit)
    assert bounded == direct
    assert "evidence_unavailable" not in bounded
