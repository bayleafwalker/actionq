"""Execution-invariant tests.

Each case is a way an execution can succeed while having operated on something other than
what the envelope named.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from actionq_runner.execution_boundary import (
    BoundaryViolation,
    resolve_revision,
    snapshot_baseline,
    verify_after_completion,
    verify_before_dispatch,
)
from actionq_runner.execution_contract import ExecutionInvariants


def _repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    (path / "README.md").write_text("hello\n")
    (path / "src").mkdir(exist_ok=True)
    (path / "src" / "a.py").write_text("x = 1\n")
    subprocess.run(["git", "add", "-A"], cwd=path, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"],
        cwd=path, check=True,
    )
    return path


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return _repo(tmp_path / "repo")


# -- before dispatch -------------------------------------------------------


def test_missing_root_is_caught_before_anything_runs(tmp_path: Path) -> None:
    inv = ExecutionInvariants(root=str(tmp_path / "nope"), revision="HEAD")
    report = verify_before_dispatch(inv)
    assert not report.ok
    assert report.violations[0].name == "root.exists"


def test_non_repository_root_is_refused(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()
    report = verify_before_dispatch(ExecutionInvariants(root=str(plain), revision="HEAD"))
    assert not report.ok
    assert any(c.name == "root.is_worktree" for c in report.violations)


def test_wrong_revision_is_refused(repo: Path) -> None:
    inv = ExecutionInvariants(root=str(repo), revision="0" * 40)
    report = verify_before_dispatch(inv)
    assert not report.ok
    assert report.violations[0].name == "revision.pinned"
    with pytest.raises(BoundaryViolation, match="revision.pinned"):
        report.raise_for_violations()


def test_correct_revision_passes(repo: Path) -> None:
    head = resolve_revision(repo)
    report = verify_before_dispatch(ExecutionInvariants(root=str(repo), revision=head))
    assert report.ok


def test_subdirectory_root_is_refused(repo: Path) -> None:
    """A subdirectory would make the path allowlist relative to the wrong base."""
    inv = ExecutionInvariants(root=str(repo / "src"), revision="HEAD")
    report = verify_before_dispatch(inv)
    assert not report.ok
    assert any(c.name == "root.is_worktree_top" for c in report.violations)


# -- after completion ------------------------------------------------------


def test_empty_allowlist_means_no_changes_are_permitted(repo: Path) -> None:
    baseline = snapshot_baseline(repo)
    (repo / "src" / "a.py").write_text("x = 2\n")
    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD"), baseline=baseline
    )
    assert not report.ok
    assert report.violations[0].name == "paths.permitted"


def test_changes_inside_the_allowlist_pass(repo: Path) -> None:
    baseline = snapshot_baseline(repo)
    (repo / "src" / "a.py").write_text("x = 2\n")
    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD", permitted_paths=("src/*",)),
        baseline=baseline,
    )
    assert report.ok


def test_changes_outside_the_allowlist_are_caught(repo: Path) -> None:
    baseline = snapshot_baseline(repo)
    (repo / "src" / "a.py").write_text("x = 2\n")
    (repo / "README.md").write_text("tampered\n")
    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD", permitted_paths=("src/*",)),
        baseline=baseline,
    )
    assert not report.ok
    assert "README.md" in report.violations[0].observed


def test_agent_change_to_preexisting_dirty_disallowed_path_is_caught(repo: Path) -> None:
    """The blind spot a path-set baseline had.

    README.md is already dirty, so a name-only baseline would subtract it away and report
    clean while the execution really did modify a forbidden file.
    """
    (repo / "README.md").write_text("dirty before we started\n")
    baseline = snapshot_baseline(repo)
    (repo / "README.md").write_text("the execution changed it again\n")

    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD", permitted_paths=("src/*",)),
        baseline=baseline,
    )
    assert not report.ok
    assert report.violations[0].name == "paths.permitted"
    assert "README.md" in report.violations[0].observed


def test_reverting_pre_existing_dirt_is_attributed_to_the_execution(repo: Path) -> None:
    """A path that stops being dirty also changed. Absence from the diff is not innocence."""
    original = (repo / "README.md").read_text()
    (repo / "README.md").write_text("dirty before we started\n")
    baseline = snapshot_baseline(repo)
    (repo / "README.md").write_text(original)

    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD", permitted_paths=("src/*",)),
        baseline=baseline,
    )
    assert not report.ok
    assert "README.md" in report.violations[0].observed


def test_deleting_a_pre_existing_dirty_file_is_caught(repo: Path) -> None:
    (repo / "README.md").write_text("dirty before we started\n")
    baseline = snapshot_baseline(repo)
    (repo / "README.md").unlink()

    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD", permitted_paths=("src/*",)),
        baseline=baseline,
    )
    assert not report.ok


def test_pre_existing_dirt_is_not_blamed_on_the_execution(repo: Path) -> None:
    (repo / "README.md").write_text("dirty before we started\n")
    baseline = snapshot_baseline(repo)
    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD"), baseline=baseline
    )
    assert report.ok


def test_a_harness_that_commits_is_caught(repo: Path) -> None:
    """HEAD moving is a silent change of execution identity."""
    before = resolve_revision(repo)
    (repo / "src" / "a.py").write_text("x = 3\n")
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "sneaky"],
        cwd=repo, check=True,
    )
    report = verify_after_completion(
        ExecutionInvariants(root=str(repo), revision="HEAD", permitted_paths=("*",)),
        expected_revision=before,
    )
    assert not report.ok
    assert report.violations[0].name == "revision.unchanged"


def test_missing_acceptance_target_is_reported(repo: Path) -> None:
    report = verify_after_completion(
        ExecutionInvariants(
            root=str(repo), revision="HEAD", permitted_paths=("*",),
            acceptance_target="tests/test_it.py",
        )
    )
    assert not report.ok
    assert report.violations[0].name == "acceptance.target_present"
