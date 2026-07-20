from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from actionq.session_wrapper import (
    SessionIdentity,
    SessionWrapper,
    VerificationOutcome,
    _Marker,
    _write_marker,
    compute_prompt_digest,
    recover_stale_markers,
    run_verification_command,
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)


@pytest.fixture()
def git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-q", "-m", "initial commit")
    return repo


def _identity(**overrides) -> SessionIdentity:
    defaults = dict(repo_project="demo", actor="test-actor", harness="manual")
    defaults.update(overrides)
    return SessionIdentity(**defaults)


def _load_capsule(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# -- normal clean-end history ------------------------------------------------


def test_clean_end_history_records_valid_capsule(tmp_path: Path, git_repo: Path):
    wrapper = SessionWrapper(
        _identity(),
        repo_path=git_repo,
        capsule_dir=tmp_path / "capsules",
        marker_dir=tmp_path / "markers",
    )

    exit_code = wrapper.run([sys.executable, "-c", "pass"])

    assert exit_code == 0
    assert wrapper.last_capsule_path is not None
    capsule = _load_capsule(wrapper.last_capsule_path)
    assert capsule["schema_version"] == "session-capsule/v1"
    assert capsule["end"] == {"kind": "clean-end", "reason": "wrapped command exited", "exit_code": 0}
    assert capsule["git"]["dirty"] is False
    assert capsule["git"]["patch_digest"] is None
    assert capsule["git"]["commits"] == []
    assert capsule["harness"] == "manual"
    assert capsule["privacy"] == {"raw_transcript_captured": False, "raw_transcript_ref": None, "retention_days": None}
    # marker cleared on clean finish
    assert list((tmp_path / "markers").glob("*.json")) == []


def test_clean_end_history_records_nonzero_exit_and_verification(tmp_path: Path, git_repo: Path):
    wrapper = SessionWrapper(
        _identity(),
        repo_path=git_repo,
        capsule_dir=tmp_path / "capsules",
        marker_dir=tmp_path / "markers",
    )

    exit_code = wrapper.run(
        [sys.executable, "-c", "import sys; sys.exit(3)"],
        verification=[[sys.executable, "-c", "pass"], [sys.executable, "-c", "import sys; sys.exit(1)"]],
    )

    assert exit_code == 3
    capsule = _load_capsule(wrapper.last_capsule_path)
    assert capsule["end"]["exit_code"] == 3
    results = {entry["command"]: entry["result"] for entry in capsule["verification"]}
    assert list(results.values()) == ["pass", "fail"]


# -- crash / inferred-end history --------------------------------------------


def test_crash_history_recovers_stale_marker_as_end_inferred(tmp_path: Path, git_repo: Path):
    capsule_dir = tmp_path / "capsules"
    marker_dir = tmp_path / "markers"
    identity = _identity(runtime_session_id="aqs:crash-test")

    # Simulate a wrapper process that wrote its start marker and then died
    # before finish() ever ran (no clean capsule was produced).
    wrapper = SessionWrapper(identity, repo_path=git_repo, capsule_dir=capsule_dir, marker_dir=marker_dir)
    wrapper.start()
    assert list(marker_dir.glob("*.json")), "expected a marker to be written on start"

    # Force the marker's recorded pid to one that cannot be alive.
    marker_path = next(marker_dir.glob("*.json"))
    payload = json.loads(marker_path.read_text(encoding="utf-8"))
    payload["pid"] = 999999999
    marker_path.write_text(json.dumps(payload), encoding="utf-8")

    recorded = recover_stale_markers(marker_dir, capsule_dir)

    assert len(recorded) == 1
    capsule = _load_capsule(recorded[0])
    assert capsule["end"]["kind"] == "end-inferred"
    assert capsule["end"]["exit_code"] is None
    assert capsule["end"]["reason"] == "wrapper-process-crash-recovery"
    assert capsule["runtime_session_id"] == "aqs:crash-test"
    assert list(marker_dir.glob("*.json")) == []


def test_crash_history_leaves_live_pid_marker_untouched(tmp_path: Path, git_repo: Path):
    capsule_dir = tmp_path / "capsules"
    marker_dir = tmp_path / "markers"
    wrapper = SessionWrapper(_identity(), repo_path=git_repo, capsule_dir=capsule_dir, marker_dir=marker_dir)
    wrapper.start()

    # Live pid (our own test process): recovery must not touch it, since a
    # session may still be genuinely in progress.
    recorded = recover_stale_markers(marker_dir, capsule_dir)

    assert recorded == []
    assert list(marker_dir.glob("*.json")), "live marker should not be cleared"
    assert list(capsule_dir.glob("*.json")) == []


def test_next_wrapper_start_sweeps_prior_crash_before_new_session(tmp_path: Path, git_repo: Path):
    capsule_dir = tmp_path / "capsules"
    marker_dir = tmp_path / "markers"

    stale = _Marker(
        session_id="aqs:stale",
        capsule_id="0f1e2d3c-4b5a-4978-8b6c-1a2b3c4d5e6f",
        origin_stream_id="1a2b3c4d-5e6f-4788-9a0b-1c2d3e4f5061",
        repo_path=str(git_repo),
        base_commit=subprocess.run(
            ["git", "-C", str(git_repo), "rev-parse", "HEAD"], text=True, capture_output=True
        ).stdout.strip(),
        branch="main",
        worktree=str(git_repo),
        started_at="2026-07-19T00:00:00Z",
        pid=999999999,
        identity={
            "repo_project": "demo", "actor": "old-actor", "harness": "manual",
            "model_name": None, "model_version": None, "repo_id": None,
            "target": None, "claim": None, "runtime_session_id": "aqs:stale",
            "starting_watermark_ingest_offset": 0, "starting_watermark_age_seconds": 0.0,
        },
    )
    _write_marker(marker_dir, stale)

    wrapper = SessionWrapper(_identity(), repo_path=git_repo, capsule_dir=capsule_dir, marker_dir=marker_dir)
    exit_code = wrapper.run([sys.executable, "-c", "pass"])

    assert exit_code == 0
    capsules = sorted(capsule_dir.glob("*.json"))
    assert len(capsules) == 2
    kinds = sorted(_load_capsule(path)["end"]["kind"] for path in capsules)
    assert kinds == ["clean-end", "end-inferred"]


# -- dirty-worktree history ---------------------------------------------------


def test_dirty_worktree_history_records_patch_digest(tmp_path: Path, git_repo: Path):
    wrapper = SessionWrapper(
        _identity(),
        repo_path=git_repo,
        capsule_dir=tmp_path / "capsules",
        marker_dir=tmp_path / "markers",
    )

    script = (
        "from pathlib import Path; "
        f"Path({str(git_repo / 'new_file.txt')!r}).write_text('changed\\n')"
    )
    exit_code = wrapper.run([sys.executable, "-c", script])

    assert exit_code == 0
    capsule = _load_capsule(wrapper.last_capsule_path)
    assert capsule["git"]["dirty"] is True
    # Untracked files are folded into the digest via a synthetic diff, so
    # a digest is present even though `git diff HEAD` alone would miss them.
    assert capsule["git"]["patch_digest"] is not None
    assert len(capsule["git"]["patch_digest"]) == 64
    assert "new_file.txt" in capsule["git"]["touched_paths"]
    assert capsule["git"]["diff_stat"]["files_changed"] >= 1


def test_dirty_worktree_history_with_tracked_modification(tmp_path: Path, git_repo: Path):
    wrapper = SessionWrapper(
        _identity(),
        repo_path=git_repo,
        capsule_dir=tmp_path / "capsules",
        marker_dir=tmp_path / "markers",
    )

    script = (
        "from pathlib import Path; "
        f"Path({str(git_repo / 'README.md')!r}).write_text('changed content\\n')"
    )
    exit_code = wrapper.run([sys.executable, "-c", script])

    assert exit_code == 0
    capsule = _load_capsule(wrapper.last_capsule_path)
    assert capsule["git"]["dirty"] is True
    assert capsule["git"]["patch_digest"] is not None
    assert len(capsule["git"]["patch_digest"]) == 64
    assert capsule["git"]["diff_stat"]["files_changed"] >= 1
    assert "README.md" in capsule["git"]["touched_paths"]


# -- recorder-failure history --------------------------------------------------


def test_recorder_failure_history_fails_open(tmp_path: Path, git_repo: Path):
    errors: list[tuple[str, BaseException | None]] = []
    capsule_dir = tmp_path / "capsules"
    # Pre-create the capsule dir path as a *file* so the wrapper's later
    # mkdir(parents=True, exist_ok=True) raises inside capsule recording.
    capsule_dir.parent.mkdir(parents=True, exist_ok=True)
    capsule_dir.write_text("not a directory")

    wrapper = SessionWrapper(
        _identity(),
        repo_path=git_repo,
        capsule_dir=capsule_dir,
        marker_dir=tmp_path / "markers",
        on_recorder_error=lambda message, exc: errors.append((message, exc)),
    )

    exit_code = wrapper.run([sys.executable, "-c", "import sys; sys.exit(7)"])

    assert exit_code == 7, "wrapped command's own outcome must propagate unchanged"
    assert wrapper.last_capsule_path is None
    assert errors, "recorder failure must be reported, not silently dropped"
    assert "failed to record session capsule" in errors[0][0]


def test_recorder_failure_during_marker_write_still_fails_open(tmp_path: Path, git_repo: Path):
    errors: list[tuple[str, BaseException | None]] = []
    marker_dir = tmp_path / "markers"
    marker_dir.parent.mkdir(parents=True, exist_ok=True)
    marker_dir.write_text("not a directory")

    wrapper = SessionWrapper(
        _identity(),
        repo_path=git_repo,
        capsule_dir=tmp_path / "capsules",
        marker_dir=marker_dir,
        on_recorder_error=lambda message, exc: errors.append((message, exc)),
    )

    exit_code = wrapper.run([sys.executable, "-c", "pass"])

    assert exit_code == 0
    assert any("marker" in message for message, _ in errors)
    # Recording itself should still succeed even without a marker on disk.
    assert wrapper.last_capsule_path is not None
    assert _load_capsule(wrapper.last_capsule_path)["end"]["kind"] == "clean-end"


def test_validation_failure_is_swallowed(tmp_path: Path, git_repo: Path, monkeypatch):
    import actionq.session_wrapper as sw

    errors: list[tuple[str, BaseException | None]] = []

    def _boom(payload):
        raise ValueError("simulated schema violation")

    monkeypatch.setattr(sw, "_validate_capsule", _boom)
    wrapper = SessionWrapper(
        _identity(),
        repo_path=git_repo,
        capsule_dir=tmp_path / "capsules",
        marker_dir=tmp_path / "markers",
        on_recorder_error=lambda message, exc: errors.append((message, exc)),
    )

    exit_code = wrapper.run([sys.executable, "-c", "pass"])

    assert exit_code == 0
    assert wrapper.last_capsule_path is None
    assert errors and isinstance(errors[0][1], ValueError)


# -- helper coverage -----------------------------------------------------------


def test_compute_prompt_digest():
    assert compute_prompt_digest(None) is None
    digest = compute_prompt_digest("hello world")
    assert len(digest) == 64
    assert digest == compute_prompt_digest("hello world")
    assert digest != compute_prompt_digest("hello world!")


def test_run_verification_command_classifies_outcomes(git_repo: Path):
    ok = run_verification_command([sys.executable, "-c", "pass"], cwd=git_repo)
    assert ok.result == "pass"
    failed = run_verification_command([sys.executable, "-c", "import sys; sys.exit(1)"], cwd=git_repo)
    assert failed.result == "fail"
    missing = run_verification_command(["definitely-not-a-real-binary-xyz"], cwd=git_repo)
    assert missing.result == "error"
