from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).parents[1]
VERIFY = ROOT / ".github/scripts/verify-release-image-tag.py"


def run(*args: str, cwd: Path) -> None:
    subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True)


def make_workspace(tmp_path: Path, tag: str = "v1.2.3") -> tuple[Path, Path]:
    """Model separate default-branch tools and an older release checkout."""
    tooling = tmp_path / "workflow-tools" / ".github" / "scripts"
    tooling.mkdir(parents=True)
    verifier = tooling / VERIFY.name
    shutil.copy(VERIFY, verifier)

    release = tmp_path / "release-src"
    release.mkdir()
    (release / "pyproject.toml").write_text('[project]\nversion = "1.2.3"\n')
    run("git", "init", cwd=release)
    run("git", "config", "user.email", "tests@example.invalid", cwd=release)
    run("git", "config", "user.name", "Tests", cwd=release)
    run("git", "add", "pyproject.toml", cwd=release)
    run("git", "commit", "-m", "release", cwd=release)
    run("git", "tag", "-a", tag, "-m", tag, cwd=release)
    return verifier, release


def verify(verifier: Path, checkout: Path, tag: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(verifier), "--tag", tag],
        cwd=checkout,
        capture_output=True,
        text=True,
    )


def test_release_tag_verifier_accepts_matching_annotated_tag_at_head(tmp_path: Path):
    verifier, checkout = make_workspace(tmp_path)
    result = verify(verifier, checkout, "v1.2.3")

    assert result.returncode == 0
    assert "tag=v1.2.3" in result.stdout
    assert "sha=" in result.stdout


@pytest.mark.parametrize("tag", ["main", "v1.2", "v1.2.3-rc.1", "v01.2.3"])
def test_release_tag_verifier_rejects_non_release_tags(tmp_path: Path, tag: str):
    verifier, checkout = make_workspace(tmp_path, tag)
    result = verify(verifier, checkout, tag)

    assert result.returncode != 0
    assert "vX.Y.Z" in result.stderr


def test_release_tag_verifier_rejects_tag_that_is_not_checked_out_head(tmp_path: Path):
    verifier, checkout = make_workspace(tmp_path)
    (checkout / "after-release").write_text("later\n")
    run("git", "add", "after-release", cwd=checkout)
    run("git", "commit", "-m", "after release", cwd=checkout)

    result = verify(verifier, checkout, "v1.2.3")

    assert result.returncode != 0
    assert "not checked-out HEAD" in result.stderr


def test_release_tag_verifier_rejects_version_mismatch(tmp_path: Path):
    verifier, checkout = make_workspace(tmp_path, "v9.9.9")
    result = verify(verifier, checkout, "v9.9.9")

    assert result.returncode != 0
    assert "does not match pyproject version v1.2.3" in result.stderr


def test_workflow_validates_before_registry_login_or_push():
    workflow = (ROOT / ".github/workflows/publish-server-image.yaml").read_text()

    assert "format('refs/tags/{0}', inputs.tag)" in workflow
    assert "ref: ${{ github.event.repository.default_branch }}" in workflow
    assert "path: workflow-tools" in workflow
    assert "path: release-src" in workflow
    assert "working-directory: release-src" in workflow
    assert "../workflow-tools/.github/scripts/verify-release-image-tag.py" in workflow
    assert workflow.index("verify-release-image-tag.py") < workflow.index("docker/login-action@v4")
    assert workflow.index("verify-release-image-tag.py") < workflow.index("docker/build-push-action@v7")
    assert "          context: release-src" in workflow
    assert "          context: ." not in workflow
    assert "steps.source.outputs.tag" in workflow
