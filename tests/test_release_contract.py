from __future__ import annotations

from pathlib import Path
import sys
import tomllib

import pytest

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

from verification.validate_release_contract import (
    _adapter_requirement,
    _locked_adapter_requirement,
    _locked_schema_runtime_requirement,
    _schema_runtime_requirement,
    _validate_adapter_pin,
    _validate_schema_runtime_pin,
    validate_wheel,
)

WORKFLOW = ROOT / ".github" / "workflows" / "release-actionq-wheel.yaml"


def test_adapter_dependency_is_an_immutable_github_wheel_pin() -> None:
    requirement = _adapter_requirement()
    lock_url, lock_digest = _locked_adapter_requirement()
    digest = _validate_adapter_pin(requirement)

    assert requirement.split("#", 1)[0] == lock_url
    assert digest == lock_digest
    assert digest == "0037898a4c9f01720a42302365b0172ecd203732070326ea2abdf549a44bf0c2"


def test_schema_runtime_dependency_is_an_immutable_github_wheel_pin() -> None:
    requirement = _schema_runtime_requirement()
    lock_url, lock_digest = _locked_schema_runtime_requirement()
    digest = _validate_schema_runtime_pin(requirement)

    assert requirement.split("#", 1)[0] == lock_url
    assert digest == lock_digest
    assert digest == "b66c9357c99aa9e1a7353991ce54105a8621958ecfac47f8c121d80b90b77912"


def test_release_workflow_is_tag_only_and_github_only() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert 'tags:\n      - "v*"' in workflow
    assert "workflow_dispatch" not in workflow
    assert "pypi" not in workflow.lower()
    assert "gh release upload" not in workflow
    assert "uv publish" not in workflow
    assert "attestations: write" in workflow
    assert "contents: write" in workflow
    assert "id-token: write" in workflow
    assert "--verify-tag" in workflow
    assert "uses: actions/attest@v4" in workflow


def test_retired_server_and_daemon_are_not_release_surfaces() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert set(project["project"]["scripts"]) == {
        "actionctl",
        "actionq-completion-outbox",
    }
    for retired in (
        ROOT / "Dockerfile",
        ROOT / ".github/workflows/publish-server-image.yaml",
        ROOT / "ops/systemd/actionq-daemon.service",
        ROOT / "examples/actionq-daemon.toml",
    ):
        assert not retired.exists(), f"retired execution surface returned: {retired}"


def test_release_workflow_runs_full_tests_before_one_gated_build() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert workflow.count("uv build --wheel --package actionq --out-dir dist") == 1
    assert "Run the full PostgreSQL-backed suite" in workflow
    assert workflow.index("uv run --all-packages --extra dev pytest -q") < workflow.index(
        "uv build --wheel --package actionq --out-dir dist"
    )
    assert workflow.index("uv build --wheel --package actionq --out-dir dist") < workflow.index(
        "verification/validate_release_contract.py"
    )
    assert workflow.index("uses: actions/attest@v4") < workflow.index(
        'gh release create "$tag"'
    )


@pytest.mark.skipif(
    not list((ROOT / "dist").glob("actionq-0.1.27-*.whl")),
    reason="release wheel is built by the release workflow",
)
def test_built_wheel_satisfies_release_contract() -> None:
    wheels = sorted((ROOT / "dist").glob("actionq-0.1.27-*.whl"))
    assert len(wheels) == 1
    validate_wheel(wheels[0], tag="v0.1.27")
