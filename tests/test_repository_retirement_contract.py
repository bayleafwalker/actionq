from __future__ import annotations

import json
import shlex
from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_registered_hybrid_test_commands_reference_existing_files() -> None:
    manifest = json.loads((ROOT / "actionq.dispatch.json").read_text(encoding="utf-8"))

    commands = manifest["hybrid"]["commands"]
    assert commands
    for name, command in commands.items():
        test_paths = [token for token in shlex.split(command) if token.startswith("tests/")]
        assert test_paths, f"{name} has no explicit falsifying test path"
        for token in test_paths:
            path = token.split("::", 1)[0]
            assert (ROOT / path).is_file(), f"{name} references missing {path}"


def test_verification_context_implementation_anchors_exist() -> None:
    for context_path in sorted((ROOT / "verification/contexts").glob("*.json")):
        context = json.loads(context_path.read_text(encoding="utf-8"))
        for anchor in context.get("implementation_anchors", []):
            path = anchor.split(":", 1)[0]
            assert (ROOT / path).exists(), f"{context_path.name} references missing {path}"


def test_retired_execution_entrypoints_do_not_return_as_repo_surfaces() -> None:
    retired_paths = (
        "Dockerfile",
        ".dockerignore",
        ".github/workflows/publish-server-image.yaml",
        "ops/systemd/actionq-daemon.service",
        "examples/actionq-daemon.toml",
        "actionq/daemon.py",
        "actionq/server.py",
        "actionq/session_wrapper.py",
        "verification/fixtures/action-resource-owner-v1/legacy-quarantine.json",
        "verification/history-receipts/legacy-quarantine.json",
        "verification/results/action-resource-owner-legacy-quarantine.json",
    )
    for path in retired_paths:
        assert not (ROOT / path).exists(), f"retired execution surface returned: {path}"

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized_readme = " ".join(readme.split())
    assert "Do not install or invoke that launcher with this ActionQ revision" in normalized_readme

    required_histories = json.loads(
        (ROOT / "verification/fixtures/action-resource-owner-v1/required-histories.json").read_text(
            encoding="utf-8"
        )
    )
    assert "legacy-http-route-quarantine" not in required_histories["histories"]
