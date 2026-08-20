"""Executable completeness gate for the tranche-4 W1 reachability freeze.

This test deliberately observes structure only.  W1 pins every currently reachable
surface before a later, separately reviewed packet is allowed to move code.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
import tomllib
from pathlib import Path


ROOT = Path(__file__).parents[1]
MANIFEST_PATH = ROOT / "docs/contracts/tranche4-reachability-v1.json"
DISPOSITIONS = {
    "retain-as-frozen-archive",
    "replace-by-federation",
    "historicalize/delete",
    "unresolved-owner-decision",
}
LOCAL_PACKAGES = ("actionq", "actionq_runner", "actionq_contracts")
PYTHON_ROOTS = (
    (ROOT / "actionq", "actionq"),
    (ROOT / "packages/actionq-runner/actionq_runner", "actionq_runner"),
    (ROOT / "packages/actionq-contracts/actionq_contracts", "actionq_contracts"),
)


def _manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _module_name(path: Path, root: Path, prefix: str) -> tuple[str, str]:
    relative = path.relative_to(root)
    if path.name == "__init__.py":
        parts = relative.parent.parts
    else:
        parts = relative.with_suffix("").parts
    module = ".".join((prefix, *parts))
    package = module if path.name == "__init__.py" else module.rsplit(".", 1)[0]
    return module, package


def _python_symbols() -> dict[str, list[str]]:
    observed: dict[str, list[str]] = {}
    for path in sorted((ROOT / "actionq").glob("*.py")):
        names: list[str] = []
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(node.name)
                if isinstance(node, ast.ClassDef):
                    names.extend(
                        f"{node.name}.{child.name}"
                        for child in node.body
                        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                    )
        observed[path.relative_to(ROOT).as_posix()] = names
    return observed


def _local_import_edges() -> set[str]:
    edges: set[str] = set()
    for root, prefix in PYTHON_ROOTS:
        for path in sorted(root.rglob("*.py")):
            source, package = _module_name(path, root, prefix)
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith(LOCAL_PACKAGES):
                            edges.add(f"{source}|import|{alias.name}|")
                elif isinstance(node, ast.ImportFrom):
                    if node.level:
                        base = package.split(".")
                        base = base[: len(base) - (node.level - 1)]
                        target = ".".join(
                            base + ((node.module or "").split(".") if node.module else [])
                        )
                    else:
                        target = node.module or ""
                    if target.startswith(LOCAL_PACKAGES):
                        names = ",".join(sorted(alias.name for alias in node.names))
                        edges.add(f"{source}|from|{target}|{names}")
    return edges


def _console_scripts() -> set[str]:
    scripts: set[str] = set()
    projects = [ROOT / "pyproject.toml", *sorted((ROOT / "packages").glob("*/pyproject.toml"))]
    for project in projects:
        metadata = tomllib.loads(project.read_text(encoding="utf-8"))
        for name, target in metadata.get("project", {}).get("scripts", {}).items():
            scripts.add(f"{project.relative_to(ROOT).as_posix()}|{name}|{target}")
    return scripts


def _cli_commands() -> set[str]:
    from actionq.cli import cli

    commands: set[str] = set()

    def walk(group, prefix: tuple[str, ...] = ()) -> None:
        for name, command in group.commands.items():
            command_path = (*prefix, name)
            callback = command.callback
            commands.add(
                f"{' '.join(command_path)}|{callback.__module__}:{callback.__name__}"
            )
            if hasattr(command, "commands"):
                walk(command, command_path)

    walk(cli)
    return commands


def _catalog_operations_and_handlers() -> tuple[set[str], dict[str, str]]:
    from actionq.application import ActionQApplication
    from actionq.vuoro import build_operations, catalog_metadata

    policy_enabled = ActionQApplication(managed_dispatch_policy=object())
    built = build_operations(policy_enabled)
    operations = {definition["name"] for definition in catalog_metadata()}
    operations.update(operation.definition["name"] for operation in built)
    handlers: dict[str, str] = {}
    for operation in built:
        handler = operation.handler
        callables = [handler]
        callables.extend(
            cell.cell_contents
            for cell in (handler.__closure__ or ())
            if callable(cell.cell_contents)
        )
        callables.extend(value for value in (handler.__defaults__ or ()) if callable(value))
        method_names = {
            name
            for callback in callables
            for name in callback.__code__.co_names
            if callable(getattr(policy_enabled, name, None))
        }
        assert len(method_names) == 1, (operation.definition["name"], method_names)
        handlers[operation.definition["name"]] = method_names.pop()
    return operations, handlers


def _tracked_files() -> list[str]:
    raw = subprocess.check_output(
        ["git", "ls-files", "-z"], cwd=ROOT, text=False
    ).decode("utf-8")
    return [path for path in raw.split("\0") if path]


def _scan_paths(pattern: str) -> set[str]:
    expression = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
    ignored = {
        "uv.lock",
        "docs/contracts/tranche4-reachability-v1.json",
        "tests/test_tranche4_reachability_contract.py",
    }
    matches: set[str] = set()
    for relative in _tracked_files():
        if relative in ignored or relative.startswith("docs/evidence/"):
            continue
        path = ROOT / relative
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if expression.search(content):
            matches.add(relative)
    return matches


def _flatten(entries: list[dict], field: str) -> set[str]:
    values: list[str] = []
    for entry in entries:
        values.extend(entry[field])
    assert len(values) == len(set(values)), f"duplicate {field} classification"
    return set(values)


def _assert_classified(entries: list[dict]) -> None:
    for entry in entries:
        assert entry["disposition"] in DISPOSITIONS, entry
        assert entry["owner"].strip(), entry
        assert entry["falsifying_test"].strip(), entry


def test_manifest_identity_and_every_entry_is_owned_and_falsifiable():
    manifest = _manifest()
    assert manifest["contract_id"] == "actionq/tranche4-reachability-v1"
    assert manifest["basis"]["actionq_commit"] == "89ec87607d71af1156771db2e0c927d74017f5a0"
    assert manifest["basis"]["w0_contract"] == (
        "docs/plans/2026-08-20-tranche4-federation-storage-contract-freeze.md"
    )
    assert set(manifest["dispositions"]) == DISPOSITIONS
    for section in (
        "python_surfaces",
        "import_groups",
        "console_script_groups",
        "cli_command_groups",
        "catalog_operation_groups",
        "migration_groups",
        "workspace_package_groups",
        "repository_consumer_groups",
        "retired_plane_anchor_groups",
        "critical_reachability",
        "external_consumers",
    ):
        _assert_classified(manifest[section])


def test_every_current_root_symbol_is_pinned_to_one_module_disposition():
    manifest = _manifest()
    expected = {
        entry["path"]: entry["symbols"] for entry in manifest["python_surfaces"]
    }
    assert expected == _python_symbols()


def test_critical_claim_runner_publication_cas_and_rebuild_symbols_are_bound():
    manifest = _manifest()
    symbols = _python_symbols()
    for entry in manifest["critical_reachability"]:
        assert entry["paths"], entry
        assert entry["operations"], entry
        for path in entry["paths"]:
            assert (ROOT / path).exists(), path
        for reference in entry["symbols"]:
            path, symbol = reference.split("#", 1)
            assert symbol in symbols[path], reference


def test_every_local_import_edge_is_pinned_and_runner_has_zero_root_reach():
    manifest = _manifest()
    expected = _flatten(manifest["import_groups"], "edges")
    observed = _local_import_edges()
    assert expected == observed
    assert not {
        edge
        for edge in observed
        if edge.startswith("actionq_runner")
        and ("|from|actionq|" in edge or "|import|actionq." in edge)
    }


def test_every_console_script_cli_command_and_catalog_operation_is_pinned():
    manifest = _manifest()
    assert _flatten(manifest["console_script_groups"], "scripts") == _console_scripts()
    assert _flatten(manifest["cli_command_groups"], "commands") == _cli_commands()
    operations, handlers = _catalog_operations_and_handlers()
    assert _flatten(manifest["catalog_operation_groups"], "operations") == operations
    assert manifest["catalog_handler_bindings"] == handlers


def test_execution_migrations_001_through_012_are_exact_and_no_013_exists():
    manifest = _manifest()
    expected = {}
    for group in manifest["migration_groups"]:
        for migration in group["migrations"]:
            expected[migration["path"]] = migration
    observed_paths = sorted(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "actionq/migrations").glob("*.sql")
    )
    assert sorted(expected) == observed_paths
    assert [expected[path]["version"] for path in observed_paths] == list(range(1, 13))
    for relative, record in expected.items():
        assert record["domain"] == "execution"
        assert record["api_version"] == "v1"
        digest = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
        assert digest == record["sha256"]
    assert not list((ROOT / "actionq/migrations").glob("013_*.sql"))


def test_workspace_package_inventory_and_mandatory_runner_decision_are_pinned():
    manifest = _manifest()
    expected = _flatten(manifest["workspace_package_groups"], "paths")
    observed = {
        path
        for path in _tracked_files()
        if path.startswith(("packages/actionq-runner/", "packages/actionq-contracts/"))
    }
    assert expected == observed
    runner = next(
        entry
        for entry in manifest["workspace_package_groups"]
        if entry["id"] == "actionq-runner-mandatory-w6-decision"
    )
    assert runner["disposition"] == "unresolved-owner-decision"
    assert runner["blocks"] == ["W6-closure"]
    assert runner["allowed_outcomes"] == ["retire", "extract", "transfer"]


def test_repository_wide_consumers_and_removed_plane_anchors_are_classified():
    manifest = _manifest()
    api_scan = manifest["repository_scans"]["api_consumers"]
    retired_scan = manifest["repository_scans"]["retired_plane"]
    assert _flatten(manifest["repository_consumer_groups"], "paths") == _scan_paths(
        api_scan["pattern"]
    )
    assert _flatten(manifest["retired_plane_anchor_groups"], "paths") == _scan_paths(
        retired_scan["pattern"]
    )


def test_external_consumers_have_immutable_refs_and_operator_gates():
    manifest = _manifest()
    consumers = {entry["id"]: entry for entry in manifest["external_consumers"]}
    assert set(consumers) == {
        "vuoro-composition",
        "actionq-dispatcher-tombstone",
        "q-spec-stale-normative-contracts",
        "agentops-generated-guidance",
        "gitops-nixos-daemon-retirement",
        "appservice-ordered-retirement",
    }
    sha40 = re.compile(r"^[0-9a-f]{40}$")
    for identifier, consumer in consumers.items():
        for revision in consumer.get("commit_refs", {}).values():
            assert sha40.fullmatch(revision), (identifier, revision)
    q_spec = consumers["q-spec-stale-normative-contracts"]
    assert q_spec["revision_kind"] == "unversioned-file-digests"
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in q_spec["file_sha256"].values())
    runner = consumers["actionq-dispatcher-tombstone"]
    assert runner["release"] == "actionq-dispatcher-v0.2.0"
    gitops = consumers["gitops-nixos-daemon-retirement"]
    assert gitops["pull_request"] == 16 and gitops["merge_gate"] == "operator-after-appservice-phase2"
    appservice = consumers["appservice-ordered-retirement"]
    assert appservice["operator_only"] is True
    assert appservice["required_order"] == [
        "merge-and-reconcile-phase1",
        "prove-vscode-namespace-cnpg-and-cockpit-health",
        "merge-and-reconcile-phase2",
        "prove-server-removal",
        "disable-and-stop-devbox-actionq-dispatch-service",
        "merge-and-deploy-gitops-nixos-pr16",
    ]
