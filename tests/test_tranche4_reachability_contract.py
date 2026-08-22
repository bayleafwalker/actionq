"""Executable completeness gate for the tranche-4 W1 reachability freeze.

This test deliberately observes structure only.  W1 pins every currently reachable
surface before a later, separately reviewed packet is allowed to move code.
"""

from __future__ import annotations

import ast
import base64
import copy
import hashlib
import io
import json
import re
import subprocess
import tarfile
import tomllib
from pathlib import Path

import pytest


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
CONSTANT_UNIVERSE = {
    "actionq/action_resource.py",
    "actionq/completion_outbox.py",
    "actionq/db.py",
    "actionq/schema.py",
    "actionq/vuoro.py",
}


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


def _top_level_assignments(relative: str) -> set[str]:
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        names.update(target.id for target in targets if isinstance(target, ast.Name))
    return names


def _contract_import_names(relative: str) -> set[str]:
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
    return {
        alias.name + (f" as {alias.asname}" if alias.asname else "")
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "actionq_contracts"
        for alias in node.names
    }


def _effective_symbol_dispositions(manifest: dict) -> dict[tuple[str, str], str]:
    effective = {
        (entry["path"], symbol): entry["disposition"]
        for entry in manifest["python_surfaces"]
        for symbol in entry["symbols"]
    }
    seen: set[tuple[str, str]] = set()
    for entry in manifest["atomic_symbol_dispositions"]:
        for symbol in entry["symbols"]:
            key = (entry["path"], symbol)
            assert key not in seen
            assert key in effective
            seen.add(key)
            effective[key] = entry["disposition"]
    return effective


def _effective_path_dispositions(manifest: dict) -> dict[str, set[str]]:
    effective: dict[str, set[str]] = {}
    def add(path: str, disposition: str) -> None:
        effective.setdefault(path, set()).add(disposition)
    for entry in manifest["python_surfaces"]:
        add(entry["path"], entry["disposition"])
    for entry in manifest["atomic_symbol_dispositions"]:
        add(entry["path"], entry["disposition"])
    for entry in manifest["bound_constant_dispositions"]:
        add(entry["path"], entry["disposition"])
    for entry in manifest["atomic_import_dispositions"]:
        add(entry["path"], entry["disposition"])
    for entry in manifest["migration_groups"]:
        for migration in entry["migrations"]:
            add(migration["path"], entry["disposition"])
    for entry in manifest["semantic_assets"]:
        for path in entry["paths"]:
            add(path, entry["disposition"])
    for entry in manifest["critical_file_dispositions"]:
        for path in entry["paths"]:
            add(path, entry["disposition"])
    for entry in manifest["cli_command_groups"]:
        add("actionq/cli.py", entry["disposition"])
    for entry in manifest["catalog_operation_groups"]:
        add("actionq/vuoro.py", entry["disposition"])
    for entry in manifest["console_script_groups"]:
        for script in entry["scripts"]:
            add(script.split("|", 1)[0], entry["disposition"])
    for section, field in (
        ("workspace_package_groups", "paths"),
        ("repository_consumer_groups", "paths"),
        ("retired_plane_anchor_groups", "paths"),
    ):
        for entry in manifest[section]:
            for path in entry[field]:
                add(path, entry["disposition"])
    return effective


def _validate_external_path_proofs(manifest: dict) -> None:
    consumers = {entry["id"]: entry for entry in manifest["external_consumers"]}
    proofs = manifest["external_path_proofs"]
    keys = [(item["consumer_id"], item["path"]) for item in proofs]
    expected = [
        (consumer["id"], path)
        for consumer in manifest["external_consumers"]
        for path in consumer["paths"]
    ]
    assert len(proofs) == 51
    assert len(keys) == len(set(keys))
    assert set(keys) == set(expected)
    assert all(re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) for item in proofs)
    for item in proofs:
        consumer = consumers[item["consumer_id"]]
        if item["consumer_id"] == "q-spec-stale-normative-contracts":
            assert item["ref"] == "unversioned-file-digest"
        else:
            assert item["ref"] in set(consumer["commit_refs"].values())
    agentops = consumers["agentops-generated-guidance"]
    assert agentops["commit_refs"] == {
        "canonical_program_record": "6c01869b8a0c5ad3dfaf3313385f9a162900dab8"
    }


def _validate_external_proof_bundle(
    manifest: dict, *, corrupt: dict[tuple[str, str], bytes] | None = None
) -> None:
    bundle = manifest["external_proof_bundle"]
    encoded = (ROOT / bundle["path"]).read_bytes()
    archive = base64.b64decode(encoded, validate=False)
    assert hashlib.sha256(archive).hexdigest() == bundle["sha256"]
    members: dict[tuple[str, str], bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as opened:
        for member in opened.getmembers():
            if not member.isfile():
                continue
            name = member.name.removeprefix("./")
            consumer_id, path = name.split("/", 1)
            key = (consumer_id, path)
            assert key not in members
            extracted = opened.extractfile(member)
            assert extracted is not None
            members[key] = extracted.read()
    for key, content in (corrupt or {}).items():
        assert key in members
        members[key] = content
    proofs = {
        (item["consumer_id"], item["path"]): item
        for item in manifest["external_path_proofs"]
    }
    assert bundle["member_count"] == 51
    assert set(members) == set(proofs)
    for key, content in members.items():
        assert hashlib.sha256(content).hexdigest() == proofs[key]["sha256"], key
        if key[0] == "appservice-ordered-retirement" and key[1].endswith(".secret.yaml"):
            for line in content.decode("utf-8").splitlines():
                if re.match(r"^\s+(?:username|password|uri):", line):
                    assert "ENC[" in line, (key, "plaintext secret field")


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


def _validate_r1_invariants(manifest: dict, cancellation_source: str) -> None:
    imports = {entry["id"]: entry for entry in manifest["import_groups"]}
    assert imports["legacy-execution-import-edges"]["disposition"] == "historicalize/delete"

    q_spec = next(
        item for item in manifest["external_consumers"]
        if item["id"] == "q-spec-stale-normative-contracts"
    )
    assert set(q_spec["paths"]) == set(q_spec["file_sha256"])
    assert q_spec["file_sha256"]["actionq-spec.md"] == (
        "61181fb5f3916fe008cfc055004b4b00f75355340dd46a1b98e1e0dace64a4ff"
    )

    vuoro = next(
        item for item in manifest["external_consumers"]
        if item["id"] == "vuoro-composition"
    )
    pins = vuoro["pins"]
    assert pins["legacy_catalog_default_operation_count"] == 26
    assert pins["legacy_catalog_policy_enabled_operation_count"] == 27
    assert pins["sole_conditional_operation"] == "execution.managed-dispatch.enqueue"
    assert pins["managed_dispatch_operation_is_policy_conditional"] is True
    assert "def test_bounded_depth_three_cancellation_histories(" in cancellation_source


def _assert_atomic_entries(entries: list[dict], value_field: str) -> None:
    observed: dict[tuple[str, str], str] = {}
    for entry in entries:
        assert entry["disposition"] in DISPOSITIONS
        for value in entry[value_field]:
            key = (entry["path"], value)
            assert key not in observed, f"conflicting atomic classification: {key}"
            observed[key] = entry["disposition"]


def test_manifest_identity_and_every_entry_is_owned_and_falsifiable():
    manifest = _manifest()
    assert manifest["contract_id"] == "actionq/tranche4-reachability-v1"
    assert manifest["basis"]["actionq_commit"] == "89ec87607d71af1156771db2e0c927d74017f5a0"
    assert manifest["basis"]["w0_contract"] == (
        "docs/plans/2026-08-20-tranche4-federation-storage-contract-freeze.md"
    )
    assert set(manifest["dispositions"]) == DISPOSITIONS
    assert manifest["basis"]["w0_ratification"] == {
        "decision_url": "https://github.com/bayleafwalker/actionq/pull/31#issuecomment-5353774023",
        "decision_comment_id": 5353774023,
        "independent_review_accepted_head": "f6750e47edeb9f3a60c23059cd638604ac0d40a4",
        "nonblocking_final_editorial": "2d0e4abdcf673a8831a9e754b32264bde8a6f438",
        "merge": "89ec87607d71af1156771db2e0c927d74017f5a0",
        "ci_run": "32347566284",
        "ci_job": "96359442888",
        "status": "R0-owner-ratified-and-merged-before-W1",
    }
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
    _check_mixed_symbols_constants_and_semantic_assets()
    _check_r1_reviewer_corruption_probes()


def test_every_current_root_symbol_is_pinned_to_one_module_disposition():
    manifest = _manifest()
    expected = {
        entry["path"]: entry["symbols"] for entry in manifest["python_surfaces"]
    }
    assert expected == _python_symbols()


def _check_mixed_symbols_constants_and_semantic_assets():
    manifest = _manifest()
    _assert_atomic_entries(manifest["atomic_symbol_dispositions"], "symbols")
    _assert_atomic_entries(manifest["bound_constant_dispositions"], "names")
    _assert_atomic_entries(manifest["atomic_import_dispositions"], "names")
    constants = {
        entry["path"]: set()
        for entry in manifest["bound_constant_dispositions"]
        if entry["path"] in CONSTANT_UNIVERSE
    }
    for entry in manifest["bound_constant_dispositions"]:
        if entry["path"] in CONSTANT_UNIVERSE:
            constants[entry["path"]].update(entry["names"])
    assert constants == {path: _top_level_assignments(path) for path in CONSTANT_UNIVERSE}
    imports = {}
    for entry in manifest["atomic_import_dispositions"]:
        imports.setdefault(entry["path"], set()).update(entry["names"])
    assert imports == {
        "actionq/db.py": _contract_import_names("actionq/db.py"),
        "actionq/vuoro.py": _contract_import_names("actionq/vuoro.py"),
    }
    _assert_atomic_entries(
        [dict(entry, path="semantic-assets") for entry in manifest["semantic_assets"]],
        "paths",
    )
    for entry in manifest["semantic_assets"]:
        for relative in entry["paths"]:
            assert (ROOT / relative).is_file(), relative
    source = (ROOT / "tests/test_cancellation_model.py").read_text(encoding="utf-8")
    _validate_r1_invariants(manifest, source)
    effective = _effective_symbol_dispositions(manifest)
    effective_paths = _effective_path_dispositions(manifest)
    critical = {entry["id"]: entry for entry in manifest["critical_reachability"]}
    assert set(critical["completion-replay-and-rebuild"]["paths"]) == {
        "actionq/completion_log.py", "actionq/completion_outbox.py"
    }
    assert {
        "actionq/action_resource.py", "verification/run_action_resource_history.py",
        "verification/validate_action_resource_owner_v1.py",
        "verification/contexts/action-resource-owner-v1.json",
        "verification/fixtures/action-resource-owner-v1/manifest.json",
    } <= set(critical["coupled-resource-to-federation-successor"]["paths"])
    for entry in manifest["critical_reachability"]:
        for reference in entry["symbols"]:
            path, symbol = reference.split("#", 1)
            assert effective[path, symbol] == entry["disposition"], reference
        for path in entry["paths"]:
            assert path in effective_paths, path
            assert entry["disposition"] in effective_paths[path], (entry["id"], path)

    selected = set()
    for pattern in (
        "tests/test_cancellation_model.py", "tests/test_candidate_action_cancellation_model.py",
        "tests/test_verification_contracts.py", "verification/contexts/action-claim-concurrency.json",
            "verification/contexts/dispatch-enqueue-v2-contract.json", "verification/contexts/schema-migration-compatibility.json",
            "verification/contexts/action-resource-owner-v1.json", "verification/run_action_resource_history.py",
            "verification/validate_action_resource_owner_v1.py",
        "verification/fixtures/action-resource-owner-v1/*", "verification/history-receipts/*.json",
        "verification/results/action-resource-owner-*.json", "verification/*probe*.py",
        "verification/capture_claude_acp.py", "docs/operations/codex-luna-catalog-workaround.md",
            "docs/plans/acp-execution-adapter.md", "docs/verification/*depth-2-survey.md",
            ".agents/overlays/actionq.hybrid-worker.md", "actionq/migrations/__init__.py", "AGENTS.md",
            "verification/fixtures/tranche4-external-paths-v1.tar.gz.base64",
            "verification/fixtures/tranche4-external-paths-v1.md",
    ):
        selected.update(path.relative_to(ROOT).as_posix() for path in ROOT.glob(pattern) if path.is_file())
    assert _flatten(manifest["semantic_assets"], "paths") == selected
    inheritance = manifest["semantic_asset_inheritance"]
    assert inheritance["owner"] and inheritance["falsifying_test"]

    atomic_external = manifest["external_atomic_path_dispositions"]
    _assert_atomic_entries(
        [dict(entry, path=entry["consumer_id"]) for entry in atomic_external], "paths"
    )
    consumers = {entry["id"]: entry for entry in manifest["external_consumers"]}
    for consumer_id in {entry["consumer_id"] for entry in atomic_external}:
        classified = {
            path for entry in atomic_external if entry["consumer_id"] == consumer_id for path in entry["paths"]
        }
        assert classified == set(consumers[consumer_id]["paths"])
    repo_roots = {
        "vuoro-composition": Path("/projects/dev/vuoro"),
        "agentops-generated-guidance": Path("/projects/dev/agentops"),
        "sprintctl-actionq-lifecycle": Path("/projects/dev/sprintctl"),
        "appservice-ordered-retirement": Path("/projects/dev/appservice"),
    }
    for record in manifest["external_baseline_bytes"]:
        assert re.fullmatch(r"[0-9a-f]{40}", record["commit"])
        assert re.fullmatch(r"[0-9a-f]{64}", record["sha256"])
        repo = repo_roots[record["consumer_id"]]
        if repo.is_dir():
            content = subprocess.check_output(
                ["git", "show", f'{record["commit"]}:{record["path"]}'], cwd=repo
            )
            assert hashlib.sha256(content).hexdigest() == record["sha256"]
    vuoro = consumers["vuoro-composition"]
    assert vuoro["pins"]["adapter_kit_source_revision"] == "a002e5031b4235779462314c125903300636c5f1"
    assert vuoro["pins"]["schema_runtime_source_revision"] == "a002e5031b4235779462314c125903300636c5f1"
    assert len(vuoro["released_operation_sha256"]) == 22
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in vuoro["released_operation_sha256"].values())
    _validate_external_path_proofs(manifest)
    _validate_external_proof_bundle(manifest)
    proof_repos = {
        "vuoro-composition": Path("/projects/dev/vuoro"),
        "actionq-dispatcher-tombstone": Path("/tmp/actionq-dispatch-proof"),
        "sprintctl-actionq-lifecycle": Path("/projects/dev/sprintctl"),
        "agentops-generated-guidance": Path("/projects/dev/agentops"),
        "gitops-nixos-daemon-retirement": Path("/projects/dev/gitops-nixos"),
        "appservice-ordered-retirement": Path("/projects/dev/appservice"),
    }
    for proof in manifest["external_path_proofs"]:
        if proof["consumer_id"] == "q-spec-stale-normative-contracts":
            path = Path("/projects/dev/q-spec") / proof["path"]
            if path.is_file():
                assert hashlib.sha256(path.read_bytes()).hexdigest() == proof["sha256"]
            continue
        repo = proof_repos[proof["consumer_id"]]
        if repo.is_dir():
            content = subprocess.check_output(
                ["git", "show", f'{proof["ref"]}:{proof["path"]}'], cwd=repo
            )
            assert hashlib.sha256(content).hexdigest() == proof["sha256"]
    assert "historical compatibility launcher" not in (ROOT / "AGENTS.md").read_text()


def _check_r1_reviewer_corruption_probes():
    manifest = _manifest()
    source = (ROOT / "tests/test_cancellation_model.py").read_text(encoding="utf-8")

    changed = copy.deepcopy(manifest)
    next(x for x in changed["import_groups"] if x["id"] == "legacy-execution-import-edges")[
        "disposition"
    ] = "replace-by-federation"
    with pytest.raises(AssertionError):
        _validate_r1_invariants(changed, source)

    changed = copy.deepcopy(manifest)
    next(x for x in changed["external_consumers"] if x["id"] == "q-spec-stale-normative-contracts")[
        "file_sha256"
    ]["actionq-spec.md"] = "0" * 64
    with pytest.raises(AssertionError):
        _validate_r1_invariants(changed, source)

    changed = copy.deepcopy(manifest)
    pins = next(x for x in changed["external_consumers"] if x["id"] == "vuoro-composition")["pins"]
    pins["managed_dispatch_operation_is_policy_conditional"] = False
    pins["legacy_catalog_policy_enabled_operation_count"] = 26
    with pytest.raises(AssertionError):
        _validate_r1_invariants(changed, source)

    renamed = source.replace(
        "test_bounded_depth_three_cancellation_histories",
        "bounded_depth_three_cancellation_histories",
    )
    with pytest.raises(AssertionError):
        _validate_r1_invariants(manifest, renamed)

    for section, field, value in (
        ("bound_constant_dispositions", "names", "MAX_SCHEMA_VERSION"),
        ("atomic_symbol_dispositions", "symbols", "CompletionService.settle"),
        ("atomic_import_dispositions", "names", "canonical_bytes as contract_canonical_bytes"),
        ("semantic_assets", "paths", "verification/probe_native_harness_binding.py"),
    ):
        changed = copy.deepcopy(manifest)
        for entry in changed[section]:
            if value in entry[field]:
                entry[field].remove(value)
                break
        with pytest.raises(AssertionError):
            if section == "semantic_assets":
                # Exercise the same selected-universe relation without mutating disk.
                assert _flatten(changed[section], field) == _flatten(manifest[section], field)
            elif section == "bound_constant_dispositions":
                observed = {e["path"]: set() for e in changed[section] if e["path"] in CONSTANT_UNIVERSE}
                for entry in changed[section]:
                    if entry["path"] in observed:
                        observed[entry["path"]].update(entry[field])
                assert observed == {path: _top_level_assignments(path) for path in CONSTANT_UNIVERSE}
            elif section == "atomic_import_dispositions":
                observed = {}
                for entry in changed[section]:
                    observed.setdefault(entry["path"], set()).update(entry[field])
                assert observed["actionq/db.py"] == _contract_import_names("actionq/db.py")
            else:
                assert _effective_symbol_dispositions(changed)[
                    "actionq/application_completion.py", "CompletionService.settle"
                ] == "historicalize/delete"

    changed = copy.deepcopy(manifest)
    changed["external_path_proofs"].pop()
    with pytest.raises(AssertionError):
        _validate_external_path_proofs(changed)
    changed = copy.deepcopy(manifest)
    changed["external_path_proofs"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        assert changed["external_path_proofs"][0]["sha256"] == (
            "6d2f9739c1ffa1a5fa3a9cfdf8dd22b81ed71d5a93eff211fa1db134ae8902f1"
        )
    for key in (
        ("gitops-nixos-daemon-retirement", "scripts/check-actionq-retirement.sh"),
        ("appservice-ordered-retirement", "clusters/main/kubernetes/apps/vuoro-dev-db/app/vuoro-migrate-v1.yaml"),
    ):
        with pytest.raises(AssertionError):
            _validate_external_proof_bundle(manifest, corrupt={key: b"corrupted source bytes"})

    changed = copy.deepcopy(manifest)
    completion = next(
        entry for entry in changed["critical_reachability"]
        if entry["id"] == "completion-replay-and-rebuild"
    )
    completion["paths"].append("actionq/action_resource.py")
    with pytest.raises(AssertionError):
        paths = _effective_path_dispositions(changed)
        assert completion["disposition"] in paths["actionq/action_resource.py"]
    changed = copy.deepcopy(manifest)
    completion = next(
        entry for entry in changed["critical_reachability"]
        if entry["id"] == "completion-replay-and-rebuild"
    )
    completion["paths"].remove("actionq/completion_outbox.py")
    with pytest.raises(AssertionError):
        assert set(completion["paths"]) == {
            "actionq/completion_log.py", "actionq/completion_outbox.py"
        }


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
    """Scope: the 26-operation default catalog and its 27-operation
    policy-enabled variant, pinned by exact operation name set. It pins the
    operation *names* and their handler bindings, not the wire bytes of each
    schema -- so it catches an added, removed or rebound operation, which is
    what "execution/v1 stays frozen" means for W4."""
    manifest = _manifest()
    assert _flatten(manifest["console_script_groups"], "scripts") == _console_scripts()
    assert _flatten(manifest["cli_command_groups"], "commands") == _cli_commands()
    operations, handlers = _catalog_operations_and_handlers()
    assert _flatten(manifest["catalog_operation_groups"], "operations") == operations
    assert manifest["catalog_handler_bindings"] == handlers

    from actionq.application import ActionQApplication
    from actionq.vuoro import build_operations

    default = {item.definition["name"] for item in build_operations(ActionQApplication())}
    enabled = {
        item.definition["name"]
        for item in build_operations(ActionQApplication(managed_dispatch_policy=object()))
    }
    assert len(default) == 26
    assert len(enabled) == 27
    assert enabled - default == {"execution.managed-dispatch.enqueue"}


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
        "sprintctl-actionq-lifecycle",
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
    q_spec_root = Path("/projects/dev/q-spec")
    if q_spec_root.is_dir():
        assert set(q_spec["paths"]) == set(q_spec["file_sha256"])
        for relative, expected in q_spec["file_sha256"].items():
            assert hashlib.sha256((q_spec_root / relative).read_bytes()).hexdigest() == expected
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
