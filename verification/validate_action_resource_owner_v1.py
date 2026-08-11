#!/usr/bin/env python3
"""Dependency-free closure and digest validator for the frozen #2027 bundle."""
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "../../../docs/contracts/action-resource-owner-v1.md": None,
    "legacy-quarantine.json": "actionq-legacy-quarantine-golden/v1",
    "not-found.json": "actionq-http-golden/v1",
    "protocol-responses.json": "actionq-owner-response-goldens/v1",
    "required-histories.json": "actionq-required-histories/v1",
}
def candidate_paths(root: Path) -> list[str]:
    paths = set()
    for pattern in ("actionq/**/*.py", "actionq/migrations/*.sql", "tests/**/*.py"):
        paths.update(path.relative_to(root).as_posix() for path in root.glob(pattern) if path.is_file())
    paths.update(("verification/run_action_resource_history.py", "verification/validate_action_resource_owner_v1.py"))
    return sorted(paths)
RESULT_COMMANDS = {
    "action-resource-owner-pruning.json": "uv run --extra dev python verification/run_action_resource_history.py pruning",
    "action-resource-owner-snapshot-race.json": "uv run --extra dev python verification/run_action_resource_history.py snapshot-race",
    "action-resource-owner-non-disclosure.json": "uv run --extra dev python verification/run_action_resource_history.py non-disclosure",
    "action-resource-owner-redaction.json": "uv run --extra dev python verification/run_action_resource_history.py redaction",
    "action-resource-owner-response-loss.json": "uv run --extra dev python verification/run_action_resource_history.py response-loss",
    "action-resource-owner-bounded-wait.json": "uv run --extra dev python verification/run_action_resource_history.py bounded-wait",
    "action-resource-owner-legacy-quarantine.json": "uv run --extra dev python verification/run_action_resource_history.py legacy-quarantine",
    "action-resource-owner-fencing.json": "uv run --extra dev python verification/run_action_resource_history.py fencing",
}
RESULT_ASSERTIONS = {
    name: [f"action-resource-owner.{name.removeprefix('action-resource-owner-').removesuffix('.json')}.v1"]
    for name in RESULT_COMMANDS
}

class ValidationError(RuntimeError):
    pass

def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)

def validate(root: Path = ROOT) -> None:
    bundle = root / "verification/fixtures/action-resource-owner-v1"
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    require(set(manifest) == {"schema_version", "contract_id", "work_item", "status", "authority_root", "session_role", "reference_pattern", "recovery_floor", "snapshot_handoff", "wait_seconds", "projection_allowlist", "session_allowlist", "event_allowlist", "forbidden_classes", "registered_files"}, "manifest schema is not closed")
    registered = manifest["registered_files"]
    require(set(registered) == set(EXPECTED), "missing, extra, or unregistered normative file")
    actual_files = {
        p.relative_to(bundle).as_posix()
        for p in bundle.rglob("*")
        if p.is_file() and p != manifest_path
    }
    expected_files = {Path(p).name for p in EXPECTED if not p.startswith("../")}
    require(actual_files == expected_files, "bundle contains a missing, extra, or unregistered regular file")
    for relative, expected_schema in EXPECTED.items():
        path = (bundle / relative).resolve()
        record = registered[relative]
        require(set(record) == {"sha256", "schema_version"}, f"registration schema is not closed: {relative}")
        require(record["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest(), f"digest mismatch: {relative}")
        require(record["schema_version"] == expected_schema, f"registered schema mismatch: {relative}")
        if expected_schema is not None:
            require(json.loads(path.read_text(encoding="utf-8"))["schema_version"] == expected_schema, f"fixture schema mismatch: {relative}")
    candidate = hashlib.sha256()
    for relative in candidate_paths(root):
        candidate.update(relative.encode() + b"\0" + (root / relative).read_bytes())
    candidate_ref = "candidate-tree:" + candidate.hexdigest()
    context = json.loads((root / "verification/contexts/action-resource-owner-v1.json").read_text())
    require(context.get("schema_version") == "test-context/v1" and context.get("id") == "actionq.action-resource-owner-v1", "owner evidence context linkage invalid")
    results_dir = root / "verification/results"
    actual_results = {p.name for p in results_dir.glob("action-resource-owner-*.json")}
    require(actual_results == set(RESULT_COMMANDS), "owner result packet set is not closed")
    for name, command in RESULT_COMMANDS.items():
        packet = json.loads((results_dir / name).read_text())
        require(packet.get("schema_version") == "verification-result/v1", f"result schema invalid: {name}")
        require(packet.get("context_id") == context["id"], f"result context linkage invalid: {name}")
        require(packet.get("implementation_sha") == candidate_ref, f"candidate tree digest mismatch: {name}")
        require(packet.get("execution", {}).get("command") == command, f"result command/node mismatch: {name}")
        require(packet.get("execution", {}).get("result") == "passed", f"result is not passed: {name}")
        assertion_ids = packet.get("execution", {}).get("assertion_ids")
        require(assertion_ids == RESULT_ASSERTIONS[name], f"result assertion linkage mismatch: {name}")
        receipt_record = packet.get("execution", {}).get("receipt", {})
        receipt_path = root / str(receipt_record.get("path", ""))
        require(receipt_path.is_file(), f"runtime assertion receipt missing: {name}")
        receipt_bytes = receipt_path.read_bytes()
        require(receipt_record.get("sha256") == hashlib.sha256(receipt_bytes).hexdigest(), f"runtime assertion receipt digest mismatch: {name}")
        receipt = json.loads(receipt_bytes)
        history = assertion_ids[0].removeprefix("action-resource-owner.").removesuffix(".v1")
        require(set(receipt) == {"schema_version", "history", "node", "assertion_ids", "candidate_tree_digest", "command", "returncode", "assertion_stream", "assertion_stream_sha256"}, f"runtime receipt schema is not closed: {name}")
        require(receipt["schema_version"] == "action-resource-history-receipt/v1", f"runtime receipt version invalid: {name}")
        require(receipt["history"] == history and receipt["assertion_ids"] == assertion_ids, f"runtime assertion content mismatch: {name}")
        require(receipt["candidate_tree_digest"] == candidate.hexdigest(), f"runtime receipt candidate mismatch: {name}")
        expected_argv = [".venv/bin/python3", "-m", "pytest", receipt["node"], "-q", "-s"]
        require(receipt["command"] == expected_argv and receipt["returncode"] == 0, f"runtime command proof invalid: {name}")
        expected_stream = "".join(f"OWNER_ASSERTION {assertion_id}\n" for assertion_id in sorted(assertion_ids))
        require(receipt["assertion_stream"] == expected_stream, f"runtime assertion stream mismatch: {name}")
        require(receipt["assertion_stream_sha256"] == hashlib.sha256(expected_stream.encode()).hexdigest(), f"runtime assertion stream digest invalid: {name}")

if __name__ == "__main__":
    selected_root = Path(sys.argv[1]).resolve() if len(sys.argv) == 2 else ROOT
    if len(sys.argv) > 2:
        raise SystemExit("usage: validate_action_resource_owner_v1.py [root]")
    try:
        validate(selected_root)
    except (ValidationError, OSError, KeyError, TypeError, ValueError) as exc:
        print(f"invalid: {exc}", file=sys.stderr)
        raise SystemExit(1)
    print(f"ok {selected_root / 'verification/fixtures/action-resource-owner-v1/manifest.json'}")
