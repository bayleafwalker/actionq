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
