import json
import re
import runpy
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_action_claim_context_has_stable_protocol_anchors():
    packet = json.loads(
        (ROOT / "verification/contexts/action-claim-concurrency.json").read_text(encoding="utf-8")
    )

    assert packet["schema_version"] == "test-context/v1"
    assert packet["contract_ref"]["doc_id"] == "actionq.action-lifecycle"
    assert packet["depth"] == 2
    assert "actionq/db.py:claim" in packet["implementation_anchors"]
    assert "at-most-one-completed-claim-per-action-incarnation" in packet["invariants"]


def test_protocol_document_records_terminal_fencing():
    protocol = (ROOT / "docs/protocols/action-lifecycle.md").read_text(encoding="utf-8")

    assert "Terminal transitions require matching worker and claim receipt" in protocol
    assert "expired-but-unswept claimant cannot settle" in protocol


def _owner_fixture(name: str):
    path = ROOT / "verification/fixtures/action-resource-owner-v1" / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_2027_frozen_owner_fixture_resolves_authority_and_bounds():
    manifest = _owner_fixture("manifest.json")

    assert manifest["status"] == "frozen-awaiting-independent-go"
    assert manifest["authority_root"] == "action-root"
    assert manifest["session_role"] == "projection-only"
    assert manifest["recovery_floor"] == {
        "meaning": "smallest-resumable-cursor-position", "nullable": False,
        "initial": 0, "monotonic": True, "scope": "per-resource"
    }
    assert manifest["snapshot_handoff"]["allowed"] == ["one-statement", "repeatable-read"]
    assert manifest["wait_seconds"] == {
        "minimum": 0, "maximum": 30, "default": 0, "grace_seconds": 1
    }
    assert re.fullmatch(manifest["reference_pattern"], "aqr1_" + "A" * 43)
    assert not re.fullmatch(manifest["reference_pattern"], "aqr1_" + "A" * 42)


def test_2027_not_found_golden_is_identical_for_every_nondisclosure_class():
    golden = _owner_fixture("not-found.json")
    expected = (
        b'{"error":{"code":"resource_not_found","message":"resource not found"},'
        b'"schema_version":"resource-reference/v1"}\n'
    )

    assert list(golden["cases"]) == ["malformed", "absent", "foreign", "unauthorized"]
    wire = []
    for case in golden["cases"].values():
        assert case["body_utf8"].encode("utf-8") == expected
        assert case["headers"] == [["content-type", "application/json"], ["content-length", "112"], ["cache-control", "no-store"]]
        wire.append((case["status"], tuple(map(tuple, case["headers"])), case["body_utf8"].encode()))
    assert len(set(wire)) == 1


def test_2027_projection_and_event_allowlists_exclude_required_sensitive_classes():
    manifest = _owner_fixture("manifest.json")
    serialized_allowlists = " ".join(
        manifest["projection_allowlist"] + manifest["session_allowlist"] + manifest["event_allowlist"]
    )

    assert manifest["forbidden_classes"] == [
        "secret-like", "result-reference", "failure-detail", "claim", "provenance", "request-snapshot"
    ]
    for forbidden_fragment in ("secret", "result", "failure", "claim", "provenance", "request", "payload", "metadata"):
        assert forbidden_fragment not in serialized_allowlists


def test_2027_legacy_quarantine_matrix_is_fixed_and_precedes_side_effects():
    golden = _owner_fixture("legacy-quarantine.json")
    expected_body = (
        b'{"error":{"code":"legacy_route_not_found","message":"route not found"},'
        b'"schema_version":"actionq-quarantine/v1"}\n'
    )

    assert golden["methods"] == ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "FROB"]
    assert golden["classification_order"] == ["split-query-from-raw-target", "classify-raw-path-octets"]
    assert golden["cross_product_count"] == len(golden["methods"]) * len(golden["path_cases"]) == 64
    assert golden["response"]["body_utf8"].encode("utf-8") == expected_body
    assert golden["response"]["headers"] == [["content-type", "application/json"], ["content-length", "113"], ["cache-control", "no-store"]]
    cases = {case["raw_target"]: case for case in golden["path_cases"]}
    assert cases["/v2/dispatch/%2e%2e"] == {"raw_target": "/v2/dispatch/%2e%2e", "raw_path": "/v2/dispatch/%2e%2e", "decoded_path": "/v2/dispatch/..", "normalized_path": "/v2", "quarantined": True}
    assert cases["/v2//dispatch"]["normalized_path"] == "/v2/dispatch"
    assert cases["/v2//dispatch"]["quarantined"] is False
    assert golden["must_precede"] == [
        "body-read", "authentication", "authorization", "application-construction", "database-access"
    ]


def test_2027_history_fixture_requires_all_falsifying_postgres_proofs():
    fixture = _owner_fixture("required-histories.json")

    assert fixture["backend"] == "disposable-postgresql"
    assert len(fixture["histories"]) == 8
    assert {
        "retention-none-some-all-terminal-restart",
        "snapshot-cursor-concurrent-terminal",
        "non-disclosure-malformed-absent-foreign-unauthorized",
        "recursive-redaction-canaries",
        "enqueue-commit-response-loss-original-reference-revision",
        "bounded-wait-zero-thirty-early-spurious-disconnect-restart",
        "legacy-quarantine-pre-auth-app-body-db",
        "stale-claim-session-fencing",
    } == set(fixture["histories"])
    assert "recovery-floor-durable-non-null-in-every-pruning-state" in fixture["required_assertions"]


def test_2027_protocol_goldens_close_every_required_response_and_binding():
    fixture = _owner_fixture("protocol-responses.json")
    responses = fixture["responses"]
    assert set(responses) == {
        "enqueue_success", "enqueue_original_retry", "enqueue_conflict", "snapshot", "changes",
        "cursor_expired", "wait_empty", "wait_early", "wait_terminal", "wait_invalid_negative",
        "wait_invalid_fractional", "wait_invalid_non_numeric", "wait_invalid_over_max", "cursor_wrong_resource",
        "cursor_wrong_principal_scope", "cursor_tampered",
    }
    assert responses["enqueue_success"]["body"]["resource_ref"] == responses["enqueue_original_retry"]["body"]["resource_ref"]
    assert responses["enqueue_success"]["body"]["revision"] == responses["enqueue_original_retry"]["body"]["revision"] == 1
    assert responses["cursor_expired"]["body"]["recovery_floor"] == 5
    assert responses["wait_empty"]["body"]["changes"] == []
    assert responses["wait_terminal"]["body"]["changes"][0]["terminal"] is True
    assert fixture["cursor_binding"] == ["resource_ref", "principal_scope", "integrity"]


def test_2027_bundle_validator_accepts_closed_digest_linked_bundle():
    namespace = runpy.run_path(str(ROOT / "verification/validate_action_resource_owner_v1.py"))
    namespace["validate"]()


def test_2027_bundle_validator_falsifies_missing_and_digest_drift(tmp_path):
    namespace = runpy.run_path(str(ROOT / "verification/validate_action_resource_owner_v1.py"))
    bundle = tmp_path / "verification/fixtures/action-resource-owner-v1"
    bundle.parent.mkdir(parents=True)
    shutil.copytree(ROOT / "verification/fixtures/action-resource-owner-v1", bundle)
    (tmp_path / "docs/contracts").mkdir(parents=True)
    shutil.copy2(ROOT / "docs/contracts/action-resource-owner-v1.md", tmp_path / "docs/contracts")

    (bundle / "protocol-responses.json").unlink()
    try:
        namespace["validate"](tmp_path)
    except namespace["ValidationError"]:
        pass
    else:
        raise AssertionError("missing registered fixture was accepted")

    shutil.copy2(ROOT / "verification/fixtures/action-resource-owner-v1/protocol-responses.json", bundle)
    (bundle / "protocol-responses.json").write_text("{}\n", encoding="utf-8")
    try:
        namespace["validate"](tmp_path)
    except namespace["ValidationError"]:
        pass
    else:
        raise AssertionError("fixture digest drift was accepted")


def test_2027_bundle_validator_rejects_tamper_and_stray_file_under_python_optimized(tmp_path):
    validator = ROOT / "verification/validate_action_resource_owner_v1.py"
    bundle = tmp_path / "verification/fixtures/action-resource-owner-v1"
    bundle.parent.mkdir(parents=True)
    shutil.copytree(ROOT / "verification/fixtures/action-resource-owner-v1", bundle)
    (tmp_path / "docs/contracts").mkdir(parents=True)
    shutil.copy2(ROOT / "docs/contracts/action-resource-owner-v1.md", tmp_path / "docs/contracts")

    (bundle / "stray.txt").write_text("unregistered\n", encoding="utf-8")
    stray = subprocess.run([sys.executable, "-O", str(validator), str(tmp_path)], capture_output=True, text=True)
    assert stray.returncode == 1
    assert "unregistered regular file" in stray.stderr

    (bundle / "stray.txt").unlink()
    (bundle / "nested").mkdir()
    (bundle / "nested/stray.txt").write_text("unregistered nested\n", encoding="utf-8")
    nested_stray = subprocess.run([sys.executable, "-O", str(validator), str(tmp_path)], capture_output=True, text=True)
    assert nested_stray.returncode == 1
    assert "unregistered regular file" in nested_stray.stderr

    (bundle / "nested/stray.txt").unlink()
    (bundle / "nested").rmdir()
    (bundle / "protocol-responses.json").write_text("{}\n", encoding="utf-8")
    tamper = subprocess.run([sys.executable, "-O", str(validator), str(tmp_path)], capture_output=True, text=True)
    assert tamper.returncode == 1
    assert "digest mismatch" in tamper.stderr
