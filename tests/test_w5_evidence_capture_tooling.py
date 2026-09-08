"""Tests for the W5 evidence-capture tooling (verification/capture_w5_evidence.py).

This suite proves the two falsifiers named by the frozen contract for
packet actionq-evidence-tooling:

  1. Record 1 is producible fully offline, with zero live inputs, and its
     output validates against the w5-repository-check/v1 envelope.
  2. Records 2-7, invoked with any required live input missing, exit
     non-zero / raise and produce nothing record-shaped -- never a
     source-tree substitute, never a fixture fallback.

It never authors, edits, or asserts the content of an actual evidence
record under docs/evidence/, and never touches
docs/contracts/tranche4-reachability-v1.json.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

from verification.capture_w5_evidence import (  # noqa: E402
    RECORD_REQUIREMENTS,
    EvidenceCaptureError,
    capture_live_record,
    capture_record_1,
)
from verification.w5_evidence.envelope import (  # noqa: E402
    Actor,
    CapturedEvidenceRecord,
    RepositoryCheckRecord,
    ToolVersions,
)


# ---------------------------------------------------------------------------
# Record 1: fully offline, zero live inputs.
# ---------------------------------------------------------------------------


def test_record_1_succeeds_offline_with_zero_live_inputs():
    record = capture_record_1()
    assert isinstance(record, RepositoryCheckRecord)
    assert record.schema_version == "w5-repository-check/v1"


def test_record_1_output_validates_against_the_envelope():
    record = capture_record_1()
    as_dict = record.to_dict()
    # Round-trip through the exact validating constructor: an envelope
    # cannot be reconstructed with any field missing.
    rebuilt = RepositoryCheckRecord(
        schema_version=as_dict["schema_version"],
        record_id=as_dict["record_id"],
        captured_at=as_dict["captured_at"],
        environment=as_dict["environment"],
        actor=Actor(**as_dict["actor"]),
        tool_versions=ToolVersions(versions=as_dict["tool_versions"]),
        payload=as_dict["payload"],
        result_digest=as_dict["result_digest"],
    )
    assert rebuilt.to_dict() == as_dict
    # JSON-serializable, as a capture record must be.
    json.dumps(as_dict)


def test_record_1_diff_sections_report_the_current_real_drift():
    """Record 1 must report whatever drift genuinely exists today, not a
    hardcoded expectation. As of this packet, two independent, real,
    nonempty diffs exist against docs/contracts/tranche4-reachability-v1.json:
    a retired-plane-pattern match on the tracked generated file
    .agents/project.generated.md, and an unclassified api-consumer match on
    tests/test_federation_ownership_authority.py (added by an earlier
    packet in this chain). This test asserts the tool surfaces both -- it
    must never paper over a real diff to make its own output look clean."""
    record = capture_record_1()
    payload = record.payload
    assert payload["consumer_diff_is_empty"] is False
    assert payload["retired_plane_diff_is_empty"] is False
    assert "tests/test_federation_ownership_authority.py" in payload["consumer_diff"]["extra_in_repository"]
    assert ".agents/project.generated.md" in payload["retired_plane_diff"]["extra_in_repository"]


def test_record_1_job_status_unavailable_without_a_live_jobs_source():
    record = capture_record_1()
    assert record.payload["job_status"] == {
        "available": False,
        "reason": "no live jobs source provided to this capture run",
    }


def test_record_1_job_status_reports_status_not_mere_existence_when_provided():
    """Backlog 5.9: a suspended job is not a run. When a live jobs source IS
    provided, the tool must carry status counts through untouched -- it must
    never collapse a job's mere existence into an inferred 'ran' status."""
    live_jobs = {"job-a": "suspended", "job-b": "completed", "job-c": "suspended"}
    record = capture_record_1(live_jobs=live_jobs)
    job_status = record.payload["job_status"]
    assert job_status["available"] is True
    assert job_status["job_count"] == 3
    assert job_status["status_counts"] == {"suspended": 2, "completed": 1}


def test_record_1_never_writes_under_docs_evidence():
    """capture_record_1 returns a record in memory; it must never write
    anything to disk on its own, least of all under docs/evidence/."""
    before = set((ROOT / "docs/evidence").rglob("*")) if (ROOT / "docs/evidence").exists() else set()
    capture_record_1()
    after = set((ROOT / "docs/evidence").rglob("*")) if (ROOT / "docs/evidence").exists() else set()
    assert before == after


def test_record_1_cli_runs_fully_offline_and_exits_zero(tmp_path):
    """The falsifier: a record-1 path that needs network, a database, or a
    cluster is a failure. Run the actual CLI subprocess with a stripped,
    minimal environment (no credentials, no database URL, no network-shaped
    variables) and confirm it still succeeds."""
    out_path = tmp_path / "record1.json"
    stripped_env = {
        key: value
        for key, value in os.environ.items()
        if not any(
            token in key.upper()
            for token in ("DATABASE", "DB_", "CREDENTIAL", "TOKEN", "PASSWORD", "SECRET", "ENDPOINT", "URL")
        )
    }
    stripped_env["HOME"] = str(tmp_path)
    result = subprocess.run(
        [sys.executable, str(ROOT / "verification/capture_w5_evidence.py"), "record-1", "--out", str(out_path)],
        cwd=ROOT,
        env=stripped_env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    written = json.loads(out_path.read_text(encoding="utf-8"))
    assert written["schema_version"] == "w5-repository-check/v1"


# ---------------------------------------------------------------------------
# Records 2-7: fail closed with no live input, and no fallback even with
# inputs present but no wired capture backend.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("number", sorted(RECORD_REQUIREMENTS))
def test_live_record_fails_closed_with_no_inputs_and_writes_nothing(number, tmp_path):
    before = set((ROOT / "docs/evidence").rglob("*")) if (ROOT / "docs/evidence").exists() else set()
    with pytest.raises(EvidenceCaptureError) as excinfo:
        capture_live_record(number, {})
    assert "missing required live input" in str(excinfo.value)
    after = set((ROOT / "docs/evidence").rglob("*")) if (ROOT / "docs/evidence").exists() else set()
    assert before == after


@pytest.mark.parametrize("number", sorted(RECORD_REQUIREMENTS))
def test_live_record_fails_closed_with_partial_inputs(number):
    requirements = RECORD_REQUIREMENTS[number]
    if len(requirements) < 2:
        pytest.skip("record has a single required input; partial-input case is the no-input case")
    partial = {requirements[0].name: "present-but-not-real"}
    with pytest.raises(EvidenceCaptureError) as excinfo:
        capture_live_record(number, partial)
    assert "missing required live input" in str(excinfo.value)
    assert requirements[1].name in str(excinfo.value)


@pytest.mark.parametrize("number", sorted(RECORD_REQUIREMENTS))
def test_live_record_fails_closed_even_with_all_inputs_present_and_no_backend(number):
    """No fixture fallback, no source-tree substitute: even with every
    required input string present, absent a real capture backend wired by
    an operator integration, the tool must still refuse to produce a
    record-shaped object rather than synthesizing one."""
    inputs = {req.name: "present-but-not-real" for req in RECORD_REQUIREMENTS[number]}
    with pytest.raises(EvidenceCaptureError) as excinfo:
        capture_live_record(number, inputs)
    assert "no live capture backend is wired" in str(excinfo.value)


@pytest.mark.parametrize("number", sorted(RECORD_REQUIREMENTS))
def test_live_record_cli_exits_nonzero_with_no_inputs_and_writes_nothing(number, tmp_path):
    """Reviewer check named by the contract: run each of records 2-7 with
    the environment stripped and confirm a non-zero exit and an empty
    output directory."""
    out_dir = tmp_path / "evidence-out"
    out_dir.mkdir()
    stripped_env = {
        key: value
        for key, value in os.environ.items()
        if not any(
            token in key.upper()
            for token in ("DATABASE", "DB_", "CREDENTIAL", "TOKEN", "PASSWORD", "SECRET", "ENDPOINT", "URL")
        )
    }
    stripped_env["HOME"] = str(tmp_path)
    result = subprocess.run(
        [sys.executable, str(ROOT / "verification/capture_w5_evidence.py"), f"record-{number}"],
        cwd=out_dir,
        env=stripped_env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "error:" in result.stderr
    assert list(out_dir.iterdir()) == []


def test_a_captured_evidence_record_cannot_be_constructed_with_an_unset_binding_field():
    """Direct falsifier for the envelope's structural guarantee: no code
    path -- not even a direct call bypassing capture_live_record -- can
    build a w5-captured-evidence/v1 object with a binding field unset or
    literal-filled."""
    complete_kwargs = dict(
        schema_version="w5-captured-evidence/v1",
        record_id="w5-record-3",
        captured_at="2026-09-08T00:00:00+00:00",
        environment={"cluster": "prod", "namespace_or_scope": "actionq"},
        database_endpoint_fingerprint="a" * 64,
        actionq_release="0.1.28",
        actionq_deployment_revision="deadbeefcafefeed0000000000000000000dead",
        vuoro_release="0.1.1",
        vuoro_deployment_revision="cafefeed0000000000000000000deadbeefcafe",
        actor=Actor(identity="operator@example.com", method="operator-supplied"),
        tool_versions=ToolVersions(versions={"tool": "1.0.0"}),
        payload={"finding": "denied"},
        result_digest="b" * 64,
    )
    # Sanity: the fully-populated construction succeeds.
    CapturedEvidenceRecord(**complete_kwargs)

    for missing_field, bad_value in [
        ("actionq_release", ""),
        ("actionq_release", "unknown"),
        ("actionq_release", "TODO"),
        ("vuoro_deployment_revision", "placeholder"),
        ("database_endpoint_fingerprint", "not-a-real-fingerprint"),
    ]:
        bad_kwargs = dict(complete_kwargs)
        bad_kwargs[missing_field] = bad_value
        with pytest.raises(ValueError):
            CapturedEvidenceRecord(**bad_kwargs)


def test_a_repository_check_record_cannot_carry_the_captured_evidence_schema_version():
    """The two schema_version strings are structurally distinct: a
    RepositoryCheckRecord literally cannot be constructed with the
    'w5-captured-evidence/v1' schema_version, so a repository test result
    can never masquerade as deployment evidence."""
    with pytest.raises(ValueError):
        RepositoryCheckRecord(
            schema_version="w5-captured-evidence/v1",
            record_id="x",
            captured_at="2026-09-08T00:00:00+00:00",
            environment={"git_commit": "a" * 40, "git_branch": "main", "repo_root": "/x"},
            actor=Actor(identity="a@example.com", method="test"),
            tool_versions=ToolVersions(versions={"t": "1"}),
            payload={"k": "v"},
            result_digest="c" * 64,
        )


def test_no_function_in_the_capture_module_converts_one_record_type_to_the_other():
    """There must be no code path anywhere in the capture module whose
    input is a RepositoryCheckRecord (or its .to_dict()) and whose output
    is a CapturedEvidenceRecord, or vice versa."""
    import inspect

    import verification.capture_w5_evidence as module

    for name, obj in vars(module).items():
        if not inspect.isfunction(obj) or obj.__module__ != module.__name__:
            continue
        signature = inspect.signature(obj)
        params = {
            p.annotation
            for p in signature.parameters.values()
            if isinstance(p.annotation, str)
        }
        return_annotation = signature.return_annotation
        involves_repo_check = "RepositoryCheckRecord" in params or return_annotation == "RepositoryCheckRecord"
        involves_captured = "CapturedEvidenceRecord" in params or return_annotation == "CapturedEvidenceRecord"
        assert not (involves_repo_check and involves_captured), (
            f"{name} touches both record types -- no function may convert one into the other"
        )
