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
import socket
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _repo_write_snapshot() -> str:
    """A cheap whole-repository proxy for 'nothing was written or changed':
    the porcelain status (tracked-file changes plus every untracked file,
    anywhere in the tree -- not just under docs/evidence/). A fail-closed
    path that writes a record-shaped file anywhere in the repo, including
    outside docs/evidence/ (e.g. verification/results/), changes this."""
    result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout

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
    binding_digest,
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


def test_record_1_diff_computation_matches_an_independent_recomputation():
    """Record 1's diff must equal whatever drift genuinely exists today,
    recomputed independently here from the same reachability manifest and
    scan logic the tool claims to reuse -- not a value the tool merely
    asserts about itself, and not a literal snapshot of today's known
    drift, which would go stale (and this test would then fail as a false
    alarm) the moment the manifest owner fixes the underlying
    classification. A capture_record_1 that returned a hardcoded diff
    would fail this test."""
    from tests import test_tranche4_reachability_contract as reachability

    manifest = reachability._manifest()
    api_scan = manifest["repository_scans"]["api_consumers"]
    retired_scan = manifest["repository_scans"]["retired_plane"]
    manifest_consumers = reachability._flatten(manifest["repository_consumer_groups"], "paths")
    manifest_retired = reachability._flatten(manifest["retired_plane_anchor_groups"], "paths")
    observed_consumers = reachability._scan_paths(api_scan["pattern"])
    observed_retired = reachability._scan_paths(retired_scan["pattern"])

    expected_consumer_diff = {
        "extra_in_repository": sorted(observed_consumers - manifest_consumers),
        "missing_from_repository": sorted(manifest_consumers - observed_consumers),
    }
    expected_retired_plane_diff = {
        "extra_in_repository": sorted(observed_retired - manifest_retired),
        "missing_from_repository": sorted(manifest_retired - observed_retired),
    }

    record = capture_record_1()
    payload = record.payload
    assert payload["consumer_diff"] == expected_consumer_diff
    assert payload["retired_plane_diff"] == expected_retired_plane_diff
    assert payload["consumer_diff_is_empty"] == (
        not (expected_consumer_diff["extra_in_repository"] or expected_consumer_diff["missing_from_repository"])
    )
    assert payload["retired_plane_diff_is_empty"] == (
        not (expected_retired_plane_diff["extra_in_repository"] or expected_retired_plane_diff["missing_from_repository"])
    )


def test_record_1_diff_sections_report_the_currently_known_drift():
    """Pins the specific drift known at the time this packet landed, as a
    reported finding for a human to act on -- not as the tooling's
    acceptance gate (see test_record_1_diff_computation_matches_an_independent_recomputation
    for that). When the manifest owner classifies either path, this test
    (and only this test) is expected to need updating; that failure names
    the resolved discrepancy rather than looking like a tooling
    regression."""
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


def test_record_1_never_writes_anywhere_in_the_repository():
    """capture_record_1 returns a record in memory; it must never write
    anything to disk on its own, anywhere in the repository -- not just
    under docs/evidence/."""
    before = _repo_write_snapshot()
    capture_record_1()
    after = _repo_write_snapshot()
    assert before == after


def test_record_1_cli_refuses_to_write_under_docs_evidence(tmp_path):
    """The --out guard: the CLI must refuse to author a record-shaped file
    under docs/evidence/, which is the operator's directory alone."""
    before = _repo_write_snapshot()
    forbidden_out = ROOT / "docs/evidence" / "should-not-be-written.json"
    result = subprocess.run(
        [sys.executable, str(ROOT / "verification/capture_w5_evidence.py"), "record-1", "--out", str(forbidden_out)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    after = _repo_write_snapshot()
    assert result.returncode != 0
    assert not forbidden_out.exists()
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


def test_record_1_makes_no_network_call(monkeypatch):
    """Falsifier 1, made behavioural rather than only environment-shaped:
    block every socket-creation path before calling capture_record_1() and
    confirm it still succeeds. A record-1 implementation that needs a
    live network peer or a cluster config to run would fail this test even
    though it might still exit 0 under a merely-stripped environment."""

    def _forbidden(*_args, **_kwargs):
        raise RuntimeError("network use in an offline record-1 capture")

    monkeypatch.setattr(socket, "socket", _forbidden)
    monkeypatch.setattr(socket, "create_connection", _forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", _forbidden)

    record = capture_record_1()
    assert record.schema_version == "w5-repository-check/v1"


# ---------------------------------------------------------------------------
# Records 2-7: fail closed with no live input, and no fallback even with
# inputs present but no wired capture backend.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("number", sorted(RECORD_REQUIREMENTS))
def test_live_record_fails_closed_with_no_inputs_and_writes_nothing(number, tmp_path):
    before = _repo_write_snapshot()
    with pytest.raises(EvidenceCaptureError) as excinfo:
        capture_live_record(number, {})
    assert "missing required live input" in str(excinfo.value)
    after = _repo_write_snapshot()
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


# ---------------------------------------------------------------------------
# Records 2-7: the with-a-backend branch (an operator's own integration).
# ---------------------------------------------------------------------------


def test_live_record_with_a_complete_backend_result_yields_a_captured_evidence_record():
    inputs = {req.name: "present-but-not-real" for req in RECORD_REQUIREMENTS[3]}

    def stub_backend(number, given_inputs):
        assert number == 3
        assert given_inputs == inputs
        return {
            "environment": {"cluster": "staging", "namespace_or_scope": "actionq"},
            "database_endpoint_fingerprint": "d" * 64,
            "actionq_release": "0.1.28",
            "actionq_deployment_revision": "deadbeefcafefeed0000000000000000000dead",
            "vuoro_release": "0.1.1",
            "vuoro_deployment_revision": "cafefeed0000000000000000000deadbeefcafe",
            "finding": "role x has no direct DML grant",
        }

    record = capture_live_record(3, inputs, capture_backend=stub_backend)
    assert isinstance(record, CapturedEvidenceRecord)
    assert record.schema_version == "w5-captured-evidence/v1"
    assert record.payload == {"finding": "role x has no direct DML grant"}


def test_live_record_with_a_backend_missing_a_binding_field_fails_closed_not_with_keyerror():
    inputs = {req.name: "present-but-not-real" for req in RECORD_REQUIREMENTS[3]}

    def stub_backend(number, given_inputs):
        return {"environment": {"cluster": "staging", "namespace_or_scope": "actionq"}}
        # every other required binding field is missing

    with pytest.raises(EvidenceCaptureError) as excinfo:
        capture_live_record(3, inputs, capture_backend=stub_backend)
    assert "missing" in str(excinfo.value)
    assert "database_endpoint_fingerprint" in str(excinfo.value)


def test_live_record_with_a_backend_returning_invalid_values_fails_closed():
    inputs = {req.name: "present-but-not-real" for req in RECORD_REQUIREMENTS[3]}

    def stub_backend(number, given_inputs):
        return {
            "environment": {"cluster": "staging", "namespace_or_scope": "actionq"},
            "database_endpoint_fingerprint": "not-a-real-fingerprint",
            "actionq_release": "0.1.28",
            "actionq_deployment_revision": "deadbeefcafefeed0000000000000000000dead",
            "vuoro_release": "0.1.1",
            "vuoro_deployment_revision": "cafefeed0000000000000000000deadbeefcafe",
        }

    with pytest.raises(EvidenceCaptureError) as excinfo:
        capture_live_record(3, inputs, capture_backend=stub_backend)
    assert "envelope validation" in str(excinfo.value)


def test_a_captured_evidence_record_cannot_be_constructed_with_an_unset_binding_field():
    """Direct falsifier for the envelope's structural guarantee: no code
    path -- not even a direct call bypassing capture_live_record -- can
    build a w5-captured-evidence/v1 object with a binding field unset or
    literal-filled."""
    actor = Actor(identity="operator@example.com", method="operator-supplied")
    tool_versions = ToolVersions(versions={"tool": "1.0.0"})
    binding_fields = dict(
        schema_version="w5-captured-evidence/v1",
        record_id="w5-record-3",
        captured_at="2026-09-08T00:00:00+00:00",
        environment={"cluster": "prod", "namespace_or_scope": "actionq"},
        database_endpoint_fingerprint="a" * 64,
        actionq_release="0.1.28",
        actionq_deployment_revision="deadbeefcafefeed0000000000000000000dead",
        vuoro_release="0.1.1",
        vuoro_deployment_revision="cafefeed0000000000000000000deadbeefcafe",
    )
    result_digest = binding_digest(
        schema_version=binding_fields["schema_version"],
        record_id=binding_fields["record_id"],
        captured_at=binding_fields["captured_at"],
        environment=binding_fields["environment"],
        actor=actor,
        tool_versions=tool_versions,
        payload={"finding": "denied"},
        extra={
            "database_endpoint_fingerprint": binding_fields["database_endpoint_fingerprint"],
            "actionq_release": binding_fields["actionq_release"],
            "actionq_deployment_revision": binding_fields["actionq_deployment_revision"],
            "vuoro_release": binding_fields["vuoro_release"],
            "vuoro_deployment_revision": binding_fields["vuoro_deployment_revision"],
        },
    )
    complete_kwargs = dict(
        binding_fields,
        actor=actor,
        tool_versions=tool_versions,
        payload={"finding": "denied"},
        result_digest=result_digest,
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
    actor = Actor(identity="a@example.com", method="operator-supplied")
    tool_versions = ToolVersions(versions={"t": "1"})
    with pytest.raises(ValueError, match="schema_version must be exactly"):
        RepositoryCheckRecord(
            schema_version="w5-captured-evidence/v1",
            record_id="x",
            captured_at="2026-09-08T00:00:00+00:00",
            environment={"git_commit": "a" * 40, "git_branch": "main", "repo_root": "/x"},
            actor=actor,
            tool_versions=tool_versions,
            payload={"k": "v"},
            result_digest="c" * 64,
        )


def test_no_function_in_the_capture_module_converts_one_record_type_to_the_other():
    """No callable in either verification module may take a
    RepositoryCheckRecord and produce a CapturedEvidenceRecord, or vice
    versa -- checked syntactically (a lint, not a proof) across functions,
    methods, and classmethods in both capture_w5_evidence.py and
    envelope.py, and by substring rather than exact match so a union
    annotation or an unparenthesized default does not evade it."""
    import inspect

    import verification.capture_w5_evidence as capture_module
    import verification.w5_evidence.envelope as envelope_module

    def _mentions(text: str, name: str) -> bool:
        return name in text

    def _callables(module):
        for _, obj in vars(module).items():
            if inspect.isfunction(obj) and obj.__module__ == module.__name__:
                yield obj
            elif inspect.isclass(obj) and obj.__module__ == module.__name__:
                for _, member in vars(obj).items():
                    fn = inspect.unwrap(member) if isinstance(member, (staticmethod, classmethod)) else member
                    if inspect.isfunction(fn):
                        yield fn

    for module in (capture_module, envelope_module):
        for obj in _callables(module):
            try:
                signature = inspect.signature(obj)
            except (TypeError, ValueError):
                continue
            annotation_text = " ".join(
                str(p.annotation) for p in signature.parameters.values() if p.annotation is not inspect.Parameter.empty
            )
            annotation_text += " " + str(signature.return_annotation)
            involves_repo_check = _mentions(annotation_text, "RepositoryCheckRecord")
            involves_captured = _mentions(annotation_text, "CapturedEvidenceRecord")
            assert not (involves_repo_check and involves_captured), (
                f"{module.__name__}.{obj.__qualname__} touches both record types -- "
                "no callable may convert one into the other"
            )

    # Behavioural spot-check: record 1's real output must not itself be
    # accepted anywhere a CapturedEvidenceRecord is expected.
    import verification.capture_w5_evidence as module

    record = capture_record_1()
    for name, obj in vars(module).items():
        if not inspect.isfunction(obj) or obj.__module__ != module.__name__:
            continue
        if name in ("capture_record_1",):
            continue
        try:
            result = obj(record)
        except Exception:
            continue
        assert not isinstance(result, CapturedEvidenceRecord), (
            f"{name}(record_1_output) produced a CapturedEvidenceRecord"
        )
