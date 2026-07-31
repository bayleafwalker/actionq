from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from actionq_contracts import EXECUTION_ENVELOPE_V1, EXECUTION_V1
from actionq_runner.publisher import (
    FilesystemCAS,
    MAX_REDACTED_LOG_BYTES,
    acknowledge_settlement,
    artifact_ref,
    list_publications,
    publish,
    query_settlement,
    recover_publication,
    resume_publication,
)


def git(repo: Path, *args: str, input: bytes | None = None) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], input=input, capture_output=True, check=True,
    )
    return result.stdout.decode().strip()


@pytest.fixture
def candidate(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "runner@example.invalid")
    git(repo, "config", "user.name", "Runner Test")
    (repo / "rename-me").write_text("old\n")
    (repo / "mode").write_text("#!/bin/sh\nexit 0\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "source")
    source = git(repo, "rev-parse", "HEAD")
    git(repo, "mv", "rename-me", "renamed")
    (repo / "binary").write_bytes(b"\x00\xff\x10binary\x00")
    os.symlink("renamed", repo / "link")
    os.chmod(repo / "mode", 0o755)
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "candidate")
    head = git(repo, "rev-parse", "HEAD")
    root = tmp_path / "durable"
    root.mkdir(mode=0o700)
    packet = {
        "artifact_root": str(root), "action_id": 2032, "attempt_id": "attempt-1",
        "worktree": str(repo), "source_commit": source, "candidate_commit": head,
        "execution_envelope": {
            "contract_id": EXECUTION_ENVELOPE_V1, "action_id": 2032, "attempt_id": "attempt-1",
            "source_commit": source, "command_id": "registered:test", "allowed_paths": [],
        },
        "execution": {
            "contract_id": EXECUTION_V1, "action_id": 2032, "attempt_id": "attempt-1",
            "command_id": "registered:test", "exit_code": 0, "timed_out": False,
        },
        "verification_outcome": "passed", "verification_evidence": {"tests": ["ok"]},
        "redacted_log": "all checks passed\n",
    }
    return repo, root, packet


def test_publication_is_idempotent_restartable_and_bundle_is_cloneable(candidate, tmp_path: Path):
    repo, root, packet = candidate
    first = publish(packet, allow_unsafe_test_root=True)
    second = publish(packet, allow_unsafe_test_root=True)
    assert first == second
    assert first["journal_ref"].startswith("artifact:sha256:")
    assert list_publications(root, 2032, allow_unsafe_test_root=True) == [first]
    assert recover_publication(root, 2032, "attempt-1", allow_unsafe_test_root=True) == first

    cas = FilesystemCAS(root, allow_unsafe_test_root=True)
    bundle = tmp_path / "candidate.bundle"
    bundle.write_bytes(cas.get(first["records"]["bundle"]))
    git(repo, "bundle", "verify", str(bundle))
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", "-q", str(bundle), str(clone)], check=True)
    assert (clone / "binary").read_bytes() == b"\x00\xff\x10binary\x00"
    assert (clone / "link").is_symlink() and os.readlink(clone / "link") == "renamed"
    assert (clone / "mode").stat().st_mode & stat.S_IXUSR
    assert not (clone / "rename-me").exists() and (clone / "renamed").read_text() == "old\n"

    manifest = json.loads(cas.get(first["records"]["manifest"]))
    assert manifest["changed_paths"] == sorted(["binary", "link", "mode", "renamed"])
    receipt = json.loads(cas.get(first["journal_ref"]))
    assert receipt["records"] == first["records"]


@pytest.mark.parametrize("stage", ["bundle", "objects"])
def test_fault_recovery_after_durable_stage(candidate, stage):
    _, root, packet = candidate
    with pytest.raises(RuntimeError, match="injected"):
        publish(packet, fault_after=stage, allow_unsafe_test_root=True)
    result = publish(packet, allow_unsafe_test_root=True)
    assert recover_publication(root, 2032, allow_unsafe_test_root=True) == result


@pytest.mark.parametrize("stage", ["bundle", "objects"])
def test_fresh_process_resumes_from_sanitized_intent_without_packet(candidate, stage):
    _, root, packet = candidate
    with pytest.raises(RuntimeError, match="injected"):
        publish(packet, fault_after=stage, allow_unsafe_test_root=True)
    completed = subprocess.run(
        [sys.executable, "-c", (
            "import json,sys; from actionq_runner.publisher import resume_publication; "
            "print(json.dumps(resume_publication(sys.argv[1],2032,'attempt-1',"
            "allow_unsafe_test_root=True),sort_keys=True))"
        ), str(root)],
        capture_output=True, text=True, check=True,
    )
    result = json.loads(completed.stdout)
    assert result["action_id"] == 2032
    assert recover_publication(root, 2032, allow_unsafe_test_root=True) == result


def test_intent_precedes_cas_objects_lists_incomplete_and_rejects_conflict(candidate, monkeypatch):
    _, root, packet = candidate
    original_put = FilesystemCAS.put

    def checked_put(store, value):
        assert (root / "journals/2032/attempt-1/intent.json").is_file()
        return original_put(store, value)

    monkeypatch.setattr(FilesystemCAS, "put", checked_put)
    with pytest.raises(RuntimeError, match="injected"):
        publish(packet, fault_after="bundle", allow_unsafe_test_root=True)
    intent = json.loads((root / "journals/2032/attempt-1/intent.json").read_bytes())
    assert intent["stage"] == "intent"
    assert "artifact_root" not in intent and "claim_receipt" not in intent
    incomplete = list_publications(root, 2032, allow_unsafe_test_root=True)
    assert incomplete == [{
        "action_id": 2032, "attempt_id": "attempt-1", "status": "incomplete",
        "latest_stage": "bundle", "source_commit": packet["source_commit"],
        "candidate_commit": packet["candidate_commit"],
        "source_tree": intent["source_tree"], "candidate_tree": intent["candidate_tree"],
        "bundle_ref": json.loads((root / "journals/2032/attempt-1/bundle.json").read_bytes())["bundle_ref"],
    }]
    conflicting = {**packet, "verification_evidence": {"tests": ["different"]}}
    with pytest.raises(RuntimeError, match="conflict at intent"):
        publish(conflicting, allow_unsafe_test_root=True)


def test_cas_collision_and_corruption_fail_closed(tmp_path: Path):
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    cas = FilesystemCAS(root, allow_unsafe_test_root=True)
    reference = cas.put(b"right")
    target = root / "objects" / "sha256" / reference[-64:-62] / reference[-62:]
    target.write_bytes(b"wrong")
    with pytest.raises(RuntimeError, match="corrupt"):
        cas.get(reference)
    with pytest.raises(RuntimeError, match="collision|corrupt"):
        cas.put(b"right")


def test_rejects_credentials_before_any_write(candidate):
    _, root, packet = candidate
    packet["claim_receipt"] = "do-not-store"
    with pytest.raises(ValueError, match="forbidden credential"):
        publish(packet, allow_unsafe_test_root=True)
    assert list(root.iterdir()) == []
    packet.pop("claim_receipt")
    packet["redacted_log"] = "PROVIDER_TOKEN=still-secret"
    with pytest.raises(ValueError, match="forbidden credential material"):
        publish(packet, allow_unsafe_test_root=True)
    assert list(root.iterdir()) == []


def test_rejects_dirty_candidate_before_any_write(candidate):
    repo, root, packet = candidate
    (repo / "untracked").write_text("dirty")
    with pytest.raises(ValueError, match="clean"):
        publish(packet, allow_unsafe_test_root=True)
    assert list(root.iterdir()) == []


def test_rejects_source_unavailable_and_promotion_stale(candidate):
    repo, root, packet = candidate
    packet["source_commit"] = "a" * 40
    packet["execution_envelope"]["source_commit"] = "a" * 40
    with pytest.raises(ValueError, match="source commit is unavailable|cat-file"):
        publish(packet, allow_unsafe_test_root=True)
    assert list(root.iterdir()) == []

    packet["source_commit"] = git(repo, "rev-parse", "HEAD^")
    packet["execution_envelope"]["source_commit"] = packet["source_commit"]
    packet["candidate_commit"] = packet["source_commit"]
    with pytest.raises(ValueError, match="promotion-stale"):
        publish(packet, allow_unsafe_test_root=True)
    assert list(root.iterdir()) == []


def test_forbids_unprovisioned_unsafe_and_worktree_roots(candidate, tmp_path: Path):
    repo, _, packet = candidate
    packet["artifact_root"] = str(tmp_path / "missing")
    with pytest.raises(ValueError, match="provisioned"):
        publish(packet, allow_unsafe_test_root=True)
    packet["artifact_root"] = str(repo)
    with pytest.raises(ValueError, match="worktree"):
        publish(packet, allow_unsafe_test_root=True)


def test_settlement_acknowledgement_is_bound_idempotent_and_queryable(candidate):
    _, root, packet = candidate
    result = publish(packet, allow_unsafe_test_root=True)
    ack_packet = {
        "artifact_root": str(root), "action_id": 2032, "attempt_id": "attempt-1",
        "journal_ref": result["journal_ref"], "terminal_status": "completed",
        "result_ref": result["journal_ref"],
        "authoritative_decision": {
            "action_id": 2032, "status": "completed", "result_ref": result["journal_ref"],
            "completed_at": "2026-07-31T12:00:00Z",
        },
    }
    expected = acknowledge_settlement(ack_packet, allow_unsafe_test_root=True)
    assert acknowledge_settlement(ack_packet, allow_unsafe_test_root=True) == expected
    assert query_settlement(root, 2032, "attempt-1", allow_unsafe_test_root=True) == expected
    conflicting = {**ack_packet, "authoritative_decision": {
        **ack_packet["authoritative_decision"], "completed_at": "2026-07-31T12:00:01Z",
    }}
    with pytest.raises(RuntimeError, match="conflict"):
        acknowledge_settlement(conflicting, allow_unsafe_test_root=True)


@pytest.mark.parametrize("field,value", [
    ("terminal_status", "failed"),
    ("result_ref", "artifact:sha256:" + "0" * 64),
])
def test_settlement_rejects_ambiguous_or_nonmatching_terminal_decision(candidate, field, value):
    _, root, packet = candidate
    result = publish(packet, allow_unsafe_test_root=True)
    ack = {
        "artifact_root": str(root), "action_id": 2032, "attempt_id": "attempt-1",
        "journal_ref": result["journal_ref"], "terminal_status": "completed",
        "result_ref": result["journal_ref"], "authoritative_decision": {
            "action_id": 2032, "status": "completed", "result_ref": result["journal_ref"],
            "completed_at": "2026-07-31T12:00:00Z",
        },
    }
    ack[field] = value
    with pytest.raises(ValueError):
        acknowledge_settlement(ack, allow_unsafe_test_root=True)


def test_missing_settlement_query_does_not_create_journal(candidate):
    _, root, _ = candidate
    assert query_settlement(root, 9999, "missing", allow_unsafe_test_root=True) is None
    assert list(root.iterdir()) == []


def test_redacted_log_is_deterministically_capped_with_retention_policy(candidate):
    _, root, packet = candidate
    packet["redacted_log"] = "x" * (MAX_REDACTED_LOG_BYTES + 100)
    result = publish(packet, allow_unsafe_test_root=True)
    stored = FilesystemCAS(root, allow_unsafe_test_root=True).get(result["records"]["redacted_log"])
    assert len(stored) == MAX_REDACTED_LOG_BYTES
    assert stored.endswith(b"[actionq-runner: redacted log truncated]\n")
    assert result["redacted_log_retention_days"] == 30
