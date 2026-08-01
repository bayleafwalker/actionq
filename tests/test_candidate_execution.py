from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from actionq_contracts import (
    CANDIDATE_INTEGRATION_RESULT_V1,
    CANDIDATE_INTEGRATION_SPEC_V1,
    CANDIDATE_VERIFICATION_RESULT_V1,
    CANDIDATE_VERIFICATION_SPEC_V1,
    VERIFICATION_PROFILE_V1,
    artifact_digest,
    canonical_bytes,
)
from actionq_runner.candidate_execution import (
    CandidateExecutionCancelled,
    integrate_wave,
    verify_candidate,
)
from actionq_runner.candidates import resolve_exact_contract
from actionq_runner.publisher import artifact_ref


class MemoryStore:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put(self, value: bytes) -> str:
        reference = artifact_ref(value)
        self.objects.setdefault(reference, value)
        return reference

    def get(self, reference: str) -> bytes:
        return self.objects[reference]


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()


def _candidate_bundle(tmp_path: Path, name: str, base: str | None = None) -> tuple[bytes, str, str]:
    repo = tmp_path / name
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    _git(repo, "config", "user.name", "ActionQ Test")
    _git(repo, "config", "user.email", "test@example.invalid")
    (repo / "base.txt").write_text("base\n")
    _git(repo, "add", "base.txt")
    _git(repo, "commit", "-qm", "base")
    initial = _git(repo, "rev-parse", "HEAD")
    if base is not None:
        # Each independent candidate has the same base object via an explicit
        # local clone, never a remote branch.
        subprocess.run(["git", "-C", str(repo), "fetch", str(tmp_path / "first.bundle"), "HEAD"], check=True, capture_output=True)
        _git(repo, "checkout", "-q", "-B", "candidate", base)
    (repo / f"{name}.txt").write_text(name + "\n")
    _git(repo, "add", f"{name}.txt")
    _git(repo, "commit", "-qm", name)
    head = _git(repo, "rev-parse", "HEAD")
    bundle = tmp_path / f"{name}.bundle"
    _git(repo, "bundle", "create", str(bundle), "HEAD")
    return bundle.read_bytes(), initial if base is None else base, head


def _put_contract(store: MemoryStore, value: dict) -> str:
    return store.put(canonical_bytes(value))


def _trusted_registry(store: MemoryStore) -> str:
    return _put_contract(store, {"commands": {"pass": ["true"]}})


def _passed_result(store: MemoryStore, *, candidate_ref: str, evidence_ref: str, registry_ref: str) -> str:
    profile_ref = _put_contract(store, {
        "contract_id": VERIFICATION_PROFILE_V1, "profile_id": "default", "command_id": "pass",
        "registry_ref": registry_ref, "registry_digest": artifact_digest(registry_ref),
    })
    spec_ref = _put_contract(store, {
        "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
        "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
        "profile_ref": profile_ref, "profile_digest": artifact_digest(profile_ref),
    })
    return _put_contract(store, {
        "contract_id": CANDIDATE_VERIFICATION_RESULT_V1,
        "spec_ref": spec_ref, "spec_digest": artifact_digest(spec_ref),
        "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
        "outcome": "passed", "evidence_ref": evidence_ref, "evidence_digest": artifact_digest(evidence_ref),
    })


def test_verification_runs_only_registered_frozen_command_in_fresh_bundle(tmp_path):
    store = MemoryStore()
    bundle, _base, _head = _candidate_bundle(tmp_path, "one")
    candidate_ref = store.put(bundle)
    registry = {"commands": {"pass": ["git", "rev-parse", "--is-inside-work-tree"]}}
    registry_ref = _put_contract(store, registry)
    profile = {"contract_id": VERIFICATION_PROFILE_V1, "profile_id": "default", "command_id": "pass",
               "registry_ref": registry_ref, "registry_digest": artifact_digest(registry_ref)}
    profile_ref = _put_contract(store, profile)
    spec = {"contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
            "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
            "profile_ref": profile_ref, "profile_digest": artifact_digest(profile_ref)}
    spec_ref = _put_contract(store, spec)

    result_ref = verify_candidate(store, spec_ref=spec_ref, trusted_registry_ref=registry_ref)
    result = resolve_exact_contract(store, result_ref, CANDIDATE_VERIFICATION_RESULT_V1)
    assert result["outcome"] == "passed"
    assert result["candidate_ref"] == candidate_ref
    before = set(store.objects)
    with pytest.raises(CandidateExecutionCancelled):
        verify_candidate(store, spec_ref=spec_ref, cancelled=lambda: True, trusted_registry_ref=registry_ref)
    assert set(store.objects) == before


def test_wave_integration_preserves_frozen_member_ordinal_order(tmp_path):
    store = MemoryStore()
    first, base, first_head = _candidate_bundle(tmp_path, "first")
    # Build a second independent candidate from the exact first bundle's base.
    second_repo = tmp_path / "second"
    subprocess.run(["git", "clone", "-q", str(tmp_path / "first.bundle"), str(second_repo)], check=True)
    _git(second_repo, "config", "user.name", "ActionQ Test")
    _git(second_repo, "config", "user.email", "test@example.invalid")
    _git(second_repo, "checkout", "-q", "--detach", base)
    (second_repo / "second.txt").write_text("second\n")
    _git(second_repo, "add", "second.txt")
    _git(second_repo, "commit", "-qm", "second")
    second_head = _git(second_repo, "rev-parse", "HEAD")
    second_bundle = tmp_path / "second.bundle"
    _git(second_repo, "bundle", "create", str(second_bundle), "HEAD")
    first_ref, second_ref = store.put(first), store.put(second_bundle.read_bytes())
    evidence = store.put(canonical_bytes({"ok": True}))
    registry_ref = _trusted_registry(store)
    first_result = _passed_result(store, candidate_ref=first_ref, evidence_ref=evidence, registry_ref=registry_ref)
    second_result = _passed_result(store, candidate_ref=second_ref, evidence_ref=evidence, registry_ref=registry_ref)
    integration = {"contract_id": CANDIDATE_INTEGRATION_SPEC_V1,
        "topology": "wave-integrated", "base_commit": base,
        "member_result_refs": [second_result, first_result],
        "input_set_digest": "sha256:" + "d" * 64}
    integration_ref = _put_contract(store, integration)

    result_ref = integrate_wave(store, spec_ref=integration_ref, author_name="ActionQ", author_email="actionq@example.invalid", commit_timestamp="2026-08-01T00:00:00Z", trusted_registry_ref=registry_ref)
    result = resolve_exact_contract(store, result_ref, CANDIDATE_INTEGRATION_RESULT_V1)
    assert result["outcome"] == "integrated"
    assert result["candidate_ref"] is not None
    before_replay = set(store.objects)
    assert integrate_wave(store, spec_ref=integration_ref, author_name="ActionQ", author_email="actionq@example.invalid", commit_timestamp="2026-08-01T00:00:00Z", trusted_registry_ref=registry_ref) == result_ref
    assert set(store.objects) == before_replay
    recovery = tmp_path / "recovery"
    recovery.mkdir(mode=0o700)
    assert integrate_wave(
        store, spec_ref=integration_ref, author_name="ActionQ", author_email="actionq@example.invalid",
        commit_timestamp="2026-08-01T00:00:00Z", trusted_registry_ref=registry_ref,
        recovery_root=recovery, operation_id="wave-one",
    ) == result_ref
    assert integrate_wave(
        store, spec_ref=integration_ref, author_name="ActionQ", author_email="actionq@example.invalid",
        commit_timestamp="2026-08-01T00:00:00Z", trusted_registry_ref=registry_ref,
        recovery_root=recovery, operation_id="wave-one",
    ) == result_ref
    with pytest.raises(CandidateExecutionCancelled):
        integrate_wave(store, spec_ref=integration_ref, author_name="ActionQ", author_email="actionq@example.invalid", commit_timestamp="2026-08-01T00:00:00Z", cancelled=lambda: True, trusted_registry_ref=registry_ref)
    assert set(store.objects) == before_replay


def test_stacked_integration_returns_the_existing_tip_without_a_synthetic_bundle(tmp_path):
    store = MemoryStore()
    first, base, first_head = _candidate_bundle(tmp_path, "first")
    second_repo = tmp_path / "second"
    subprocess.run(["git", "clone", "-q", str(tmp_path / "first.bundle"), str(second_repo)], check=True)
    _git(second_repo, "config", "user.name", "ActionQ Test")
    _git(second_repo, "config", "user.email", "test@example.invalid")
    (second_repo / "second.txt").write_text("second\n")
    _git(second_repo, "add", "second.txt")
    _git(second_repo, "commit", "-qm", "second")
    second_bundle = tmp_path / "second.bundle"
    _git(second_repo, "bundle", "create", str(second_bundle), "HEAD")
    first_ref, second_ref = store.put(first), store.put(second_bundle.read_bytes())
    evidence = store.put(canonical_bytes({"ok": True}))
    registry_ref = _trusted_registry(store)
    first_result = _passed_result(store, candidate_ref=first_ref, evidence_ref=evidence, registry_ref=registry_ref)
    second_result = _passed_result(store, candidate_ref=second_ref, evidence_ref=evidence, registry_ref=registry_ref)
    spec = {"contract_id": CANDIDATE_INTEGRATION_SPEC_V1,
        "topology": "stacked", "base_commit": base,
        "member_result_refs": [first_result, second_result], "input_set_digest": "sha256:" + "2" * 64}
    result_ref = integrate_wave(store, spec_ref=_put_contract(store, spec), author_name="ActionQ", author_email="actionq@example.invalid", commit_timestamp="2026-08-01T00:00:00Z", trusted_registry_ref=registry_ref)
    result = resolve_exact_contract(store, result_ref, CANDIDATE_INTEGRATION_RESULT_V1)
    assert result["outcome"] == "integrated"
    assert result["candidate_ref"] == second_ref


def test_wave_conflict_from_individually_passed_candidates_publishes_no_bundle(tmp_path):
    store = MemoryStore()
    base_repo = tmp_path / "base"
    subprocess.run(["git", "init", "-q", str(base_repo)], check=True)
    _git(base_repo, "config", "user.name", "ActionQ Test")
    _git(base_repo, "config", "user.email", "test@example.invalid")
    (base_repo / "shared.txt").write_text("base\n")
    _git(base_repo, "add", "shared.txt")
    _git(base_repo, "commit", "-qm", "base")
    base = _git(base_repo, "rev-parse", "HEAD")

    bundles: list[bytes] = []
    for name, value in (("first", "first\n"), ("second", "second\n")):
        repo = tmp_path / name
        subprocess.run(["git", "clone", "-q", str(base_repo), str(repo)], check=True)
        _git(repo, "config", "user.name", "ActionQ Test")
        _git(repo, "config", "user.email", "test@example.invalid")
        (repo / "shared.txt").write_text(value)
        _git(repo, "add", "shared.txt")
        _git(repo, "commit", "-qm", name)
        bundle = tmp_path / f"{name}.bundle"
        _git(repo, "bundle", "create", str(bundle), "HEAD")
        bundles.append(bundle.read_bytes())
    candidate_refs = [store.put(bundle) for bundle in bundles]
    evidence = store.put(canonical_bytes({"ok": True}))
    registry_ref = _trusted_registry(store)
    result_refs = [
        _passed_result(store, candidate_ref=candidate_ref, evidence_ref=evidence, registry_ref=registry_ref)
        for candidate_ref in candidate_refs
    ]
    integration = {
        "contract_id": CANDIDATE_INTEGRATION_SPEC_V1,
        "topology": "wave-integrated", "base_commit": base,
        "member_result_refs": result_refs, "input_set_digest": "sha256:" + "b" * 64,
    }
    before = set(store.objects)
    result_ref = integrate_wave(store, spec_ref=_put_contract(store, integration), author_name="ActionQ", author_email="actionq@example.invalid", commit_timestamp="2026-08-01T00:00:00Z", trusted_registry_ref=registry_ref)
    result = resolve_exact_contract(store, result_ref, CANDIDATE_INTEGRATION_RESULT_V1)
    assert result == {
        "contract_id": CANDIDATE_INTEGRATION_RESULT_V1,
        "spec_ref": _put_contract(store, integration), "spec_digest": artifact_digest(_put_contract(store, integration)),
        "outcome": "conflict", "candidate_ref": None, "candidate_digest": None,
    }
    assert set(store.objects) - before == {_put_contract(store, integration), result_ref}


def test_missing_candidate_and_wrong_frozen_base_publish_no_result(tmp_path):
    store = MemoryStore()
    bundle, base, _head = _candidate_bundle(tmp_path, "one")
    candidate_ref = store.put(bundle)
    registry_ref = _put_contract(store, {"commands": {"pass": ["true"]}})
    profile_ref = _put_contract(store, {
        "contract_id": VERIFICATION_PROFILE_V1, "profile_id": "default", "command_id": "pass",
        "registry_ref": registry_ref, "registry_digest": artifact_digest(registry_ref),
    })
    verification_spec = {
        "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
        "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
        "profile_ref": profile_ref, "profile_digest": artifact_digest(profile_ref),
    }
    verification_spec_ref = _put_contract(store, verification_spec)
    del store.objects[candidate_ref]
    before_missing = set(store.objects)
    with pytest.raises(KeyError):
        verify_candidate(store, spec_ref=verification_spec_ref, trusted_registry_ref=registry_ref)
    assert set(store.objects) == before_missing

    # A syntactically valid but unavailable base cannot be turned into a
    # conflict artifact: it is a protocol failure before integration starts.
    candidate_ref = store.put(bundle)
    evidence = store.put(canonical_bytes({"ok": True}))
    verification_result_ref = _put_contract(store, {
        "contract_id": CANDIDATE_VERIFICATION_RESULT_V1,
        "spec_ref": verification_spec_ref, "spec_digest": artifact_digest(verification_spec_ref),
        "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
        "outcome": "passed", "evidence_ref": evidence, "evidence_digest": artifact_digest(evidence),
    })
    integration = {
        "contract_id": CANDIDATE_INTEGRATION_SPEC_V1,
        "topology": "wave-integrated", "base_commit": "f" * 40,
        "member_result_refs": [verification_result_ref], "input_set_digest": "sha256:" + "3" * 64,
    }
    integration_ref = _put_contract(store, integration)
    before_base = set(store.objects)
    with pytest.raises(RuntimeError, match="frozen integration base"):
        integrate_wave(store, spec_ref=integration_ref, author_name="ActionQ", author_email="actionq@example.invalid", commit_timestamp="2026-08-01T00:00:00Z", trusted_registry_ref=registry_ref)
    assert set(store.objects) == before_base
