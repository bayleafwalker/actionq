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
    sha256_digest,
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


def _eligible_publication(store: MemoryStore) -> str:
    return _put_contract(store, {
        "contract_id": "publication/v1", "action_id": 1, "attempt_id": "attempt-1",
        "candidate_digest": "sha256:" + "1" * 64,
        "verification_digest": "sha256:" + "2" * 64, "terminal_status": "verified",
    })


def _input_set_digest(refs: list[str]) -> str:
    return sha256_digest({"contract_id": "immutable-input-set/v1", "inputs": refs})


def _passed_result(store: MemoryStore, *, candidate_ref: str, evidence_ref: str, registry_ref: str) -> str:
    publication_ref = _eligible_publication(store)
    profile_ref = _put_contract(store, {
        "contract_id": VERIFICATION_PROFILE_V1, "profile_id": "default", "command_id": "pass",
        "registry_ref": registry_ref, "registry_digest": artifact_digest(registry_ref),
    })
    spec_ref = _put_contract(store, {
        "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
        "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
        "profile_ref": profile_ref, "profile_digest": artifact_digest(profile_ref),
        "publication_ref": publication_ref, "publication_digest": artifact_digest(publication_ref),
    })
    return _put_contract(store, {
        "contract_id": CANDIDATE_VERIFICATION_RESULT_V1,
        "spec_ref": spec_ref, "spec_digest": artifact_digest(spec_ref),
        "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
        "publication_ref": publication_ref, "publication_digest": artifact_digest(publication_ref),
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
            "profile_ref": profile_ref, "profile_digest": artifact_digest(profile_ref),
            "publication_ref": _eligible_publication(store), "publication_digest": artifact_digest(_eligible_publication(store))}
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
        "input_set_digest": _input_set_digest([second_result, first_result])}
    integration_ref = _put_contract(store, integration)

    result_ref = integrate_wave(store, spec_ref=integration_ref, trusted_registry_ref=registry_ref)
    result = resolve_exact_contract(store, result_ref, CANDIDATE_INTEGRATION_RESULT_V1)
    assert result["outcome"] == "integrated"
    assert result["candidate_ref"] is not None
    integrated_bundle = tmp_path / "integrated.bundle"
    integrated_bundle.write_bytes(store.get(result["candidate_ref"]))
    integrated_repo = tmp_path / "integrated"
    subprocess.run(["git", "clone", "-q", str(integrated_bundle), str(integrated_repo)], check=True)
    assert _git(integrated_repo, "show", "-s", "--format=%an%x00%ae%x00%aI", "HEAD") == (
        "ActionQ Integration\x00integration@actionq.invalid\x002000-01-01T00:00:00Z"
    )
    before_replay = set(store.objects)
    assert integrate_wave(store, spec_ref=integration_ref, trusted_registry_ref=registry_ref) == result_ref
    assert set(store.objects) == before_replay
    recovery = tmp_path / "recovery"
    recovery.mkdir(mode=0o700)
    assert integrate_wave(
        store, spec_ref=integration_ref, trusted_registry_ref=registry_ref,
        recovery_root=recovery, operation_id="wave-one",
    ) == result_ref
    assert integrate_wave(
        store, spec_ref=integration_ref, trusted_registry_ref=registry_ref,
        recovery_root=recovery, operation_id="wave-one",
    ) == result_ref
    with pytest.raises(CandidateExecutionCancelled):
        integrate_wave(store, spec_ref=integration_ref, cancelled=lambda: True, trusted_registry_ref=registry_ref)
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
        "member_result_refs": [first_result, second_result], "input_set_digest": _input_set_digest([first_result, second_result])}
    result_ref = integrate_wave(store, spec_ref=_put_contract(store, spec), trusted_registry_ref=registry_ref)
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
        "member_result_refs": result_refs, "input_set_digest": _input_set_digest(result_refs),
    }
    before = set(store.objects)
    result_ref = integrate_wave(store, spec_ref=_put_contract(store, integration), trusted_registry_ref=registry_ref)
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
        "publication_ref": _eligible_publication(store), "publication_digest": artifact_digest(_eligible_publication(store)),
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
        "publication_ref": verification_spec["publication_ref"], "publication_digest": verification_spec["publication_digest"],
        "outcome": "passed", "evidence_ref": evidence, "evidence_digest": artifact_digest(evidence),
    })
    integration = {
        "contract_id": CANDIDATE_INTEGRATION_SPEC_V1,
        "topology": "wave-integrated", "base_commit": "f" * 40,
        "member_result_refs": [verification_result_ref], "input_set_digest": _input_set_digest([verification_result_ref]),
    }
    integration_ref = _put_contract(store, integration)
    before_base = set(store.objects)
    with pytest.raises(RuntimeError, match="frozen integration base"):
        integrate_wave(store, spec_ref=integration_ref, trusted_registry_ref=registry_ref)
    assert set(store.objects) == before_base


def test_integration_recomputes_ordered_input_set_and_publication_binding(tmp_path):
    store = MemoryStore()
    bundle, base, _head = _candidate_bundle(tmp_path, "one")
    candidate_ref = store.put(bundle)
    evidence_ref = store.put(canonical_bytes({"ok": True}))
    registry_ref = _trusted_registry(store)
    result_ref = _passed_result(
        store, candidate_ref=candidate_ref, evidence_ref=evidence_ref, registry_ref=registry_ref,
    )
    invalid = {
        "contract_id": CANDIDATE_INTEGRATION_SPEC_V1, "topology": "stacked",
        "base_commit": base, "member_result_refs": [result_ref],
        "input_set_digest": "sha256:" + "0" * 64,
    }
    before = set(store.objects)
    with pytest.raises(RuntimeError, match="input_set_digest"):
        integrate_wave(store, spec_ref=_put_contract(store, invalid), trusted_registry_ref=registry_ref)
    assert set(store.objects) - before == {_put_contract(store, invalid)}

    result = resolve_exact_contract(store, result_ref, CANDIDATE_VERIFICATION_RESULT_V1)
    changed = dict(result)
    changed["publication_ref"] = store.put(b"other publication")
    changed["publication_digest"] = artifact_digest(changed["publication_ref"])
    changed_ref = _put_contract(store, changed)
    bound = {
        **invalid, "member_result_refs": [changed_ref],
        "input_set_digest": _input_set_digest([changed_ref]),
    }
    with pytest.raises(RuntimeError, match="eligible publication"):
        integrate_wave(store, spec_ref=_put_contract(store, bound), trusted_registry_ref=registry_ref)


def test_final_artifact_writes_are_cancel_fenced(tmp_path):
    store = MemoryStore()
    bundle, _base, _head = _candidate_bundle(tmp_path, "one")
    candidate_ref = store.put(bundle)
    registry_ref = _trusted_registry(store)
    profile_ref = _put_contract(store, {
        "contract_id": VERIFICATION_PROFILE_V1, "profile_id": "default", "command_id": "pass",
        "registry_ref": registry_ref, "registry_digest": artifact_digest(registry_ref),
    })
    publication_ref = _eligible_publication(store)
    spec_ref = _put_contract(store, {
        "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
        "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref),
        "profile_ref": profile_ref, "profile_digest": artifact_digest(profile_ref),
        "publication_ref": publication_ref, "publication_digest": artifact_digest(publication_ref),
    })
    checks = 0

    def cancel_before_result() -> bool:
        nonlocal checks
        checks += 1
        return checks == 4

    with pytest.raises(CandidateExecutionCancelled):
        verify_candidate(
            store, spec_ref=spec_ref, trusted_registry_ref=registry_ref,
            cancelled=cancel_before_result,
        )
    assert not any(
        CANDIDATE_VERIFICATION_RESULT_V1.encode() in raw for raw in store.objects.values()
    )
