from __future__ import annotations

import json

import pytest

from actionq_contracts import CANDIDATE_VERIFICATION_SPEC_V1, canonical_bytes
from actionq_runner.candidates import CandidateRecoveryJournal, resolve_exact_contract
from actionq_runner.publisher import FilesystemCAS, artifact_ref


def _ref(letter: str) -> str:
    return "artifact:sha256:" + letter * 64


def _spec() -> dict:
    return {
        "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
        "candidate_ref": _ref("b"), "candidate_digest": "sha256:" + "b" * 64,
        "profile_ref": _ref("c"), "profile_digest": "sha256:" + "c" * 64,
    }


def test_exact_resolver_requires_canonical_contract_bytes(tmp_path):
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    cas = FilesystemCAS(root, allow_unsafe_test_root=True)
    raw = canonical_bytes(_spec())
    reference = cas.put(raw)
    assert resolve_exact_contract(cas, reference, CANDIDATE_VERIFICATION_SPEC_V1) == _spec()
    with pytest.raises(ValueError, match="must be"):
        resolve_exact_contract(cas, reference, "candidate-review-spec/v1")


def test_exact_resolver_rejects_noncanonical_contract_storage():
    value = _spec()
    raw = json.dumps(value, sort_keys=False, separators=(",", ": ")).encode()

    class Store:
        def get(self, reference):
            return raw

    with pytest.raises(ValueError, match="canonical"):
        resolve_exact_contract(Store(), artifact_ref(raw), CANDIDATE_VERIFICATION_SPEC_V1)


def test_candidate_recovery_journal_is_namespaced_create_only_and_restartable(tmp_path):
    root = tmp_path / "recovery"
    root.mkdir(mode=0o700)
    journal = CandidateRecoveryJournal(root, namespace="verification", operation_id="action-1")
    value = {"spec_ref": "artifact:sha256:" + "a" * 64}
    assert journal.write("intent", value)["stage"] == "intent"
    assert CandidateRecoveryJournal(root, namespace="verification", operation_id="action-1").read("intent")["spec_ref"] == value["spec_ref"]
    assert journal.write("intent", value)["spec_ref"] == value["spec_ref"]
    with pytest.raises(RuntimeError, match="conflict"):
        journal.write("intent", {"spec_ref": "artifact:sha256:" + "b" * 64})
    assert not (root / "journals").exists()  # #2032 namespace remains separate.
