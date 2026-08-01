from __future__ import annotations

import copy

import pytest

from actionq_contracts import (
    ACTION_CREATION_REQUEST_V1,
    CANDIDATE_INTEGRATION_RESULT_V1,
    CANDIDATE_INTEGRATION_SPEC_V1,
    CANDIDATE_REVIEW_RESULT_V1,
    CANDIDATE_REVIEW_SPEC_V1,
    CANDIDATE_VERIFICATION_RESULT_V1,
    CANDIDATE_VERIFICATION_SPEC_V1,
    VERIFICATION_PROFILE_V1,
    artifact_digest,
    canonical_bytes,
    require_compatible,
    sha256_digest,
)


def ref(letter: str) -> str:
    return "artifact:sha256:" + letter * 64


def digest(letter: str) -> str:
    return "sha256:" + letter * 64


@pytest.fixture
def records() -> list[dict]:
    creation = {
        "contract_id": ACTION_CREATION_REQUEST_V1,
        "plan_ref": ref("a"), "topology": "independent", "role": "candidate-verification",
        "subject": "candidate:one", "spec_ref": ref("b"), "spec_digest": digest("b"),
        "input_set_digest": digest("c"),
    }
    profile = {
        "contract_id": VERIFICATION_PROFILE_V1, "profile_id": "test", "command_id": "pytest",
        "registry_ref": ref("d"), "registry_digest": digest("d"),
    }
    verification_spec = {
        "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
        "candidate_ref": ref("1"), "candidate_digest": digest("1"),
        "profile_ref": ref("2"), "profile_digest": digest("2"),
    }
    verification_result = {
        "contract_id": CANDIDATE_VERIFICATION_RESULT_V1,
        "spec_ref": ref("3"), "spec_digest": digest("3"),
        "candidate_ref": ref("4"), "candidate_digest": digest("4"),
        "outcome": "passed", "evidence_ref": ref("5"), "evidence_digest": digest("5"),
    }
    integration_spec = {
        "contract_id": CANDIDATE_INTEGRATION_SPEC_V1, "topology": "wave-integrated",
        "base_commit": "a" * 40, "member_result_refs": [ref("7"), ref("8")],
        "input_set_digest": digest("9"),
    }
    integration_result = {
        "contract_id": CANDIDATE_INTEGRATION_RESULT_V1,
        "spec_ref": ref("a"), "spec_digest": digest("a"), "outcome": "integrated",
        "candidate_ref": ref("b"), "candidate_digest": digest("b"),
    }
    review_spec = {
        "contract_id": CANDIDATE_REVIEW_SPEC_V1,
        "candidate_ref": ref("d"), "candidate_digest": digest("d"),
        "verification_result_ref": ref("e"), "verification_result_digest": digest("e"),
        "publication_ref": ref("f"), "publication_digest": digest("f"),
        "subject_kind": "candidate", "reviewed_commit": "a" * 40,
    }
    review_result = {
        "contract_id": CANDIDATE_REVIEW_RESULT_V1,
        "spec_ref": ref("f"), "spec_digest": digest("f"), "outcome": "no-findings",
        "findings_ref": ref("1"), "findings_digest": digest("1"),
    }
    return [creation, profile, verification_spec, verification_result, integration_spec,
            integration_result, review_spec, review_result]


def test_all_frozen_v1_records_are_exact_and_canonical(records):
    for record in records:
        assert require_compatible(record) == record["contract_id"]
        assert canonical_bytes(record) == canonical_bytes(dict(reversed(record.items())))
        assert sha256_digest(record).startswith("sha256:")


def test_unknown_keys_floats_and_recursive_authority_fields_are_rejected(records):
    record = copy.deepcopy(records[0])
    record["unknown"] = "no"
    with pytest.raises(ValueError, match="invalid"):
        require_compatible(record)
    record = copy.deepcopy(records[0])
    record["subject"] = {"nested": {"claim_token": "never"}}
    with pytest.raises(ValueError, match="forbidden field"):
        require_compatible(record)
    record = copy.deepcopy(records[0])
    record["subject"] = {"value": 1.2}
    with pytest.raises(ValueError, match="floats"):
        require_compatible(record)


def test_artifact_binding_requires_byte_identity(records):
    record = copy.deepcopy(records[2])
    record["candidate_digest"] = digest("f")
    with pytest.raises(ValueError, match="same bytes"):
        require_compatible(record)
    assert artifact_digest(ref("a")) == digest("a")


def test_conflict_result_has_no_candidate_artifact(records):
    record = copy.deepcopy(records[5])
    record.update({"outcome": "conflict", "candidate_ref": None, "candidate_digest": None})
    assert require_compatible(record) == CANDIDATE_INTEGRATION_RESULT_V1
    record["candidate_ref"] = ref("a")
    record["candidate_digest"] = digest("a")
    with pytest.raises(ValueError, match="conflict"):
        require_compatible(record)


def test_wave_members_are_frozen_in_ordinal_input_order(records):
    record = copy.deepcopy(records[4])
    record["member_result_refs"] = list(reversed(record["member_result_refs"]))
    assert require_compatible(record) == CANDIDATE_INTEGRATION_SPEC_V1
    record["member_result_refs"].append(record["member_result_refs"][0])
    with pytest.raises(ValueError, match="unique ordinal"):
        require_compatible(record)
