from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from actionq_contracts import (
    CANDIDATE_REVIEW_RESULT_V1,
    CANDIDATE_REVIEW_SPEC_V1,
    CANDIDATE_VERIFICATION_RESULT_V1,
    canonical_bytes,
)
from actionq_runner.publisher import artifact_ref
from actionq_runner.candidates import resolve_exact_contract
from actionq_runner.review import ReviewObservation, publish_review, publish_review_result, reconcile_review


class MemoryStore:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put(self, value: bytes) -> str:
        reference = artifact_ref(value)
        self.objects.setdefault(reference, value)
        return reference

    def get(self, reference: str) -> bytes:
        return self.objects[reference]


def _ref(letter: str) -> str:
    return "artifact:sha256:" + letter * 64


def _observation(**changes) -> ReviewObservation:
    metadata = {
        "action_id": "2035", "attempt_id": "attempt-1", "plan_ref": _ref("a"),
        "subject_kind": "candidate", "publication_ref": _ref("b"),
        "verification_result_ref": _ref("c"), "review_result_artifact_ref": _ref("d"),
        "topology": "independent", "findings_digest": "sha256:" + "e" * 64,
        "review_outcome": "no-findings",
    }
    metadata.update(changes.pop("metadata", {}))
    actor = changes.pop("actor", "reviewer:one")
    reviewed_commit = changes.pop("reviewed_commit", "f" * 40)
    return ReviewObservation(actor=actor, reviewed_commit=reviewed_commit, metadata=metadata, **changes)


@pytest.fixture
def fake_auditctl(tmp_path: Path, monkeypatch):
    events = tmp_path / "events.json"
    events.write_text("[]")
    binary = tmp_path / "auditctl"
    binary.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "p=os.environ['FAKE_AUDIT_EVENTS']\n"
        "events=json.loads(open(p).read())\n"
        "if sys.argv[1] == 'list': print(json.dumps(events)); raise SystemExit(0)\n"
        "if os.environ.get('FAKE_AUDIT_FAIL') == '1': raise SystemExit(2)\n"
        "def one(name): return sys.argv[sys.argv.index(name)+1]\n"
        "refs=[sys.argv[i+1] for i,x in enumerate(sys.argv[:-1]) if x == '--ref']\n"
        "events.append({'type':one('--type'),'source':one('--source'),'actor':one('--actor'),'refs':refs,'metadata':json.loads(one('--metadata'))})\n"
        "open(p,'w').write(json.dumps(events)); print('{}')\n"
    )
    binary.chmod(0o755)
    monkeypatch.setenv("FAKE_AUDIT_EVENTS", str(events))
    return binary, events


def test_publish_review_is_exact_and_reconciles_before_retry(fake_auditctl):
    binary, path = fake_auditctl
    observation = _observation(work_item_id=2035, sprint_id=543)
    assert publish_review(str(binary), observation) == "published"
    events = json.loads(path.read_text())
    assert len(events) == 1
    assert events[0]["type"] == "candidate.reviewed"
    assert events[0]["source"] == "actionq-review"
    assert events[0]["refs"] == ["sha:" + "f" * 40, "wi:2035", "sprint:543"]
    assert events[0]["metadata"] == observation.metadata
    assert publish_review(str(binary), observation) == "published"
    assert len(json.loads(path.read_text())) == 1


def test_reconcile_fails_closed_on_divergent_immutable_result(fake_auditctl):
    binary, path = fake_auditctl
    observed = _observation()
    conflicting = _observation(metadata={"review_result_artifact_ref": _ref("1")})
    path.write_text(json.dumps([{
        "type": "candidate.reviewed", "source": "actionq-review", "metadata": conflicting.metadata,
    }]))
    assert reconcile_review(str(binary), observed) == "conflict"
    assert publish_review(str(binary), observed) == "conflict"


def test_reconcile_bound_exhaustion_is_inconclusive_and_never_retries(fake_auditctl):
    binary, path = fake_auditctl
    path.write_text(json.dumps([{"type": "other", "source": "x", "metadata": {}}] * 1000))
    assert reconcile_review(str(binary), _observation()) == "inconclusive"
    assert publish_review(str(binary), _observation()) == "inconclusive"
    assert len(json.loads(path.read_text())) == 1000


def test_review_metadata_has_no_authority_or_hidden_fields():
    with pytest.raises(ValueError, match="exact"):
        _observation(metadata={"approval": "yes"})
    with pytest.raises(ValueError, match="full lowercase"):
        _observation(reviewed_commit="short")


def test_review_result_is_immutable_and_requires_passed_bound_verification():
    store = MemoryStore()
    candidate_ref = store.put(b"candidate bundle")
    evidence_ref = store.put(canonical_bytes({"ok": True}))
    verification_ref = store.put(canonical_bytes({
        "contract_id": CANDIDATE_VERIFICATION_RESULT_V1,
        "spec_ref": _ref("a"), "spec_digest": "sha256:" + "a" * 64,
        "candidate_ref": candidate_ref, "candidate_digest": candidate_ref.removeprefix("artifact:"),
        "outcome": "passed", "evidence_ref": evidence_ref, "evidence_digest": evidence_ref.removeprefix("artifact:"),
    }))
    spec = {
        "contract_id": CANDIDATE_REVIEW_SPEC_V1,
        "candidate_ref": candidate_ref, "candidate_digest": candidate_ref.removeprefix("artifact:"),
        "verification_result_ref": verification_ref,
        "verification_result_digest": verification_ref.removeprefix("artifact:"),
    }
    spec_ref = store.put(canonical_bytes(spec))

    result_ref = publish_review_result(store, spec_ref=spec_ref, findings=[])
    result = resolve_exact_contract(store, result_ref, CANDIDATE_REVIEW_RESULT_V1)
    assert result["outcome"] == "no-findings"
    with pytest.raises(ValueError, match="forbidden"):
        publish_review_result(store, spec_ref=spec_ref, findings=[{
            "id": "one", "category": "protocol", "summary": "approval granted",
        }])
