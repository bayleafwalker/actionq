import json
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


def test_protocol_document_records_current_ownership_limit():
    protocol = (ROOT / "docs/protocols/action-lifecycle.md").read_text(encoding="utf-8")

    assert "claimed_by` is metadata, not proof" in protocol
    assert "do not claim exclusive terminal authority" in protocol
