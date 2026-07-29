import hashlib

import pytest

from actionq import db
from actionq.application import ActionQApplication
from actionq.vuoro import catalog_metadata


def _request():
    return {
        "contract_version": "v2", "action_type": "scope-iterate",
        "output_expectation": "implementation", "repo_id": "actionq",
        "sprint_id": None, "work_item_id": "2027", "title": "enqueue",
        "prompt": "", "harness": "codex", "model": None, "priority": "normal",
        "refs": ["wi:2027"], "dispatch_group_id": None,
        "requested_by": "compiler:test",
    }


def test_v2_normalization_is_byte_stable_and_full_shape_only():
    normalized, raw = ActionQApplication._normalize_dispatch_v2(_request())
    assert normalized == _request()
    assert hashlib.sha256(raw).hexdigest() == hashlib.sha256(raw).hexdigest()
    with pytest.raises(db.ActionQError, match="unknown"):
        ActionQApplication._normalize_dispatch_v2({**_request(), "kind": "implement"})
    with pytest.raises(db.ActionQError, match="missing"):
        ActionQApplication._normalize_dispatch_v2({key: value for key, value in _request().items() if key != "prompt"})


def test_v2_catalog_exposes_strict_enqueue_result_without_caller_requested_by():
    definition = next(item for item in catalog_metadata() if item["name"] == "execution.dispatch.enqueue")
    assert "requested_by" not in definition["input_schema"]["properties"]
    assert definition["result_schema"]["required"] == ["action_id", "status", "request_ref", "request_sha256"]
    assert definition["result_schema"]["additionalProperties"] is False
