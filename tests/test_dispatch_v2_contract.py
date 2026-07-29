import hashlib
from types import SimpleNamespace

import pytest

from actionq import db
from actionq.application import ActionQApplication
from actionq import server
from actionq.vuoro import build_operations, catalog_metadata


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
    assert definition["result_schema"]["properties"]["action_id"] == {"type": "integer", "minimum": 1}
    assert definition["result_schema"]["properties"]["request_ref"]["pattern"] == "^req:[0-9a-f]{32}$"
    input_schema = definition["input_schema"]
    assert input_schema["additionalProperties"] is False
    assert input_schema["properties"]["repo_id"]["pattern"] == "^(?!ALL$).+"
    assert input_schema["properties"]["sprint_id"]["minimum"] == 1


def test_v2_served_path_requires_repository_scoped_authorizer_before_db_access():
    provenance = SimpleNamespace(actor="compiler:test", environment="dev", request_id="r", catalog_revision="c", idempotency_key="k", basis_revision=None, authorized_repositories=())
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: False)
    with pytest.raises(db.ActionQError, match="authorization denied"):
        app.enqueue_dispatch_v2(_request(), provenance=provenance)


def test_v2_requested_by_must_match_authenticated_provenance_before_persistence():
    provenance = SimpleNamespace(actor="compiler:trusted", environment="dev", request_id="r", catalog_revision="c", idempotency_key="k", basis_revision=None, authorized_repositories=("actionq",))
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: pytest.fail("must reject before authorization/database access"))
    with pytest.raises(db.ActionQError, match="must equal the authenticated actor"):
        app.enqueue_dispatch_v2({**_request(), "requested_by": "caller:forged"}, provenance=provenance)


def test_http_identity_is_fail_closed_and_never_uses_spoofed_actor_headers(monkeypatch):
    headers = {"x-actionq-actor": "attacker", "x-actionq-environment": "prod", "idempotency-key": "key"}
    monkeypatch.setattr(server, "_served_authenticator", server._no_authenticator)
    with pytest.raises(db.ActionQError, match="not configured"):
        server._request_provenance(headers)
    server.set_served_authenticator_for_testing(lambda _headers: server.AuthenticatedDispatchIdentity("compiler:trusted", "dev", ("actionq",)))
    provenance = server._request_provenance(headers)
    assert provenance.actor == "compiler:trusted"
    assert provenance.environment == "dev"
    assert provenance.authorized_repositories == ("actionq",)


def test_vuoro_context_carries_only_trusted_repository_authorization():
    context = SimpleNamespace(identity=SimpleNamespace(actor="compiler:trusted", environment="dev", authorized_repositories=("actionq",)), request_id="r", catalog_revision="c", basis_revision=None, idempotency_key="k")
    operation = next(item for item in build_operations() if item.definition["name"] == "execution.dispatch.enqueue")
    # The handler will fail closed without the injected application authorizer;
    # this proves the public catalog does not admit caller requested_by instead.
    assert "requested_by" not in operation.definition["input_schema"]["properties"]
