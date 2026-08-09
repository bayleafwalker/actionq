from datetime import datetime, timezone

import pytest

from actionq.action_resource import (
    EVENT_KEYS, PROJECTION_KEYS, RESOURCE_RE, SESSION_KEYS, ActionResourceOwner,
    ResourceNotFound, _event, _projection, canonical_request, new_resource_ref,
    serialize_envelope,
)
from actionq.application import ActionQApplication, AuthenticatedActionResourcePrincipal, InvocationProvenance


def _owner(connection=lambda: pytest.fail("database must not be accessed")):
    return ActionResourceOwner(schema="aq", connection=connection, cursor_secret=b"s" * 32)


def test_reference_is_256_random_bits_in_frozen_grammar():
    refs = {new_resource_ref() for _ in range(100)}
    assert len(refs) == 100
    assert all(RESOURCE_RE.fullmatch(ref) for ref in refs)


def test_canonical_request_is_stable_and_rejects_non_json_numbers():
    assert canonical_request({"b": 2, "a": 1}) == b'{"a":1,"b":2}'
    with pytest.raises(ValueError):
        canonical_request({"unsafe": float("nan")})


def test_malformed_reference_fails_before_database_access():
    with pytest.raises(ResourceNotFound, match="resource not found"):
        _owner().snapshot(resource_ref="aqr1_bad", principal_scope="scope-a")


def test_cursor_is_opaque_integrity_protected_and_scope_root_bound():
    owner = _owner()
    ref = new_resource_ref()
    cursor = owner._cursor(ref, "scope-a", 17)
    assert "17" not in cursor and ref not in cursor
    assert owner._parse_cursor(cursor, ref, "scope-a") == 17
    assert owner._cursor(ref, "scope-a", 17) != cursor
    for wrong_ref, wrong_scope, wrong_cursor in (
        (new_resource_ref(), "scope-a", cursor),
        (ref, "scope-b", cursor),
        (ref, "scope-a", cursor[:-1] + ("A" if cursor[-1] != "A" else "B")),
    ):
        with pytest.raises(ResourceNotFound, match="resource not found"):
            owner._parse_cursor(wrong_cursor, wrong_ref, wrong_scope)


def test_projection_and_event_are_constructed_from_exact_recursive_allowlists():
    now = datetime(2026, 8, 9, tzinfo=timezone.utc)
    row = {"resource_ref": new_resource_ref(), "revision": 3, "state": "claimed",
           "created_at": now, "updated_at": now, "secret": "canary",
           "result_ref": "artifact:secret", "failure_reason": "secret"}
    session = {"ordinal": 1, "state": "running", "started_at": now, "ended_at": None,
               "claim_receipt": "secret", "worker": "secret"}
    with pytest.raises(Exception, match="unknown action resource owner field"):
        _projection(row, [session])
    clean_row = {"resource_ref": row["resource_ref"], "action_id": 7, "principal_scope": "scope-a",
                 "operation": "enqueue", "idempotency_key": "key", "request_digest": "a" * 64,
                 "request_snapshot": b"{}", "revision": 3, "enqueue_revision": 1,
                 "recovery_floor": 0, "state": "claimed", "created_at": now, "updated_at": now}
    clean_session = {key: session[key] for key in SESSION_KEYS}
    projection = _projection(clean_row, [clean_session])
    assert set(projection) == PROJECTION_KEYS
    assert set(projection["execution_sessions"][0]) == SESSION_KEYS
    assert "secret" not in repr(projection)
    with pytest.raises(Exception, match="unknown action resource event field"):
        _event({"revision": 4, "kind": "state_changed", "state": "failed",
                "occurred_at": now, "payload": {"prompt": "secret"}})
    event = _event({"revision": 4, "kind": "state_changed", "state": "failed", "occurred_at": now})
    assert set(event) == EVENT_KEYS
    assert event["outcome"] == "failed" and "secret" not in repr(event)


@pytest.mark.parametrize("value", [-1, 1.5, "1", True, 31])
def test_wait_rejects_every_non_integer_or_out_of_bounds_value_before_db(value):
    owner = _owner()
    ref = new_resource_ref()
    cursor = owner._cursor(ref, "scope-a", 0)
    with pytest.raises(ValueError, match="integer from 0 through 30"):
        owner.changes(resource_ref=ref, principal_scope="scope-a", cursor=cursor, wait_seconds=value)


def test_application_owner_boundary_requires_secret_and_serializes_exact_envelope():
    app = ActionQApplication(schema="aq")
    provenance = InvocationProvenance(actor="actor", environment="test", request_id="r", catalog_revision="c", idempotency_key="k")
    principal = AuthenticatedActionResourcePrincipal.derive(provenance)
    with pytest.raises(Exception, match="cursor secret is not configured"):
        ActionQApplication(schema="aq", authorizer=lambda *_: True).action_resource_snapshot(resource_ref=new_resource_ref(), principal=principal)
    envelope = {"schema_version": "resource-reference/v1", "resource_ref": "aqr1_" + "A" * 43, "revision": 1}
    expected = b'{"resource_ref":"aqr1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","revision":1,"schema_version":"resource-reference/v1"}\n'
    assert serialize_envelope(envelope) == expected
    assert app.serialize_action_resource_envelope(envelope) == expected
