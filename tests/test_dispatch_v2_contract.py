import hashlib
import io
import json
import socket
import threading
from http.server import HTTPServer
from types import MappingProxyType, SimpleNamespace

import pytest

from actionq import db
from actionq.application import ActionQApplication, InvocationProvenance
from actionq import application_dispatch
from actionq.managed_dispatch import ManagedDispatchPolicy, ManagedEnqueueAdmission
from actionq.vuoro import build_operations, catalog_metadata


class _Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _ObservationConnection:
    def __init__(self, *, snapshot=None, owner=None, events=()):
        self.snapshot = snapshot
        self.owner = owner
        self.events = events
        self.executed = []

    def transaction(self):
        return _Transaction()

    def execute(self, statement, params=None):
        text = " ".join(str(statement).split())
        self.executed.append((text, params))
        if text.startswith("SET TRANSACTION"):
            return _Rows()
        if text.startswith("SELECT a.id"):
            return _Rows([self.snapshot] if self.snapshot else [])
        if text.startswith("SELECT a.project"):
            return _Rows([self.owner] if self.owner else [])
        if text.startswith("SELECT id, event_type"):
            return _Rows(self.events)
        raise AssertionError(f"unexpected query: {text}")


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


def test_v2_enqueue_sets_its_floor_from_the_explicit_root_event_not_minimum_history(monkeypatch):
    class Connection:
        def __init__(self):
            self.executed = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def transaction(self):
            return _Transaction()

        def execute(self, statement, params=None):
            text = " ".join(str(statement).split())
            self.executed.append((text, params))
            assert "MIN(" not in text
            if text.startswith("SELECT action_id"):
                return _Rows()
            return _Rows()

    conn = Connection()
    provenance = InvocationProvenance(actor="compiler:test", environment="dev", request_id="r", catalog_revision="c", idempotency_key="k", authorized_repositories=("actionq",))
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: True)
    app.connection = lambda: conn
    monkeypatch.setattr(db, "enqueue", lambda *_args, **_kwargs: {"id": 7})
    monkeypatch.setattr(db, "insert_event", lambda *_args, **_kwargs: {"id": 44})

    app.enqueue_dispatch_v2(_request(), provenance=provenance)

    watermark = next(params for statement, params in conn.executed if "INSERT INTO \"aq\".\"dispatch_observation_watermarks\"" in statement)
    assert watermark == (7, 44, 44)


def test_managed_enqueue_persists_an_immutable_envelope_with_its_v2_root(monkeypatch):
    class Connection:
        def __init__(self):
            self.executed = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def transaction(self):
            return _Transaction()

        def execute(self, statement, params=None):
            self.executed.append((" ".join(str(statement).split()), params))
            return _Rows()

    payload = _request()
    admission = ManagedEnqueueAdmission(
        normalized_snapshot=b'{"managed":true}\n',
        request_sha256="a" * 64,
        capsule_sha256="b" * 64,
        rendered_prompt_sha256="c" * 64,
        queue_payload=MappingProxyType(payload),
    )
    conn = Connection()
    provenance = InvocationProvenance(actor="compiler:test", environment="dev", request_id="r", catalog_revision="c", idempotency_key="managed-key", authorized_repositories=("actionq",))
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: True)
    app.connection = lambda: conn
    monkeypatch.setattr(application_dispatch, "admit_managed_enqueue", lambda *_args, **_kwargs: admission)
    monkeypatch.setattr(db, "enqueue", lambda *_args, **_kwargs: {"id": 8})
    monkeypatch.setattr(db, "insert_event", lambda *_args, **_kwargs: {"id": 45})

    result = app.enqueue_managed_dispatch({}, provenance=provenance, expected_source_shas={}, registered_authority={})

    assert result["action_id"] == 8
    envelope_insert = next(params for statement, params in conn.executed if 'INSERT INTO "aq"."managed_dispatch_envelopes"' in statement)
    assert envelope_insert == (8, "a" * 64, b'{"managed":true}\n', "b" * 64, "c" * 64)


def test_vuoro_context_carries_only_trusted_repository_authorization():
    context = SimpleNamespace(identity=SimpleNamespace(actor="compiler:trusted", environment="dev", authorized_repositories=("actionq",)), request_id="r", catalog_revision="c", basis_revision=None, idempotency_key="k")
    operation = next(item for item in build_operations() if item.definition["name"] == "execution.dispatch.enqueue")
    # The handler will fail closed without the injected application authorizer;
    # this proves the public catalog does not admit caller requested_by instead.
    assert "requested_by" not in operation.definition["input_schema"]["properties"]


def test_vuoro_exposes_managed_enqueue_only_when_release_policy_is_injected(monkeypatch):
    assert "execution.managed-dispatch.enqueue" not in {
        item.definition["name"] for item in build_operations()
    }
    policy = ManagedDispatchPolicy(
        expected_source_shas={"agentops": "a" * 40},
        registered_authority={"commands": ["pytest"], "network": "none"},
    )
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: True, managed_dispatch_policy=policy)
    calls = []
    app.enqueue_managed_dispatch = lambda envelope, *, provenance: calls.append((envelope, provenance)) or {"action_id": 8, "status": "pending", "request_ref": "req:" + "a" * 32, "request_sha256": "b" * 64}
    operation = next(item for item in build_operations(app) if item.definition["name"] == "execution.managed-dispatch.enqueue")
    assert "prompt" not in operation.definition["input_schema"]["properties"]["dispatch"]["properties"]
    assert operation.definition["input_schema"]["additionalProperties"] is False

    envelope = {"contract_id": "managed-dispatch-enqueue/v1", "dispatch": {}, "managed_request": {}}
    context = SimpleNamespace(
        identity=SimpleNamespace(actor="operator:trusted", environment="dev", authorized_repositories=("actionq",)),
        request_id="r", catalog_revision="c", basis_revision=None, idempotency_key="managed-key",
    )
    result = operation.handler(envelope, context)
    assert result["action_id"] == 8
    assert calls[0][0] == envelope
    assert calls[0][1].actor == "operator:trusted"


def test_v2_snapshot_and_changes_are_allowlisted_and_recover_from_all_pruned_history():
    provenance = SimpleNamespace(actor="compiler:test", environment="dev", request_id="r", catalog_revision="c", idempotency_key="k", basis_revision=None, authorized_repositories=("actionq",))
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: True)
    snapshot_conn = _ObservationConnection(snapshot={
        "id": 7, "project": "actionq", "status": "completed", "action_type": "scope-iterate",
        "request_ref": "req:" + "a" * 32, "request_sha256": "b" * 64, "cursor": 44,
        "claim_receipt": "must-not-leak",
    })
    app._read = lambda reader: reader(snapshot_conn)
    snapshot = app.dispatch_action_snapshot(7, provenance=provenance)
    assert snapshot == {
        "action_id": 7, "status": "completed", "action_type": "scope-iterate",
        "request_ref": "req:" + "a" * 32, "request_sha256": "b" * 64,
        "cursor": "actionq:event:44",
    }

    changes_conn = _ObservationConnection(
        owner={"project": "actionq", "first_retained_event_id": 45, "cursor": 44},
    )
    app._read = lambda reader: reader(changes_conn)
    changes = app.dispatch_action_changes(7, cursor="actionq:event:44", wait_seconds=0, provenance=provenance)
    assert changes == {"status": "ok", "snapshot_required": False, "events": [], "cursor": "actionq:event:44"}

    migrated_all_pruned = _ObservationConnection(
        owner={"project": "actionq", "first_retained_event_id": 1, "cursor": 0},
    )
    app._read = lambda reader: reader(migrated_all_pruned)
    expired = app.dispatch_action_changes(7, cursor="actionq:event:44", wait_seconds=0, provenance=provenance)
    assert expired == {"status": "cursor_expired", "snapshot_required": True, "events": [], "cursor": "actionq:event:0"}


@pytest.mark.parametrize("cursor", ["event:4", "actionq:event:-1", "actionq:event:1x", "other:event:4"])
def test_v2_changes_rejects_noncanonical_cursor_without_database_access(cursor):
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: True)
    app._read = lambda _reader: pytest.fail("invalid cursor must not query the database")
    with pytest.raises(db.ActionQError, match="resource-not-found"):
        app.dispatch_action_changes(7, cursor=cursor, wait_seconds=0, provenance=SimpleNamespace())


def test_v2_changes_redacts_event_payload_and_non_enumerates_denied_access():
    provenance = SimpleNamespace(actor="compiler:test", environment="dev", request_id="r", catalog_revision="c", idempotency_key="k", basis_revision=None, authorized_repositories=("actionq",))
    app = ActionQApplication(schema="aq", authorizer=lambda *_args: True)
    conn = _ObservationConnection(
        owner={"project": "actionq", "first_retained_event_id": 3, "cursor": 4},
        events=[{"id": 4, "event_type": "action.completed", "payload": {"claim_receipt": "secret"}}],
    )
    app._read = lambda reader: reader(conn)
    result = app.dispatch_action_changes(7, cursor="actionq:event:3", wait_seconds=0, provenance=provenance)
    assert result["events"] == [{"id": "actionq:event:4", "type": "lifecycle", "terminal": True, "data": {}}]

    denied = ActionQApplication(schema="aq", authorizer=lambda *_args: False)
    denied._read = lambda reader: reader(_ObservationConnection(owner={"project": "actionq", "first_retained_event_id": 3, "cursor": 4}))
    with pytest.raises(db.ActionQError, match="^resource-not-found$"):
        denied.dispatch_action_changes(7, cursor=None, wait_seconds=0, provenance=provenance)


def test_observation_prune_preserves_highwater_when_all_events_are_removed():
    class PruneConnection:
        def __init__(self):
            self.update_params = None

        def transaction(self):
            return _Transaction()

        def execute(self, statement, params=None):
            text = " ".join(str(statement).split())
            if text.startswith("SELECT status"):
                return _Rows([{"status": "pending"}])
            if text.startswith("SELECT first_retained_event_id"):
                return _Rows([{"first_retained_event_id": 43, "last_observed_event_id": 44}])
            if text.startswith("DELETE FROM"):
                return _Rows([{"id": 43}, {"id": 44}])
            if text.startswith("UPDATE"):
                self.update_params = params
                return _Rows()
            raise AssertionError(text)

    conn = PruneConnection()
    assert db.prune_dispatch_observation_events(conn, "aq", action_id=7, through_event_id=44) == 2
    assert conn.update_params == (45, 44, 7)


def test_observation_prune_preserves_terminal_history_and_never_uses_minimum_event_id():
    class TerminalConnection:
        def transaction(self):
            return _Transaction()

        def execute(self, statement, params=None):
            text = " ".join(str(statement).split())
            assert "MIN(" not in text
            if text.startswith("SELECT status"):
                return _Rows([{"status": "completed"}])
            pytest.fail(f"terminal history must not be queried or deleted: {text}")

    with pytest.raises(db.ActionQError, match="terminal dispatch histories cannot be pruned"):
        db.prune_dispatch_observation_events(TerminalConnection(), "aq", action_id=7, through_event_id=44)


