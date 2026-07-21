from __future__ import annotations

import os
import threading
from types import SimpleNamespace
import uuid

import pytest

from actionq import db
from actionq.application import ActionQApplication
from actionq.vuoro import (
    SCHEMA_DIALECT,
    build_operations,
    catalog_metadata,
    compatibility_record,
    register_operations,
)


def _context(
    *,
    actor: str = "runner:test",
    request_id: str = "request-1",
    idempotency_key: str | None = "idempotency-1",
):
    return SimpleNamespace(
        identity=SimpleNamespace(actor=actor, environment="vuoro-dev"),
        request_id=request_id,
        catalog_revision="catalog-test",
        basis_revision="basis-test",
        idempotency_key=idempotency_key,
    )


def test_execution_catalog_is_domain_owned_and_runtime_only():
    catalog = catalog_metadata()
    names = [definition["name"] for definition in catalog]

    assert len(names) == len(set(names))
    assert set(names) == {
        "execution.action.enqueue",
        "execution.action.list",
        "execution.action.show",
        "execution.action.claim",
        "execution.action.renew",
        "execution.action.complete",
        "execution.action.fail",
        "execution.action.reject",
        "execution.action.cancel",
        "execution.action.sweep",
        "execution.event.list",
        "execution.session.list",
        "execution.session.record",
        "execution.dispatch.enqueue",
        "execution.dispatch.list",
    }
    assert not any("migrate" in name for name in names)
    for definition in catalog:
        assert definition["owning_domain"] == "execution"
        assert definition["name"].startswith("execution.")
        assert definition["input_schema"]["$schema"] == SCHEMA_DIALECT
        assert definition["result_schema"]["$schema"] == SCHEMA_DIALECT
        if definition["execution_semantics"] != "read":
            assert definition["idempotency"] == "required"

    by_name = {definition["name"]: definition for definition in catalog}
    assert (
        "worker" not in by_name["execution.action.claim"]["input_schema"]["properties"]
    )
    assert (
        "worker" not in by_name["execution.action.renew"]["input_schema"]["properties"]
    )
    assert (
        "requested_by"
        not in by_name["execution.dispatch.enqueue"]["input_schema"]["properties"]
    )


def test_registry_composition_accepts_an_injected_protocol_definition_factory():
    class Definition:
        def __init__(self, **values):
            self.__dict__.update(values)

    class Registry:
        def __init__(self):
            self.registered = []

        def register(self, definition, handler):
            self.registered.append((definition, handler))

    registry = Registry()
    register_operations(registry, definition_factory=Definition)

    assert [definition.name for definition, _handler in registry.registered] == [
        definition["name"] for definition in catalog_metadata()
    ]
    assert all(callable(handler) for _definition, handler in registry.registered)


@pytest.fixture
def runtime_application(monkeypatch):
    schema = "aqvuoro_" + uuid.uuid4().hex
    with db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"]) as conn:
        db.migrate(conn, schema)
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    return ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(os.environ["ACTIONQ_TEST_RUNTIME_URL"]),
    )


def _handlers(application):
    return {
        operation.definition["name"]: operation.handler
        for operation in build_operations(application)
    }


def test_adapter_idempotency_and_durable_provenance(runtime_application):
    handlers = _handlers(runtime_application)
    enqueue = handlers["execution.action.enqueue"]
    arguments = {
        "action_type": "scope-iterate",
        "project": "actionq",
        "target_ref": "1197",
        "source_refs": ["wi:1197"],
    }

    first = enqueue(arguments, _context(request_id="enqueue-1", idempotency_key="enq"))
    replay = enqueue(arguments, _context(request_id="enqueue-2", idempotency_key="enq"))
    conflict = enqueue(
        {**arguments, "target_ref": "different"},
        _context(request_id="enqueue-3", idempotency_key="enq"),
    )

    assert first["decision"]["status"] == "accepted"
    assert first["decision"]["replayed"] is False
    assert replay["decision"]["replayed"] is True
    assert replay["decision"]["decision_ref"] == first["decision"]["decision_ref"]
    assert replay["result"]["id"] == first["result"]["id"]
    assert conflict["decision"]["status"] == "rejected"
    assert conflict["decision"]["code"] == "idempotency-key-conflict"

    actions = runtime_application.list_actions(limit=50)
    assert len(actions) == 1
    history = runtime_application.show_action(first["result"]["id"])
    assert history is not None
    enqueued = next(
        event for event in history["events"] if event["event_type"] == "action_enqueued"
    )
    assert enqueued["payload"]["provenance"] == {
        "actor": "runner:test",
        "environment": "vuoro-dev",
        "request_id": "enqueue-1",
        "catalog_revision": "catalog-test",
        "basis_revision": "basis-test",
        "idempotency_key": "enq",
        "operation": "execution.action.enqueue",
    }
    assert any(
        reference.startswith("actionq:event:")
        for reference in first["decision"]["event_refs"]
    )


def test_adapter_claim_renew_stale_worker_and_terminal_retry(runtime_application):
    handlers = _handlers(runtime_application)
    enqueue = handlers["execution.action.enqueue"]
    claim = handlers["execution.action.claim"]
    renew = handlers["execution.action.renew"]
    complete = handlers["execution.action.complete"]

    enqueued = enqueue(
        {"action_type": "scope-iterate"},
        _context(request_id="enqueue", idempotency_key="enqueue"),
    )["result"]
    claimed = claim(
        {"timeout_minutes": 30},
        _context(actor="runner:one", request_id="claim", idempotency_key="claim"),
    )
    assert claimed["result"]["id"] == enqueued["id"]
    assert claimed["result"]["claimed_by"] == "runner:one"

    renewed = renew(
        {"action_id": enqueued["id"], "timeout_minutes": 45},
        _context(actor="runner:one", request_id="renew", idempotency_key="renew"),
    )
    stale = renew(
        {"action_id": enqueued["id"], "timeout_minutes": 45},
        _context(actor="runner:stale", request_id="stale", idempotency_key="stale"),
    )
    assert renewed["decision"]["status"] == "accepted"
    assert stale["decision"]["status"] == "rejected"
    assert stale["decision"]["code"] == "claim-rejected"
    assert stale["decision"]["message"] == "claimed-by-different-worker"

    completed = complete(
        {"action_id": enqueued["id"], "result_ref": "sha:abc"},
        _context(
            actor="runner:one", request_id="complete-1", idempotency_key="complete"
        ),
    )
    replay = complete(
        {"action_id": enqueued["id"], "result_ref": "sha:abc"},
        _context(
            actor="runner:one", request_id="complete-2", idempotency_key="complete"
        ),
    )
    assert completed["result"]["status"] == "completed"
    assert replay["decision"]["replayed"] is True


def test_adapter_concurrent_claims_remain_unique(runtime_application):
    handlers = _handlers(runtime_application)
    enqueue = handlers["execution.action.enqueue"]
    claim = handlers["execution.action.claim"]
    for index in range(2):
        enqueue(
            {"action_type": "scope-iterate", "target_ref": str(index)},
            _context(request_id=f"enqueue-{index}", idempotency_key=f"enqueue-{index}"),
        )

    barrier = threading.Barrier(2)
    results = []
    errors = []

    def claim_once(index: int):
        try:
            barrier.wait(timeout=5)
            results.append(
                claim(
                    {},
                    _context(
                        actor=f"runner:{index}",
                        request_id=f"claim-{index}",
                        idempotency_key=f"claim-{index}",
                    ),
                )
            )
        except Exception as error:  # pragma: no cover - asserted below
            errors.append(error)

    threads = [threading.Thread(target=claim_once, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors
    assert len(results) == 2
    assert {result["decision"]["status"] for result in results} == {"accepted"}
    assert len({result["result"]["id"] for result in results}) == 2


def test_adapter_dispatch_and_session_histories_use_identity(runtime_application):
    handlers = _handlers(runtime_application)
    dispatched = handlers["execution.dispatch.enqueue"](
        {
            "contract_version": "v1",
            "repo_id": "actionq",
            "kind": "implement",
            "title": "Adapter integration",
            "work_item_id": "1197",
        },
        _context(
            actor="operator:vuoro",
            request_id="dispatch",
            idempotency_key="dispatch",
        ),
    )
    action_id = dispatched["result"]["id"]
    session = handlers["execution.session.record"](
        {
            "event_type": "session.dispatch",
            "action_id": action_id,
            "payload": {
                "session_id": "aqs:1197",
                "project": "actionq",
                "ttl_seconds": 120,
            },
        },
        _context(
            actor="runner:local",
            request_id="session",
            idempotency_key="session",
        ),
    )
    assert session["decision"]["status"] == "accepted"

    dispatches = handlers["execution.dispatch.list"]({}, _context(idempotency_key=None))
    sessions = handlers["execution.session.list"]({}, _context(idempotency_key=None))
    assert dispatches[0]["title"] == "Adapter integration"
    assert sessions[0]["session_id"] == "aqs:1197"
    assert sessions[0]["last_event_type"] == "session.dispatch"
    history = runtime_application.show_action(action_id)
    assert history is not None
    requested = next(
        event
        for event in history["events"]
        if event["event_type"] == "dispatch.requested"
    )
    assert requested["actor"] == "operator:vuoro"


def test_adapter_compatibility_uses_runtime_role_and_refuses_migration_role(
    runtime_application,
):
    record = compatibility_record(runtime_application)
    assert record == {
        "api_version": "v1",
        "schema_version": "1",
        "state": "compatible",
        "reason": None,
    }

    migration_application = ActionQApplication(
        schema=runtime_application.schema,
        connection_factory=lambda: db.connect(os.environ["ACTIONQ_TEST_MIGRATION_URL"]),
    )
    refused = compatibility_record(migration_application)
    assert refused["state"] == "incompatible"
    assert "runtime principal" in refused["reason"]
