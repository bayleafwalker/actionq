"""Executable protocol oracles for immutable execution groups (#2034).

The database histories in this module run only against the disposable
``ACTIONQ_TEST_URL`` installed by ``tests/conftest.py``.  The small model at
the bottom exhausts every operation sequence through depth three.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from itertools import product
import json
import os
import threading
import uuid

import pytest

from actionq import db, schema as schema_contract
from actionq.application import ActionQApplication, InvocationProvenance
from actionq_contracts import canonical_bytes, sha256_digest


pytestmark = pytest.mark.skipif(
    not os.environ.get("ACTIONQ_TEST_URL"),
    reason="ACTIONQ_TEST_URL is required for PostgreSQL protocol tests",
)


@pytest.fixture
def group_db():
    schema = "aqgroup_" + uuid.uuid4().hex
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    schema_contract.migrate(conn, schema, runtime_role="actionq_runtime")
    conn.commit()
    try:
        yield conn, schema
    finally:
        conn.close()


def _enqueue(conn, schema: str, target: str) -> dict:
    row = db.enqueue(
        conn, schema, action_type="scope-iterate", project="demo",
        target_ref=target, source_refs=[], priority=100, parent_id=None,
        created_by="human:test",
    )
    conn.commit()
    return row


def _envelope(action_id: int, *, attempt: str | None = None) -> dict:
    return {
        "contract_id": "execution-envelope/v1",
        "action_id": action_id,
        "attempt_id": attempt or f"attempt-{action_id}",
        "source_commit": "a" * 40,
        "command_id": "test:bounded",
        "allowed_paths": ["src", f"items/{action_id}"],
    }


def _spec(actions: list[dict], *, max_parallel: int = 1) -> dict:
    return {
        "plan_ref": "artifact:sha256:" + "b" * 64,
        "max_parallel": max_parallel,
        "failure_policy": "continue-independent",
        "members": [
            {"action_id": action["id"], "envelope": _envelope(action["id"])}
            for action in actions
        ],
        "created_by": "coordinator:test",
    }


def _realize(conn, schema: str, spec: dict) -> dict:
    result = db.realize_execution_group(conn, schema, **spec)
    conn.commit()
    return result


def test_realization_is_exactly_idempotent_and_rejects_conflicts(group_db):
    conn, schema = group_db
    actions = [_enqueue(conn, schema, str(index)) for index in range(2)]
    spec = _spec(actions, max_parallel=2)

    first = _realize(conn, schema, spec)
    replay = _realize(conn, schema, spec)
    assert replay["id"] == first["id"]
    assert replay["spec_sha256"] == first["spec_sha256"]
    assert [member["action_id"] for member in replay["members"]] == [
        action["id"] for action in actions
    ]
    events = conn.execute(
        f"SELECT count(*) AS n FROM {db.qname(schema, 'events')} "
        "WHERE event_type='execution_group.realized'"
    ).fetchone()
    assert events["n"] == 1

    conflict = dict(spec, max_parallel=1)
    with pytest.raises(db.ActionQError, match="different immutable spec"):
        db.realize_execution_group(conn, schema, **conflict)
    conn.rollback()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"plan_ref": "mutable:plan"}, "immutable artifact"),
        ({"max_parallel": 0}, "between 1 and 32"),
        ({"max_parallel": 33}, "between 1 and 32"),
        ({"failure_policy": "fail-fast"}, "continue-independent"),
        ({"members": []}, "at least one member"),
    ],
)
def test_realization_validates_group_contract(group_db, change, message):
    conn, schema = group_db
    action = _enqueue(conn, schema, "validation")
    spec = _spec([action]) | change
    with pytest.raises(db.ActionQError, match=message):
        db.realize_execution_group(conn, schema, **spec)


def test_realization_rejects_unknown_duplicate_nonpending_and_bad_envelopes(group_db):
    conn, schema = group_db
    first = _enqueue(conn, schema, "first")
    second = _enqueue(conn, schema, "second")

    duplicate = _spec([first])
    duplicate["members"] *= 2
    with pytest.raises(db.ActionQError, match="must be unique"):
        db.realize_execution_group(conn, schema, **duplicate)

    unknown = _spec([first])
    unknown["members"][0] = {
        "action_id": 999999999, "envelope": _envelope(999999999),
    }
    with pytest.raises(db.ActionQError, match="unknown action"):
        db.realize_execution_group(conn, schema, **unknown)
    conn.rollback()

    claimed = db.claim(conn, schema, worker="worker:pre", timeout_minutes=30)
    conn.commit()
    assert claimed["id"] == first["id"]
    with pytest.raises(db.ActionQError, match="must all be pending"):
        db.realize_execution_group(conn, schema, **_spec([first]))
    conn.rollback()

    mismatch = _spec([second])
    mismatch["members"][0]["envelope"]["action_id"] = first["id"]
    with pytest.raises(db.ActionQError, match="identity mismatch"):
        db.realize_execution_group(conn, schema, **mismatch)


def test_max_parallel_capacity_is_released_by_completion_and_failure(group_db):
    conn, schema = group_db
    actions = [_enqueue(conn, schema, str(index)) for index in range(3)]
    group = _realize(conn, schema, _spec(actions, max_parallel=1))

    first = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
    conn.commit()
    assert first["execution_group_id"] == str(group["id"])
    with pytest.raises(db.NoActionAvailable):
        db.claim(conn, schema, worker="worker:two", timeout_minutes=30)
    conn.rollback()

    db.complete(
        conn, schema, first["id"], "artifact:one", worker="worker:one",
        claim_receipt=first["claim_receipt"],
    )
    conn.commit()
    second = db.claim(conn, schema, worker="worker:two", timeout_minutes=30)
    conn.commit()
    assert second["id"] != first["id"]

    db.fail(
        conn, schema, second["id"], "expected failure", worker="worker:two",
        claim_receipt=second["claim_receipt"],
    )
    conn.commit()
    third = db.claim(conn, schema, worker="worker:three", timeout_minutes=30)
    conn.commit()
    assert {first["id"], second["id"], third["id"]} == {
        action["id"] for action in actions
    }


def test_cancelling_member_holds_capacity_until_terminal_finalization(group_db):
    conn, schema = group_db
    actions = [_enqueue(conn, schema, str(index)) for index in range(2)]
    group = _realize(conn, schema, _spec(actions, max_parallel=1))
    claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
    conn.commit()

    cancelling = db.cancel(conn, schema, claimed["id"], "operator stop", actor="operator:test")
    conn.commit()
    assert cancelling["status"] == "cancelling"
    with pytest.raises(db.NoActionAvailable):
        db.claim(conn, schema, worker="worker:two", timeout_minutes=30)
    conn.rollback()
    projection = db.get_execution_group(conn, schema, group["id"])
    assert projection["counts"]["cancelling"] == 1


def test_concurrent_claims_cannot_exceed_group_parallelism(group_db):
    setup, schema = group_db
    actions = [_enqueue(setup, schema, str(index)) for index in range(3)]
    _realize(setup, schema, _spec(actions, max_parallel=2))
    barrier = threading.Barrier(3)
    lock = threading.Lock()
    claimed: list[int] = []
    empty: list[str] = []
    errors: list[BaseException] = []

    def claim(worker: str) -> None:
        conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
        try:
            barrier.wait(timeout=5)
            try:
                row = db.claim(conn, schema, worker=worker, timeout_minutes=30)
                conn.commit()
                with lock:
                    claimed.append(row["id"])
            except db.NoActionAvailable:
                conn.rollback()
                with lock:
                    empty.append(worker)
        except BaseException as exc:  # preserve thread failures for the main test
            with lock:
                errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=claim, args=(f"worker:{index}",)) for index in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert len(claimed) == len(set(claimed)) == 2
    assert len(claimed) + len(empty) == 3


def test_full_interleaved_groups_cannot_deadlock_ordinary_claimers(group_db):
    setup, schema = group_db
    a1 = _enqueue(setup, schema, "a-1")
    b1 = _enqueue(setup, schema, "b-1")
    a2 = _enqueue(setup, schema, "a-2")
    b2 = _enqueue(setup, schema, "b-2")
    _realize(setup, schema, _spec([a1, a2], max_parallel=1))
    second_spec = _spec([b1, b2], max_parallel=1)
    second_spec["plan_ref"] = "artifact:sha256:" + "c" * 64
    _realize(setup, schema, second_spec)
    db.claim(setup, schema, worker="worker:a", timeout_minutes=30)
    setup.commit()
    db.claim(setup, schema, worker="worker:b", timeout_minutes=30)
    setup.commit()
    ordinary = [_enqueue(setup, schema, f"ordinary-{index}") for index in range(2)]
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    claimed: list[int] = []
    errors: list[BaseException] = []

    def claim_ordinary(worker: str) -> None:
        conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
        try:
            conn.execute("SET statement_timeout = '2s'")
            barrier.wait(timeout=5)
            row = db.claim(conn, schema, worker=worker, timeout_minutes=30)
            conn.commit()
            with lock:
                claimed.append(row["id"])
        except BaseException as exc:
            conn.rollback()
            with lock:
                errors.append(exc)
        finally:
            conn.close()

    threads = [
        threading.Thread(target=claim_ordinary, args=(f"worker:ordinary-{index}",))
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert set(claimed) == {row["id"] for row in ordinary}


def test_claim_versus_realize_serializes_on_the_action_row(group_db):
    setup, schema = group_db
    action = _enqueue(setup, schema, "claim-versus-realize")
    spec = _spec([action])
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    outcomes: dict[str, object] = {}
    errors: list[BaseException] = []

    def claim() -> None:
        conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
        try:
            barrier.wait(timeout=5)
            try:
                row = db.claim(conn, schema, worker="worker:race", timeout_minutes=30)
                conn.commit()
                with lock:
                    outcomes["claim"] = row
            except db.NoActionAvailable:
                conn.rollback()
                with lock:
                    outcomes["claim_empty"] = True
        except BaseException as exc:
            with lock:
                errors.append(exc)
        finally:
            conn.close()

    def realize() -> None:
        conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
        try:
            barrier.wait(timeout=5)
            try:
                row = db.realize_execution_group(conn, schema, **spec)
                conn.commit()
                with lock:
                    outcomes["realize"] = row
            except db.ActionQError as exc:
                conn.rollback()
                with lock:
                    outcomes["realize_rejected"] = str(exc)
        except BaseException as exc:
            with lock:
                errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=claim), threading.Thread(target=realize)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []

    if "realize" in outcomes:
        group = outcomes["realize"]
        if "claim" not in outcomes:
            # Claim may observe a transient empty queue because realization
            # holds the action row and claim uses SKIP LOCKED.  A retry after
            # the serializing commit must receive the frozen group envelope.
            assert outcomes["claim_empty"] is True
            outcomes["claim"] = db.claim(
                setup, schema, worker="worker:retry", timeout_minutes=30,
            )
            setup.commit()
        claimed = outcomes["claim"]
        assert claimed["execution_group_id"] == str(group["id"])
        assert claimed["execution_envelope_digest"] == sha256_digest(_envelope(action["id"]))
    else:
        claimed = outcomes["claim"]
        assert "must all be pending" in outcomes["realize_rejected"]
        assert "execution_group_id" not in claimed
        count = setup.execute(
            f"SELECT count(*) AS n FROM {db.qname(schema, 'execution_groups')}"
        ).fetchone()["n"]
        assert count == 0
    current = db.get_action(setup, schema, action["id"])
    assert current["status"] == "claimed"


def test_claim_versus_stop_has_only_serialized_outcomes(group_db):
    setup, schema = group_db
    action = _enqueue(setup, schema, "claim-versus-stop")
    group = _realize(setup, schema, _spec([action]))
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    outcomes: dict[str, object] = {}
    errors: list[BaseException] = []

    def claim() -> None:
        conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
        try:
            barrier.wait(timeout=5)
            try:
                row = db.claim(conn, schema, worker="worker:race", timeout_minutes=30)
                conn.commit()
                with lock:
                    outcomes["claim"] = row
            except db.NoActionAvailable:
                conn.rollback()
                with lock:
                    outcomes["claim_empty"] = True
        except BaseException as exc:
            with lock:
                errors.append(exc)
        finally:
            conn.close()

    def stop() -> None:
        conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
        try:
            barrier.wait(timeout=5)
            row = db.stop_execution_group(
                conn, schema, group_id=group["id"], actor="operator:race", reason="race",
            )
            conn.commit()
            with lock:
                outcomes["stop"] = row
        except BaseException as exc:
            with lock:
                errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=claim), threading.Thread(target=stop)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert outcomes["stop"]["claim_state"] == "stopped"

    projection = db.get_execution_group(setup, schema, group["id"])
    if "claim" in outcomes:
        assert outcomes["claim"]["execution_group_id"] == str(group["id"])
        assert projection["counts"]["claimed"] == 1
        assert projection["counts"]["pending_stopped"] == 0
    else:
        assert outcomes["claim_empty"] is True
        assert projection["counts"]["claimed"] == 0
        assert projection["counts"]["pending_stopped"] == 1


def test_stop_fences_new_claims_but_not_existing_claim_or_ungrouped_action(group_db):
    conn, schema = group_db
    grouped = [_enqueue(conn, schema, f"grouped-{index}") for index in range(2)]
    group = _realize(conn, schema, _spec(grouped))
    claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
    conn.commit()

    stopped = db.stop_execution_group(
        conn, schema, group_id=group["id"], actor="operator:test", reason="bounded stop",
    )
    conn.commit()
    replay = db.stop_execution_group(
        conn, schema, group_id=group["id"], actor="operator:other", reason="ignored",
    )
    conn.commit()
    assert stopped["claim_state"] == replay["claim_state"] == "stopped"
    assert replay["stopped_by"] == "operator:test"

    completed = db.complete(
        conn, schema, claimed["id"], "artifact:done", worker="worker:one",
        claim_receipt=claimed["claim_receipt"],
    )
    conn.commit()
    assert completed["status"] == "completed"
    ungrouped = _enqueue(conn, schema, "ordinary")
    ordinary_claim = db.claim(conn, schema, worker="worker:two", timeout_minutes=30)
    conn.commit()
    assert ordinary_claim["id"] == ungrouped["id"]
    projection = db.get_execution_group(conn, schema, group["id"])
    assert projection["counts"]["pending_stopped"] == 1


def test_sweep_requeues_claim_but_stop_keeps_it_unclaimable(group_db):
    conn, schema = group_db
    action = _enqueue(conn, schema, "sweep-after-stop")
    group = _realize(conn, schema, _spec([action]))
    claimed = db.claim(conn, schema, worker="worker:expired", timeout_minutes=30)
    conn.commit()
    db.stop_execution_group(
        conn, schema, group_id=group["id"], actor="operator:test", reason="permanent stop",
    )
    conn.execute(
        f"UPDATE {db.qname(schema, 'actions')} "
        "SET claim_deadline=now() - interval '1 second' WHERE id=%s",
        (claimed["id"],),
    )
    conn.commit()

    swept = db.sweep(conn, schema, actor="sweeper:test")
    conn.commit()
    assert [row["id"] for row in swept] == [action["id"]]
    assert swept[0]["status"] == "pending"
    assert swept[0]["claimed_by"] is None
    with pytest.raises(db.NoActionAvailable):
        db.claim(conn, schema, worker="worker:late", timeout_minutes=30)
    conn.rollback()
    projection = db.get_execution_group(conn, schema, group["id"])
    assert projection["counts"]["pending_stopped"] == 1
    assert projection["counts"]["claimed"] == 0


def test_stopped_group_prefix_cannot_starve_later_ordinary_action(group_db):
    conn, schema = group_db
    grouped = [_enqueue(conn, schema, f"stopped-{index}") for index in range(64)]
    group = _realize(conn, schema, _spec(grouped, max_parallel=32))
    db.stop_execution_group(
        conn, schema, group_id=group["id"], actor="operator:test", reason="permanent stop",
    )
    conn.commit()
    ordinary = _enqueue(conn, schema, "ordinary-after-stopped-prefix")

    claimed = db.claim(conn, schema, worker="worker:ordinary", timeout_minutes=30)
    conn.commit()
    assert claimed["id"] == ordinary["id"]
    assert "execution_group_id" not in claimed


def test_claim_returns_exact_frozen_envelope_and_digest(group_db):
    conn, schema = group_db
    action = _enqueue(conn, schema, "exact")
    envelope = _envelope(action["id"], attempt="attempt-exact")
    spec = _spec([action])
    spec["members"] = [{"action_id": action["id"], "envelope": envelope}]
    _realize(conn, schema, spec)

    claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
    conn.commit()
    assert canonical_bytes(claimed["execution_envelope"]) == canonical_bytes(envelope)
    assert claimed["execution_envelope_digest"] == sha256_digest(envelope)


def test_corrupt_stored_envelope_fails_closed_before_claim_mutation(group_db):
    conn, schema = group_db
    action = _enqueue(conn, schema, "corrupt")
    group = _realize(conn, schema, _spec([action]))
    conn.execute(
        f"UPDATE {db.qname(schema, 'execution_group_members')} "
        "SET envelope_snapshot=%s WHERE group_id=%s",
        (json.dumps(_envelope(action["id"]) | {"command_id": "tampered"}).encode(), group["id"]),
    )
    conn.commit()

    with pytest.raises(db.ActionQError, match="envelope"):
        db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
    conn.rollback()
    current = db.get_action(conn, schema, action["id"])
    assert current["status"] == "pending"
    assert current["claimed_by"] is None


def test_group_projections_redact_snapshots_and_authority_secrets(group_db):
    conn, schema = group_db
    action = _enqueue(conn, schema, "redaction")
    group = _realize(conn, schema, _spec([action]))
    shown = db.get_execution_group(conn, schema, group["id"])
    listed = db.list_execution_groups(conn, schema)
    serialized = json.dumps({"shown": shown, "listed": listed}, default=str)
    assert "spec_snapshot" not in serialized
    assert "envelope_snapshot" not in serialized
    assert "claim_receipt" not in serialized
    assert "runner_auth" not in serialized
    assert "command_id" not in serialized


def test_served_realization_authorization_and_idempotency(group_db):
    setup, schema = group_db
    action = _enqueue(setup, schema, "served")
    allowed_calls: list[tuple[str, str]] = []

    def authorize(_provenance, resource, verb):
        allowed_calls.append((resource, verb))
        return resource == "execution.group.manage" and verb == "create"

    runtime_url = os.environ.get("ACTIONQ_TEST_RUNTIME_URL") or os.environ[
        "ACTIONQ_TEST_URL"
    ].replace("//actionq:", "//actionq_runtime:")
    app = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(runtime_url),
        authorizer=authorize,
    )
    provenance = InvocationProvenance(
        actor="coordinator:test", environment="served:test",
        request_id="request-one", catalog_revision="catalog:test",
        idempotency_key="group-one",
    )
    spec = _spec([action])
    first = app.realize_execution_group(**spec, provenance=provenance)
    replay = app.realize_execution_group(**spec, provenance=replace(provenance, request_id="request-two"))
    assert first["result"]["id"] == replay["result"]["id"]
    assert replay["decision"]["replayed"] is True
    assert allowed_calls == [
        ("execution.group.manage", "create"),
        ("execution.group.manage", "create"),
    ]

    denied = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(runtime_url),
        authorizer=lambda *_args: False,
    )
    with pytest.raises(db.ActionQError, match="authorization denied"):
        denied.realize_execution_group(
            **(spec | {"plan_ref": "artifact:sha256:" + "c" * 64}),
            provenance=replace(provenance, idempotency_key="denied"),
        )


def test_served_stop_authorization_idempotency_and_denial(group_db):
    setup, schema = group_db
    allowed_action = _enqueue(setup, schema, "served-stop")
    denied_action = _enqueue(setup, schema, "served-stop-denied")
    allowed_group = _realize(setup, schema, _spec([allowed_action]))
    denied_spec = _spec([denied_action]) | {
        "plan_ref": "artifact:sha256:" + "d" * 64,
    }
    denied_group = _realize(setup, schema, denied_spec)
    runtime_url = os.environ.get("ACTIONQ_TEST_RUNTIME_URL") or os.environ[
        "ACTIONQ_TEST_URL"
    ].replace("//actionq:", "//actionq_runtime:")
    authorization_calls: list[tuple[str, str]] = []

    def authorize(_provenance, resource, verb):
        authorization_calls.append((resource, verb))
        return resource == "execution.group.manage" and verb == "update"

    app = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(runtime_url),
        authorizer=authorize,
    )
    provenance = InvocationProvenance(
        actor="coordinator:test", environment="served:test",
        request_id="stop-request-one", catalog_revision="catalog:test",
        idempotency_key="stop-group-one",
    )
    arguments = {
        "group_id": str(allowed_group["id"]),
        "actor": "coordinator:test",
        "reason": "served bounded stop",
    }
    first = app.stop_execution_group(**arguments, provenance=provenance)
    replay = app.stop_execution_group(
        **arguments, provenance=replace(provenance, request_id="stop-request-two"),
    )
    assert first["result"]["claim_state"] == "stopped"
    assert replay["result"] == first["result"]
    assert replay["decision"]["replayed"] is True
    assert authorization_calls == [
        ("execution.group.manage", "update"),
        ("execution.group.manage", "update"),
    ]

    denied = ActionQApplication(
        schema=schema,
        connection_factory=lambda: db.connect(runtime_url),
        authorizer=lambda *_args: False,
    )
    with pytest.raises(db.ActionQError, match="authorization denied"):
        denied.stop_execution_group(
            group_id=str(denied_group["id"]), actor="coordinator:test", reason="denied",
            provenance=replace(
                provenance, request_id="stop-denied", idempotency_key="stop-denied",
            ),
        )
    still_active = db.get_execution_group(setup, schema, denied_group["id"])
    assert still_active["claim_state"] == "active"


@dataclass(frozen=True)
class _Model:
    group_active: bool = True
    member_status: str = "pending"
    ungrouped_status: str = "pending"


def _step(state: _Model, operation: str) -> _Model:
    if operation == "stop":
        return replace(state, group_active=False)
    if operation == "claim-member" and state.group_active and state.member_status == "pending":
        return replace(state, member_status="claimed")
    if operation == "complete-member" and state.member_status == "claimed":
        return replace(state, member_status="completed")
    if operation == "fail-member" and state.member_status == "claimed":
        return replace(state, member_status="failed")
    if operation == "claim-ungrouped" and state.ungrouped_status == "pending":
        return replace(state, ungrouped_status="claimed")
    return state


def test_depth_three_stop_claim_terminal_model():
    operations = (
        "stop", "claim-member", "complete-member", "fail-member", "claim-ungrouped",
    )
    for history in product(operations, repeat=3):
        state = _Model()
        stopped_once = False
        terminal_once = False
        for operation in history:
            before = state
            state = _step(state, operation)
            stopped_once |= operation == "stop"
            terminal_once |= state.member_status in {"completed", "failed"}
            if stopped_once and before.member_status == "pending" and operation == "claim-member":
                assert state.member_status == "pending"
            if terminal_once:
                assert state.member_status in {"completed", "failed"}
            if operation == "claim-ungrouped" and before.ungrouped_status == "pending":
                assert state.ungrouped_status == "claimed"
        assert state.member_status in {"pending", "claimed", "completed", "failed"}
