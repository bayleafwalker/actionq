"""Disposable-Postgres histories for the verified dispatch settlement fence."""

from __future__ import annotations

import os
import uuid

import pytest

from actionq import db, schema as schema_contract
from actionq.application import ActionQApplication, InvocationProvenance
from actionq.cas import _DaemonCAS
from actionq_contracts import DISPATCH_RESULT_V1, artifact_digest

pytestmark = pytest.mark.skipif(
    not os.environ.get("ACTIONQ_TEST_URL"),
    reason="ACTIONQ_TEST_URL is required for disposable Postgres verification",
)


def _connect(schema: str):
    migration_url = os.environ.get("ACTIONQ_TEST_MIGRATION_URL", os.environ["ACTIONQ_TEST_URL"])
    conn = db.connect(migration_url)
    runtime_role = "actionq_runtime" if os.environ.get("ACTIONQ_TEST_MIGRATION_URL") else None
    schema_contract.migrate(conn, schema, runtime_role=runtime_role)
    conn.commit()
    return conn


def _enqueue(conn, schema: str, action_type: str = "scope-iterate") -> int:
    row = db.enqueue(
        conn, schema, action_type=action_type, project="test", target_ref=None,
        source_refs=[], priority=100, parent_id=None, created_by="test:dispatch-result",
    )
    conn.commit()
    return int(row["id"])


def _result(
    store: _DaemonCAS, action_id: int, attempt_id: str, *, terminal_status: str = "completed",
    stop_reason: str | None = None,
) -> dict:
    result_ref = store.put(b"dispatch-result")
    return {
        "contract_id": DISPATCH_RESULT_V1,
        "action_id": action_id,
        "attempt_id": attempt_id,
        "terminal_status": terminal_status,
        "result_ref": result_ref,
        "result_digest": artifact_digest(result_ref),
        "stop_reason": stop_reason,
    }


def _state(conn, schema: str, action_id: int):
    return dict(conn.execute(
        f'SELECT status, result_ref, failure_reason, claim_receipt, claim_attempt_id '
        f'FROM "{schema}".actions WHERE id=%s', (action_id,)
    ).fetchone())


def _event_count(conn, schema: str, action_id: int) -> int:
    return int(conn.execute(
        f'SELECT count(*) AS count FROM "{schema}".events WHERE action_id=%s', (action_id,)
    ).fetchone()["count"])


def test_missing_and_rehashed_referents_leave_claim_and_events_untouched(tmp_path):
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    schema = "aqdispatch_" + uuid.uuid4().hex
    conn = _connect(schema)
    try:
        action_id = _enqueue(conn, schema)
        claim = db.claim(conn, schema, worker="worker:test", timeout_minutes=30)
        conn.commit()
        before = _state(conn, schema, action_id)
        events_before = _event_count(conn, schema, action_id)

        missing = _result(store, action_id, claim["attempt_id"])
        store._path(missing["result_ref"]).unlink()
        with pytest.raises(db.ActionQError, match="referent"):
            db.settle_dispatch_result(
                conn, schema, action_id=action_id, result=missing,
                worker="worker:test", claim_receipt=claim["claim_receipt"], _artifact_store=store,
            )
        assert _state(conn, schema, action_id) == before
        assert _event_count(conn, schema, action_id) == events_before

        rehashed = _result(store, action_id, claim["attempt_id"])
        store._path(rehashed["result_ref"]).write_bytes(b"different bytes")
        store._path(rehashed["result_ref"]).chmod(0o600)
        with pytest.raises(db.ActionQError, match="referent"):
            db.settle_dispatch_result(
                conn, schema, action_id=action_id, result=rehashed,
                worker="worker:test", claim_receipt=claim["claim_receipt"], _artifact_store=store,
            )
        assert _state(conn, schema, action_id) == before
        assert _event_count(conn, schema, action_id) == events_before

        # The prior ref was deliberately corrupted; this is a new valid object.
        valid_ref = store.put(b"valid dispatch result")
        valid = {
            "contract_id": DISPATCH_RESULT_V1, "action_id": action_id,
            "attempt_id": claim["attempt_id"], "terminal_status": "completed",
            "result_ref": valid_ref, "result_digest": artifact_digest(valid_ref),
            "stop_reason": None,
        }
        settled = db.settle_dispatch_result(
            conn, schema, action_id=action_id, result=valid,
            worker="worker:test", claim_receipt=claim["claim_receipt"], _artifact_store=store,
        )
        conn.commit()
        assert settled["status"] == "completed"
    finally:
        conn.close()


def test_attempt_binding_rejects_fabricated_cross_action_and_cross_claim_packets(tmp_path):
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    schema = "aqdispatch_" + uuid.uuid4().hex
    conn = _connect(schema)
    try:
        action_a, action_b = _enqueue(conn, schema), _enqueue(conn, schema)
        claim_a = db.claim(conn, schema, worker="worker:a", timeout_minutes=30)
        claim_b = db.claim(conn, schema, worker="worker:b", timeout_minutes=30)
        conn.commit()
        before = _state(conn, schema, action_a)
        events_before = _event_count(conn, schema, action_a)

        with pytest.raises(db.ActionQError):
            db.settle_dispatch_result(
                conn, schema, action_id=action_a,
                result=_result(store, action_b, claim_b["attempt_id"]), _artifact_store=store,
                actor="worker:a", worker="worker:a", claim_receipt=claim_a["claim_receipt"],
            )
        with pytest.raises(db.ActionQError):
            db.settle_dispatch_result(
                conn, schema, action_id=action_a,
                result=_result(store, action_a, "aqa:invented"), _artifact_store=store,
                actor="worker:a", worker="worker:a", claim_receipt=claim_a["claim_receipt"],
            )
        with pytest.raises(db.ActionQError):
            db.settle_dispatch_result(
                conn, schema, action_id=action_a,
                result=_result(store, action_a, claim_b["attempt_id"]), _artifact_store=store,
                actor="worker:a", worker="worker:a", claim_receipt=claim_a["claim_receipt"],
            )
        conn.commit()
        assert _state(conn, schema, action_a) == before
        assert _event_count(conn, schema, action_a) == events_before
    finally:
        conn.close()


@pytest.mark.parametrize("history", ["expired", "cancelled", "swept", "reclaimed"])
def test_stale_claim_histories_leave_no_verified_settlement_residue(history: str, tmp_path):
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    schema = "aqdispatch_" + uuid.uuid4().hex
    conn = _connect(schema)
    try:
        action_id = _enqueue(conn, schema)
        first = db.claim(conn, schema, worker="worker:first", timeout_minutes=1)
        if history == "expired":
            conn.execute(
                f'UPDATE "{schema}".actions SET claim_deadline=now()-interval \'1 minute\' WHERE id=%s',
                (action_id,),
            )
            conn.commit()
            before = _state(conn, schema, action_id)
            with pytest.raises(db.ActionQError):
                db.settle_dispatch_result(
                    conn, schema, action_id=action_id, result=_result(store, action_id, first["attempt_id"]), _artifact_store=store,
                    worker="worker:first", claim_receipt=first["claim_receipt"],
                )
            conn.commit()
            assert _state(conn, schema, action_id) == before
            return
        if history == "cancelled":
            cancelled = db.cancel(conn, schema, action_id, "operator stop", actor="controller")
            conn.commit()
            assert cancelled["status"] == "cancelling"
            with pytest.raises(db.ActionQError):
                db.settle_dispatch_result(
                    conn, schema, action_id=action_id, result=_result(store, action_id, first["attempt_id"]), _artifact_store=store,
                    worker="worker:first", claim_receipt=first["claim_receipt"],
                )
            conn.commit()
            assert _state(conn, schema, action_id)["status"] == "cancelling"
            return

        conn.execute(
            f'UPDATE "{schema}".actions SET claim_deadline=now()-interval \'1 minute\' WHERE id=%s',
            (action_id,),
        )
        conn.commit()
        db.sweep(conn, schema)
        conn.commit()
        if history == "swept":
            before = _state(conn, schema, action_id)
            with pytest.raises(db.ActionQError):
                db.settle_dispatch_result(
                    conn, schema, action_id=action_id, result=_result(store, action_id, first["attempt_id"]), _artifact_store=store,
                    worker="worker:first", claim_receipt=first["claim_receipt"],
                )
            conn.commit()
            assert _state(conn, schema, action_id) == before
            return

        replacement = db.claim(conn, schema, worker="worker:replacement", timeout_minutes=30)
        conn.commit()
        before = _state(conn, schema, action_id)
        with pytest.raises(db.ActionQError):
            db.settle_dispatch_result(
                conn, schema, action_id=action_id, result=_result(store, action_id, first["attempt_id"]), _artifact_store=store,
                worker="worker:replacement", claim_receipt=replacement["claim_receipt"],
            )
        conn.commit()
        assert _state(conn, schema, action_id) == before
        settled = db.settle_dispatch_result(
            conn, schema, action_id=action_id, result=_result(store, action_id, replacement["attempt_id"]), _artifact_store=store,
            worker="worker:replacement", claim_receipt=replacement["claim_receipt"],
        )
        conn.commit()
        assert settled["status"] == "completed"
    finally:
        conn.close()


def test_malformed_packet_and_event_rollback_leave_claim_untouched(monkeypatch, tmp_path):
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    schema = "aqdispatch_" + uuid.uuid4().hex
    conn = _connect(schema)
    try:
        action_id = _enqueue(conn, schema)
        claim = db.claim(conn, schema, worker="worker:test", timeout_minutes=30)
        conn.commit()
        before = _state(conn, schema, action_id)
        events_before = _event_count(conn, schema, action_id)
        malformed = _result(store, action_id, claim["attempt_id"])
        malformed["stop_reason"] = "secret=do-not-store"
        with pytest.raises(db.ActionQError):
            db.settle_dispatch_result(
                conn, schema, action_id=action_id, result=malformed,
                worker="worker:test", claim_receipt=claim["claim_receipt"], _artifact_store=store,
            )
        conn.commit()
        assert _state(conn, schema, action_id) == before
        assert _event_count(conn, schema, action_id) == events_before

        original_insert = db.insert_event
        def fail_terminal_event(*args, **kwargs):
            if kwargs.get("event_type") == "action_completed":
                raise RuntimeError("injected event insert failure")
            return original_insert(*args, **kwargs)
        monkeypatch.setattr(db, "insert_event", fail_terminal_event)
        with pytest.raises(RuntimeError, match="event insert failure"):
            db.settle_dispatch_result(
                conn, schema, action_id=action_id, result=_result(store, action_id, claim["attempt_id"]),
                worker="worker:test", claim_receipt=claim["claim_receipt"], _artifact_store=store,
            )
        conn.commit()
        assert _state(conn, schema, action_id) == before
        assert _event_count(conn, schema, action_id) == events_before
    finally:
        conn.close()


def test_response_loss_replays_decision_and_reconciles_terminal_history(monkeypatch, tmp_path):
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    schema = "aqdispatch_" + uuid.uuid4().hex
    conn = _connect(schema)
    conn.close()
    def connect():
        return db.connect(os.environ.get("ACTIONQ_TEST_RUNTIME_URL", os.environ["ACTIONQ_TEST_URL"]))
    app = ActionQApplication(
        schema=schema, connection_factory=connect, artifact_root=store.root,
        cas_factory=lambda root: _DaemonCAS(root, allow_unsafe_test_root=True),
    )
    try:
        with connect() as setup:
            action_id = _enqueue(setup, schema)
            claim = db.claim(setup, schema, worker="worker:replay", timeout_minutes=30)
            setup.commit()
        provenance = InvocationProvenance(
            actor="worker:replay", environment="test", request_id="settle-1",
            catalog_revision="test", idempotency_key="settle-1",
        )
        original_mutate = app._mutate
        def lose_response(*args, **kwargs):
            value = original_mutate(*args, **kwargs)
            raise ConnectionError("response lost after commit")
        monkeypatch.setattr(app, "_mutate", lose_response)
        with pytest.raises(ConnectionError, match="response lost"):
            app.settle(
                action_id=action_id, result=_result(store, action_id, claim["attempt_id"]),
                actor="worker:replay", claim_receipt=claim["claim_receipt"], provenance=provenance,
            )
        monkeypatch.setattr(app, "_mutate", original_mutate)
        replay = app.settle(
            action_id=action_id, result=_result(store, action_id, claim["attempt_id"]),
            actor="worker:replay", claim_receipt=claim["claim_receipt"], provenance=provenance,
        )
        assert replay["decision"]["replayed"] is True
        with connect() as check:
            row = _state(check, schema, action_id)
            assert row["status"] == "completed"
            terminal_events = check.execute(
                f'SELECT event_type FROM "{schema}".events '
                "WHERE action_id=%s AND event_type IN ('action_completed', 'action_failed')",
                (action_id,),
            ).fetchall()
            assert [event["event_type"] for event in terminal_events] == ["action_completed"]
    finally:
        cleanup_url = os.environ.get("ACTIONQ_TEST_MIGRATION_URL", os.environ["ACTIONQ_TEST_URL"])
        with db.connect(cleanup_url) as cleanup:
            cleanup.execute(f'DROP SCHEMA "{schema}" CASCADE')
            cleanup.commit()
