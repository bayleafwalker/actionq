"""Claim/lease authority-command tests for work item #1117.

Exercises actionq.db.renew and actionq.cli's `renew` command, plus a real
concurrent-claim scenario, against a disposable Postgres schema -- per
.agents/overlays/actionq.state-protocols.md's required scenarios for this
subject (two independent connections claiming concurrently; stale-worker
histories).
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import timedelta

import pytest
from click.testing import CliRunner

try:
    import psycopg  # noqa: F401
except ModuleNotFoundError:
    psycopg = None

from actionq import db
from actionq.cli import cli


pytestmark = pytest.mark.skipif(
    psycopg is None or not os.environ.get("ACTIONQ_TEST_URL"),
    reason="ACTIONQ_TEST_URL and psycopg are required for Postgres integration tests",
)


@pytest.fixture
def schema():
    return "aqclaim_" + uuid.uuid4().hex


def _connect_migrated(schema_name: str):
    conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    db.migrate(conn, schema_name)
    conn.commit()
    return conn


def _enqueue_and_claim(conn, schema_name: str, worker: str = "worker:one", timeout_minutes: int = 30):
    action = db.enqueue(
        conn, schema_name, action_type="scope-iterate", project="demo", target_ref="42",
        source_refs=[], priority=100, parent_id=None, created_by="human:test",
    )
    conn.commit()
    claimed = db.claim(conn, schema_name, worker=worker, timeout_minutes=timeout_minutes)
    conn.commit()
    return claimed


# -- db.renew: grant path -------------------------------------------------


def test_renew_extends_deadline_and_emits_claim_renewed(schema):
    conn = _connect_migrated(schema)
    claimed = _enqueue_and_claim(conn, schema, worker="worker:one")

    renewed = db.renew(conn, schema, action_id=claimed["id"], worker="worker:one", timeout_minutes=60)
    conn.commit()

    assert renewed["claim_deadline"] > claimed["claim_deadline"]
    events = db.action_events(conn, schema, claimed["id"])
    event_types = [e["event_type"].decode() if isinstance(e["event_type"], bytes) else e["event_type"] for e in events]
    assert event_types == ["action_enqueued", "action_claimed", "claim_renewed"]
    renewed_event = events[-1]
    payload = renewed_event["payload"] if isinstance(renewed_event["payload"], dict) else json.loads(renewed_event["payload"])
    assert payload["renewed_by"] == "worker:one"
    assert payload["requested_timeout_minutes"] == 60


def test_duplicate_renew_retry_by_same_worker_is_safe(schema):
    """A retried renewal (network blip, caller retries the same logical
    request) is not deduplicated by an idempotency key, but it is safe: it
    just extends the deadline again from `now()`, never corrupts state,
    and each attempt is granted and durably recorded."""
    conn = _connect_migrated(schema)
    claimed = _enqueue_and_claim(conn, schema, worker="worker:one")

    first = db.renew(conn, schema, action_id=claimed["id"], worker="worker:one", timeout_minutes=30)
    conn.commit()
    second = db.renew(conn, schema, action_id=claimed["id"], worker="worker:one", timeout_minutes=30)
    conn.commit()

    assert second["claim_deadline"] >= first["claim_deadline"]
    events = db.action_events(conn, schema, claimed["id"])
    renewed_events = [e for e in events if _event_type(e) == "claim_renewed"]
    assert len(renewed_events) == 2


# -- db.renew: rejection paths --------------------------------------------


def _event_type(event) -> str:
    value = event["event_type"]
    return value.decode() if isinstance(value, bytes) else value


def test_renew_rejects_wrong_worker_without_mutating_state(schema):
    conn = _connect_migrated(schema)
    claimed = _enqueue_and_claim(conn, schema, worker="worker:one")

    with pytest.raises(db.ClaimRejected) as excinfo:
        db.renew(conn, schema, action_id=claimed["id"], worker="worker:impostor", timeout_minutes=30)
    conn.commit()

    assert excinfo.value.reason == "claimed-by-different-worker"
    current = db.get_action(conn, schema, claimed["id"])
    assert current["claim_deadline"] == claimed["claim_deadline"]
    current_claimed_by = current["claimed_by"].decode() if isinstance(current["claimed_by"], bytes) else current["claimed_by"]
    assert current_claimed_by == "worker:one"
    events = db.action_events(conn, schema, claimed["id"])
    assert _event_type(events[-1]) == "claim_renewal_rejected"
    payload = events[-1]["payload"]
    payload = payload if isinstance(payload, dict) else json.loads(payload)
    assert payload["requested_by"] == "worker:impostor"
    assert payload["reason"] == "claimed-by-different-worker"
    assert payload["current_claimed_by"] == "worker:one"


def test_renew_rejects_expired_claim(schema):
    conn = _connect_migrated(schema)
    claimed = _enqueue_and_claim(conn, schema, worker="worker:one")
    # Force the deadline into the past directly (test setup only -- not a
    # production mutation path).
    conn.execute(
        f'UPDATE "{schema}"."actions" SET claim_deadline = now() - interval \'1 minute\' WHERE id = %s',
        (claimed["id"],),
    )
    conn.commit()

    with pytest.raises(db.ClaimRejected) as excinfo:
        db.renew(conn, schema, action_id=claimed["id"], worker="worker:one", timeout_minutes=30)
    conn.commit()

    assert excinfo.value.reason == "claim-already-expired"
    events = db.action_events(conn, schema, claimed["id"])
    assert _event_type(events[-1]) == "claim_renewal_rejected"


def test_renew_rejects_pending_unclaimed_action(schema):
    conn = _connect_migrated(schema)
    action = db.enqueue(
        conn, schema, action_type="scope-iterate", project="demo", target_ref="42",
        source_refs=[], priority=100, parent_id=None, created_by="human:test",
    )
    conn.commit()

    with pytest.raises(db.ClaimRejected) as excinfo:
        db.renew(conn, schema, action_id=action["id"], worker="worker:one", timeout_minutes=30)
    conn.commit()

    assert excinfo.value.reason == "not-claimed:pending"
    current = db.get_action(conn, schema, action["id"])
    assert (current["status"].decode() if isinstance(current["status"], bytes) else current["status"]) == "pending"


def test_renew_rejects_unknown_action_id(schema):
    conn = _connect_migrated(schema)
    db.migrate(conn, schema)

    with pytest.raises(db.ClaimRejected) as excinfo:
        db.renew(conn, schema, action_id=999999, worker="worker:one", timeout_minutes=30)
    conn.commit()

    assert excinfo.value.reason == "action-not-found"
    events = db.list_events(conn, schema, event_type="claim_renewal_rejected")
    assert events, "expected the rejection to be recorded even with action_id NULL"
    payload = events[-1]["payload"]
    payload = payload if isinstance(payload, dict) else json.loads(payload)
    assert payload["requested_action_id"] == 999999


def test_renew_after_reassignment_by_a_different_worker_is_rejected_for_the_stale_worker(schema):
    """Stale-worker history: worker:one's claim times out, actionctl sweep
    requeues it, worker:two claims it -- worker:one's later renewal attempt
    must be rejected, not silently accepted."""
    conn = _connect_migrated(schema)
    claimed = _enqueue_and_claim(conn, schema, worker="worker:one", timeout_minutes=1)
    conn.execute(
        f'UPDATE "{schema}"."actions" SET claim_deadline = now() - interval \'1 minute\' WHERE id = %s',
        (claimed["id"],),
    )
    conn.commit()
    swept = db.sweep(conn, schema)
    conn.commit()
    assert len(swept) == 1
    reclaimed = db.claim(conn, schema, worker="worker:two", timeout_minutes=30)
    conn.commit()
    assert reclaimed["id"] == claimed["id"]

    with pytest.raises(db.ClaimRejected) as excinfo:
        db.renew(conn, schema, action_id=claimed["id"], worker="worker:one", timeout_minutes=30)
    conn.commit()

    assert excinfo.value.reason == "claimed-by-different-worker"
    current = db.get_action(conn, schema, claimed["id"])
    claimed_by = current["claimed_by"].decode() if isinstance(current["claimed_by"], bytes) else current["claimed_by"]
    assert claimed_by == "worker:two"


# -- CLI surface -----------------------------------------------------------


def test_renew_cli_exits_nonzero_with_rejection_reason(schema, monkeypatch):
    monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_URL"])
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    runner = CliRunner()
    assert runner.invoke(cli, ["migrate"]).exit_code == 0

    action = json.loads(
        runner.invoke(
            cli, ["add", "--type", "scope-iterate", "--project", "demo", "--target", "42", "--created-by", "human:test"]
        ).output
    )
    claimed = json.loads(runner.invoke(cli, ["claim", "--worker", "worker:one"]).output)
    assert claimed["id"] == action["id"]

    ok = runner.invoke(cli, ["renew", str(action["id"]), "--worker", "worker:one", "--timeout", "45"])
    assert ok.exit_code == 0, ok.output
    assert json.loads(ok.output)["id"] == action["id"]

    rejected = runner.invoke(cli, ["renew", str(action["id"]), "--worker", "worker:impostor"])
    assert rejected.exit_code == 2
    assert "claimed-by-different-worker" in rejected.output

    history = json.loads(runner.invoke(cli, ["show", str(action["id"])]).output)
    assert [e["event_type"] for e in history["events"]] == [
        "action_enqueued", "action_claimed", "claim_renewed", "claim_renewal_rejected",
    ]


# -- concurrency: two independent connections claim concurrently ---------


def test_concurrent_claim_only_one_connection_wins(schema):
    setup_conn = _connect_migrated(schema)
    action = db.enqueue(
        setup_conn, schema, action_type="scope-iterate", project="demo", target_ref="42",
        source_refs=[], priority=100, parent_id=None, created_by="human:test",
    )
    setup_conn.commit()
    setup_conn.close()

    results: list[dict] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(2)

    def _claim_once(worker: str):
        conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
        try:
            barrier.wait(timeout=5)
            try:
                claimed = db.claim(conn, schema, worker=worker, timeout_minutes=30)
                conn.commit()
                results.append(claimed)
            except db.NoActionAvailable:
                conn.rollback()
        except Exception as exc:  # pragma: no cover - surfaced via errors list
            errors.append(exc)
        finally:
            conn.close()

    threads = [
        threading.Thread(target=_claim_once, args=(f"worker:{i}",)) for i in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors, errors
    assert len(results) == 1
    assert results[0]["id"] == action["id"]

    verify_conn = db.connect(os.environ["ACTIONQ_TEST_URL"])
    final = db.get_action(verify_conn, schema, action["id"])
    status = final["status"].decode() if isinstance(final["status"], bytes) else final["status"]
    assert status == "claimed"
    verify_conn.close()
