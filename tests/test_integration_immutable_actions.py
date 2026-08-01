"""Disposable-PostgreSQL histories for #2035 immutable compiler bindings."""
from __future__ import annotations

import uuid

import pytest

from actionq import db, schema
from actionq_contracts import (
    CANDIDATE_VERIFICATION_SPEC_V1,
    canonical_bytes,
    sha256_digest,
)


def _ref(letter: str) -> str:
    return "artifact:sha256:" + letter * 64


def _spec() -> dict:
    return {
        "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
        "request_ref": _ref("a"), "request_digest": "sha256:" + "a" * 64,
        "candidate_ref": _ref("b"), "candidate_digest": "sha256:" + "b" * 64,
        "profile_ref": _ref("c"), "profile_digest": "sha256:" + "c" * 64,
    }


def _request(spec: dict, *, subject: str = "candidate:one") -> tuple[dict, list[str]]:
    refs = [_ref("d"), _ref("e")]
    digest = sha256_digest(spec)
    return {
        "contract_id": "action-creation-request/v1",
        "plan_ref": _ref("f"), "topology": "independent", "role": "candidate-verification",
        "subject": subject, "spec_ref": "artifact:" + digest,
        "spec_digest": digest, "input_set_digest": db.immutable_input_set_digest(refs),
    }, refs


@pytest.fixture
def immutable_db(postgres_urls):
    name = "aqimmutable_" + uuid.uuid4().hex
    migrate = db.connect(postgres_urls["migration"])
    schema.migrate(migrate, name, runtime_role="actionq_runtime")
    migrate.commit()
    migrate.close()
    runtime = db.connect(postgres_urls["runtime"])
    try:
        yield runtime, name
    finally:
        runtime.close()


def _create(conn, name: str, *, spec: dict | None = None) -> dict:
    actual = spec or _spec()
    request, refs = _request(actual)
    return db.create_immutable_action(
        conn, name, request=request, spec=actual, input_refs=refs,
        action_type="candidate-verification", project="demo", priority=100,
        created_by="compiler:test",
    )


def test_exact_replay_returns_one_ordinary_action_and_changed_bytes_conflict(immutable_db):
    conn, name = immutable_db
    first = _create(conn, name)
    replay = _create(conn, name)
    assert replay == {**first, "replayed": True}
    assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(name, 'actions')}").fetchone()["n"] == 1

    changed = _spec() | {"candidate_ref": _ref("1"), "candidate_digest": "sha256:" + "1" * 64}
    with pytest.raises(db.ActionQError, match="logical identity conflicts"):
        _create(conn, name, spec=changed)
    conn.rollback()
    assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(name, 'actions')}").fetchone()["n"] == 1


def test_claim_revalidates_authoritative_bytes_and_emits_one_runtime_grant(immutable_db):
    conn, name = immutable_db
    created = _create(conn, name)
    claimed = db.claim(conn, name, worker="worker:one", timeout_minutes=30)
    conn.commit()
    assert claimed["id"] == created["action_id"]
    grant = claimed["immutable_runtime_grant"]
    assert grant["request_ref"] == created["request_ref"]
    assert grant["grant_digest"].startswith("sha256:")

    grants = conn.execute(
        f"SELECT request_digest, spec_digest, input_set_digest, grant_digest FROM {db.qname(name, 'immutable_action_runtime_grants')}"
    ).fetchall()
    assert len(grants) == 1


def test_claim_fails_closed_when_authoritative_request_bytes_are_corrupt(immutable_db, postgres_urls):
    conn, name = immutable_db
    created = _create(conn, name)
    with db.connect(postgres_urls["migration"]) as corruptor:
        corruptor.execute(
            f"UPDATE {db.qname(name, 'immutable_action_requests')} SET request_snapshot=%s WHERE action_id=%s",
            (canonical_bytes({"not": "a valid contract"}), created["action_id"]),
        )
        corruptor.commit()
    with pytest.raises(db.ActionQError, match="request bytes are invalid|claim-time integrity"):
        db.claim(conn, name, worker="worker:one", timeout_minutes=30)
    conn.rollback()
    assert db.get_action(conn, name, created["action_id"])["status"] == "pending"
