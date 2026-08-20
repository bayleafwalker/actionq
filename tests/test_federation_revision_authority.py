from __future__ import annotations

import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from pathlib import Path

import psycopg
from psycopg import sql

from actionq import db, federation_schema, schema
from actionq.federation import FederationAuthority, FederationPrincipal


def _factory(url: str):
    return lambda: db.connect(url)


def _principal(name: str, *authorities: str) -> FederationPrincipal:
    return FederationPrincipal.authenticated(environment="test", principal_id=name, authorities=authorities)


def _new_schema(prefix: str = "fed") -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _migrate(url: str, selected: str) -> None:
    with db.connect(url) as conn:
        federation_schema.migrate(conn, selected)


def test_domain_assets_and_execution_migrations_are_independent(postgres_urls) -> None:
    selected = _new_schema()
    execution_root = Path(__file__).parents[1] / "actionq" / "migrations"
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in execution_root.glob("*.sql")}

    _migrate(postgres_urls["admin"], selected)

    after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in execution_root.glob("*.sql")}
    assert before == after
    assert sorted(before) == [f"{version:03d}_{name}" for version, name in [
        (1, "init.sql"), (2, "claim_receipts.sql"), (3, "dispatch_request_v2.sql"),
        (4, "cancellation_fencing.sql"), (5, "execution_groups.sql"),
        (6, "immutable_candidate_actions.sql"), (7, "dispatch_observation_watermarks.sql"),
        (8, "dispatch_v2_quarantine.sql"), (9, "action_resource_owner.sql"),
        (10, "claim_attempt_binding.sql"), (11, "session_completion_log.sql"),
        (12, "managed_dispatch_envelopes.sql"),
    ]]
    assert schema.MAX_SCHEMA_VERSION == 12
    assert [asset.version for asset in federation_schema.load_migrations()] == [1]
    with db.connect(postgres_urls["admin"]) as conn:
        assert federation_schema.require_compatible(conn, selected).compatibility_label == "federation-schema/v1"
        assert conn.execute("SELECT to_regclass(%s) AS relation", (db.qname(selected, "actions"),)).fetchone()["relation"] is None


def test_federation_compatibility_rejects_shape_loss(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f"DROP TABLE {db.qname(selected, 'federation_settlements')}")
        result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "shape-mismatch"
        assert "table-missing:federation_settlements" in result.detail


def test_create_is_action_independent_and_response_loss_replays_exact_bytes(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("creator", "federation.create")

    first = authority.create(principal=creator, idempotency_key="create-1", expected_revision=0)
    replay = authority.create(principal=creator, idempotency_key="create-1", expected_revision=0)
    conflict = authority.create(
        principal=creator, idempotency_key="create-1", expected_revision=0,
        resource_ref="aqf1_" + "A" * 43,
    )

    assert first.status == "accepted" and first.before_revision == 0 and first.after_revision == 1
    assert replay.replayed is True and replay.response_bytes == first.response_bytes
    assert replay.response_digest == first.response_digest
    assert conflict.status == "rejected" and conflict.code == "idempotency-key-conflict"
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_resources')}").fetchone()["n"] == 1
        assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_resource_changes')}").fetchone()["n"] == 1
        assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_command_decisions')}").fetchone()["n"] == 2


def test_acl_cas_state_machine_and_rejected_decisions_do_not_change_projection(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("owner", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    denied = authority.record_evidence(
        principal=_principal("intruder"), idempotency_key="denied", resource_ref=resource,
        evidence_ref="artifact:sha256:" + "0" * 64, evidence_bytes=b"evidence",
        assurance_type="verified", expected_revision=1,
    )
    stale = authority.record_execution_ref(
        principal=_principal("owner", "federation.relate"), idempotency_key="stale",
        resource_ref=resource, execution_ref="provider:1", assurance_type="observation",
        expected_revision=0,
    )
    mismatch = authority.record_evidence(
        principal=_principal("ingester", "federation.evidence.ingest"), idempotency_key="mismatch",
        resource_ref=resource, evidence_ref="artifact:sha256:" + "0" * 64,
        evidence_bytes=b"evidence", assurance_type="verified", expected_revision=1,
    )
    assert (denied.code, stale.code, mismatch.code) == ("authority-denied", "stale-revision", "evidence-digest-mismatch")
    assert authority.snapshot(resource_ref=resource)["revision"] == 1

    evidence_bytes = b"verified evidence"
    evidence_ref = "artifact:sha256:" + hashlib.sha256(evidence_bytes).hexdigest()
    recorded = authority.record_evidence(
        principal=_principal("ingester", "federation.evidence.ingest"), idempotency_key="evidence",
        resource_ref=resource, evidence_ref=evidence_ref, evidence_bytes=evidence_bytes,
        assurance_type="byte-verification", expected_revision=1,
    )
    accepted = authority.decide_acceptance(
        principal=_principal("reviewer", "federation.acceptance.decide"), idempotency_key="accept",
        resource_ref=resource, outcome="accepted", policy_ref="policy:v1",
        evidence_ref=evidence_ref, expected_revision=2,
    )
    settled = authority.record_settlement(
        principal=_principal("reconciler", "federation.settlement.record"), idempotency_key="settle",
        resource_ref=resource, fact_ref="fact:durable", expected_revision=3,
    )
    superseded = authority.supersede(
        principal=_principal("owner", "federation.supersede"), idempotency_key="supersede",
        resource_ref=resource, expected_revision=4,
    )
    immutable = authority.record_execution_ref(
        principal=_principal("owner", "federation.relate"), idempotency_key="after-supersede",
        resource_ref=resource, execution_ref="provider:late", assurance_type="observation",
        expected_revision=5,
    )
    assert [recorded.after_revision, accepted.after_revision, settled.after_revision, superseded.after_revision] == [2, 3, 4, 5]
    assert immutable.code == "resource-superseded"
    snapshot = authority.snapshot(resource_ref=resource)
    assert snapshot["state"] == "superseded" and snapshot["revision"] == 5
    assert snapshot["owner_principal_id"] == "owner" and snapshot["recovery_floor"] == 0
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_resource_changes')} WHERE resource_ref=%s", (resource,)).fetchone()["n"] == 5
        assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_command_decisions')} WHERE status='rejected'").fetchone()["n"] == 4


def test_relations_are_source_owned_directed_and_do_not_mutate_target(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    owner_a = _principal("a", "federation.create", "federation.relate")
    owner_b = _principal("b", "federation.create", "federation.relate")
    source = authority.create(principal=owner_a, idempotency_key="a", expected_revision=0).resource_ref
    target = authority.create(principal=owner_b, idempotency_key="b", expected_revision=0).resource_ref
    assert source and target

    relation = authority.add_relation(
        principal=owner_a, idempotency_key="edge", source_ref=source,
        relation_type="depends-on", target_ref=target, expected_revision=1,
    )
    owner_mismatch = authority.add_relation(
        principal=owner_b, idempotency_key="foreign-edge", source_ref=source,
        relation_type="derived-from", target_ref=target, expected_revision=2,
    )
    cycle = authority.add_relation(
        principal=owner_b, idempotency_key="cycle", source_ref=target,
        relation_type="depends-on", target_ref=source, expected_revision=1,
    )

    assert relation.after_revision == 2
    assert owner_mismatch.code == "owner-mismatch"
    assert cycle.code == "relation-cycle"
    assert authority.snapshot(resource_ref=source)["revision"] == 2
    assert authority.snapshot(resource_ref=target)["revision"] == 1


def test_opposite_relation_attempts_serialize_cycle_check(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    owner_a = _principal("a", "federation.create", "federation.relate")
    owner_b = _principal("b", "federation.create", "federation.relate")
    left = authority.create(principal=owner_a, idempotency_key="left", expected_revision=0).resource_ref
    right = authority.create(principal=owner_b, idempotency_key="right", expected_revision=0).resource_ref
    assert left and right
    barrier = Barrier(2)

    def relate(principal, key, source, target):
        barrier.wait()
        return authority.add_relation(
            principal=principal, idempotency_key=key, source_ref=source,
            relation_type="parent-of", target_ref=target, expected_revision=1,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            executor.submit(relate, owner_a, "left-right", left, right),
            executor.submit(relate, owner_b, "right-left", right, left),
        ]
        decisions = [future.result() for future in results]

    assert sorted(decision.status for decision in decisions) == ["accepted", "rejected"]
    assert {decision.code for decision in decisions} == {"accepted", "relation-cycle"}
    assert sorted((authority.snapshot(resource_ref=left)["revision"], authority.snapshot(resource_ref=right)["revision"])) == [1, 2]


def test_postgres_roles_separate_migration_command_and_end_actor_authority(postgres_urls) -> None:
    selected = _new_schema("fed_acl")
    suffix = uuid.uuid4().hex[:10]
    owner_role = f"fed_owner_{suffix}"
    migrator_role = f"fed_migrate_{suffix}"
    command_role = f"fed_command_{suffix}"
    actor_role = f"fed_actor_{suffix}"
    legacy_role = f"fed_legacy_{suffix}"
    admin_url = postgres_urls["admin"]
    with psycopg.connect(admin_url, autocommit=True) as conn:
        for role in (owner_role, migrator_role, command_role, actor_role, legacy_role):
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
    _migrate(admin_url, selected)
    with psycopg.connect(admin_url, autocommit=True) as conn:
        federation_schema.configure_role_boundaries(
            conn, selected, object_owner_role=owner_role, migration_role=migrator_role,
            command_role=command_role, denied_roles=(actor_role, legacy_role),
        )
        def privileges(role: str, table: str) -> tuple[bool, bool, bool]:
            row = conn.execute(
                "SELECT has_table_privilege(%s,%s,'INSERT'), has_table_privilege(%s,%s,'UPDATE'), has_table_privilege(%s,%s,'SELECT')",
                (role, f"{selected}.{table}", role, f"{selected}.{table}", role, f"{selected}.{table}"),
            ).fetchone()
            return tuple(bool(item) for item in row)
        assert privileges(command_role, "federation_resources") == (True, True, True)
        assert privileges(command_role, "federation_resource_changes") == (True, False, True)
        assert privileges(command_role, "federation_command_decisions") == (True, False, True)
        assert privileges(command_role, "schema_migrations") == (False, False, True)
        assert privileges(migrator_role, "schema_migrations") == (True, False, True)
        assert privileges(migrator_role, "federation_resources") == (False, False, False)
        for role in (actor_role, legacy_role):
            assert privileges(role, "federation_resources") == (False, False, False)
            assert privileges(role, "federation_command_decisions") == (False, False, False)
            assert privileges(role, "federation_idempotency_bindings") == (False, False, False)
