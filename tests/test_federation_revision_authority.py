from __future__ import annotations

import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from pathlib import Path

import psycopg
import pytest
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


class _EndpointLockGate:
    """Connection factory that deterministically synchronizes two concurrent
    add_relation() calls right after both of their endpoint advisory locks
    are taken -- the exact window a relation-type-scoped cycle lock has to
    close. Relying only on a barrier before add_relation() is called leaves
    several unsynchronized round trips (schema check, idempotency binding)
    before that window, so plain thread scheduling can accidentally
    serialize the two calls and let the race go unexercised. Gating is
    disabled (`armed=False`) during setup so seeding calls are not blocked.
    """

    def __init__(self, url: str):
        self._url = url
        self.armed = False
        self.barrier = Barrier(2)

    def __call__(self):
        conn = db.connect(self._url)
        real_execute = conn.execute
        state = {"n": 0}

        def gated_execute(query, params=None, **kwargs):
            result = real_execute(query, params, **kwargs)
            if (
                self.armed and isinstance(query, str) and "pg_advisory_xact_lock" in query
                and params and str(params[0]).startswith("federation-resource/v1:")
            ):
                state["n"] += 1
                if state["n"] == 2:
                    # A bounded timeout turns "the other side errored out
                    # before reaching its second endpoint lock" into a clear
                    # BrokenBarrierError instead of hanging the test (and the
                    # whole run) indefinitely.
                    self.barrier.wait(timeout=30)
            return result

        conn.execute = gated_execute
        return conn


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


def test_federation_compatibility_rejects_column_constraint_and_index_drift(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f"ALTER TABLE {db.qname(selected, 'federation_resources')} ALTER COLUMN recovery_floor DROP DEFAULT")
        result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "shape-mismatch"
        assert "column-default:federation_resources.recovery_floor" in result.detail

    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f"ALTER TABLE {db.qname(selected, 'federation_resources')} ALTER COLUMN owner_principal_id DROP NOT NULL")
        result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "shape-mismatch"
        assert "column-nullability:federation_resources.owner_principal_id" in result.detail

    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f"ALTER TABLE {db.qname(selected, 'federation_resources')} DROP CONSTRAINT federation_resources_revision_check")
        result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "shape-mismatch"
        assert "constraint-count:federation_resources.c:2!=3" in result.detail

    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f"DROP INDEX {db.qname(selected, 'federation_relations_target_idx')}")
        result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "shape-mismatch"
        assert "index-missing:federation_relations_target_idx" in result.detail

    # A same-count PRIMARY KEY swap: constraint-count alone would not catch
    # this (still exactly one 'p' constraint), the way it would not have
    # caught a same-count CHECK swap before that gap was closed.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f"ALTER TABLE {db.qname(selected, 'federation_idempotency_bindings')} DROP CONSTRAINT federation_idempotency_bindings_pkey")
        conn.execute(f"ALTER TABLE {db.qname(selected, 'federation_idempotency_bindings')} ADD PRIMARY KEY (environment, principal_id, operation, idempotency_key, request_digest)")
        result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "shape-mismatch"
        assert "primary-key:federation_idempotency_bindings" in result.detail

    # A same-count FOREIGN KEY swap to a vacuous self-reference.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f"ALTER TABLE {db.qname(selected, 'federation_settlements')} DROP CONSTRAINT federation_settlements_resource_ref_fkey")
        conn.execute(
            f"ALTER TABLE {db.qname(selected, 'federation_settlements')} ADD FOREIGN KEY (resource_ref, source_revision) "
            f"REFERENCES {db.qname(selected, 'federation_settlements')}(resource_ref, source_revision)"
        )
        result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "shape-mismatch"
        assert "foreign-key:federation_settlements" in result.detail


def test_federation_compatibility_too_new_and_checksum_mismatch_states(postgres_urls) -> None:
    # Ledger-verdict classification now delegates to
    # vuoro_schema_runtime.compatibility_report; pin the states it must
    # still distinguish for a domain with exactly one migration.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(
            f"INSERT INTO {db.qname(selected, federation_schema.MIGRATION_TABLE)} (domain, version, name, checksum) "
            "VALUES ('federation', 2, 'fake.sql', %s)",
            ("0" * 64,),
        )
        result = federation_schema.check_compatibility(conn, selected)
    assert result.state == "too-new"
    assert result.observed_schema_version == 2

    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(
            f"UPDATE {db.qname(selected, federation_schema.MIGRATION_TABLE)} SET checksum=%s WHERE domain='federation' AND version=1",
            ("1" * 64,),
        )
        result = federation_schema.check_compatibility(conn, selected)
    assert result.state == "checksum-mismatch"


def test_foreign_key_check_is_immune_to_schema_name_and_search_path_rendering(postgres_urls) -> None:
    # The FK check compares catalog OIDs and resolved column names, not
    # PostgreSQL's rendered pg_get_constraintdef text -- that text quotes
    # the schema qualifier only when the identifier needs it (mixed case, a
    # reserved word, db.SCHEMA_RE permits both), and omits the qualifier
    # entirely once the schema is visible via search_path (including via
    # the implicit "$user" schema, or the schema literally being "public").
    # A comparison built from any rendered string -- correctly quoted or
    # not -- is drift-sensitive to session state that has nothing to do
    # with the schema's actual shape.
    for selected in (_new_schema(prefix="FedMixedCase"), "select"):
        _migrate(postgres_urls["admin"], selected)
        with db.connect(postgres_urls["admin"]) as conn:
            result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "compatible", (selected, result.detail)

    search_path_schema = _new_schema()
    _migrate(postgres_urls["admin"], search_path_schema)
    with db.connect(postgres_urls["admin"]) as conn:
        conn.execute(f'SET search_path TO "{search_path_schema}", public')
        result = federation_schema.check_compatibility(conn, search_path_schema)
    assert result.state == "compatible", result.detail

    with db.connect(postgres_urls["admin"]) as conn:
        federation_schema.migrate(conn, "public")
    try:
        with db.connect(postgres_urls["admin"]) as conn:
            result = federation_schema.check_compatibility(conn, "public")
        assert result.state == "compatible", result.detail
    finally:
        # "public" is the hermetic cluster's shared default schema, reused
        # by the whole test session -- clean up only the tables this test
        # migrated into it, not the schema itself.
        with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
            for table in reversed(federation_schema.REQUIRED_TABLES):
                conn.execute(f'DROP TABLE IF EXISTS "public"."{table}" CASCADE')


def test_strip_pg_catalog_qualifier_normalizes_defaults_and_check_bodies() -> None:
    # A search_path that demotes pg_catalog behind a schema shadowing a
    # built-in function name (e.g. a same-named now()) makes PostgreSQL
    # qualify every reference to the real built-in to disambiguate it -- a
    # rendering difference with no bearing on the schema's actual shape.
    assert federation_schema._canonical_default("pg_catalog.now()") == "now()"
    assert federation_schema._canonical_default("'0'::pg_catalog.bigint") == "0"
    assert federation_schema._strip_pg_catalog_qualifier(
        "CHECK ((request_digest ~ '^sha256:[0-9a-f]{64}$'::pg_catalog.text))"
    ) == "CHECK ((request_digest ~ '^sha256:[0-9a-f]{64}$'::text))"


def test_column_default_check_tolerates_a_shadowed_now_function(postgres_urls) -> None:
    selected = _new_schema("fed_now_shadow")
    _migrate(postgres_urls["admin"], selected)
    shadow_schema = "fed_now_shadow_ns_" + uuid.uuid4().hex[:10]
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f'CREATE SCHEMA "{shadow_schema}"')
        conn.execute(f'CREATE FUNCTION "{shadow_schema}".now() RETURNS timestamptz AS \'SELECT pg_catalog.now()\' LANGUAGE sql')
    try:
        with db.connect(postgres_urls["admin"]) as conn:
            conn.execute(f'SET search_path TO "{shadow_schema}", pg_catalog, public')
            result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "compatible", result.detail
    finally:
        with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
            conn.execute(f'DROP SCHEMA "{shadow_schema}" CASCADE')


def test_column_type_check_tolerates_a_shadowed_text_domain(postgres_urls) -> None:
    # information_schema.columns.data_type is subject to the same
    # pg_catalog-demoted rendering as CHECK/default text: a domain literally
    # named "text" ahead of pg_catalog on search_path makes every plain
    # "text" column's data_type report as "pg_catalog.text" rather than
    # "text", with no bearing on the schema's actual shape.
    selected = _new_schema("fed_text_shadow")
    _migrate(postgres_urls["admin"], selected)
    shadow_schema = "fed_text_shadow_ns_" + uuid.uuid4().hex[:10]
    with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
        conn.execute(f'CREATE SCHEMA "{shadow_schema}"')
        conn.execute(f'CREATE DOMAIN "{shadow_schema}".text AS text')
    try:
        with db.connect(postgres_urls["admin"]) as conn:
            conn.execute(f'SET search_path TO "{shadow_schema}", pg_catalog, public')
            result = federation_schema.check_compatibility(conn, selected)
        assert result.state == "compatible", result.detail
    finally:
        with db.connect(postgres_urls["admin"]) as conn, conn.transaction():
            conn.execute(f'DROP SCHEMA "{shadow_schema}" CASCADE')


def test_snapshot_requires_authenticated_federation_read_authority(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("creator", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    with pytest.raises(db.ActionQError):
        authority.snapshot(principal=_principal("intruder"), resource_ref=resource)
    with pytest.raises(db.ActionQError):
        authority.snapshot(principal=_principal("intruder", "federation.create"), resource_ref=resource)
    # An invalid resource_ref must raise the same public db.ActionQError as
    # every other snapshot() failure, not the module-private _Rejected that
    # every command path routes through _execute() to translate.
    with pytest.raises(db.ActionQError):
        authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref="not-a-federation-ref")
    assert authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=resource)["resource_ref"] == resource


def test_unserializable_request_argument_raises_public_error_not_a_bare_typeerror(postgres_urls) -> None:
    # _execute() canonicalizes the whole request dict to derive request_digest
    # before any transaction opens, so a caller argument that json.dumps
    # cannot serialize (e.g. bytes where a resource_ref str is expected)
    # cannot become a durable "malformed-command" decision the way a bad
    # value *inside* apply() does -- there is no digest to key it with. It
    # must still surface as the module's public exception, not leak the
    # underlying TypeError the way every other command-argument failure in
    # this module does not.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    owner = _principal("owner", "federation.supersede")

    with pytest.raises(db.ActionQError):
        authority.supersede(principal=owner, idempotency_key="bad-arg", resource_ref=b"not-a-string", expected_revision=0)


def test_non_string_idempotency_key_raises_public_error_not_a_bare_typeerror(postgres_urls) -> None:
    # idempotency_key is never part of the canonicalized request -- it is
    # joined directly into an advisory-lock key string inside an already-open
    # transaction -- so it needs its own type check ahead of that join
    # instead of relying on the request-serialization guard.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    owner = _principal("owner", "federation.create")

    with pytest.raises(db.ActionQError):
        authority.create(principal=owner, idempotency_key=b"not-a-string", expected_revision=0)
    with pytest.raises(db.ActionQError):
        authority.create(principal=owner, idempotency_key="", expected_revision=0)


def test_federation_principal_rejects_non_string_environment_or_principal_id() -> None:
    with pytest.raises(ValueError):
        FederationPrincipal.authenticated(environment=1, principal_id="p", authorities=())
    with pytest.raises(ValueError):
        FederationPrincipal.authenticated(environment="env", principal_id=1, authorities=())


def test_record_evidence_tolerates_evidence_bytes_whose_repr_raises(postgres_urls) -> None:
    class _Unrepresentable:
        def __repr__(self):
            raise RuntimeError("boom")

    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("creator", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    decision = authority.record_evidence(
        principal=_principal("ingester", "federation.evidence.ingest"), idempotency_key="unrepresentable",
        resource_ref=resource, evidence_ref="artifact:sha256:" + "0" * 64,
        evidence_bytes=_Unrepresentable(), assurance_type="verified", expected_revision=1,
    )
    assert decision.status == "rejected" and decision.code == "invalid-evidence-bytes"
    replay = authority.record_evidence(
        principal=_principal("ingester", "federation.evidence.ingest"), idempotency_key="unrepresentable",
        resource_ref=resource, evidence_ref="artifact:sha256:" + "0" * 64,
        evidence_bytes=_Unrepresentable(), assurance_type="verified", expected_revision=1,
    )
    assert replay.replayed is True and replay.response_digest == decision.response_digest


def test_record_evidence_valid_and_malformed_bytes_cannot_collide_on_request_digest(postgres_urls) -> None:
    # actual_digest on the valid path must stay plain sha256(evidence_bytes)
    # -- honest callers independently compute that same hash to construct
    # evidence_ref, so it can't carry a domain-separating prefix -- which
    # means its value space, by construction, can coincide with some other
    # value's repr() descriptor on the malformed path. b"'abc'" (valid
    # bytes) and "abc" (a str -- malformed) hash identically once encoded:
    # sha256(b"'abc'") == sha256(repr("abc").encode()). Without a field
    # distinguishing which path produced evidence_digest, that coincidence
    # collides the two calls' canonical request bytes under the same
    # idempotency identity, and the second (malformed) call would silently
    # replay the first (valid) call's accepted decision instead of being
    # independently evaluated as invalid-evidence-bytes.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("owner", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource
    ingester = _principal("ingester", "federation.evidence.ingest")
    evidence_ref = "artifact:sha256:" + hashlib.sha256(b"'abc'").hexdigest()

    valid = authority.record_evidence(
        principal=ingester, idempotency_key="collision", resource_ref=resource,
        evidence_ref=evidence_ref, evidence_bytes=b"'abc'", assurance_type="verified", expected_revision=1,
    )
    malformed = authority.record_evidence(
        principal=ingester, idempotency_key="collision", resource_ref=resource,
        evidence_ref=evidence_ref, evidence_bytes="abc", assurance_type="verified", expected_revision=1,
    )
    assert valid.status == "accepted" and valid.replayed is False
    assert malformed.replayed is False
    assert malformed.code != "accepted"
    assert malformed.response_digest != valid.response_digest


def test_malformed_command_with_valid_idempotency_identity_is_durable_and_replayable(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("creator", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource
    ingester = _principal("ingester", "federation.evidence.ingest")

    first = authority.record_evidence(
        principal=ingester, idempotency_key="malformed-evidence", resource_ref=resource,
        evidence_ref="artifact:sha256:" + "0" * 64, evidence_bytes="not-bytes",
        assurance_type="verified", expected_revision=1,
    )
    replay = authority.record_evidence(
        principal=ingester, idempotency_key="malformed-evidence", resource_ref=resource,
        evidence_ref="artifact:sha256:" + "0" * 64, evidence_bytes="not-bytes",
        assurance_type="verified", expected_revision=1,
    )

    assert first.status == "rejected" and first.code == "invalid-evidence-bytes" and first.replayed is False
    assert replay.replayed is True and replay.response_bytes == first.response_bytes
    assert replay.response_digest == first.response_digest
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_command_decisions')} WHERE idempotency_key=%s",
            ("malformed-evidence",),
        ).fetchone()["n"] == 1
        assert conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_idempotency_bindings')} WHERE idempotency_key=%s",
            ("malformed-evidence",),
        ).fetchone()["n"] == 1
    # A resource_ref of the wrong type is malformed input entirely outside
    # the specific evidence-bytes guard; it must still be recorded durably.
    # (It is JSON-serializable so the failure surfaces inside apply(), where
    # the idempotency identity is already bound -- not before, in the request
    # canonicalization that happens ahead of the transaction.)
    type_malformed = authority.record_execution_ref(
        principal=_principal("owner", "federation.relate"), idempotency_key="bad-ref-type",
        resource_ref=12345, execution_ref="provider:1", assurance_type="observation",
        expected_revision=0,
    )
    type_malformed_replay = authority.record_execution_ref(
        principal=_principal("owner", "federation.relate"), idempotency_key="bad-ref-type",
        resource_ref=12345, execution_ref="provider:1", assurance_type="observation",
        expected_revision=0,
    )
    assert type_malformed.status == "rejected" and type_malformed.code == "malformed-command"
    assert type_malformed_replay.replayed is True
    assert type_malformed_replay.response_digest == type_malformed.response_digest


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


def test_non_string_free_form_fields_are_rejected_not_silently_stored_or_crashed(postgres_urls) -> None:
    # execution_ref, assurance_type, policy_ref, evidence_ref, and fact_ref
    # were only truthiness-checked, not type-checked. A non-str value that is
    # still JSON-serializable (so canonical_bytes doesn't catch it) escaped
    # to a SQL comparison or assignment: WHERE execution_ref=%s with an int
    # raised a bare psycopg.errors.UndefinedFunction that the module's
    # (TypeError, ValueError, AttributeError) durability handler misses
    # entirely, while assurance_type/policy_ref/fact_ref would get silently
    # assignment-cast and durably stored as the wrong type.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("owner", "federation.create", "federation.relate")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    bad_execution_ref = authority.record_execution_ref(
        principal=creator, idempotency_key="bad-execution-ref", resource_ref=resource,
        execution_ref=12345, assurance_type="observation", expected_revision=1,
    )
    bad_assurance_type = authority.record_execution_ref(
        principal=creator, idempotency_key="bad-assurance-type", resource_ref=resource,
        execution_ref="provider:1", assurance_type=7, expected_revision=1,
    )
    assert bad_execution_ref.code == "invalid-execution-reference"
    assert bad_assurance_type.code == "invalid-assurance-type"
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_execution_refs')}"
        ).fetchone()["n"] == 0


def test_decide_acceptance_type_checks_evidence_ref_on_the_rejected_path_too(postgres_urls) -> None:
    # evidence_ref was only isinstance-checked inside the outcome=="accepted"
    # branch; on the rejected branch a non-str/non-None value went straight
    # to the INSERT with no guard, either getting silently assignment-cast
    # and durably stored under a command decision that reports the command
    # itself succeeded, or (for a type PostgreSQL can't cast at all) leaking
    # a bare psycopg error past _execute's durability handler.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("owner", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    decision = authority.decide_acceptance(
        principal=_principal("reviewer", "federation.acceptance.decide"), idempotency_key="bad-evidence-ref",
        resource_ref=resource, outcome="rejected", policy_ref="policy:v1",
        evidence_ref=12345, expected_revision=1,
    )
    assert decision.status == "rejected" and decision.code == "invalid-evidence-reference"
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_acceptance_decisions')}"
        ).fetchone()["n"] == 0
    # None must remain legal on the rejected path -- the column is nullable
    # and rejection commonly carries no evidence at all.
    none_evidence = authority.decide_acceptance(
        principal=_principal("reviewer", "federation.acceptance.decide"), idempotency_key="no-evidence",
        resource_ref=resource, outcome="rejected", policy_ref="policy:v1",
        evidence_ref=None, expected_revision=1,
    )
    assert none_evidence.status == "accepted" and none_evidence.after_revision == 2


def test_decide_acceptance_rejects_an_unverified_evidence_ref_on_the_rejected_path_too(postgres_urls) -> None:
    # The accepted branch verifies a cited evidence_ref actually exists in
    # federation_evidence; the rejected branch skipped that check entirely,
    # letting a rejection durably cite evidence that was never submitted.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("owner", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    fabricated = authority.decide_acceptance(
        principal=_principal("reviewer", "federation.acceptance.decide"), idempotency_key="fabricated-evidence",
        resource_ref=resource, outcome="rejected", policy_ref="policy:v1",
        evidence_ref="artifact:sha256:" + "0" * 64, expected_revision=1,
    )
    assert fabricated.status == "rejected" and fabricated.code == "evidence-not-found"
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_acceptance_decisions')}"
        ).fetchone()["n"] == 0


def test_missing_assurance_type_is_distinguished_from_evidence_or_reference_mismatch(postgres_urls) -> None:
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("owner", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    evidence_bytes = b"verified evidence"
    evidence_ref = "artifact:sha256:" + hashlib.sha256(evidence_bytes).hexdigest()
    missing_assurance = authority.record_evidence(
        principal=_principal("ingester", "federation.evidence.ingest"), idempotency_key="missing-assurance",
        resource_ref=resource, evidence_ref=evidence_ref, evidence_bytes=evidence_bytes,
        assurance_type="", expected_revision=1,
    )
    true_mismatch = authority.record_evidence(
        principal=_principal("ingester", "federation.evidence.ingest"), idempotency_key="true-mismatch",
        resource_ref=resource, evidence_ref="artifact:sha256:" + "0" * 64,
        evidence_bytes=evidence_bytes, assurance_type="verified", expected_revision=1,
    )
    missing_execution_ref = authority.record_execution_ref(
        principal=_principal("owner", "federation.relate"), idempotency_key="missing-execution-ref",
        resource_ref=resource, execution_ref="", assurance_type="observation", expected_revision=1,
    )
    missing_execution_assurance = authority.record_execution_ref(
        principal=_principal("owner", "federation.relate"), idempotency_key="missing-execution-assurance",
        resource_ref=resource, execution_ref="provider:1", assurance_type="", expected_revision=1,
    )
    assert missing_assurance.code == "invalid-assurance-type"
    assert true_mismatch.code == "evidence-digest-mismatch"
    assert missing_execution_ref.code == "invalid-execution-reference"
    assert missing_execution_assurance.code == "invalid-assurance-type"


def test_record_settlement_rejects_a_second_settlement_of_the_same_resource(postgres_urls) -> None:
    # Every other side-effect-recording command (add_relation,
    # record_execution_ref, record_evidence) rejects a repeat with a
    # dedicated '*-exists' code; record_settlement had no such guard, and
    # its PK (resource_ref, source_revision) does not block a second insert
    # since source_revision increments on every call.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    creator = _principal("owner", "federation.create")
    resource = authority.create(principal=creator, idempotency_key="create", expected_revision=0).resource_ref
    assert resource
    evidence_bytes = b"verified evidence"
    evidence_ref = "artifact:sha256:" + hashlib.sha256(evidence_bytes).hexdigest()
    authority.record_evidence(
        principal=_principal("ingester", "federation.evidence.ingest"), idempotency_key="evidence",
        resource_ref=resource, evidence_ref=evidence_ref, evidence_bytes=evidence_bytes,
        assurance_type="byte-verification", expected_revision=1,
    )
    authority.decide_acceptance(
        principal=_principal("reviewer", "federation.acceptance.decide"), idempotency_key="accept",
        resource_ref=resource, outcome="accepted", policy_ref="policy:v1",
        evidence_ref=evidence_ref, expected_revision=2,
    )
    reconciler = _principal("reconciler", "federation.settlement.record")
    first = authority.record_settlement(
        principal=reconciler, idempotency_key="settle-1", resource_ref=resource, fact_ref="fact:durable", expected_revision=3,
    )
    second = authority.record_settlement(
        principal=reconciler, idempotency_key="settle-2", resource_ref=resource, fact_ref="fact:durable-again", expected_revision=4,
    )
    assert first.status == "accepted"
    assert second.status == "rejected" and second.code == "settlement-exists"
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_settlements')}"
        ).fetchone()["n"] == 1


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
    assert authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=resource)["revision"] == 1

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
    snapshot = authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=resource)
    assert snapshot["state"] == "superseded" and snapshot["revision"] == 5
    assert snapshot["owner_principal_id"] == "owner" and snapshot["recovery_floor"] == 0
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_resource_changes')} WHERE resource_ref=%s", (resource,)).fetchone()["n"] == 5
        assert conn.execute(f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_command_decisions')} WHERE status='rejected'").fetchone()["n"] == 4


def test_invalid_relation_type_is_rejected_before_any_resource_lookup(postgres_urls) -> None:
    # relation_type must be validated before the endpoint locks (its value
    # picks the cycle-lock key), so this decision durably records with no
    # resource_ref/before_revision, and it is checked ahead of stale-revision,
    # owner-mismatch, and resource-not-found -- unlike every other rejection
    # in this module, which carries the source resource's identity.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    owner = _principal("owner", "federation.create", "federation.relate")
    source = authority.create(principal=owner, idempotency_key="create", expected_revision=0).resource_ref
    assert source

    decision = authority.add_relation(
        principal=owner, idempotency_key="bad-type", source_ref=source,
        relation_type="not-a-real-type", target_ref="aqf1_" + "Z" * 43, expected_revision=99,
    )
    assert decision.code == "invalid-relation-type"
    assert decision.resource_ref is None
    assert decision.before_revision is None
    assert decision.after_revision is None
    replay = authority.add_relation(
        principal=owner, idempotency_key="bad-type", source_ref=source,
        relation_type="not-a-real-type", target_ref="aqf1_" + "Z" * 43, expected_revision=99,
    )
    assert replay.replayed is True and replay.response_digest == decision.response_digest


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
    assert authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=source)["revision"] == 2
    assert authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=target)["revision"] == 1


def test_add_relation_rejects_a_superseded_target(postgres_urls) -> None:
    # The source endpoint is guarded against superseded state via
    # _locked_existing; the target endpoint was only checked for existence,
    # letting a live resource gain a persisted edge into one that is
    # supposed to be frozen.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)
    owner_a = _principal("a", "federation.create", "federation.relate", "federation.supersede")
    owner_b = _principal("b", "federation.create", "federation.relate", "federation.supersede")
    source = authority.create(principal=owner_a, idempotency_key="a", expected_revision=0).resource_ref
    target = authority.create(principal=owner_b, idempotency_key="b", expected_revision=0).resource_ref
    assert source and target
    superseded = authority.supersede(principal=owner_b, idempotency_key="supersede-target", resource_ref=target, expected_revision=1)
    assert superseded.status == "accepted"

    decision = authority.add_relation(
        principal=owner_a, idempotency_key="edge-to-superseded", source_ref=source,
        relation_type="depends-on", target_ref=target, expected_revision=1,
    )
    assert decision.code == "target-superseded"
    with db.connect(postgres_urls["admin"]) as conn:
        assert conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(selected, 'federation_relations')}"
        ).fetchone()["n"] == 0


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
        barrier.wait(timeout=30)
        return authority.add_relation(
            principal=principal, idempotency_key=key, source_ref=source,
            relation_type="parent-of", target_ref=target, expected_revision=1,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            executor.submit(relate, owner_a, "left-right", left, right),
            executor.submit(relate, owner_b, "right-left", right, left),
        ]
        decisions = [future.result(timeout=30) for future in results]

    assert sorted(decision.status for decision in decisions) == ["accepted", "rejected"]
    assert {decision.code for decision in decisions} == {"accepted", "relation-cycle"}
    assert sorted((authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=left)["revision"], authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=right)["revision"])) == [1, 2]


def test_disjoint_endpoint_relation_attempts_cannot_jointly_create_a_cycle(postgres_urls) -> None:
    # Endpoint locks alone only serialize writes sharing an endpoint. A -> B
    # and C -> D exist already; concurrent B -> C and D -> A share no
    # endpoint, so each could pass a cycle probe against committed-only
    # state and jointly close a cycle A -> B -> C -> D -> A unless writes of
    # a cyclic relation type are also serialized against each other.
    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    gate = _EndpointLockGate(postgres_urls["admin"])
    authority = FederationAuthority(connection=gate, schema=selected)
    owners = {name: _principal(name, "federation.create", "federation.relate") for name in "abcd"}
    nodes = {
        name: authority.create(principal=owners[name], idempotency_key=name, expected_revision=0).resource_ref
        for name in "abcd"
    }
    assert all(nodes.values())
    seed_ab = authority.add_relation(
        principal=owners["a"], idempotency_key="seed-ab", source_ref=nodes["a"],
        relation_type="parent-of", target_ref=nodes["b"], expected_revision=1,
    )
    seed_cd = authority.add_relation(
        principal=owners["c"], idempotency_key="seed-cd", source_ref=nodes["c"],
        relation_type="parent-of", target_ref=nodes["d"], expected_revision=1,
    )
    assert seed_ab.status == "accepted" and seed_cd.status == "accepted"
    gate.armed = True

    def relate(principal, key, source, target, expected_revision):
        return authority.add_relation(
            principal=principal, idempotency_key=key, source_ref=source,
            relation_type="parent-of", target_ref=target, expected_revision=expected_revision,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            executor.submit(relate, owners["b"], "b-to-c", nodes["b"], nodes["c"], 1),
            executor.submit(relate, owners["d"], "d-to-a", nodes["d"], nodes["a"], 1),
        ]
        decisions = [future.result(timeout=30) for future in results]
    gate.armed = False

    assert sorted(decision.status for decision in decisions) == ["accepted", "rejected"]
    assert {decision.code for decision in decisions} == {"accepted", "relation-cycle"}
    reader = _principal("reader", "federation.read")
    # a and c are each at revision 2 from seeding (they were each a source
    # once); exactly one of b or d advances from 1 to 2 for the accepted
    # edge, the other stays at 1 because its attempt was rejected.
    total_revision = sum(authority.snapshot(principal=reader, resource_ref=nodes[name])["revision"] for name in "abcd")
    assert total_revision == 7


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


def test_configure_role_boundaries_rejects_a_denied_role_that_inherits_command_role(postgres_urls) -> None:
    # REVOKE ALL is issued directly against each denied role; a denied role
    # that also inherits command_role's grants via role membership would
    # keep those privileges regardless, making the direct REVOKE cosmetic.
    selected = _new_schema("fed_acl_inherit")
    suffix = uuid.uuid4().hex[:10]
    owner_role = f"fed_owner_{suffix}"
    migrator_role = f"fed_migrate_{suffix}"
    command_role = f"fed_command_{suffix}"
    inheriting_denied_role = f"fed_shadow_{suffix}"
    admin_url = postgres_urls["admin"]
    with psycopg.connect(admin_url, autocommit=True) as conn:
        for role in (owner_role, migrator_role, command_role):
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN IN ROLE {}").format(sql.Identifier(inheriting_denied_role), sql.Identifier(command_role)))
    _migrate(admin_url, selected)
    with psycopg.connect(admin_url, autocommit=True) as conn:
        with pytest.raises(federation_schema.FederationSchemaError):
            federation_schema.configure_role_boundaries(
                conn, selected, object_owner_role=owner_role, migration_role=migrator_role,
                command_role=command_role, denied_roles=(inheriting_denied_role,),
            )


def test_configure_role_boundaries_rejects_an_indirect_role_membership_chain(postgres_urls) -> None:
    # pg_has_role's MEMBER option checks the whole role graph, not just
    # direct membership -- a denied role two hops from command_role via an
    # intermediate role must be caught too.
    selected = _new_schema("fed_acl_chain")
    suffix = uuid.uuid4().hex[:10]
    owner_role = f"fed_owner_{suffix}"
    migrator_role = f"fed_migrate_{suffix}"
    command_role = f"fed_command_{suffix}"
    middle_role = f"fed_middle_{suffix}"
    denied_role = f"fed_shadow_{suffix}"
    admin_url = postgres_urls["admin"]
    with psycopg.connect(admin_url, autocommit=True) as conn:
        for role in (owner_role, migrator_role, command_role, middle_role):
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        conn.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(command_role), sql.Identifier(middle_role)))
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN IN ROLE {}").format(sql.Identifier(denied_role), sql.Identifier(middle_role)))
    _migrate(admin_url, selected)
    with psycopg.connect(admin_url, autocommit=True) as conn:
        with pytest.raises(federation_schema.FederationSchemaError):
            federation_schema.configure_role_boundaries(
                conn, selected, object_owner_role=owner_role, migration_role=migrator_role,
                command_role=command_role, denied_roles=(denied_role,),
            )


def test_configure_role_boundaries_rejects_a_denied_role_that_inherits_the_object_owner(postgres_urls) -> None:
    # Owner-role membership is strictly worse than service-role membership:
    # it confers ownership itself, not just a set of grants, and was not
    # checked at all -- only migration_role/command_role membership was.
    selected = _new_schema("fed_acl_owner_inherit")
    suffix = uuid.uuid4().hex[:10]
    owner_role = f"fed_owner_{suffix}"
    migrator_role = f"fed_migrate_{suffix}"
    command_role = f"fed_command_{suffix}"
    inheriting_denied_role = f"fed_shadow_{suffix}"
    admin_url = postgres_urls["admin"]
    with psycopg.connect(admin_url, autocommit=True) as conn:
        for role in (owner_role, migrator_role, command_role):
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN IN ROLE {}").format(sql.Identifier(inheriting_denied_role), sql.Identifier(owner_role)))
    _migrate(admin_url, selected)
    with psycopg.connect(admin_url, autocommit=True) as conn:
        with pytest.raises(federation_schema.FederationSchemaError):
            federation_schema.configure_role_boundaries(
                conn, selected, object_owner_role=owner_role, migration_role=migrator_role,
                command_role=command_role, denied_roles=(inheriting_denied_role,),
            )
