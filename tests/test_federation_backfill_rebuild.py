"""W3 -- deterministic backfill, rebuild, export and restore for federation v1.

Covers every scenario the tranche-4 freeze names for W3 (every legacy lifecycle
state, reclaims, renewals, stale receipts, cancellations, candidate
publications, action-resource cursor pruning, completion recovery floors) and
one explicit test per named falsifier:

* rerun changes identity/digest
* revision gaps or duplicates pass
* any v1 change was pruned
* rebuilt projection differs from live projection
* legacy claimed/completed implies acceptance/settlement
* export/restore changes canonical projection
* a configured retention objective cannot be restored

Run:
    uv run --extra dev pytest tests/test_federation_backfill_rebuild.py \
      tests/test_claim_authority.py tests/test_completion_log_integration.py -q
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import psycopg.sql
import pytest
from actionq_contracts import CANDIDATE_VERIFICATION_SPEC_V1, canonical_bytes, sha256_digest

from actionq import db, federation_schema
from actionq.action_resource import ActionResourceOwner
from actionq.completion_log import CompletionLog
from actionq.federation import FederationAuthority, FederationPrincipal
from actionq.federation_backfill import (
    _EXPORT_TABLES,
    BACKFILL_AUTHORITIES,
    BACKFILL_MAPPING_VERSION,
    BACKFILL_PRINCIPAL_ID,
    FORBIDDEN_BACKFILL_AUTHORITIES,
    LEGACY_ASSURANCE_PREFIX,
    BackfillCommand,
    BackfillDrift,
    BackfillRejected,
    ExportError,
    FederationBackfill,
    LegacySource,
    RebuildError,
    backfill_principal,
    deterministic_resource_ref,
    export_federation,
    is_backfilled_change,
    legacy_assurance_type,
    legacy_completion_ref,
    legacy_execution_ref,
    live_projection,
    rebuild_from_changes,
    require_rebuild_matches,
    restore_federation,
    verify_rebuild,
)


ROOT = Path(__file__).parents[1]
ENVIRONMENT = "test"
SOURCE_ID = "primary"
CONTRACT_DOC = ROOT / "docs/plans/2026-08-21-w3-retention-export-restore.md"


def _factory(url: str):
    return lambda: db.connect(url)


def _principal(name: str, *authorities: str) -> FederationPrincipal:
    return FederationPrincipal.authenticated(environment=ENVIRONMENT, principal_id=name, authorities=authorities)


def _new_schema(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _ref(letter: str) -> str:
    return "artifact:sha256:" + letter * 64


def _enqueue(conn, schema: str, target: str, *, parent_id: int | None = None) -> dict:
    return db.enqueue(
        conn, schema, action_type="scope-iterate", project="demo", target_ref=target,
        source_refs=[], priority=100, parent_id=parent_id, created_by="human:test",
    )


def _expire_claim(conn, schema: str, action_id: int) -> None:
    conn.execute(
        f"UPDATE {db.qname(schema, 'actions')} SET claim_deadline = now() - interval '1 hour' WHERE id=%s",
        (action_id,),
    )


def _legacy_history(url: str, schema: str) -> dict:
    """Every legacy lifecycle state the freeze doc names, in one schema.

    Ordering matters: db.claim takes the oldest pending action, so each state is
    driven to completion before the next pending action exists.
    """
    history: dict[str, object] = {}
    with db.connect(url) as conn:
        completed = _enqueue(conn, schema, "completed")
        claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        db.complete(conn, schema, completed["id"], _ref("a"), worker="worker:one",
                    claim_receipt=claimed["claim_receipt"])
        history["completed"] = completed["id"]
        history["completed_receipt"] = claimed["claim_receipt"]

        failed = _enqueue(conn, schema, "failed")
        claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        db.fail(conn, schema, failed["id"], "boom", worker="worker:one",
                claim_receipt=claimed["claim_receipt"])
        history["failed"] = failed["id"]

        rejected = _enqueue(conn, schema, "rejected")
        claimed = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        db.reject(conn, schema, rejected["id"], reason="invalid", validator="validator:test",
                  worker="worker:one", claim_receipt=claimed["claim_receipt"])
        history["rejected"] = rejected["id"]

        cancelled = _enqueue(conn, schema, "cancelled-from-pending")
        db.cancel(conn, schema, cancelled["id"], "operator stop")
        history["cancelled"] = cancelled["id"]

        cancelling = _enqueue(conn, schema, "cancelling")
        db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        db.cancel(conn, schema, cancelling["id"], "operator stop")
        history["cancelling"] = cancelling["id"]

        reaped = _enqueue(conn, schema, "cancel-reaped")
        db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        db.cancel(conn, schema, reaped["id"], "operator stop")
        conn.execute(
            f"UPDATE {db.qname(schema, 'actions')} SET cancel_stop_deadline = now() - interval '1 minute' WHERE id=%s",
            (reaped["id"],),
        )
        assert [row["id"] for row in db.reap_cancellations(conn, schema)] == [reaped["id"]]
        history["reaped"] = reaped["id"]

        # Renewal, stale receipt, reclaim: renew the lease, expire it, sweep it
        # back to pending, then claim it again under a second receipt.
        reclaimed = _enqueue(conn, schema, "reclaimed")
        first = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        db.renew(conn, schema, action_id=reclaimed["id"], worker="worker:one", timeout_minutes=30,
                 claim_receipt=first["claim_receipt"])
        _expire_claim(conn, schema, reclaimed["id"])
        assert [row["id"] for row in db.sweep(conn, schema)] == [reclaimed["id"]]
        second = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        assert second["id"] == reclaimed["id"]
        history["reclaimed"] = reclaimed["id"]
        history["stale_receipt"] = first["claim_receipt"]
        history["live_receipt"] = second["claim_receipt"]

        # Candidate publication.
        spec = {
            "contract_id": CANDIDATE_VERIFICATION_SPEC_V1,
            "candidate_ref": _ref("b"), "candidate_digest": "sha256:" + "b" * 64,
            "profile_ref": _ref("c"), "profile_digest": "sha256:" + "c" * 64,
            "publication_ref": _ref("d"), "publication_digest": "sha256:" + "d" * 64,
        }
        refs = [spec["candidate_ref"], spec["profile_ref"], spec["publication_ref"]]
        spec_digest = sha256_digest(spec)
        request = {
            "contract_id": "action-creation-request/v1", "plan_ref": _ref("f"),
            "topology": "independent", "role": "candidate-verification", "subject": "candidate:one",
            "spec_ref": "artifact:" + spec_digest, "spec_digest": spec_digest,
            "input_set_digest": db.immutable_input_set_digest(refs),
        }
        candidate = db.create_immutable_action(
            conn, schema, request=request, spec=spec, input_refs=refs,
            action_type="candidate-verification", project="demo", priority=100,
            created_by="compiler:test",
        )
        candidate_claim = db.claim(conn, schema, worker="worker:one", timeout_minutes=30)
        assert candidate_claim["id"] == candidate["action_id"]
        db.register_publication(
            conn, schema, action_id=candidate["action_id"], runner_id="worker:one",
            runner_request_id=str(uuid.uuid4()), claim_receipt=candidate_claim["claim_receipt"],
            attempt_id="attempt-1", journal_ref=_ref("e"), source_commit="b" * 40,
            candidate_commit="c" * 40,
        )
        history["candidate"] = candidate["action_id"]
        history["candidate_request_ref"] = candidate["request_ref"]

        parent = _enqueue(conn, schema, "parent")
        child = _enqueue(conn, schema, "child", parent_id=parent["id"])
        history["parent"] = parent["id"]
        history["child"] = child["id"]
        conn.commit()

    # Action-resource root with a pruned cursor, so a non-zero legacy recovery
    # floor exists to import.
    owner = ActionResourceOwner(schema=schema, connection=_factory(url), cursor_secret=b"\x01" * 32)
    enqueued = owner.enqueue(
        principal_scope="tenant:test", operation="execution.action.enqueue", idempotency_key="idem-1",
        request={"action_type": "scope-iterate"},
        create_action=lambda conn: _enqueue(conn, schema, "action-resource"),
    )
    with db.connect(url) as conn:
        action_id = conn.execute(
            f"SELECT action_id FROM {db.qname(schema, 'action_resources')} WHERE resource_ref=%s",
            (enqueued.resource_ref,),
        ).fetchone()["action_id"]
        with conn.transaction():
            owner.project_state(conn, action_id=int(action_id))
        conn.commit()
    pruned = owner.prune(resource_ref=enqueued.resource_ref, through_revision=1)
    assert pruned == 1
    history["action_resource_action"] = int(action_id)
    history["action_resource_ref"] = enqueued.resource_ref

    # Completion recovery floor: ingest, age the row past retention, compact.
    log = CompletionLog(schema=schema, connection_factory=_factory(url))
    event = {
        "schema_version": "session.completion-observed/v1",
        "event_id": "4d5e6f70-8192-4a3b-8c0d-3e4f50617283",
        "origin_stream_id": "1a2b3c4d-5e6f-4788-9a0b-1c2d3e4f5061",
        "origin_sequence": 1, "runtime_session_id": "session-1", "attempt_id": None,
        "action_id": None, "repo": {"project": "actionq"}, "harness": "manual", "model": None,
        "terminal": {"kind": "succeeded", "exit_code": 0, "reason_code": "completed", "retryable": False},
        "started_at": "2026-08-11T10:00:00Z", "completed_at": "2026-08-11T10:00:01Z",
        "observed_at": "2026-08-11T10:00:01Z", "duration_ms": 1000, "refs": [],
        "evidence": {"dirty": False, "commit_count": 0, "verification": {"pass": 0, "fail": 0, "error": 0}},
        "privacy": {
            "prompt_absent": True, "transcript_absent": True, "raw_output_absent": True,
            "environment_absent": True, "credentials_absent": True,
            "absolute_paths_absent": True, "claim_proofs_absent": True,
        },
    }
    log.ingest(event)
    with db.connect(url) as conn:
        conn.execute(
            f"UPDATE {db.qname(schema, 'session_completion_events')} SET received_at = now() - interval '90 days'"
        )
        conn.commit()
    assert log.compact() == 1

    # The one action left pending, created last so nothing above claims it.
    with db.connect(url) as conn:
        pending = _enqueue(conn, schema, "pending")
        conn.commit()
    history["pending"] = pending["id"]
    return history


@pytest.fixture
def planes(postgres_urls):
    url = postgres_urls["admin"]
    execution = _new_schema("aqbf_exec")
    federation = _new_schema("aqbf_fed")
    with db.connect(url) as conn:
        db.migrate(conn, execution)
        federation_schema.migrate(conn, federation)
        conn.commit()
    history = _legacy_history(url, execution)
    yield {"url": url, "execution": execution, "federation": federation, "history": history}
    with db.connect(url) as conn:
        conn.execute(f'DROP SCHEMA "{execution}" CASCADE')
        conn.execute(f'DROP SCHEMA "{federation}" CASCADE')
        conn.commit()


def _authority(planes: dict) -> FederationAuthority:
    return FederationAuthority(connection=_factory(planes["url"]), schema=planes["federation"])


def _backfill(planes: dict) -> FederationBackfill:
    return FederationBackfill(
        authority=_authority(planes),
        source=LegacySource(connection=_factory(planes["url"]), schema=planes["execution"]),
        environment=ENVIRONMENT, source_id=SOURCE_ID,
    )


def _resource_ref(action_id: int) -> str:
    return deterministic_resource_ref(legacy_execution_ref(environment=ENVIRONMENT, source_id=SOURCE_ID, action_id=action_id))


def _execution_refs(planes: dict, resource_ref: str) -> dict[str, str]:
    with db.connect(planes["url"]) as conn:
        rows = conn.execute(
            f"SELECT execution_ref, assurance_type FROM "
            f"{db.qname(planes['federation'], 'federation_execution_refs')} WHERE resource_ref=%s",
            (resource_ref,),
        ).fetchall()
    return {str(row["execution_ref"]): str(row["assurance_type"]) for row in rows}


def _projection(planes: dict, resource_ref: str) -> dict:
    with db.connect(planes["url"]) as conn:
        return live_projection(conn, planes["federation"], resource_ref)


def _count(planes: dict, table: str) -> int:
    with db.connect(planes["url"]) as conn:
        return int(conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(planes['federation'], table)}"
        ).fetchone()["n"])


# --- identity, principal and contract ---------------------------------------


def test_retention_export_restore_contract_is_checked_in() -> None:
    """The freeze doc makes this contract a prerequisite to W3, not an output."""
    assert CONTRACT_DOC.is_file()
    text = CONTRACT_DOC.read_text(encoding="utf-8")
    # Collapsed so an assertion is about the commitment, not where it wrapped.
    flowed = " ".join(text.split())
    assert "indefinitely" in text
    assert "/mnt/truenas/storage_layer/" in text
    assert "Best effort" in text
    assert "Manual only" in text
    # The existing whole-cluster policy is named as untouched, not reinterpreted.
    assert "30d" in text and "unchanged by W3" in text
    # Scope boundary, the W7 dependency it creates, and the ordering constraint
    # deterministic references impose are all recorded, not left implicit.
    assert "identity and shape" in flowed
    assert "prerequisite of any W7 destructive-retirement plan" in flowed
    assert "before any native principal is granted `federation.create`" in flowed
    assert "Known gap, owned by W5" in flowed
    assert "must not be scheduled to run periodically" in flowed
    assert "required, explicit operator statement" in flowed
    assert "has never been applied to any database" in flowed


def test_deterministic_identity_is_stable_shaped_and_mapping_scoped() -> None:
    legacy = legacy_execution_ref(environment=ENVIRONMENT, source_id=SOURCE_ID, action_id=7)
    assert legacy == "actionq-execution/v1:test:primary:7"
    first = deterministic_resource_ref(legacy)
    assert first == deterministic_resource_ref(legacy)
    assert first.startswith("aqf1_") and len(first) == len("aqf1_") + 43
    assert first != deterministic_resource_ref(legacy, mapping_version="federation-backfill/v2")
    assert first != deterministic_resource_ref(legacy_execution_ref(environment=ENVIRONMENT, source_id=SOURCE_ID, action_id=8))
    assert deterministic_resource_ref(legacy_completion_ref(environment=ENVIRONMENT, source_id=SOURCE_ID)) != first
    with pytest.raises(db.ActionQError):
        legacy_execution_ref(environment="bad:environment", source_id=SOURCE_ID, action_id=1)
    with pytest.raises(db.ActionQError):
        legacy_execution_ref(environment=ENVIRONMENT, source_id=SOURCE_ID, action_id=0)
    with pytest.raises(db.ActionQError):
        legacy_execution_ref(environment=ENVIRONMENT, source_id="bad:source", action_id=1)
    # Two legacy databases under one environment name must not collide.
    assert first != deterministic_resource_ref(
        legacy_execution_ref(environment=ENVIRONMENT, source_id="secondary", action_id=7)
    )


def test_backfill_principal_cannot_decide_accept_settle_or_supersede(planes) -> None:
    principal = backfill_principal(environment=ENVIRONMENT)
    assert principal.principal_id == BACKFILL_PRINCIPAL_ID
    assert not principal.authorities & FORBIDDEN_BACKFILL_AUTHORITIES
    assert BACKFILL_AUTHORITIES >= {"federation.backfill", "federation.create", "federation.relate"}

    authority = _authority(planes)
    resource_ref = _resource_ref(int(planes["history"]["completed"]))
    created = authority.create(principal=principal, idempotency_key="probe", expected_revision=0,
                               resource_ref=resource_ref)
    assert created.status == "accepted"

    denied = [
        authority.decide_acceptance(principal=principal, idempotency_key="probe-accept",
                                    resource_ref=resource_ref, outcome="accepted",
                                    policy_ref="policy:test", evidence_ref=None, expected_revision=1),
        authority.record_settlement(principal=principal, idempotency_key="probe-settle",
                                    resource_ref=resource_ref, fact_ref="fact:test", expected_revision=1),
        authority.supersede(principal=principal, idempotency_key="probe-supersede",
                            resource_ref=resource_ref, expected_revision=1),
        authority.record_evidence(principal=principal, idempotency_key="probe-evidence",
                                  resource_ref=resource_ref, evidence_ref="artifact:" + db.raw_digest(b"x"),
                                  evidence_bytes=b"x", assurance_type="test", expected_revision=1),
    ]
    assert [decision.status for decision in denied] == ["rejected"] * 4
    assert {decision.code for decision in denied} == {"authority-denied"}
    # A denied decision is durable but advances nothing.
    assert authority.snapshot(principal=_principal("reader", "federation.read"),
                              resource_ref=resource_ref)["revision"] == 1
    assert _count(planes, "federation_acceptance_decisions") == 0
    assert _count(planes, "federation_settlements") == 0


# --- the import itself -------------------------------------------------------


def test_backfill_imports_every_legacy_lifecycle_state_as_provenance(planes) -> None:
    """Scope: identity and shape only; descriptive and temporal legacy content
    is deliberately not imported. This asserts that each named legacy fact has a
    corresponding federation reference, never that the fact's content survives.
    """
    report = _backfill(planes).run()
    history = planes["history"]
    assert report.mapping_version == BACKFILL_MAPPING_VERSION
    assert report.replayed == 0 and report.applied == report.planned

    expected_states = {
        "completed": "completed", "failed": "failed", "rejected": "rejected",
        "cancelled": "cancelled", "cancelling": "cancelling", "reaped": "cancelled",
        "reclaimed": "claimed", "pending": "pending",
    }
    for name, status in expected_states.items():
        resource_ref = _resource_ref(int(history[name]))
        legacy = legacy_execution_ref(environment=ENVIRONMENT, source_id=SOURCE_ID, action_id=int(history[name]))
        refs = _execution_refs(planes, resource_ref)
        assert refs[legacy] == legacy_assurance_type("action")
        assert refs[f"{legacy}#status:{status}"] == legacy_assurance_type("status")
        # Legacy terminal state never becomes a federation decision.
        assert _projection(planes, resource_ref)["state"] == "registered"

    # Reclaims, renewals, stale receipts and cancellations arrive as event facts.
    reclaimed = _execution_refs(planes, _resource_ref(int(history["reclaimed"])))
    kinds = {value.rsplit(":", 1)[-1] for value in reclaimed.values()}
    assert {"claim_renewed", "claim_timed_out"} <= kinds
    assert "action_cancelling" in {
        value.rsplit(":", 1)[-1] for value in _execution_refs(planes, _resource_ref(int(history["cancelling"]))).values()
    }
    assert "action_cancel_reaped" in {
        value.rsplit(":", 1)[-1] for value in _execution_refs(planes, _resource_ref(int(history["reaped"]))).values()
    }

    # Candidate publication.
    candidate = _execution_refs(planes, _resource_ref(int(history["candidate"])))
    assert "publication.registered" in {value.rsplit(":", 1)[-1] for value in candidate.values()}
    assert legacy_assurance_type("candidate-request") in candidate.values()
    assert any(str(history["candidate_request_ref"]) in ref for ref in candidate)

    # Action-resource root and its pruned legacy cursor floor.
    resource_root = _execution_refs(planes, _resource_ref(int(history["action_resource_action"])))
    assert legacy_assurance_type("action-resource") in resource_root.values()
    floor_refs = [ref for ref, kind in resource_root.items()
                  if kind == legacy_assurance_type("action-resource-recovery-floor")]
    assert len(floor_refs) == 1 and floor_refs[0].endswith("#action-resource-recovery-floor:1")

    # Completion recovery floor.
    completion = _execution_refs(planes, deterministic_resource_ref(legacy_completion_ref(environment=ENVIRONMENT, source_id=SOURCE_ID)))
    assert legacy_assurance_type("completion-recovery-floor") in completion.values()
    assert any(ref.endswith("#recovery-floor:1") for ref in completion)

    # Parent/child chains become directed federation relations.
    with db.connect(planes["url"]) as conn:
        relations = conn.execute(
            f"SELECT source_ref, relation_type, target_ref FROM "
            f"{db.qname(planes['federation'], 'federation_relations')}"
        ).fetchall()
    assert [(str(row["source_ref"]), str(row["relation_type"]), str(row["target_ref"])) for row in relations] == [
        (_resource_ref(int(history["parent"])), "parent-of", _resource_ref(int(history["child"])))
    ]


def test_legacy_claim_receipts_and_event_payloads_never_enter_federation(planes) -> None:
    _backfill(planes).run()
    history = planes["history"]
    secrets = {str(history["completed_receipt"]), str(history["stale_receipt"]), str(history["live_receipt"])}
    with db.connect(planes["url"]) as conn:
        blob = b"".join(
            bytes(row["payload_bytes"]) for row in conn.execute(
                f"SELECT payload_bytes FROM {db.qname(planes['federation'], 'federation_resource_changes')}"
            ).fetchall()
        ).decode("utf-8")
        blob += "".join(
            str(row["execution_ref"]) + str(row["assurance_type"]) for row in conn.execute(
                f"SELECT execution_ref, assurance_type FROM "
                f"{db.qname(planes['federation'], 'federation_execution_refs')}"
            ).fetchall()
        )
    for secret in secrets:
        assert secret and secret not in blob


def test_rerun_changes_no_identity_digest_or_row_count(planes) -> None:
    """Falsifier: rerun changes identity/digest."""
    backfill = _backfill(planes)
    first = backfill.run()
    before_export = _export(planes)
    before_counts = {table: _count(planes, table) for table in
                     ("federation_resources", "federation_resource_changes", "federation_execution_refs",
                      "federation_relations", "federation_command_decisions")}

    second = backfill.run()
    assert second.planned == first.planned
    assert second.replayed == second.planned and second.applied == 0
    assert second.resource_refs == first.resource_refs
    assert {table: _count(planes, table) for table in before_counts} == before_counts
    assert _export(planes) == before_export

    # A fresh instance re-derives the same plan from the same source rows.
    assert _backfill(planes).plan() == backfill.plan()


def test_backfilled_changes_are_distinguishable_from_native_changes(planes) -> None:
    """Scope: by reserved actor principal id and legacy-provenance assurance
    prefix. The frozen change ledger has no provenance column, so this is a
    convention over reserved values, not a schema-enforced property."""
    _backfill(planes).run()
    native = _principal("native:one", "federation.create", "federation.relate")
    native_ref = _authority(planes).create(principal=native, idempotency_key="native-1",
                                           expected_revision=0).resource_ref
    with db.connect(planes["url"]) as conn:
        rows = conn.execute(
            f"SELECT resource_ref, actor_principal_id FROM "
            f"{db.qname(planes['federation'], 'federation_resource_changes')}"
        ).fetchall()
        assurances = {str(row["assurance_type"]) for row in conn.execute(
            f"SELECT assurance_type FROM {db.qname(planes['federation'], 'federation_execution_refs')}"
        ).fetchall()}
    backfilled = [row for row in rows if is_backfilled_change(row)]
    assert backfilled and all(str(row["actor_principal_id"]) == BACKFILL_PRINCIPAL_ID for row in backfilled)
    assert [row for row in rows if not is_backfilled_change(row)] == [
        row for row in rows if str(row["resource_ref"]) == native_ref
    ]
    assert assurances and all(item.startswith(LEGACY_ASSURANCE_PREFIX) for item in assurances)


def test_backfill_principal_id_is_pinned_not_caller_supplied(planes) -> None:
    """The reserved id is half of invariant 5; a caller must not be able to
    write backfilled changes that report as native."""
    import inspect
    from actionq import federation_backfill

    assert "principal_id" not in inspect.signature(backfill_principal).parameters
    assert "principal_id" not in inspect.signature(FederationBackfill.__init__).parameters
    assert "source_id" in inspect.signature(FederationBackfill.__init__).parameters
    assert backfill_principal(environment=ENVIRONMENT).principal_id == BACKFILL_PRINCIPAL_ID
    assert federation_backfill.BACKFILL_PRINCIPAL_ID == BACKFILL_PRINCIPAL_ID


def test_a_truncated_legacy_read_is_refused_not_silently_imported(planes) -> None:
    """A short read would produce a quietly incomplete import that verify()
    still passes, because verification never sees the legacy side."""
    narrow = FederationBackfill(
        authority=_authority(planes),
        source=LegacySource(connection=_factory(planes["url"]), schema=planes["execution"], limit=2),
        environment=ENVIRONMENT, source_id=SOURCE_ID,
    )
    with pytest.raises(db.ActionQError, match="exceeded the 2-row limit"):
        narrow.plan()
    assert _count(planes, "federation_resources") == 0

    # A read that returns exactly the bound was not truncated and must import.
    with db.connect(planes["url"]) as conn:
        total = int(conn.execute(
            f"SELECT count(*) AS n FROM {db.qname(planes['execution'], 'actions')}"
        ).fetchone()["n"])
    exact = FederationBackfill(
        authority=_authority(planes),
        source=LegacySource(connection=_factory(planes["url"]), schema=planes["execution"], limit=total),
        environment=ENVIRONMENT, source_id=SOURCE_ID,
    )
    assert exact.plan()


def _add_event(planes: dict, action_id: int) -> None:
    with db.connect(planes["url"]) as conn:
        db.insert_event(conn, planes["execution"], action_id=action_id,
                        event_type="dispatch.observed", actor="test", payload={})
        conn.commit()


def test_a_new_legacy_event_is_appended_not_refused(planes) -> None:
    """Variable-cardinality facts are emitted last in each chain, so a new event
    never needs a revision an already-recorded fact was written at."""
    backfill = _backfill(planes)
    backfill.run()
    before = _count(planes, "federation_resource_changes")

    action_id = int(planes["history"]["action_resource_action"])
    _add_event(planes, action_id)
    report = backfill.run()

    assert report.applied == 1
    assert _count(planes, "federation_resource_changes") == before + 1
    assert all(result["matches"] for result in backfill.verify(report.resource_refs))


def test_a_legacy_status_transition_is_appended_keeping_the_earlier_status(planes) -> None:
    """The mutation the reviewer called the most common one: an action moves on
    after import. The earlier status stays recorded -- "at import time this was
    pending" is still true -- and the new one appends."""
    backfill = _backfill(planes)
    backfill.run()
    with db.connect(planes["url"]) as conn:
        claimed = db.claim(conn, planes["execution"], worker="worker:one", timeout_minutes=30)
        conn.commit()
    pending_id = int(claimed["id"])
    resource_ref = _resource_ref(pending_id)
    legacy = legacy_execution_ref(environment=ENVIRONMENT, source_id=SOURCE_ID, action_id=pending_id)
    assert f"{legacy}#status:pending" in _execution_refs(planes, resource_ref)

    report = backfill.run()
    refs = _execution_refs(planes, resource_ref)
    assert f"{legacy}#status:pending" in refs
    assert f"{legacy}#status:claimed" in refs
    assert all(result["matches"] for result in backfill.verify(report.resource_refs))
    # Still provenance-only: a legacy claim is not a federation decision.
    assert _projection(planes, resource_ref)["state"] == "registered"


def test_a_legacy_fact_that_disappeared_is_replayed_not_refused(planes) -> None:
    """Provenance is cumulative. A legacy row the source no longer offers does
    not invalidate the fact already recorded about it, and db.prune_dispatch_
    observation_events deletes events as a matter of routine."""
    backfill = _backfill(planes)
    first = backfill.run()
    before = _count(planes, "federation_resource_changes")

    action_id = int(planes["history"]["completed"])
    with db.connect(planes["url"]) as conn:
        events = db.qname(planes["execution"], "events")
        conn.execute(
            f"DELETE FROM {events} WHERE id = (SELECT max(id) FROM {events} WHERE action_id=%s)",
            (action_id,),
        )
        conn.commit()

    second = backfill.run()
    assert second.applied == 0
    assert second.planned < first.planned
    assert _count(planes, "federation_resource_changes") == before


def test_a_self_parenting_legacy_action_is_refused_rather_than_planned(planes) -> None:
    """federation v1 forbids self-relations, so planning that edge would emit a
    command that can only ever be rejected -- and a durable rejection against an
    unmoving revision replays for every later run, stalling the import."""
    with db.connect(planes["url"]) as conn:
        action_id = int(planes["history"]["pending"])
        conn.execute(
            f"UPDATE {db.qname(planes['execution'], 'actions')} SET parent_id=%s WHERE id=%s",
            (action_id, action_id),
        )
        conn.commit()
    with pytest.raises(BackfillDrift, match="its own parent"):
        _backfill(planes).run()
    assert _count(planes, "federation_resources") == 0


def test_a_second_legacy_database_under_one_environment_cannot_merge(planes) -> None:
    """Deterministic refs are a function of the source too, so two unrelated
    legacy databases sharing an environment name cannot silently merge their
    provenance into one resource.

    Scope: deterministic references are a function of the source, so the merge
    is impossible rather than detected -- there is no runtime check to bypass.
    """
    other = _new_schema("aqbf_other")
    with db.connect(planes["url"]) as conn:
        db.migrate(conn, other)
        conn.commit()
    with db.connect(planes["url"]) as conn:
        _enqueue(conn, other, "other-db-action")
        conn.commit()

    _backfill(planes).run()
    second = FederationBackfill(
        authority=_authority(planes),
        source=LegacySource(connection=_factory(planes["url"]), schema=other),
        environment=ENVIRONMENT, source_id="secondary",
    )
    report = second.run()
    try:
        assert set(report.resource_refs).isdisjoint(_backfill(planes).run().resource_refs)
        for resource_ref in report.resource_refs:
            refs = _execution_refs(planes, resource_ref)
            assert all(":secondary" in ref for ref in refs)
    finally:
        with db.connect(planes["url"]) as conn:
            conn.execute(f'DROP SCHEMA "{other}" CASCADE')
            conn.commit()


def test_a_moved_assurance_type_is_refused_not_permanently_rejected(planes) -> None:
    """assurance_type is in the request digest but not in the fact key, so a
    legacy event_type that moved under an already-recorded reference would
    re-issue a bound key under a different digest and be rejected forever."""
    backfill = _backfill(planes)
    backfill.run()
    action_id = int(planes["history"]["completed"])
    with db.connect(planes["url"]) as conn:
        events = db.qname(planes["execution"], "events")
        conn.execute(
            f"UPDATE {events} SET event_type='mutated' WHERE id = "
            f"(SELECT max(id) FROM {events} WHERE action_id=%s)",
            (action_id,),
        )
        conn.commit()
    with pytest.raises(BackfillDrift) as drift:
        backfill.run()
    assert drift.value.resource_ref == _resource_ref(action_id)
    assert "assurance" in str(drift.value)


def test_a_rejected_command_can_still_be_imported_by_a_later_run(planes) -> None:
    """A rejection is durably bound to the digest carrying its expected
    revision. If the key omitted that revision, the fact would be unimportable
    by every future run."""
    backfill = _backfill(planes)
    backfill.run()
    action_id = int(planes["history"]["completed"])
    resource_ref = _resource_ref(action_id)
    _add_event(planes, action_id)

    appended = [c for c in backfill.plan() if c.resource_ref == resource_ref][-1]
    # Burn the appended command's expected revision with an unrelated write, so
    # dispatching it now is durably rejected as stale.
    _authority(planes).record_execution_ref(
        principal=backfill.principal, idempotency_key="burn", resource_ref=resource_ref,
        execution_ref="burn:1", assurance_type=legacy_assurance_type("burn"),
        expected_revision=appended.expected_revision,
    )
    assert backfill._dispatch(appended).code == "stale-revision"

    report = backfill.run()
    assert report.source_id == SOURCE_ID
    assert report.applied >= 1
    refs = _execution_refs(planes, resource_ref)
    assert appended.arguments["execution_ref"] in refs
    assert all(result["matches"] for result in backfill.verify(report.resource_refs))


def test_a_native_write_to_a_backfilled_resource_does_not_stop_the_import(planes) -> None:
    """federation.py lets an evidence ingester write to a resource it does not
    own, so a native reviewer touching an imported provenance resource is
    legitimate. Aborting the whole import on it would strand every remaining
    legacy row with no way to clear the condition."""
    backfill = _backfill(planes)
    report = backfill.run()
    resource_ref = report.resource_refs[0]

    evidence = b"native evidence on a provenance resource"
    accepted = _authority(planes).record_evidence(
        principal=_principal("native:reviewer", "federation.evidence.ingest"), idempotency_key="ingest",
        resource_ref=resource_ref, evidence_ref="artifact:" + db.raw_digest(evidence),
        evidence_bytes=evidence, assurance_type="native",
        expected_revision=_projection(planes, resource_ref)["revision"],
    )
    assert accepted.status == "accepted"

    _add_event(planes, int(planes["history"]["completed"]))
    resumed = backfill.run()
    assert resumed.applied >= 1
    # The native change stays distinguishable from the backfilled ones.
    with db.connect(planes["url"]) as conn:
        rows = conn.execute(
            f"SELECT actor_principal_id FROM "
            f"{db.qname(planes['federation'], 'federation_resource_changes')} WHERE resource_ref=%s",
            (resource_ref,),
        ).fetchall()
    assert any(not is_backfilled_change(row) for row in rows)
    assert any(is_backfilled_change(row) for row in rows)


def test_is_backfilled_change_raises_on_a_row_shape_it_cannot_read(planes) -> None:
    """The contract doc names this the single reader for the distinction, so a
    confident wrong answer is worse than an error."""
    assert is_backfilled_change({"actor_principal_id": BACKFILL_PRINCIPAL_ID}) is True
    assert is_backfilled_change({"actor_principal_id": "native:one"}) is False
    with pytest.raises((TypeError, KeyError, IndexError)):
        is_backfilled_change(("aqf1_x", 1, "create", "registered", BACKFILL_PRINCIPAL_ID))


def test_the_mapping_version_scopes_identity_keys_and_assurance_together(planes) -> None:
    """A v2 mapping that still labelled its refs v1 would be a silently
    mislabelled import, not an error: assurance_type is part of the digest."""
    second = FederationBackfill(
        authority=_authority(planes),
        source=LegacySource(connection=_factory(planes["url"]), schema=planes["execution"]),
        environment=ENVIRONMENT, source_id=SOURCE_ID, mapping_version="federation-backfill/v2",
    )
    _backfill(planes).run()
    report = second.run()

    assert set(report.resource_refs).isdisjoint(_backfill(planes).run().resource_refs)
    assurances = {
        kind for resource_ref in report.resource_refs
        for kind in _execution_refs(planes, resource_ref).values()
    }
    assert assurances and all(item.startswith("legacy-provenance/federation-backfill/v2:") for item in assurances)


def test_export_column_shape_matches_the_frozen_schema(planes) -> None:
    """A migration that adds a column must fail here, not silently drop it from
    the artifact the retention contract calls the indefinite record."""
    exported = {table: [name for name, _ in columns] for table, columns, _ in _EXPORT_TABLES}
    declared = {
        table: sorted(columns)
        for table, columns in federation_schema._COLUMN_SHAPE.items()
        if table != federation_schema.MIGRATION_TABLE
    }
    assert set(exported) == set(declared)
    for table, names in exported.items():
        assert sorted(names) == declared[table], table


def test_backfill_refuses_to_issue_a_non_provenance_command(planes) -> None:
    backfill = _backfill(planes)
    command = backfill.plan()[0]
    widened = BackfillCommand("decide-acceptance", command.idempotency_key, command.resource_ref,
                            command.expected_revision, {})
    with pytest.raises(db.ActionQError, match="provenance commands"):
        backfill._dispatch(widened)


def test_squatting_a_deterministic_reference_is_reported_as_identity_takeover(planes) -> None:
    """Deterministic refs are precomputable, so any principal holding
    federation.create can occupy one before the import runs. Ownership is
    immutable in v1, so unlike a foreign change this can never clear -- backfill
    must name it rather than report a confusing create rejection."""
    backfill = _backfill(planes)
    first = backfill.plan()[0]
    _authority(planes).create(principal=_principal("other", "federation.create"),
                              idempotency_key="squat", expected_revision=0,
                              resource_ref=first.resource_ref)
    with pytest.raises(BackfillDrift) as raised:
        backfill.run()
    assert raised.value.resource_ref == first.resource_ref


def test_a_command_rejected_by_the_authority_aborts_the_import(planes) -> None:
    """run()'s own guard: a command the authority rejects must abort the import
    rather than be counted as progress. Reached by stubbing the dispatcher,
    because run() replans immediately before dispatching and the real race
    window between the two is not deterministically reproducible."""
    class _Rejecting(FederationBackfill):
        def _dispatch(self, command):
            decision = super()._dispatch(command)
            if command.operation != "record-execution-ref":
                return decision
            return type(decision)(
                status="rejected", code="stale-revision", message="expected_revision does not match",
                response_bytes=b"{}", response_digest="sha256:" + "0" * 64,
                resource_ref=command.resource_ref, before_revision=None, after_revision=None,
            )

    backfill = _Rejecting(
        authority=_authority(planes),
        source=LegacySource(connection=_factory(planes["url"]), schema=planes["execution"]),
        environment=ENVIRONMENT, source_id=SOURCE_ID,
    )
    with pytest.raises(BackfillRejected) as raised:
        backfill.run()
    assert raised.value.code == "stale-revision"
    assert raised.value.operation == "record-execution-ref"


# --- rebuild -----------------------------------------------------------------


def test_rebuild_equals_live_projection_for_every_backfilled_resource(planes) -> None:
    """Falsifier: rebuilt projection differs from live projection."""
    report = _backfill(planes).run()
    results = _backfill(planes).verify()
    assert len(results) == len(report.resource_refs)
    assert all(result["matches"] for result in results)
    assert all(result["rebuilt_digest"] == result["live_digest"] for result in results)
    # The rebuild is a real replay: it reproduces relations and execution refs,
    # not just the resource row.
    populated = [result for result in results if result["rebuilt"]["execution_refs"]]
    assert len(populated) == len(results)
    with db.connect(planes["url"]) as conn:
        for resource_ref in report.resource_refs:
            assert require_rebuild_matches(conn, planes["federation"], resource_ref)["matches"]


def test_rebuild_agrees_with_the_authority_snapshot(planes) -> None:
    report = _backfill(planes).run()
    reader = _principal("reader", "federation.read")
    authority = _authority(planes)
    with db.connect(planes["url"]) as conn:
        for resource_ref in report.resource_refs:
            snapshot = authority.snapshot(principal=reader, resource_ref=resource_ref)
            rebuilt = rebuild_from_changes(conn, planes["federation"], resource_ref)
            assert rebuilt["revision"] == snapshot["revision"]
            assert rebuilt["state"] == snapshot["state"]
            assert rebuilt["owner_principal_id"] == snapshot["owner_principal_id"]
            assert rebuilt["recovery_floor"] == snapshot["recovery_floor"]


def test_rebuild_detects_gaps_duplicates_and_digest_conflicts(planes) -> None:
    """Falsifier: revision gaps or duplicates pass."""
    report = _backfill(planes).run()
    resource_ref = report.resource_refs[0]
    with db.connect(planes["url"]) as conn:
        rows = conn.execute(
            f"SELECT revision, operation, state, actor_principal_id, payload_bytes, payload_digest FROM "
            f"{db.qname(planes['federation'], 'federation_resource_changes')} WHERE resource_ref=%s ORDER BY revision",
            (resource_ref,),
        ).fetchall()
        assert len(rows) >= 3

        with pytest.raises(RebuildError) as gap:
            rebuild_from_changes(conn, planes["federation"], resource_ref,
                                 changes=[rows[0], *rows[2:]])
        assert gap.value.code == "revision-gap"

        with pytest.raises(RebuildError) as duplicate:
            rebuild_from_changes(conn, planes["federation"], resource_ref,
                                 changes=[rows[0], rows[1], rows[1], *rows[2:]])
        assert duplicate.value.code == "duplicate-revision"

        with pytest.raises(RebuildError) as headless:
            rebuild_from_changes(conn, planes["federation"], resource_ref, changes=rows[1:])
        assert headless.value.code == "revision-gap"

        # A stored digest that no longer matches its bytes is a rebuild failure,
        # not a silently accepted projection.
        conn.execute(
            f"UPDATE {db.qname(planes['federation'], 'federation_resource_changes')} "
            "SET payload_digest=%s WHERE resource_ref=%s AND revision=2",
            ("sha256:" + "0" * 64, resource_ref),
        )
        with pytest.raises(RebuildError) as digest:
            rebuild_from_changes(conn, planes["federation"], resource_ref)
        assert digest.value.code == "payload-digest-mismatch"
        conn.rollback()

    with db.connect(planes["url"]) as conn:
        with pytest.raises(RebuildError) as missing:
            rebuild_from_changes(conn, planes["federation"],
                                 deterministic_resource_ref("actionq-execution/v1:test:999999"))
        assert missing.value.code == "resource-not-found"


def test_rebuild_detects_a_projection_that_drifted_from_its_changes(planes) -> None:
    report = _backfill(planes).run()
    resource_ref = report.resource_refs[0]
    with db.connect(planes["url"]) as conn:
        conn.execute(
            f"UPDATE {db.qname(planes['federation'], 'federation_resources')} "
            "SET revision = revision + 1 WHERE resource_ref=%s",
            (resource_ref,),
        )
        result = verify_rebuild(conn, planes["federation"], resource_ref)
        assert result["matches"] is False
        with pytest.raises(RebuildError) as drift:
            require_rebuild_matches(conn, planes["federation"], resource_ref)
        assert drift.value.code == "projection-mismatch"
        conn.rollback()


def test_no_federation_pruning_capability_exists(postgres_urls) -> None:
    """Falsifier: any v1 change was pruned.

    Proved at the database, not by reading the source: no role the frozen
    boundary installs holds DELETE or TRUNCATE on any federation table, so no
    caller -- module, migration, or an operator at a psql prompt under a
    service role -- can prune one.  A source grep would survive a
    string-concatenated statement, a third module, or a later migration.

    Scope: no role the frozen boundary installs holds DELETE or TRUNCATE on any
    federation table. It says nothing about a superuser or the table owner,
    which is why destructive archive stays a manual, owner-approved action
    rather than something the schema alone can prevent.
    """
    selected = _new_schema("aqbf_acl")
    suffix = uuid.uuid4().hex[:8]
    owner_role, migrator_role = f"aqbf_owner_{suffix}", f"aqbf_migrator_{suffix}"
    command_role, actor_role = f"aqbf_command_{suffix}", f"aqbf_actor_{suffix}"
    roles = (owner_role, migrator_role, command_role, actor_role)
    with db.connect(postgres_urls["admin"]) as conn:
        for role in roles:
            conn.execute(psycopg.sql.SQL("CREATE ROLE {}").format(psycopg.sql.Identifier(role)))
        federation_schema.migrate(conn, selected)
        federation_schema.configure_role_boundaries(
            conn, selected, object_owner_role=owner_role, migration_role=migrator_role,
            command_role=command_role, denied_roles=(actor_role,),
        )
        conn.commit()
        try:
            for table, _, _ in _EXPORT_TABLES:
                for role in (migrator_role, command_role, actor_role):
                    row = conn.execute(
                        "SELECT has_table_privilege(%s,%s,'DELETE') AS d, "
                        "has_table_privilege(%s,%s,'TRUNCATE') AS t",
                        (role, f"{selected}.{table}", role, f"{selected}.{table}"),
                    ).fetchone()
                    assert not row["d"], (role, table, "DELETE")
                    assert not row["t"], (role, table, "TRUNCATE")
        finally:
            conn.execute(f'DROP SCHEMA "{selected}" CASCADE')
            for role in roles:
                conn.execute(psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role)))
            conn.commit()


def test_a_rerun_removes_no_recorded_change(planes) -> None:
    backfill = _backfill(planes)
    backfill.run()
    before = _count(planes, "federation_resource_changes")
    backfill.run()
    assert _count(planes, "federation_resource_changes") == before


def test_rebuild_replays_the_native_evidence_acceptance_settlement_and_supersede_path(planes) -> None:
    """The rebuild's native branches matter most for post-cutover resources and
    are unreachable through backfill, which only ever issues three operations."""
    authority = _authority(planes)
    native = _principal(
        "native:full", "federation.create", "federation.relate", "federation.evidence.ingest",
        "federation.acceptance.decide", "federation.settlement.record", "federation.supersede",
    )
    resource_ref = authority.create(principal=native, idempotency_key="n-create",
                                    expected_revision=0).resource_ref
    evidence = b"native evidence bytes"
    evidence_ref = "artifact:" + db.raw_digest(evidence)
    assert authority.record_evidence(
        principal=native, idempotency_key="n-evidence", resource_ref=resource_ref,
        evidence_ref=evidence_ref, evidence_bytes=evidence, assurance_type="native/v1",
        expected_revision=1,
    ).status == "accepted"
    assert authority.decide_acceptance(
        principal=native, idempotency_key="n-accept", resource_ref=resource_ref, outcome="accepted",
        policy_ref="policy:native", evidence_ref=evidence_ref, expected_revision=2,
    ).status == "accepted"
    assert authority.record_settlement(
        principal=native, idempotency_key="n-settle", resource_ref=resource_ref,
        fact_ref="fact:native", expected_revision=3,
    ).status == "accepted"
    assert authority.supersede(
        principal=native, idempotency_key="n-supersede", resource_ref=resource_ref, expected_revision=4,
    ).status == "accepted"

    with db.connect(planes["url"]) as conn:
        result = require_rebuild_matches(conn, planes["federation"], resource_ref)
        rebuilt = result["rebuilt"]
    assert rebuilt["revision"] == 5
    assert rebuilt["state"] == "superseded"
    assert [item["outcome"] for item in rebuilt["acceptance_decisions"]] == ["accepted"]
    assert [item["decided_by"] for item in rebuilt["acceptance_decisions"]] == ["native:full"]
    assert [item["fact_ref"] for item in rebuilt["settlements"]] == ["fact:native"]
    assert [item["evidence_ref"] for item in rebuilt["evidence"]] == [evidence_ref]


def test_legacy_terminal_state_never_implies_acceptance_or_settlement(planes) -> None:
    """Falsifier: legacy claimed/completed implies acceptance/settlement."""
    report = _backfill(planes).run()
    assert _count(planes, "federation_acceptance_decisions") == 0
    assert _count(planes, "federation_settlements") == 0
    assert _count(planes, "federation_evidence") == 0
    with db.connect(planes["url"]) as conn:
        states = {str(row["state"]) for row in conn.execute(
            f"SELECT state FROM {db.qname(planes['federation'], 'federation_resources')}"
        ).fetchall()}
        operations = {str(row["operation"]) for row in conn.execute(
            f"SELECT operation FROM {db.qname(planes['federation'], 'federation_resource_changes')}"
        ).fetchall()}
    assert states == {"registered"}
    assert operations <= {"create", "record-execution-ref", "add-relation"}
    assert len(report.resource_refs) >= 8


# --- export and restore ------------------------------------------------------


def _export(planes: dict, schema: str | None = None) -> bytes:
    with db.connect(planes["url"]) as conn:
        return export_federation(conn, schema or planes["federation"])


def test_export_restore_round_trip_preserves_the_canonical_projection(planes) -> None:
    """Falsifiers: export/restore changes the canonical projection; a configured
    retention objective cannot be restored.

    Scope: restoring into a freshly migrated, empty federation schema
    reproduces every resource's canonical projection, and re-exporting it is
    byte-identical within one producing wheel, with produced_at and source
    omitted. Cross-wheel byte-identity is a narrower claim -- producer_version
    and the migration ledger are inside the digested document -- and comparing
    across versions means comparing the tables block, not the whole document.
    """
    report = _backfill(planes).run()
    exported = _export(planes)
    assert exported == _export(planes)
    document = json.loads(exported)
    assert document["schema_version"] == "federation-export/v1"
    assert document["compatibility_label"] == federation_schema.COMPATIBILITY_LABEL
    assert set(document["tables"]) >= {"federation_resources", "federation_resource_changes"}
    # Self-describing: readable decades later with no other artifact.
    provenance = document["provenance"]
    assert provenance["producer"] == "actionq"
    assert provenance["producer_version"]
    assert [row["version"] for row in provenance["migration_ledger"]] == [1]
    assert all(row["checksum"].startswith("sha256:") or len(row["checksum"]) == 64
               for row in provenance["migration_ledger"])
    # Caller-supplied fields keep the export a pure function of its inputs.
    assert provenance["produced_at"] is None and provenance["source"] is None
    with db.connect(planes["url"]) as conn:
        stamped = json.loads(export_federation(conn, planes["federation"],
                                               produced_at="2026-08-22T00:00:00Z", source="probe"))
    assert stamped["provenance"]["produced_at"] == "2026-08-22T00:00:00Z"
    assert stamped["tables"] == document["tables"]

    restored_schema = _new_schema("aqbf_restore")
    try:
        with db.connect(planes["url"]) as conn:
            federation_schema.migrate(conn, restored_schema)
            conn.commit()
        with db.connect(planes["url"]) as conn:
            counts = restore_federation(conn, restored_schema, exported)
            conn.commit()
        assert counts["federation_resources"] == len(report.resource_refs)

        with db.connect(planes["url"]) as conn:
            for resource_ref in report.resource_refs:
                original = live_projection(conn, planes["federation"], resource_ref)
                copied = live_projection(conn, restored_schema, resource_ref)
                assert canonical_bytes(original) == canonical_bytes(copied)
                assert require_rebuild_matches(conn, restored_schema, resource_ref)["matches"]
        assert _export(planes, restored_schema) == exported
    finally:
        with db.connect(planes["url"]) as conn:
            conn.execute(f'DROP SCHEMA "{restored_schema}" CASCADE')
            conn.commit()


def test_restore_refuses_a_non_empty_target_or_a_foreign_document(planes) -> None:
    """Safety posture around restore. The "configured retention objective cannot
    be restored" falsifier is discharged by the round-trip test above."""
    _backfill(planes).run()
    exported = _export(planes)
    with db.connect(planes["url"]) as conn:
        with pytest.raises(ExportError, match="empty federation schema"):
            restore_federation(conn, planes["federation"], exported)
        conn.rollback()
        with pytest.raises(ExportError, match="federation-export/v1 document"):
            restore_federation(conn, planes["federation"], b'{"schema_version":"other/v1"}')
        conn.rollback()
        with pytest.raises(ExportError, match="not JSON"):
            restore_federation(conn, planes["federation"], b"not json")
        conn.rollback()
        with pytest.raises(ExportError, match="must be bytes"):
            restore_federation(conn, planes["federation"], "not bytes")
        conn.rollback()

    document = json.loads(exported)
    document["tables"].pop("federation_settlements")
    with db.connect(planes["url"]) as conn:
        with pytest.raises(ExportError, match="every federation table"):
            restore_federation(conn, planes["federation"], canonical_bytes(document))
        conn.rollback()
