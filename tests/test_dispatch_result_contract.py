from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from actionq import db
from actionq.application import ActionQApplication, InvocationProvenance
from actionq.cli import cli
from actionq_contracts import (
    DISPATCH_RESULT_V1,
    DispatchResult,
    artifact_digest,
    require_compatible,
)


def _result(*, terminal_status: str = "completed", stop_reason: str | None = None) -> dict:
    ref = "artifact:sha256:" + "a" * 64
    return {
        "contract_id": DISPATCH_RESULT_V1,
        "action_id": 42,
        "attempt_id": "aqs:attempt-1",
        "terminal_status": terminal_status,
        "result_ref": ref,
        "result_digest": artifact_digest(ref),
        "stop_reason": stop_reason,
    }


def test_dispatch_result_dataclass_is_an_exact_immutable_packet() -> None:
    packet = DispatchResult(
        action_id=42,
        attempt_id="aqs:attempt-1",
        terminal_status="completed",
        result_ref="artifact:sha256:" + "a" * 64,
        result_digest="sha256:" + "a" * 64,
    ).as_dict()

    assert require_compatible(packet) == DISPATCH_RESULT_V1
    assert set(packet) == {
        "contract_id", "action_id", "attempt_id", "terminal_status", "result_ref",
        "result_digest", "stop_reason",
    }


@pytest.mark.parametrize(
    "terminal_status, stop_reason",
    [("completed", None), ("no_change", None), ("blocked", "start-failed"),
     ("failed", "process-exit"), ("budget_exhausted", "usage-limit")],
)
def test_all_canonical_terminal_statuses_are_result_bearing(
    terminal_status: str, stop_reason: str | None,
) -> None:
    assert require_compatible(_result(terminal_status=terminal_status, stop_reason=stop_reason)) == DISPATCH_RESULT_V1


@pytest.mark.parametrize(
    "mutator, message",
    [
        (lambda value: value.update(result_digest="sha256:" + "b" * 64), "bind"),
        (lambda value: value.update(stop_reason="raw failure detail"), "privacy-safe reason"),
        (lambda value: value.update(terminal_status="completed", stop_reason="process-exit"), "must not contain"),
        (lambda value: value.update(terminal_status="failed", stop_reason=None), "require stop_reason"),
        (lambda value: value.update(transcript="must not be inline"), "extra"),
    ],
)
def test_dispatch_result_rejects_unbound_or_unsafe_packets(mutator, message: str) -> None:
    value = _result()
    mutator(value)
    with pytest.raises(ValueError, match=message):
        require_compatible(value)


@pytest.mark.parametrize(
    ("terminal_status", "stop_reason", "expected_status", "expected_event"),
    [
        ("completed", None, "completed", "action_completed"),
        ("no_change", None, "completed", "action_completed"),
        ("blocked", "start-failed", "failed", "action_failed"),
        ("failed", "process-exit", "failed", "action_failed"),
        ("budget_exhausted", "usage-limit", "failed", "action_failed"),
    ],
)
def test_db_settlement_maps_outcome_without_interpreting_artifact(
    monkeypatch, tmp_path, terminal_status: str, stop_reason: str | None,
    expected_status: str, expected_event: str,
) -> None:
    packet = _result(terminal_status=terminal_status, stop_reason=stop_reason)
    from actionq.cas import _DaemonCAS
    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    packet["result_ref"] = store.put(b"dispatch-result")
    packet["result_digest"] = artifact_digest(packet["result_ref"])
    captured = {}

    def transition(_conn, _schema, **kwargs):
        captured.update(kwargs)
        return {"status": kwargs["status"], "result_ref": kwargs["result_ref"]}

    monkeypatch.setattr(db, "_transition_terminal", transition)
    settled = db.settle_dispatch_result(
        object(), "actionq", action_id=42, result=packet, actor="worker:test",
        worker="worker:test", claim_receipt="private-receipt", _artifact_store=store,
    )

    assert settled == {"status": expected_status, "result_ref": packet["result_ref"]}
    assert captured["event_type"] == expected_event
    assert captured["result_ref"] == packet["result_ref"]
    assert captured["failure_reason"] == packet["stop_reason"]
    assert captured["payload"]["result_digest"] == packet["result_digest"]


def test_application_cli_and_served_settle_verify_the_owner_cas_before_mutation(
    monkeypatch, tmp_path, signed_runner_proof
) -> None:
    from actionq.cas import _DaemonCAS
    from actionq.vuoro import build_operations

    root = tmp_path / "cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    app = ActionQApplication(
        schema="aq", artifact_root=root,
        cas_factory=lambda path: _DaemonCAS(path, allow_unsafe_test_root=True),
    )
    app._terminal = lambda **_kwargs: {"status": "checked"}
    provenance = InvocationProvenance(
        actor="worker:test", environment="test", request_id="application",
        catalog_revision="test", idempotency_key="application",
    )

    missing = _result()
    with pytest.raises(db.ActionQError, match="referent"):
        app.settle(
            action_id=42, result=missing, actor="worker:test",
            claim_receipt="private-receipt", provenance=provenance,
        )

    valid_ref = store.put(b"valid result")
    valid = {**missing, "result_ref": valid_ref, "result_digest": artifact_digest(valid_ref)}
    assert app.settle(
        action_id=42, result=valid, actor="worker:test",
        claim_receipt="private-receipt", provenance=provenance,
    ) == {"status": "checked"}

    proof = signed_runner_proof("worker:test", "execution.action.settle", "action:42")
    import actionq.cli as cli_module
    monkeypatch.setattr(cli_module, "_app", lambda _ctx: app)
    cli_result = CliRunner().invoke(
        cli, ["settle", "42", "--packet-stdin"], input=json.dumps({
            "claim_receipt": "private-receipt", "runner_proof": proof, "result": missing,
        }),
    )
    assert cli_result.exit_code != 0
    assert "referent" in cli_result.output

    served = {
        operation.definition["name"]: operation.handler
        for operation in build_operations(app)
    }["execution.action.settle"]
    context = SimpleNamespace(
        identity=SimpleNamespace(actor="worker:test", environment="test"),
        request_id="served", catalog_revision="test", basis_revision=None,
        idempotency_key="served",
    )
    with pytest.raises(db.ActionQError, match="referent"):
        served(
            {"action_id": 42, "result": missing, "claim_receipt": "private-receipt"},
            context,
        )
    assert served(
        {"action_id": 42, "result": valid, "claim_receipt": "private-receipt"}, context,
    ) == {"status": "checked"}


def test_served_settlement_schema_discriminates_success_and_failure_reasons() -> None:
    from actionq.vuoro import build_operations

    settle = next(
        operation for operation in build_operations()
        if operation.definition["name"] == "execution.action.settle"
    )
    result_schema = settle.definition["input_schema"]["properties"]["result"]
    assert result_schema["properties"]["terminal_status"]["enum"] == [
        "blocked", "budget_exhausted", "completed", "failed", "no_change",
    ]
    assert result_schema["allOf"] == [
        {
            "if": {"properties": {"terminal_status": {"enum": ["completed", "no_change"]}}},
            "then": {"properties": {"stop_reason": {"type": "null"}}},
        },
        {
            "if": {"properties": {"terminal_status": {"enum": ["blocked", "failed", "budget_exhausted"]}}},
            "then": {"properties": {"stop_reason": {"type": "string"}}},
        },
    ]
