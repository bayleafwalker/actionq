from __future__ import annotations

import json
import hashlib
import os
import re
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from actionq_contracts import (
    ACTION_CREATION_REQUEST_V1,
    EXECUTION_ENVELOPE_V1, canonical_bytes as contract_canonical_bytes,
    require_compatible as require_contract_compatible, sha256_digest as contract_digest,
)


MAX_CHAIN_DEPTH = int(os.environ.get("ACTIONQ_MAX_CHAIN_DEPTH", "3"))
DEFAULT_RATE_LIMIT_PER_HOUR = int(os.environ.get("ACTIONQ_RATE_LIMIT_PER_HOUR", "20"))
SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SESSION_EVENT_TYPES = (
    "runner.contract.frozen",
    "session.dispatch",
    "session.started",
    "session.heartbeat",
    "session.paused",
    "session.resumed",
    "session.exited",
    "session.end-inferred",
    "settlement.pending",
    "settlement.sprint_claim_released",
    "settlement.sprint_claim_release_failed",
    "settlement.actionq_skipped_claim_lost",
    "publication.available",
    "publication.settled",
    "workspace.destroyed",
    "workspace.destruction_failed",
)


class ActionQError(ValueError):
    pass


class NoActionAvailable(ActionQError):
    pass


class ClaimRejected(ActionQError):
    """A stale or invalid claim/lease authority command.

    Per the state-event-command matrix, queue claim and lease renewal are
    authority commands requiring remote arbitration: a stale or invalid
    command must produce a visible, durable rejection rather than mutating
    authoritative state. The rejection event is always recorded (in the
    same transaction, before this is raised) even though the command
    itself did not take effect.
    """

    def __init__(self, message: str, *, reason: str, action_id: int, requested_by: str):
        super().__init__(message)
        self.reason = reason
        self.action_id = action_id
        self.requested_by = requested_by


def receipt_digest(receipt: str) -> str:
    return "sha256:" + hashlib.sha256(receipt.encode("utf-8")).hexdigest()


def consume_runner_request(
    conn, schema: str, *, runner_id: str, request_id: str, operation: str,
    resource: str, action_id: int | None = None, allow_replay: bool = False,
) -> bool:
    lock_key = f"runner-auth:{runner_id}:{request_id}"
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))
    prior = conn.execute(
        f"""SELECT action_id, payload FROM {qname(schema, 'events')}
            WHERE event_type='runner.authenticated'
              AND payload->>'runner_id'=%s AND payload->>'request_id'=%s LIMIT 1""",
        (runner_id, request_id),
    ).fetchone()
    if prior is not None:
        payload = prior["payload"]
        if (allow_replay and payload.get("operation") == operation
                and payload.get("resource") == resource
                and (action_id is None or int(prior.get("action_id") or action_id) == action_id)):
            return False
        raise ActionQError("runner proof request id was already consumed")
    insert_event(
        conn, schema, action_id=action_id, event_type="runner.authenticated", actor=runner_id,
        payload={"runner_id": runner_id, "request_id": request_id,
                 "operation": operation, "resource": resource},
    )
    return True


_ARTIFACT_REF_RE = re.compile(r"^artifact:sha256:[0-9a-f]{64}$")
_GIT_OBJECT_RE = re.compile(r"^[0-9a-f]{7,64}$")
_ATTEMPT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_IMMUTABLE_REF_RE = re.compile(r"^artifact:sha256:[0-9a-f]{64}$")
_IMMUTABLE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_IMMUTABLE_ROLE_CONTRACTS = {
    "candidate-verification": "candidate-verification-spec/v1",
    "candidate-integration": "candidate-integration-spec/v1",
    "candidate-review": "candidate-review-spec/v1",
}


def immutable_input_set_digest(inputs: list[str]) -> str:
    """Digest the compiler's ordered immutable input set.

    Order is significant: wave integration is bound to the frozen execution
    group ordinal order, so sorting here would silently alter its semantics.
    """
    if not isinstance(inputs, list) or not inputs:
        raise ActionQError("immutable input set must be a non-empty ordered list")
    if any(not isinstance(item, str) or not _IMMUTABLE_REF_RE.fullmatch(item) for item in inputs):
        raise ActionQError("immutable input set contains an invalid artifact reference")
    if len(inputs) != len(set(inputs)):
        raise ActionQError("immutable input set entries must be unique")
    return contract_digest({"contract_id": "immutable-input-set/v1", "inputs": inputs})


def _immutable_snapshot(value: dict[str, Any], *, expected_contract: str | None = None) -> tuple[bytes, str]:
    try:
        actual = require_contract_compatible(value)
    except (TypeError, ValueError) as exc:
        raise ActionQError(f"invalid immutable contract: {exc}") from exc
    if expected_contract is not None and actual != expected_contract:
        raise ActionQError(f"expected immutable contract {expected_contract}")
    raw = contract_canonical_bytes(value)
    return raw, contract_digest(value)


def immutable_spec_input_refs(spec: dict[str, Any]) -> list[str]:
    """Return the sole ordered input set admitted by one frozen spec."""
    contract = require_contract_compatible(spec)
    if contract == "candidate-verification-spec/v1":
        return [spec["candidate_ref"], spec["profile_ref"]]
    if contract == "candidate-integration-spec/v1":
        return list(spec["member_result_refs"])
    if contract == "candidate-review-spec/v1":
        return [spec["candidate_ref"], spec["verification_result_ref"]]
    raise ActionQError("immutable action spec contract is unsupported")


def create_immutable_action(
    conn,
    schema: str,
    *,
    request: dict[str, Any],
    spec: dict[str, Any],
    input_refs: list[str],
    action_type: str,
    project: str | None,
    priority: int,
    created_by: str,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create one ordinary action from an exact compiler request.

    PostgreSQL stores both canonical request and spec bytes.  A CAS locator is
    checked as an identity claim but never read or treated as a second source
    of truth.  Logical replay returns the original action only for byte-identical
    input; changed bytes under the same plan/role/subject conflict before any
    action is enqueued.
    """
    request_raw, request_digest = _immutable_snapshot(
        request, expected_contract=ACTION_CREATION_REQUEST_V1,
    )
    expected_contract = _IMMUTABLE_ROLE_CONTRACTS.get(request["role"])
    if expected_contract is None or action_type != request["role"]:
        raise ActionQError("immutable action role must equal the ordinary action type")
    spec_raw, spec_digest = _immutable_snapshot(spec, expected_contract=expected_contract)
    if request["spec_digest"] != spec_digest or request["spec_ref"] != "artifact:" + spec_digest:
        raise ActionQError("immutable action request does not bind exact spec bytes")
    if request["role"] == "candidate-integration" and request["topology"] != spec["topology"]:
        raise ActionQError("integration request topology must equal the exact spec topology")
    expected_spec_inputs = immutable_spec_input_refs(spec)
    if input_refs != expected_spec_inputs:
        raise ActionQError("immutable action input refs must equal the exact ordered spec inputs")
    expected_inputs = immutable_input_set_digest(expected_spec_inputs)
    if request["input_set_digest"] != expected_inputs:
        raise ActionQError("immutable action request input_set_digest mismatch")
    request_ref = "artifact:" + request_digest
    if not _IMMUTABLE_REF_RE.fullmatch(request_ref):
        raise ActionQError("immutable action request digest is invalid")
    with conn.transaction():
        lock = "\x1f".join(("immutable-action", request["plan_ref"], request["topology"], request["role"], request["subject"]))
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock,))
        prior = conn.execute(
            f"""SELECT r.*, a.status FROM {qname(schema, 'immutable_action_requests')} r
                 JOIN {qname(schema, 'actions')} a ON a.id=r.action_id
                 WHERE r.plan_ref=%s AND r.topology=%s AND r.role=%s AND r.subject=%s""",
            (request["plan_ref"], request["topology"], request["role"], request["subject"]),
        ).fetchone()
        if prior is not None:
            if bytes(prior["request_snapshot"]) != request_raw:
                raise ActionQError("immutable action logical identity conflicts with different request bytes")
            return {
                "action_id": int(prior["action_id"]), "status": prior["status"],
                "request_ref": prior["request_ref"], "request_digest": prior["request_digest"],
                "replayed": True,
            }
        spec_prior = conn.execute(
            f"SELECT spec_digest, spec_snapshot FROM {qname(schema, 'immutable_action_specs')} WHERE spec_ref=%s",
            (request["spec_ref"],),
        ).fetchone()
        if spec_prior is None:
            conn.execute(
                f"INSERT INTO {qname(schema, 'immutable_action_specs')} (spec_ref, spec_digest, spec_snapshot) VALUES (%s,%s,%s)",
                (request["spec_ref"], spec_digest, spec_raw),
            )
        elif spec_prior["spec_digest"] != spec_digest or bytes(spec_prior["spec_snapshot"]) != spec_raw:
            raise ActionQError("immutable spec reference conflicts with different bytes")
        action = enqueue(
            conn, schema, action_type=action_type, project=project, target_ref=request["subject"],
            source_refs=list(input_refs), priority=priority, parent_id=None, created_by=created_by,
            provenance=provenance,
        )
        conn.execute(
            f"""INSERT INTO {qname(schema, 'immutable_action_requests')}
                (action_id, request_ref, request_digest, request_snapshot, plan_ref, topology, role, subject,
                 spec_ref, spec_digest, input_set_digest)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (action["id"], request_ref, request_digest, request_raw, request["plan_ref"], request["topology"],
             request["role"], request["subject"], request["spec_ref"], spec_digest, expected_inputs),
        )
        insert_event(
            conn, schema, action_id=action["id"], event_type="immutable_action.created", actor=created_by,
            payload=event_payload_with_provenance({
                "request_ref": request_ref, "request_digest": request_digest, "spec_ref": request["spec_ref"],
                "spec_digest": spec_digest, "input_set_digest": expected_inputs, "role": request["role"],
                "topology": request["topology"], "subject": request["subject"],
            }, provenance),
        )
    return {"action_id": int(action["id"]), "status": "pending", "request_ref": request_ref,
            "request_digest": request_digest, "replayed": False}


def register_publication(
    conn,
    schema: str,
    *,
    action_id: int,
    runner_id: str,
    runner_request_id: str,
    claim_receipt: str,
    attempt_id: str,
    journal_ref: str,
    source_commit: str,
    candidate_commit: str,
) -> dict:
    """Register an immutable publication receipt for the live claim only.

    The runner request id is the idempotency authority.  A replay is accepted
    only when every public registration field is byte-for-byte identical to
    the already durable event; a different receipt under the same proof is a
    conflict, never a second publication.
    """
    if not _ATTEMPT_ID_RE.fullmatch(attempt_id):
        raise ActionQError("publication attempt id is invalid")
    if not _ARTIFACT_REF_RE.fullmatch(journal_ref):
        raise ActionQError("publication journal ref must be an artifact sha256 ref")
    if not _GIT_OBJECT_RE.fullmatch(source_commit) or not _GIT_OBJECT_RE.fullmatch(candidate_commit):
        raise ActionQError("publication commits must be lowercase Git object ids")

    resource = f"action:{action_id}:publication:{attempt_id}"
    safe_payload = {
        "journal_ref": journal_ref,
        "attempt_id": attempt_id,
        "source_commit": source_commit,
        "candidate_commit": candidate_commit,
        "claim_receipt_digest": receipt_digest(claim_receipt),
        "runner_request_id": runner_request_id,
    }
    with conn.transaction():
        consumed = consume_runner_request(
            conn,
            schema,
            runner_id=runner_id,
            request_id=runner_request_id,
            operation="execution.action.register-publication",
            resource=resource,
            action_id=action_id,
            allow_replay=True,
        )
        action = conn.execute(
            f"SELECT * FROM {qname(schema, 'actions')} WHERE id = %s FOR UPDATE",
            (action_id,),
        ).fetchone()
        if not consumed:
            prior = conn.execute(
                f"""SELECT payload FROM {qname(schema, 'events')}
                    WHERE action_id = %s AND event_type = 'publication.registered'
                      AND payload->>'runner_request_id' = %s
                    ORDER BY id ASC LIMIT 1""",
                (action_id, runner_request_id),
            ).fetchone()
            if prior is not None and _event_payload(dict(prior)) == safe_payload:
                return {"id": action_id, **safe_payload}
            raise ActionQError("publication registration request conflicts with prior registration")
        if action is None:
            raise ActionQError(f"Action #{action_id} publication registration rejected: action-not-found")
        reason = _renewal_rejection_reason(dict(action), runner_id, claim_receipt)
        if reason is not None:
            raise ActionQError(
                f"Action #{action_id} publication registration rejected: {reason}"
            )
        insert_event(
            conn,
            schema,
            action_id=action_id,
            event_type="publication.registered",
            actor=runner_id,
            payload=safe_payload,
        )
    return {"id": action_id, **safe_payload}


def _import_psycopg():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for actionq. Install with `pip install -e .` "
            "or `uv tool install /projects/dev/actionq/`."
        ) from exc
    return psycopg, dict_row


def schema_name(value: str | None = None) -> str:
    name = value or os.environ.get("ACTIONQ_SCHEMA", "actionq")
    if not SCHEMA_RE.match(name):
        raise ActionQError(
            "ACTIONQ_SCHEMA must be a simple Postgres identifier "
            "(letters, digits, underscore; not starting with a digit)."
        )
    return name


def qname(schema: str, table: str) -> str:
    schema_name(schema)
    return f'"{schema}"."{table}"'


def connect(url: str | None = None):
    db_url = url or os.environ.get("ACTIONQ_URL")
    if not db_url:
        raise ActionQError("ACTIONQ_URL is required")
    psycopg, dict_row = _import_psycopg()
    return psycopg.connect(db_url, row_factory=dict_row)


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def to_json(value: Any, *, pretty: bool = False) -> str:
    return json.dumps(value, default=json_default, indent=2 if pretty else None)


def parse_json(raw: str | None, *, default: Any) -> Any:
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ActionQError(f"Invalid JSON: {exc}") from exc


def _event_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    if isinstance(payload, dict):
        return payload
    if payload is None:
        return {}
    if isinstance(payload, str):
        parsed = parse_json(payload, default={})
        if isinstance(parsed, dict):
            return parsed
    raise ActionQError(f"Unexpected event payload type: {type(payload)!r}")


def _text(value: Any) -> Any:
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _session_deadline(heartbeat_at: Any, ttl_seconds: int | None) -> str | None:
    if ttl_seconds is None:
        return None
    ts = _parse_timestamp(heartbeat_at)
    if ts is None:
        return None
    return json_default(ts + timedelta(seconds=ttl_seconds))


def summarize_sessions(
    rows: list[dict[str, Any]],
    *,
    active_only: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    sessions: dict[str, dict[str, Any]] = {}
    ordered_ids: list[str] = []

    for row in sorted(rows, key=lambda item: (item["timestamp"], item["id"])):
        payload = _event_payload(row)
        session_id = payload.get("session_id")
        if not session_id:
            continue

        session = sessions.get(session_id)
        if session is None:
            session = {
                "session_id": session_id,
                "runtime_session_id": None,
                "action_id": row.get("action_id"),
                "daemon_id": None,
                "action_type": None,
                "project": None,
                "target_ref": None,
                "harness": None,
                "model": None,
                "worktree": None,
                "branch": None,
                "pid": None,
                "started_at": None,
                "heartbeat_at": None,
                "last_heartbeat_at": None,
                "ttl_seconds": None,
                "deadline_at": None,
                "heartbeat_age_seconds": None,
                "exited_at": None,
                "last_event_type": None,
                "last_event_at": None,
                "status": "dispatched",
                "outcome": None,
                "exit_code": None,
                "claim": {
                    "claim_id": None,
                    "work_item_id": None,
                    "claim_type": None,
                },
            }
            sessions[session_id] = session
            ordered_ids.append(session_id)

        claim_payload = payload.get("claim")
        if claim_payload is not None and not isinstance(claim_payload, dict):
            raise ActionQError("session event claim payload must be an object when present")

        session["action_id"] = row.get("action_id") or session["action_id"]
        event_type = _text(row["event_type"])
        session["last_event_type"] = event_type
        session["last_event_at"] = row["timestamp"]
        session["runtime_session_id"] = (
            payload.get("runtime_session_id")
            or session["runtime_session_id"]
            or session_id
        )
        session["daemon_id"] = payload.get("daemon_id", session["daemon_id"])
        session["action_type"] = payload.get("action_type", session["action_type"])
        session["project"] = payload.get("project", session["project"])
        session["target_ref"] = payload.get("target_ref", session["target_ref"])
        session["harness"] = payload.get("harness", session["harness"])
        session["model"] = payload.get("model", session["model"])
        session["worktree"] = payload.get("worktree", session["worktree"])
        session["branch"] = payload.get("branch", session["branch"])
        session["pid"] = payload.get("pid", session["pid"])
        session["started_at"] = payload.get("started_at", session["started_at"])
        ttl_seconds = payload.get("ttl_seconds")
        session["ttl_seconds"] = ttl_seconds if ttl_seconds is not None else session["ttl_seconds"]
        if claim_payload:
            session["claim"]["claim_id"] = claim_payload.get("claim_id", session["claim"]["claim_id"])
            session["claim"]["work_item_id"] = claim_payload.get(
                "work_item_id", session["claim"]["work_item_id"]
            )
            session["claim"]["claim_type"] = claim_payload.get(
                "claim_type", session["claim"]["claim_type"]
            )

        if event_type == "session.dispatch":
            session["status"] = "dispatched"
            session["heartbeat_at"] = row["timestamp"]
        elif event_type == "session.started":
            session["status"] = "running"
            session["heartbeat_at"] = row["timestamp"]
        elif event_type == "session.heartbeat":
            session["status"] = payload.get("status", "running")
            session["heartbeat_at"] = row["timestamp"]
            session["last_heartbeat_at"] = row["timestamp"]
        elif event_type == "session.paused":
            session["status"] = "paused"
            session["heartbeat_at"] = row["timestamp"]
        elif event_type == "session.resumed":
            session["status"] = "running"
            session["heartbeat_at"] = row["timestamp"]
        elif event_type in {"session.exited", "session.end-inferred"}:
            session["status"] = "exited"
            session["outcome"] = payload.get("outcome")
            session["exit_code"] = payload.get("exit_code")
            session["exited_at"] = row["timestamp"]
            session["heartbeat_at"] = row["timestamp"]

        if session["ttl_seconds"] is not None:
            last_seen_at = session["heartbeat_at"] or session["started_at"] or session["last_event_at"]
            session["deadline_at"] = _session_deadline(last_seen_at, int(session["ttl_seconds"]))

    summarized = [sessions[session_id] for session_id in ordered_ids]
    summarized.sort(key=lambda item: (item["last_event_at"], item["session_id"]), reverse=True)

    now = datetime.now(timezone.utc)
    for session in summarized:
        heartbeat_at = session["last_heartbeat_at"] or session["heartbeat_at"] or session["started_at"]
        heartbeat_ts = _parse_timestamp(heartbeat_at)
        if heartbeat_ts is not None:
            session["heartbeat_age_seconds"] = max(int((now - heartbeat_ts).total_seconds()), 0)

    if active_only:
        summarized = [item for item in summarized if item["status"] != "exited"]
    return summarized[:limit]


def migrate(conn, schema: str) -> dict[str, Any]:
    """Compatibility wrapper for the deployment-owned migration API."""

    from . import schema as schema_contract

    return schema_contract.migrate(conn, schema)


def check_compatibility(conn, schema: str):
    """Return the execution adapter's read-only schema compatibility record."""

    from . import schema as schema_contract

    return schema_contract.check_compatibility(conn, schema)


def require_compatible(conn, schema: str):
    """Fail closed unless the schema is supported by this release."""

    from . import schema as schema_contract

    return schema_contract.require_compatible(conn, schema)


def insert_event(
    conn,
    schema: str,
    *,
    event_type: str,
    action_id: int | None = None,
    actor: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict:
    row = conn.execute(
        f"""
        INSERT INTO {qname(schema, "events")} (action_id, event_type, actor, payload)
        VALUES (%s, %s, %s, %s::jsonb)
        RETURNING *
        """,
        (action_id, event_type, actor, json.dumps(payload or {})),
    ).fetchone()
    return dict(row)


def event_payload_with_provenance(
    payload: dict[str, Any] | None,
    provenance: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach invocation provenance without changing legacy event payloads."""

    result = dict(payload or {})
    if provenance is not None:
        result["provenance"] = dict(provenance)
    return result


def get_action(conn, schema: str, action_id: int) -> dict | None:
    row = conn.execute(
        f"SELECT * FROM {qname(schema, 'actions')} WHERE id = %s",
        (action_id,),
    ).fetchone()
    return dict(row) if row else None


def redact_action(action: dict[str, Any]) -> dict[str, Any]:
    """Remove one-time claim authority from general-purpose read surfaces."""
    result = dict(action)
    result.pop("claim_receipt", None)
    result.pop("cancel_former_receipt_digest", None)
    result.pop("runner_auth_digest", None)
    result.pop("cancel_runner_auth_digest", None)
    result.pop("runner_auth_token", None)
    return result


def _rate_limited_source(created_by: str) -> bool:
    return created_by.startswith("agent:") or created_by.startswith("script:")


def enqueue(
    conn,
    schema: str,
    *,
    action_type: str,
    project: str | None,
    target_ref: str | None,
    source_refs: list[str],
    priority: int,
    parent_id: int | None,
    created_by: str,
    provenance: dict[str, Any] | None = None,
) -> dict:
    chain_depth = 0
    if parent_id is not None:
        parent = get_action(conn, schema, parent_id)
        if parent is None:
            raise ActionQError(f"Parent action #{parent_id} not found")
        chain_depth = int(parent["chain_depth"]) + 1
    if chain_depth > MAX_CHAIN_DEPTH:
        raise ActionQError(f"chain_depth {chain_depth} exceeds max {MAX_CHAIN_DEPTH}")

    with conn.transaction():
        if _rate_limited_source(created_by):
            count = conn.execute(
                f"""
                SELECT count(*) AS count
                FROM {qname(schema, "events")}
                WHERE event_type = 'action_enqueued'
                  AND payload->>'created_by' = %s
                  AND timestamp > now() - interval '1 hour'
                """,
                (created_by,),
            ).fetchone()["count"]
            if count >= DEFAULT_RATE_LIMIT_PER_HOUR:
                raise ActionQError(
                    f"Rate limit exceeded for {created_by}: {count}/hour"
                )

        action = conn.execute(
            f"""
            INSERT INTO {qname(schema, "actions")}
                (action_type, project, target_ref, source_refs, priority, parent_id, chain_depth, created_by)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                action_type,
                project,
                target_ref,
                json.dumps(source_refs),
                priority,
                parent_id,
                chain_depth,
                created_by,
            ),
        ).fetchone()
        insert_event(
            conn,
            schema,
            action_id=action["id"],
            event_type="action_enqueued",
            actor=created_by,
            payload=event_payload_with_provenance(
                {
                    "action_type": action_type,
                    "project": project,
                    "target_ref": target_ref,
                    "created_by": created_by,
                    "parent_id": parent_id,
                    "chain_depth": chain_depth,
                },
                provenance,
            ),
        )
    return dict(action)


def list_actions(
    conn,
    schema: str,
    *,
    status: str | None = None,
    action_type: str | None = None,
    project: str | None = None,
    limit: int = 50,
) -> list[dict]:
    clauses = []
    params: list[Any] = []
    if status:
        clauses.append("status = %s")
        params.append(status)
    if action_type:
        clauses.append("action_type = %s")
        params.append(action_type)
    if project:
        clauses.append("project = %s")
        params.append(project)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    rows = conn.execute(
        f"""
        SELECT * FROM {qname(schema, "actions")}
        {where}
        ORDER BY priority ASC, created_at ASC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        parsed = parse_json(value, default=[])
        if isinstance(parsed, list):
            return parsed
    return []


def summarize_dispatches(actions: list[dict[str, Any]], event_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events_by_action: dict[int, list[dict[str, Any]]] = {}
    for row in event_rows:
        action_id = row.get("action_id")
        if action_id is None:
            continue
        events_by_action.setdefault(int(action_id), []).append(row)

    sessions = summarize_sessions(event_rows, active_only=False, limit=max(len(event_rows), 1))
    sessions_by_action: dict[int, list[dict[str, Any]]] = {}
    for session in sessions:
        action_id = session.get("action_id")
        if action_id is None:
            continue
        sessions_by_action.setdefault(int(action_id), []).append(session)

    dispatches: list[dict[str, Any]] = []
    for action in actions:
        action_id = int(action["id"])
        action_events = sorted(
            events_by_action.get(action_id, []),
            key=lambda item: (item["timestamp"], item["id"]),
        )
        requested_payload: dict[str, Any] = {}
        for event in action_events:
            if _text(event["event_type"]) == "dispatch.requested":
                requested_payload = _event_payload(event)

        action_sessions = sorted(
            sessions_by_action.get(action_id, []),
            key=lambda item: (item.get("last_event_at") or "", item.get("session_id") or ""),
            reverse=True,
        )
        source_refs = _json_list(action.get("source_refs"))
        dispatch_group_id = (
            requested_payload.get("dispatch_group_id")
            or requested_payload.get("group_id")
            or action.get("dispatch_group_id")
        )
        dispatches.append(
            {
                "id": action_id,
                "action_type": _text(action["action_type"]),
                "kind": requested_payload.get("kind") or _text(action["action_type"]),
                "output_expectation": requested_payload.get("output_expectation"),
                "project": _text(action.get("project")),
                "target_ref": _text(action.get("target_ref")),
                "source_refs": source_refs,
                "status": _text(action["status"]),
                "priority": int(action["priority"]),
                "created_at": action["created_at"],
                "claimed_at": action.get("claimed_at"),
                "completed_at": action.get("completed_at"),
                "claimed_by": _text(action.get("claimed_by")),
                "result_ref": _text(action.get("result_ref")),
                "failure_reason": _text(action.get("failure_reason")),
                "parent_id": action.get("parent_id"),
                "chain_depth": int(action.get("chain_depth") or 0),
                "dispatch_group_id": dispatch_group_id,
                "sprint_id": requested_payload.get("sprint_id"),
                "title": requested_payload.get("title"),
                "harness": requested_payload.get("harness"),
                "model": requested_payload.get("model"),
                "session": action_sessions[0] if action_sessions else None,
                "audit_refs": [ref for ref in source_refs if isinstance(ref, str) and ref.startswith("ad:")],
            }
        )

    dispatches.sort(
        key=lambda item: (
            item["created_at"],
            item["id"],
        ),
        reverse=True,
    )
    return dispatches


def list_dispatches(
    conn,
    schema: str,
    *,
    project: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if project:
        clauses.append("project = %s")
        params.append(project)
    if status:
        clauses.append("status = %s")
        params.append(status)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    actions = conn.execute(
        f"""
        SELECT *
        FROM {qname(schema, "actions")}
        {where}
        ORDER BY created_at DESC, id DESC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    action_rows = [dict(row) for row in actions]
    if not action_rows:
        return []

    action_ids = [row["id"] for row in action_rows]
    events = conn.execute(
        f"""
        SELECT *
        FROM {qname(schema, "events")}
        WHERE action_id = ANY(%s)
        ORDER BY timestamp ASC, id ASC
        """,
        (action_ids,),
    ).fetchall()
    return summarize_dispatches(action_rows, [dict(row) for row in events])


_GROUP_CONTRACT_ID = "execution-group/v1"
_PLAN_REF_RE = re.compile(r"^artifact:sha256:[0-9a-f]{64}$")


def _group_projection(conn, schema: str, row: dict[str, Any]) -> dict[str, Any]:
    counts = conn.execute(
        f"""
        SELECT
            count(*) FILTER (WHERE a.status = 'pending' AND g.claim_state = 'active') AS pending_claimable,
            count(*) FILTER (WHERE a.status = 'pending' AND g.claim_state = 'stopped') AS pending_stopped,
            count(*) FILTER (WHERE a.status = 'claimed') AS claimed,
            count(*) FILTER (WHERE a.status = 'cancelling') AS cancelling,
            count(*) FILTER (WHERE a.status = 'completed') AS completed,
            count(*) FILTER (WHERE a.status = 'failed') AS failed,
            count(*) FILTER (WHERE a.status = 'rejected') AS rejected,
            count(*) FILTER (WHERE a.status = 'cancelled') AS cancelled
        FROM {qname(schema, 'execution_groups')} g
        JOIN {qname(schema, 'execution_group_members')} m ON m.group_id = g.id
        JOIN {qname(schema, 'actions')} a ON a.id = m.action_id
        WHERE g.id = %s
        """,
        (row["id"],),
    ).fetchone()
    members = conn.execute(
        f"""SELECT m.action_id, m.ordinal, m.envelope_digest, a.status
            FROM {qname(schema, 'execution_group_members')} m
            JOIN {qname(schema, 'actions')} a ON a.id = m.action_id
            WHERE m.group_id = %s ORDER BY m.ordinal""",
        (row["id"],),
    ).fetchall()
    result = dict(row)
    result.pop("spec_snapshot", None)
    result["id"] = str(result["id"])
    result["members"] = [dict(member) for member in members]
    result["counts"] = {key: int(counts[key] or 0) for key in (
        "pending_claimable", "pending_stopped", "claimed", "cancelling", "completed",
        "failed", "rejected", "cancelled",
    )}
    return result


def get_execution_group(conn, schema: str, group_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        f"SELECT * FROM {qname(schema, 'execution_groups')} WHERE id = %s", (group_id,),
    ).fetchone()
    return _group_projection(conn, schema, dict(row)) if row else None


def list_execution_groups(conn, schema: str, *, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"SELECT * FROM {qname(schema, 'execution_groups')} ORDER BY created_at DESC LIMIT %s",
        (limit,),
    ).fetchall()
    return [_group_projection(conn, schema, dict(row)) for row in rows]


def realize_execution_group(
    conn, schema: str, *, plan_ref: str, max_parallel: int,
    failure_policy: str, members: list[dict[str, Any]], created_by: str,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not _PLAN_REF_RE.fullmatch(plan_ref):
        raise ActionQError("execution group plan_ref must be an immutable artifact sha256 ref")
    if isinstance(max_parallel, bool) or not isinstance(max_parallel, int) or not 1 <= max_parallel <= 32:
        raise ActionQError("execution group max_parallel must be between 1 and 32")
    if failure_policy != "continue-independent":
        raise ActionQError("execution group supports only continue-independent")
    if not members:
        raise ActionQError("execution group requires at least one member")
    normalized_members: list[dict[str, Any]] = []
    seen: set[int] = set()
    for member in members:
        if set(member) != {"action_id", "envelope"}:
            raise ActionQError("execution group members require exactly action_id and envelope")
        action_id, envelope = member["action_id"], member["envelope"]
        if isinstance(action_id, bool) or not isinstance(action_id, int) or action_id <= 0:
            raise ActionQError("execution group action ids must be positive integers")
        if action_id in seen:
            raise ActionQError("execution group action ids must be unique")
        if not isinstance(envelope, dict):
            raise ActionQError("execution group envelope must be an object")
        try:
            contract_id = require_contract_compatible(envelope)
        except ValueError as exc:
            raise ActionQError(f"invalid execution group envelope: {exc}") from exc
        if contract_id != EXECUTION_ENVELOPE_V1 or envelope["action_id"] != action_id:
            raise ActionQError("execution group envelope/action identity mismatch")
        seen.add(action_id)
        normalized_members.append({"action_id": action_id, "envelope": envelope})
    spec = {
        "contract_id": _GROUP_CONTRACT_ID, "plan_ref": plan_ref,
        "max_parallel": max_parallel, "failure_policy": failure_policy,
        "members": normalized_members,
    }
    snapshot = contract_canonical_bytes(spec)
    digest = contract_digest(spec)
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"execution-group:{plan_ref}",))
        prior = conn.execute(
            f"SELECT * FROM {qname(schema, 'execution_groups')} WHERE plan_ref = %s", (plan_ref,),
        ).fetchone()
        if prior is not None:
            if prior["spec_sha256"] != digest:
                raise ActionQError("execution group plan_ref already has a different immutable spec")
            return _group_projection(conn, schema, dict(prior))
        action_ids = sorted(seen)
        rows = conn.execute(
            f"SELECT id, status FROM {qname(schema, 'actions')} WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            (action_ids,),
        ).fetchall()
        if [int(row["id"]) for row in rows] != action_ids:
            raise ActionQError("execution group references an unknown action")
        if any(row["status"] != "pending" for row in rows):
            raise ActionQError("execution group members must all be pending")
        if conn.execute(
            f"SELECT action_id FROM {qname(schema, 'execution_group_members')} WHERE action_id = ANY(%s)",
            (action_ids,),
        ).fetchone() is not None:
            raise ActionQError("an action already belongs to an execution group")
        group_id = str(uuid.uuid4())
        group = conn.execute(
            f"""INSERT INTO {qname(schema, 'execution_groups')}
                (id, plan_ref, spec_sha256, spec_snapshot, max_parallel, failure_policy, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *""",
            (group_id, plan_ref, digest, snapshot, max_parallel, failure_policy, created_by),
        ).fetchone()
        for ordinal, member in enumerate(normalized_members):
            envelope_bytes = contract_canonical_bytes(member["envelope"])
            conn.execute(
                f"""INSERT INTO {qname(schema, 'execution_group_members')}
                    (group_id, action_id, ordinal, envelope_digest, envelope_snapshot)
                    VALUES (%s, %s, %s, %s, %s)""",
                (group_id, member["action_id"], ordinal,
                 contract_digest(member["envelope"]), envelope_bytes),
            )
        insert_event(
            conn, schema, event_type="execution_group.realized", actor=created_by,
            payload=event_payload_with_provenance({
                "group_id": group_id, "plan_ref": plan_ref, "spec_sha256": digest,
                "max_parallel": max_parallel, "failure_policy": failure_policy,
                "action_ids": [member["action_id"] for member in normalized_members],
            }, provenance),
        )
        return _group_projection(conn, schema, dict(group))


def stop_execution_group(
    conn, schema: str, *, group_id: str, actor: str, reason: str,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.transaction():
        row = conn.execute(
            f"SELECT * FROM {qname(schema, 'execution_groups')} WHERE id = %s FOR UPDATE",
            (group_id,),
        ).fetchone()
        if row is None:
            raise ActionQError("execution group not found")
        if row["claim_state"] == "active":
            row = conn.execute(
                f"""UPDATE {qname(schema, 'execution_groups')}
                    SET claim_state='stopped', stopped_at=now(), stopped_by=%s, stop_reason=%s
                    WHERE id=%s RETURNING *""",
                (actor, reason, group_id),
            ).fetchone()
            insert_event(
                conn, schema, event_type="execution_group.stopped", actor=actor,
                payload=event_payload_with_provenance({"group_id": str(group_id), "reason": reason}, provenance),
            )
        return _group_projection(conn, schema, dict(row))


def action_events(conn, schema: str, action_id: int) -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT * FROM {qname(schema, "events")}
        WHERE action_id = %s
        ORDER BY timestamp ASC, id ASC
        """,
        (action_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _validate_immutable_claim(conn, schema: str, action_id: int) -> dict[str, Any] | None:
    """Return an immutable runtime grant only after byte-level revalidation."""
    request_row = conn.execute(
        f"SELECT * FROM {qname(schema, 'immutable_action_requests')} WHERE action_id=%s",
        (action_id,),
    ).fetchone()
    if request_row is None:
        return None
    action_row = conn.execute(
        f"SELECT action_type, target_ref, source_refs FROM {qname(schema, 'actions')} WHERE id=%s",
        (action_id,),
    ).fetchone()
    if action_row is None:
        raise ActionQError("immutable action is missing its ordinary action row")
    spec_row = conn.execute(
        f"SELECT * FROM {qname(schema, 'immutable_action_specs')} WHERE spec_ref=%s",
        (request_row["spec_ref"],),
    ).fetchone()
    if spec_row is None:
        raise ActionQError("immutable action request references a missing spec")
    try:
        request_value = json.loads(bytes(request_row["request_snapshot"]).decode("utf-8"))
        spec_value = json.loads(bytes(spec_row["spec_snapshot"]).decode("utf-8"))
        request_raw, request_digest = _immutable_snapshot(request_value, expected_contract=ACTION_CREATION_REQUEST_V1)
        expected_contract = _IMMUTABLE_ROLE_CONTRACTS.get(request_value["role"])
        if expected_contract is None:
            raise ActionQError("immutable action role is invalid")
        spec_raw, spec_digest = _immutable_snapshot(spec_value, expected_contract=expected_contract)
    except (UnicodeDecodeError, json.JSONDecodeError, ActionQError) as exc:
        raise ActionQError("immutable action request bytes are invalid") from exc
    if (
        request_raw != bytes(request_row["request_snapshot"])
        or spec_raw != bytes(spec_row["spec_snapshot"])
        or request_digest != request_row["request_digest"]
        or spec_digest != spec_row["spec_digest"]
        or request_value["spec_ref"] != request_row["spec_ref"]
        or request_value["spec_digest"] != request_row["spec_digest"]
        or request_value["input_set_digest"] != request_row["input_set_digest"]
        or "artifact:" + request_digest != request_row["request_ref"]
        or action_row["action_type"] != request_value["role"]
        or action_row["target_ref"] != request_value["subject"]
        or list(action_row["source_refs"]) != immutable_spec_input_refs(spec_value)
        or request_value["input_set_digest"] != immutable_input_set_digest(immutable_spec_input_refs(spec_value))
        or (request_value["role"] == "candidate-integration" and request_value["topology"] != spec_value["topology"])
    ):
        raise ActionQError("immutable action request failed claim-time integrity validation")
    grant = {
        "contract_id": "immutable-action-runtime-grant/v1", "action_id": action_id,
        "request_digest": request_digest, "spec_digest": spec_digest,
        "input_set_digest": request_row["input_set_digest"],
    }
    grant_digest = contract_digest(grant)
    prior = conn.execute(
        f"SELECT * FROM {qname(schema, 'immutable_action_runtime_grants')} WHERE action_id=%s",
        (action_id,),
    ).fetchone()
    if prior is None:
        conn.execute(
            f"""INSERT INTO {qname(schema, 'immutable_action_runtime_grants')}
                (action_id, request_digest, spec_digest, input_set_digest, grant_digest)
                VALUES (%s,%s,%s,%s,%s)""",
            (action_id, request_digest, spec_digest, request_row["input_set_digest"], grant_digest),
        )
    elif (
        prior["request_digest"] != request_digest or prior["spec_digest"] != spec_digest
        or prior["input_set_digest"] != request_row["input_set_digest"] or prior["grant_digest"] != grant_digest
    ):
        raise ActionQError("immutable action runtime grant conflicts with request bytes")
    return {"request_ref": request_row["request_ref"], "request_digest": request_digest,
            "spec_ref": request_row["spec_ref"], "spec_digest": spec_digest,
            "input_set_digest": request_row["input_set_digest"], "grant_digest": grant_digest}


def claim(
    conn,
    schema: str,
    *,
    worker: str,
    timeout_minutes: int,
    provenance: dict[str, Any] | None = None,
) -> dict:
    runner_auth_token = secrets.token_urlsafe(32)
    with conn.transaction():
        candidate_ids = conn.execute(
            f"""
            SELECT a.id
            FROM {qname(schema, 'actions')} a
            LEFT JOIN {qname(schema, 'execution_group_members')} m ON m.action_id = a.id
            LEFT JOIN {qname(schema, 'execution_groups')} g ON g.id = m.group_id
            WHERE a.status = 'pending'
              AND (
                  m.group_id IS NULL
                  OR (
                      g.claim_state = 'active'
                      AND (
                          SELECT count(*)
                          FROM {qname(schema, 'execution_group_members')} capacity_member
                          JOIN {qname(schema, 'actions')} capacity_action
                            ON capacity_action.id = capacity_member.action_id
                          WHERE capacity_member.group_id = m.group_id
                            AND capacity_action.status IN ('claimed', 'cancelling')
                      ) < g.max_parallel
                  )
              )
            ORDER BY a.priority ASC, a.created_at ASC
            """,
        ).fetchall()
        row = None
        selected = None
        selected_envelope = None
        selected_immutable_grant = None

        class SkipClaimCandidate(Exception):
            pass

        for candidate_id in candidate_ids:
            try:
                # A savepoint releases locks for an ineligible candidate. This
                # prevents crossed group locks when one claim scans multiple
                # full/stopped groups while preserving blocking serialization
                # for two claimers admitted to the same group.
                with conn.transaction():
                    candidate = conn.execute(
                        f"""SELECT a.id, m.group_id, m.envelope_digest, m.envelope_snapshot
                            FROM {qname(schema, 'actions')} a
                            LEFT JOIN {qname(schema, 'execution_group_members')} m ON m.action_id = a.id
                            WHERE a.id=%s AND a.status='pending'
                            FOR UPDATE OF a SKIP LOCKED""",
                        (candidate_id["id"],),
                    ).fetchone()
                    if candidate is None:
                        raise SkipClaimCandidate
                    envelope = None
                    if candidate["group_id"] is not None:
                        group = conn.execute(
                            f"SELECT * FROM {qname(schema, 'execution_groups')} WHERE id=%s FOR UPDATE",
                            (candidate["group_id"],),
                        ).fetchone()
                        if group is None or group["claim_state"] != "active":
                            raise SkipClaimCandidate
                        claimed = conn.execute(
                            f"""SELECT count(*) AS count
                                FROM {qname(schema, 'execution_group_members')} m
                                JOIN {qname(schema, 'actions')} a ON a.id=m.action_id
                                WHERE m.group_id=%s AND a.status IN ('claimed', 'cancelling')""",
                            (candidate["group_id"],),
                        ).fetchone()["count"]
                        if int(claimed) >= int(group["max_parallel"]):
                            raise SkipClaimCandidate
                        snapshot = candidate["envelope_snapshot"]
                        if isinstance(snapshot, memoryview):
                            snapshot = snapshot.tobytes()
                        try:
                            envelope = json.loads(bytes(snapshot).decode("utf-8"))
                            contract_id = require_contract_compatible(envelope)
                        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                            raise ActionQError("stored execution group envelope is invalid") from exc
                        if (
                            contract_id != EXECUTION_ENVELOPE_V1
                            or envelope.get("action_id") != int(candidate["id"])
                            or contract_digest(envelope) != candidate["envelope_digest"]
                            or contract_canonical_bytes(envelope) != bytes(snapshot)
                        ):
                            raise ActionQError("stored execution group envelope failed integrity validation")
                    immutable_grant = _validate_immutable_claim(conn, schema, int(candidate["id"]))
                    row = conn.execute(
                        f"""UPDATE {qname(schema, 'actions')}
                            SET status='claimed', claimed_at=now(), claimed_by=%s,
                                claim_deadline=now() + (%s * interval '1 minute'),
                                claim_receipt=%s, runner_auth_digest=%s
                            WHERE id=%s AND status='pending' RETURNING *""",
                        (worker, timeout_minutes, str(uuid.uuid4()), receipt_digest(runner_auth_token), candidate["id"]),
                    ).fetchone()
                    if row is None:
                        raise SkipClaimCandidate
            except SkipClaimCandidate:
                continue
            if row is not None:
                selected = candidate
                selected_envelope = envelope
                selected_immutable_grant = immutable_grant
                break
        if row is None:
            raise NoActionAvailable("no pending actions")
        insert_event(
            conn,
            schema,
            action_id=row["id"],
            event_type="action_claimed",
            actor=worker,
            payload=event_payload_with_provenance(
                {
                    "claimed_by": worker,
                    "claim_deadline": json_default(row["claim_deadline"]),
                    "claim_receipt_digest": receipt_digest(row["claim_receipt"]),
                },
                provenance,
            ),
        )
    result = dict(row)
    result["runner_auth_token"] = runner_auth_token
    if selected is not None and selected["group_id"] is not None:
        result["execution_group_id"] = str(selected["group_id"])
        result["execution_envelope_digest"] = selected["envelope_digest"]
        result["execution_envelope"] = selected_envelope
    if selected is not None and selected_immutable_grant is not None:
        result["immutable_runtime_grant"] = selected_immutable_grant
    return result


def _renewal_rejection_reason(current: dict[str, Any] | None, worker: str, claim_receipt: str) -> str | None:
    if current is None:
        return "action-not-found"
    status = _text(current["status"])
    if status != "claimed":
        return f"not-claimed:{status}"
    if _text(current["claimed_by"]) != worker:
        return "claimed-by-different-worker"
    if current.get("claim_receipt") != claim_receipt:
        return "claim-receipt-mismatch"
    deadline = current.get("claim_deadline")
    if deadline is not None:
        deadline_utc = deadline if deadline.tzinfo else deadline.replace(tzinfo=timezone.utc)
        if deadline_utc <= datetime.now(timezone.utc):
            return "claim-already-expired"
    return None


def renew(
    conn,
    schema: str,
    *,
    action_id: int,
    worker: str,
    timeout_minutes: int,
    claim_receipt: str,
    provenance: dict[str, Any] | None = None,
) -> dict:
    """Renew (extend) an existing claim's deadline as an authority command.

    Per the state-event-command matrix, queue claim and lease renewal are
    authority commands: a stale or invalid renewal must be visibly
    rejected, never silently mutate state and never silently succeed.

    Grants (extends ``claim_deadline`` from now, emits ``claim_renewed``)
    only when the action is currently ``claimed`` by exactly this worker
    and its lease has not already expired. Any other case -- wrong worker,
    expired lease, wrong status, or an unknown action id -- is rejected: a
    durable ``claim_renewal_rejected`` event is appended (the request and
    its rejection are kept as immutable history, matching
    ``adr-outbox-sync-model``) and the action row is left byte-for-byte
    unchanged. Raises ``ClaimRejected`` after that event has committed, so
    the rejection is never lost even though the caller sees an exception.
    """
    rejection_reason: str | None = None
    granted_row: dict[str, Any] | None = None
    with conn.transaction():
        current = conn.execute(
            f'SELECT * FROM {qname(schema, "actions")} WHERE id = %s FOR UPDATE',
            (action_id,),
        ).fetchone()
        rejection_reason = _renewal_rejection_reason(current, worker, claim_receipt)
        if rejection_reason is not None:
            insert_event(
                conn,
                schema,
                action_id=action_id if current is not None else None,
                event_type="claim_renewal_rejected",
                actor=worker,
                payload=event_payload_with_provenance(
                    {
                        "requested_by": worker,
                        "requested_action_id": action_id,
                        "requested_timeout_minutes": timeout_minutes,
                        "requested_claim_receipt_digest": receipt_digest(claim_receipt),
                        "action_status": _text(current["status"])
                        if current is not None
                        else None,
                        "current_claimed_by": _text(current["claimed_by"])
                        if current is not None
                        else None,
                        "current_claim_deadline": json_default(
                            current["claim_deadline"]
                        )
                        if current is not None
                        and current.get("claim_deadline") is not None
                        else None,
                        "reason": rejection_reason,
                    },
                    provenance,
                ),
            )
        else:
            previous_deadline = current["claim_deadline"]
            row = conn.execute(
                f"""
                UPDATE {qname(schema, "actions")}
                SET claim_deadline = now() + (%s * interval '1 minute')
                WHERE id = %s
                RETURNING *
                """,
                (timeout_minutes, action_id),
            ).fetchone()
            insert_event(
                conn,
                schema,
                action_id=action_id,
                event_type="claim_renewed",
                actor=worker,
                payload=event_payload_with_provenance(
                    {
                        "renewed_by": worker,
                        "previous_deadline": json_default(previous_deadline)
                        if previous_deadline
                        else None,
                        "new_deadline": json_default(row["claim_deadline"]),
                        "requested_timeout_minutes": timeout_minutes,
                        "claim_receipt_digest": receipt_digest(claim_receipt),
                    },
                    provenance,
                ),
            )
            granted_row = dict(row)
    if rejection_reason is not None:
        raise ClaimRejected(
            f"claim renewal rejected for action #{action_id}: {rejection_reason}",
            reason=rejection_reason,
            action_id=action_id,
            requested_by=worker,
        )
    assert granted_row is not None
    return granted_row


def _transition_terminal(
    conn,
    schema: str,
    *,
    action_id: int,
    status: str,
    event_type: str,
    actor: str | None,
    worker: str,
    claim_receipt: str,
    result_ref: str | None = None,
    failure_reason: str | None = None,
    payload: dict[str, Any] | None = None,
    allowed_statuses: tuple[str, ...] = ("claimed",),
    provenance: dict[str, Any] | None = None,
) -> dict:
    allowed_sql = ", ".join(f"'{status}'" for status in allowed_statuses)
    with conn.transaction():
        row = conn.execute(
            f"""
            UPDATE {qname(schema, "actions")}
            SET status = %s,
                completed_at = now(),
                result_ref = COALESCE(%s, result_ref),
                failure_reason = COALESCE(%s, failure_reason),
                claimed_at = NULL, claimed_by = NULL, claim_deadline = NULL,
                claim_receipt = NULL,
                runner_auth_digest = NULL
            WHERE id = %s
              AND status IN ({allowed_sql})
              AND claimed_by = %s
              AND claim_receipt = %s
              AND claim_deadline > now()
            RETURNING *
            """,
            (status, result_ref, failure_reason, action_id, worker, claim_receipt),
        ).fetchone()
        if row is None:
            raise ActionQError(f"Action #{action_id} cannot transition to {status}")
        insert_event(
            conn,
            schema,
            action_id=action_id,
            event_type=event_type,
            actor=actor,
            payload=event_payload_with_provenance(payload, provenance),
        )
    return dict(row)


def complete(
    conn,
    schema: str,
    action_id: int,
    result_ref: str,
    actor: str | None = None,
    *,
    worker: str,
    claim_receipt: str,
    provenance: dict[str, Any] | None = None,
) -> dict:
    return _transition_terminal(
        conn,
        schema,
        action_id=action_id,
        status="completed",
        event_type="action_completed",
        actor=actor,
        worker=worker,
        claim_receipt=claim_receipt,
        result_ref=result_ref,
        payload={"result_ref": result_ref},
        allowed_statuses=("claimed",),
        provenance=provenance,
    )


def fail(
    conn,
    schema: str,
    action_id: int,
    reason: str,
    actor: str | None = None,
    *,
    worker: str,
    claim_receipt: str,
    provenance: dict[str, Any] | None = None,
) -> dict:
    return _transition_terminal(
        conn,
        schema,
        action_id=action_id,
        status="failed",
        event_type="action_failed",
        actor=actor,
        worker=worker,
        claim_receipt=claim_receipt,
        failure_reason=reason,
        payload={"failure_reason": reason},
        allowed_statuses=("claimed",),
        provenance=provenance,
    )


def reject(
    conn,
    schema: str,
    action_id: int,
    *,
    reason: str,
    validator: str,
    actor: str | None = None,
    provenance: dict[str, Any] | None = None,
    worker: str,
    claim_receipt: str,
) -> dict:
    return _transition_terminal(
        conn,
        schema,
        action_id=action_id,
        status="rejected",
        event_type="action_rejected",
        actor=actor,
        worker=worker,
        claim_receipt=claim_receipt,
        failure_reason=reason,
        payload={"rejection_reason": reason, "validator": validator},
        allowed_statuses=("claimed",),
        provenance=provenance,
    )


def cancel(
    conn,
    schema: str,
    action_id: int,
    reason: str,
    actor: str | None = "human",
    *,
    provenance: dict[str, Any] | None = None,
) -> dict:
    with conn.transaction():
        row = conn.execute(
            f"SELECT * FROM {qname(schema, 'actions')} WHERE id = %s FOR UPDATE", (action_id,)
        ).fetchone()
        if row is None or row["status"] not in ("pending", "claimed"):
            raise ActionQError(f"Action #{action_id} cannot be cancelled")
        if row["status"] == "pending":
            updated = conn.execute(
                f"UPDATE {qname(schema, 'actions')} SET status='cancelled', completed_at=now(), failure_reason=%s WHERE id=%s RETURNING *",
                (reason, action_id),
            ).fetchone()
            event_type = "action_cancelled"
            payload = {"reason": reason, "cancelled_by": actor, "stop_acknowledged": True}
        else:
            request_id = str(uuid.uuid4())
            updated = conn.execute(
                f"""UPDATE {qname(schema, 'actions')} SET status='cancelling', cancel_request_id=%s,
                    cancel_stop_deadline=now() + interval '30 seconds', stop_acknowledged=false,
                    cancel_former_claimed_by=%s, cancel_former_receipt_digest=%s,
                    cancel_runner_auth_digest=runner_auth_digest,
                    claim_receipt=NULL, claim_deadline=NULL, runner_auth_digest=NULL
                    WHERE id=%s RETURNING *""",
                (request_id, row["claimed_by"], receipt_digest(row["claim_receipt"] or ""), action_id),
            ).fetchone()
            event_type = "action_cancelling"
            receipt = row["claim_receipt"] or ""
            payload = {"reason": reason, "cancelled_by": actor, "cancel_request_id": request_id,
                       "former_claimed_by": row["claimed_by"],
                       "former_receipt_digest": receipt_digest(receipt)}
        insert_event(conn, schema, action_id=action_id, event_type=event_type, actor=actor,
                     payload=event_payload_with_provenance(payload, provenance))
    return dict(updated)


def acknowledge_cancellation(
    conn, schema: str, action_id: int, *, cancel_request_id: str,
    former_claim_receipt: str, runner_auth_token: str, authenticated_runner: str,
) -> dict:
    """Idempotently finalize a fenced cancellation after supervisor stop confirmation."""
    with conn.transaction():
        proof = receipt_digest(former_claim_receipt)
        auth_proof = receipt_digest(runner_auth_token)
        row = conn.execute(
            f"""SELECT * FROM {qname(schema, 'actions')} WHERE id=%s FOR UPDATE""", (action_id,)
        ).fetchone()
        if (row is not None and row["status"] == "cancelled"
                and str(row["cancel_request_id"]) == cancel_request_id
                and row["cancel_former_receipt_digest"] == proof
                and row["cancel_former_claimed_by"] == authenticated_runner
                and row["cancel_runner_auth_digest"] == auth_proof):
            return dict(row)
        if (row is None or row["status"] != "cancelling"
                or str(row["cancel_request_id"]) != cancel_request_id
                or row["cancel_former_receipt_digest"] != proof
                or row["cancel_former_claimed_by"] != authenticated_runner
                or row["cancel_runner_auth_digest"] != auth_proof):
            raise ActionQError(f"Action #{action_id} cancellation acknowledgement rejected")
        row = conn.execute(
            f"""UPDATE {qname(schema, 'actions')} SET status='cancelled', completed_at=now(),
                stop_acknowledged=true, claimed_at=NULL, claimed_by=NULL,
                cancel_terminal_kind='stop-acknowledged',
                claim_deadline=NULL, claim_receipt=NULL
                WHERE id=%s RETURNING *""", (action_id,)
        ).fetchone()
        insert_event(conn, schema, action_id=action_id, event_type="action_cancelled",
                     actor=row["cancel_former_claimed_by"],
                     payload={"cancel_request_id": cancel_request_id, "stop_acknowledged": True})
    return dict(row)


def reap_cancellations(conn, schema: str, *, actor: str = "actionctl:cancel-reaper") -> list[dict]:
    with conn.transaction():
        rows = conn.execute(
            f"""UPDATE {qname(schema, 'actions')} SET status='cancelled', completed_at=now(),
                stop_acknowledged=false, claimed_at=NULL, claimed_by=NULL,
                cancel_terminal_kind='stop-unacknowledged-timeout',
                claim_deadline=NULL, claim_receipt=NULL
                WHERE status='cancelling' AND cancel_stop_deadline < now()
                RETURNING *"""
        ).fetchall()
        for row in rows:
            insert_event(conn, schema, action_id=row["id"], event_type="action_cancel_reaped", actor=actor,
                         payload={"cancel_request_id": str(row["cancel_request_id"]),
                                  "stop_acknowledged": False, "process_state": "unknown",
                                  "observation": "deadline-expired-without-ack"})
    return [dict(row) for row in rows]


def sweep(
    conn,
    schema: str,
    *,
    actor: str = "actionctl:sweep",
    provenance: dict[str, Any] | None = None,
) -> list[dict]:
    with conn.transaction():
        rows = conn.execute(
            f"""
            SELECT * FROM {qname(schema, "actions")}
            WHERE status = 'claimed' AND claim_deadline < now()
            ORDER BY claim_deadline ASC
            FOR UPDATE
            """
        ).fetchall()
        swept: list[dict] = []
        for row in rows:
            updated = conn.execute(
                f"""
                UPDATE {qname(schema, "actions")}
                SET status = 'pending',
                    claimed_at = NULL,
                    claimed_by = NULL,
                    claim_deadline = NULL
                    , claim_receipt = NULL
                    , runner_auth_digest = NULL
                WHERE id = %s
                RETURNING *
                """,
                (row["id"],),
            ).fetchone()
            insert_event(
                conn,
                schema,
                action_id=row["id"],
                event_type="claim_timed_out",
                actor=actor,
                payload=event_payload_with_provenance(
                    {
                        "previous_claimed_by": row["claimed_by"],
                        "timeout_seconds": None,
                    },
                    provenance,
                ),
            )
            swept.append(dict(updated))
    return swept


def list_events(
    conn,
    schema: str,
    *,
    since: str | None = None,
    event_type: str | None = None,
    action_id: int | None = None,
    limit: int = 100,
) -> list[dict]:
    clauses = []
    params: list[Any] = []
    if since:
        clauses.append("timestamp >= %s")
        params.append(since)
    if event_type:
        clauses.append("event_type = %s")
        params.append(event_type)
    if action_id is not None:
        clauses.append("action_id = %s")
        params.append(action_id)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    rows = conn.execute(
        f"""
        SELECT * FROM {qname(schema, "events")}
        {where}
        ORDER BY timestamp ASC, id ASC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def follow_events(conn, schema: str, *, event_type: str | None, action_id: int | None):
    last_id = 0
    while True:
        clauses = ["id > %s"]
        params: list[Any] = [last_id]
        if event_type:
            clauses.append("event_type = %s")
            params.append(event_type)
        if action_id is not None:
            clauses.append("action_id = %s")
            params.append(action_id)
        rows = conn.execute(
            f"""
            SELECT * FROM {qname(schema, "events")}
            WHERE {" AND ".join(clauses)}
            ORDER BY id ASC
            LIMIT 100
            """,
            params,
        ).fetchall()
        for row in rows:
            last_id = row["id"]
            yield dict(row)
        time.sleep(1)


def list_sessions(
    conn,
    schema: str,
    *,
    project: str | None = None,
    active_only: bool = False,
    limit: int = 100,
) -> list[dict]:
    clauses = [f"event_type = ANY(%s)"]
    params: list[Any] = [list(SESSION_EVENT_TYPES)]
    if project:
        clauses.append("payload->>'project' = %s")
        params.append(project)

    rows = conn.execute(
        f"""
        SELECT *
        FROM {qname(schema, "events")}
        WHERE {" AND ".join(clauses)}
        ORDER BY timestamp DESC, id DESC
        LIMIT %s
        """,
        (*params, max(limit * 20, limit)),
    ).fetchall()
    return summarize_sessions([dict(row) for row in rows], active_only=active_only, limit=limit)
