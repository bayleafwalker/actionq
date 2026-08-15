"""Dispatch v1/v2 and action-resource observation service."""

from __future__ import annotations

from typing import Mapping

from .application_core import *
from .application_core import (
    _DISPATCH_V2_FIELDS, _HARNESSES, _KIND_TO_ACTION_TYPE, _OUTPUT_EXPECTATIONS,
    _PRIORITIES,
    CANONICALIZATION_VERSION,
)
from .managed_dispatch import admit_managed_enqueue


class DispatchService:
    def list_dispatches(self, **filters: Any) -> list[dict[str, Any]]:
        return self._read(lambda conn: db.list_dispatches(conn, self.schema, **filters))

    def dispatch(
        self,
        payload: dict[str, Any],
        *,
        actor: str | None = None,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        contract = payload.get("contract_version")
        if contract != "v1":
            raise db.ActionQError(
                f"unsupported contract_version: {contract!r}; expected 'v1'"
            )
        repo_id = str(payload.get("repo_id") or "").strip()
        if not repo_id or repo_id == "ALL":
            raise db.ActionQError("repo_id must name one concrete repo")
        kind = str(payload.get("kind") or "").strip()
        action_type = _KIND_TO_ACTION_TYPE.get(kind)
        if not action_type:
            raise db.ActionQError(
                f"kind must be one of: {', '.join(_KIND_TO_ACTION_TYPE)}"
            )
        title = str(payload.get("title") or "").strip()
        if not title:
            raise db.ActionQError("title is required")
        priority_label = str(payload.get("priority") or "normal").strip()
        priority = 50 if priority_label == "high" else 100
        source_refs = list(payload.get("refs") or ())
        target_ref = str(payload.get("work_item_id") or "").strip() or None
        created_by = (
            actor
            or str(payload.get("requested_by") or "operator:cockpit").strip()
            or "operator:cockpit"
        )
        metadata = {
            "title": title,
            "kind": kind,
            "output_expectation": str(payload.get("output_expectation") or "").strip()
            or None,
            "harness": str(payload.get("harness") or "").strip() or None,
            "model": str(payload.get("model") or "").strip() or None,
            "prompt": str(payload.get("prompt") or "").strip() or None,
            "sprint_id": payload.get("sprint_id"),
            "dispatch_group_id": str(payload.get("dispatch_group_id") or "").strip()
            or None,
        }

        def mutate(conn, event_provenance):
            action = db.enqueue(
                conn,
                self.schema,
                action_type=action_type,
                project=repo_id,
                target_ref=target_ref,
                source_refs=source_refs,
                priority=priority,
                parent_id=None,
                created_by=created_by,
                provenance=event_provenance,
            )
            db.insert_event(
                conn,
                self.schema,
                action_id=action["id"],
                event_type="dispatch.requested",
                actor=created_by,
                payload=db.event_payload_with_provenance(metadata, event_provenance),
            )
            return action

        return self._mutate(
            operation="execution.dispatch.enqueue",
            arguments={"payload": payload, "actor": created_by},
            provenance=provenance,
            mutation=mutate,
        )

    @staticmethod
    def _normalize_dispatch_v2(payload: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
        if set(payload) != set(_DISPATCH_V2_FIELDS):
            missing = sorted(set(_DISPATCH_V2_FIELDS) - set(payload))
            unknown = sorted(set(payload) - set(_DISPATCH_V2_FIELDS))
            raise db.ActionQError(f"v2 request fields must be exact; missing={missing}, unknown={unknown}")
        value = dict(payload)
        if value["contract_version"] != "v2" or value["action_type"] != "scope-iterate":
            raise db.ActionQError("v2 contract_version must be 'v2' and action_type must be 'scope-iterate'")
        if value["output_expectation"] not in _OUTPUT_EXPECTATIONS or value["harness"] not in _HARNESSES or value["priority"] not in _PRIORITIES:
            raise db.ActionQError("v2 request contains an unsupported enum value")
        for name in ("repo_id", "title", "requested_by"):
            if not isinstance(value[name], str) or not value[name] or (name == "repo_id" and value[name] == "ALL"):
                raise db.ActionQError(f"v2 {name} must be a non-empty concrete string")
        if not isinstance(value["prompt"], str) or not isinstance(value["refs"], list) or not all(isinstance(ref, str) and ref for ref in value["refs"]):
            raise db.ActionQError("v2 prompt must be string and refs must be strings")
        for name in ("sprint_id",):
            if value[name] is not None and (not isinstance(value[name], int) or isinstance(value[name], bool) or value[name] < 1):
                raise db.ActionQError(f"v2 {name} must be null or a positive integer")
        for name in ("work_item_id", "model", "dispatch_group_id"):
            if value[name] is not None and (not isinstance(value[name], str) or not value[name]):
                raise db.ActionQError(f"v2 {name} must be null or a non-empty string")
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        return value, raw

    def enqueue_dispatch_v2(self, payload: dict[str, Any], *, provenance: InvocationProvenance) -> dict[str, Any]:
        """Atomically bind one immutable v2 snapshot to one pending Action root."""
        if not provenance.idempotency_key:
            raise db.ActionQError("served dispatch enqueue requires an idempotency key")
        normalized, raw = self._normalize_dispatch_v2(payload)
        if normalized["requested_by"] != provenance.actor:
            raise db.ActionQError("v2 requested_by must equal the authenticated actor")
        self._authorize(provenance, f"execution.dispatch.repo:{normalized['repo_id']}", "enqueue")
        digest = hashlib.sha256(raw).hexdigest()
        with self.connection() as conn:
            with conn.transaction():
                lock = "\x1f".join(("execution.dispatch.enqueue", provenance.actor, provenance.environment, provenance.idempotency_key))
                conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock,))
                prior = conn.execute(
                    f"SELECT action_id, request_ref, request_sha256, normalized_snapshot FROM {db.qname(self.schema, 'dispatch_requests')} WHERE identity=%s AND environment=%s AND operation=%s AND idempotency_key=%s",
                    (provenance.actor, provenance.environment, "execution.dispatch.enqueue", provenance.idempotency_key),
                ).fetchone()
                if prior:
                    if bytes(prior["normalized_snapshot"]) != raw:
                        raise db.ActionQError("idempotency-key-conflict")
                    return {"action_id": prior["action_id"], "status": "pending", "request_ref": prior["request_ref"], "request_sha256": prior["request_sha256"]}
                created_by = provenance.actor
                action = db.enqueue(conn, self.schema, action_type="scope-iterate", project=normalized["repo_id"], target_ref=normalized["work_item_id"], source_refs=normalized["refs"], priority=50 if normalized["priority"] == "high" else 100, parent_id=None, created_by=created_by, provenance=provenance.as_event_payload(operation="execution.dispatch.enqueue"))
                request_ref = "req:" + uuid.uuid4().hex
                conn.execute(
                    f"INSERT INTO {db.qname(self.schema, 'dispatch_requests')} (action_id, request_ref, normalized_snapshot, schema_version, canonicalization_version, request_sha256, identity, environment, operation, idempotency_key) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (action["id"], request_ref, raw, "v2", CANONICALIZATION_VERSION, digest, provenance.actor, provenance.environment, "execution.dispatch.enqueue", provenance.idempotency_key),
                )
                event = db.insert_event(conn, self.schema, action_id=action["id"], event_type="dispatch.v2.enqueued", actor=created_by, payload={"request_ref": request_ref, "request_sha256": digest, "schema_version": "v2", "canonicalization_version": CANONICALIZATION_VERSION, "enqueue_decision": "accepted", "provenance": provenance.as_event_payload(operation="execution.dispatch.enqueue")})
                # The observation contract begins at this explicit root event.
                # Do not infer a recovery floor from retained event rows: the
                # older v7 MIN(events.id) fallback was unsafe after pruning.
                conn.execute(
                    f"INSERT INTO {db.qname(self.schema, 'dispatch_observation_watermarks')} (action_id, first_retained_event_id, last_observed_event_id) VALUES (%s, %s, %s)",
                    (action["id"], event["id"], event["id"]),
                )
                return {"action_id": action["id"], "status": "pending", "request_ref": request_ref, "request_sha256": digest}

    def enqueue_managed_dispatch(
        self,
        envelope: dict[str, Any],
        *,
        provenance: InvocationProvenance,
        expected_source_shas: Mapping[str, str],
        registered_authority: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Atomically persist an admitted capsule beside its v2 queue root."""
        if not provenance.idempotency_key:
            raise db.ActionQError("served managed dispatch enqueue requires an idempotency key")
        admitted = admit_managed_enqueue(
            envelope,
            authenticated_actor=provenance.actor,
            expected_source_shas=expected_source_shas,
            registered_authority=registered_authority,
        )
        normalized, raw = self._normalize_dispatch_v2(dict(admitted.queue_payload))
        self._authorize(provenance, f"execution.dispatch.repo:{normalized['repo_id']}", "enqueue")
        digest = hashlib.sha256(raw).hexdigest()
        with self.connection() as conn:
            with conn.transaction():
                lock = "\x1f".join(("execution.dispatch.enqueue", provenance.actor, provenance.environment, provenance.idempotency_key))
                conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock,))
                prior = conn.execute(
                    f"SELECT d.action_id, d.request_ref, d.request_sha256, d.normalized_snapshot, m.envelope_snapshot FROM {db.qname(self.schema, 'dispatch_requests')} d LEFT JOIN {db.qname(self.schema, 'managed_dispatch_envelopes')} m ON m.action_id=d.action_id WHERE d.identity=%s AND d.environment=%s AND d.operation=%s AND d.idempotency_key=%s",
                    (provenance.actor, provenance.environment, "execution.dispatch.enqueue", provenance.idempotency_key),
                ).fetchone()
                if prior:
                    if bytes(prior["normalized_snapshot"]) != raw or prior["envelope_snapshot"] is None or bytes(prior["envelope_snapshot"]) != admitted.normalized_snapshot:
                        raise db.ActionQError("idempotency-key-conflict")
                    return {"action_id": prior["action_id"], "status": "pending", "request_ref": prior["request_ref"], "request_sha256": prior["request_sha256"]}
                action = db.enqueue(conn, self.schema, action_type="scope-iterate", project=normalized["repo_id"], target_ref=normalized["work_item_id"], source_refs=normalized["refs"], priority=50 if normalized["priority"] == "high" else 100, parent_id=None, created_by=provenance.actor, provenance=provenance.as_event_payload(operation="execution.dispatch.enqueue"))
                request_ref = "req:" + uuid.uuid4().hex
                conn.execute(
                    f"INSERT INTO {db.qname(self.schema, 'dispatch_requests')} (action_id, request_ref, normalized_snapshot, schema_version, canonicalization_version, request_sha256, identity, environment, operation, idempotency_key) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (action["id"], request_ref, raw, "v2", CANONICALIZATION_VERSION, digest, provenance.actor, provenance.environment, "execution.dispatch.enqueue", provenance.idempotency_key),
                )
                conn.execute(
                    f"INSERT INTO {db.qname(self.schema, 'managed_dispatch_envelopes')} (action_id, envelope_sha256, envelope_snapshot, capsule_sha256, rendered_prompt_sha256) VALUES (%s,%s,%s,%s,%s)",
                    (action["id"], admitted.request_sha256, admitted.normalized_snapshot, admitted.capsule_sha256, admitted.rendered_prompt_sha256),
                )
                event = db.insert_event(conn, self.schema, action_id=action["id"], event_type="managed-dispatch.admitted", actor=provenance.actor, payload={"request_ref": request_ref, "managed_request_sha256": admitted.request_sha256, "capsule_sha256": admitted.capsule_sha256, "rendered_prompt_sha256": admitted.rendered_prompt_sha256, "provenance": provenance.as_event_payload(operation="execution.dispatch.enqueue")})
                conn.execute(
                    f"INSERT INTO {db.qname(self.schema, 'dispatch_observation_watermarks')} (action_id, first_retained_event_id, last_observed_event_id) VALUES (%s, %s, %s)",
                    (action["id"], event["id"], event["id"]),
                )
                return {"action_id": action["id"], "status": "pending", "request_ref": request_ref, "request_sha256": digest}

    def dispatch_action_snapshot(self, action_id: int, *, provenance: InvocationProvenance) -> dict[str, Any]:
        def read(conn):
            with conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                row = conn.execute(
                    f"SELECT a.id, a.project, a.status, a.action_type, r.request_ref, r.request_sha256, GREATEST(w.last_observed_event_id, COALESCE((SELECT MAX(id) FROM {db.qname(self.schema, 'events')} WHERE action_id=a.id), 0)) AS cursor FROM {db.qname(self.schema, 'actions')} a JOIN {db.qname(self.schema, 'dispatch_requests')} r ON r.action_id=a.id JOIN {db.qname(self.schema, 'dispatch_observation_watermarks')} w ON w.action_id=a.id WHERE a.id=%s",
                    (action_id,),
                ).fetchone()
                if not row:
                    raise db.ActionQError("resource-not-found")
                try:
                    self._authorize(provenance, f"execution.dispatch.repo:{row['project']}", "read")
                except db.ActionQError:
                    raise db.ActionQError("resource-not-found") from None
                return {"action_id": row["id"], "status": row["status"], "action_type": row["action_type"], "request_ref": row["request_ref"], "request_sha256": row["request_sha256"], "cursor": f"actionq:event:{row['cursor']}"}
        return self._read(read)

    def _latest_event_id(self, conn, action_id: int) -> int:
        row = conn.execute(f"SELECT COALESCE(MAX(id), 0) AS id FROM {db.qname(self.schema, 'events')} WHERE action_id=%s", (action_id,)).fetchone()
        return int(row["id"])

    def dispatch_action_changes(self, action_id: int, *, cursor: str | None, wait_seconds: int, provenance: InvocationProvenance) -> dict[str, Any]:
        if wait_seconds < 0 or wait_seconds > 30: raise db.ActionQError("wait_seconds must be between 0 and 30")
        prefix = "actionq:event:"
        after = 0 if cursor is None else int(cursor.removeprefix(prefix)) if cursor.startswith(prefix) and cursor[len(prefix):].isdigit() else (_ for _ in ()).throw(db.ActionQError("resource-not-found"))
        deadline = time.monotonic() + wait_seconds
        while True:
            def read(conn):
                with conn.transaction():
                    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    owner = conn.execute(f"SELECT a.project, w.first_retained_event_id, GREATEST(w.last_observed_event_id, COALESCE((SELECT MAX(id) FROM {db.qname(self.schema, 'events')} WHERE action_id=a.id), 0)) AS cursor FROM {db.qname(self.schema, 'actions')} a JOIN {db.qname(self.schema, 'dispatch_requests')} r ON r.action_id=a.id JOIN {db.qname(self.schema, 'dispatch_observation_watermarks')} w ON w.action_id=a.id WHERE a.id=%s", (action_id,)).fetchone()
                    if not owner:
                        raise db.ActionQError("resource-not-found")
                    try:
                        self._authorize(provenance, f"execution.dispatch.repo:{owner['project']}", "read")
                    except db.ActionQError:
                        raise db.ActionQError("resource-not-found") from None
                    latest = int(owner["cursor"])
                    if cursor is not None and (
                        after < int(owner["first_retained_event_id"]) - 1
                        or after > latest
                    ):
                        return {"status": "cursor_expired", "snapshot_required": True, "events": [], "cursor": f"actionq:event:{latest}"}
                    rows = conn.execute(f"SELECT id, event_type FROM {db.qname(self.schema, 'events')} WHERE action_id=%s AND id>%s ORDER BY id ASC", (action_id, after)).fetchall()
                    terminal = {"action.completed", "action.failed", "action.rejected", "action.cancelled"}
                    events = [{"id": f"actionq:event:{row['id']}", "type": "lifecycle", "terminal": row["event_type"] in terminal, "data": {}} for row in rows]
                    return {"status": "ok", "snapshot_required": False, "events": events, "cursor": f"actionq:event:{latest}"}
            result = self._read(read)
            if result["events"] or wait_seconds == 0 or time.monotonic() >= deadline: return result
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
