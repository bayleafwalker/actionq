"""Adapter-safe application core for Actionq.

Database primitives remain in :mod:`actionq.db`.  This module is the public
application boundary used by the legacy CLI/server and by served adapters.  A
served mutation can carry invocation provenance; those calls receive a durable
decision reference and transactionally serialized idempotent retries.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
from typing import Any, Callable, Iterator

from . import db


_KIND_TO_ACTION_TYPE = {
    "implement": "scope-iterate",
    "review": "scope-iterate",
    "test": "scope-iterate",
    "investigate": "scope-iterate",
    "document": "scope-iterate",
    "custom": "scope-iterate",
}
_INVOCATION_EVENT_TYPES = (
    "invocation.requested",
    "invocation.decided",
    "invocation.replayed",
)


@dataclass(frozen=True)
class InvocationProvenance:
    actor: str
    environment: str
    request_id: str
    catalog_revision: str
    idempotency_key: str
    basis_revision: str | None = None

    def as_event_payload(self, *, operation: str) -> dict[str, Any]:
        return {
            "actor": self.actor,
            "environment": self.environment,
            "request_id": self.request_id,
            "catalog_revision": self.catalog_revision,
            "basis_revision": self.basis_revision,
            "idempotency_key": self.idempotency_key,
            "operation": operation,
        }


def _json_value(value: Any) -> Any:
    return json.loads(db.to_json(value))


def _fingerprint(arguments: dict[str, Any]) -> str:
    encoded = json.dumps(
        _json_value(arguments), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _decision_ref(event_id: int) -> str:
    return f"actionq:event:{event_id}"


class ActionQApplication:
    """One application boundary over a compatible Actionq runtime role."""

    def __init__(
        self,
        *,
        schema: str | None = None,
        connection_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.schema = db.schema_name(schema)
        self._connection_factory = connection_factory

    def _open(self):
        return self._connection_factory() if self._connection_factory else db.connect()

    @contextmanager
    def connection(self) -> Iterator[Any]:
        conn = self._open()
        with conn:
            db.require_compatible(conn, self.schema)
            conn.rollback()
            yield conn

    def compatibility(self) -> dict[str, Any]:
        conn = self._open()
        try:
            return db.check_compatibility(conn, self.schema).as_dict()
        finally:
            conn.close()

    def _read(self, reader: Callable[[Any], Any]) -> Any:
        with self.connection() as conn:
            return reader(conn)

    def _prior_decision(
        self,
        conn,
        *,
        operation: str,
        provenance: InvocationProvenance,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            f"""
            SELECT *
            FROM {db.qname(self.schema, "events")}
            WHERE event_type = 'invocation.decided'
              AND payload->>'operation' = %s
              AND payload->>'idempotency_owner' = 'true'
              AND payload->'provenance'->>'actor' = %s
              AND payload->'provenance'->>'environment' = %s
              AND payload->'provenance'->>'idempotency_key' = %s
            ORDER BY id ASC
            LIMIT 1
            """,
            (
                operation,
                provenance.actor,
                provenance.environment,
                provenance.idempotency_key,
            ),
        ).fetchone()
        return dict(row) if row else None

    def _lifecycle_event_refs(
        self,
        conn,
        *,
        operation: str,
        provenance: InvocationProvenance,
    ) -> list[str]:
        rows = conn.execute(
            f"""
            SELECT id
            FROM {db.qname(self.schema, "events")}
            WHERE event_type <> ALL(%s)
              AND payload->'provenance'->>'actor' = %s
              AND payload->'provenance'->>'environment' = %s
              AND payload->'provenance'->>'request_id' = %s
              AND payload->'provenance'->>'idempotency_key' = %s
              AND payload->'provenance'->>'operation' = %s
            ORDER BY id ASC
            """,
            (
                list(_INVOCATION_EVENT_TYPES),
                provenance.actor,
                provenance.environment,
                provenance.request_id,
                provenance.idempotency_key,
                operation,
            ),
        ).fetchall()
        return [_decision_ref(int(row["id"])) for row in rows]

    def _decision_result(
        self,
        *,
        event: dict[str, Any],
        replayed: bool,
    ) -> dict[str, Any]:
        payload = db._event_payload(event)
        provenance = payload["provenance"]
        return {
            "decision": {
                "decision_ref": _decision_ref(int(event["id"])),
                "operation": payload["operation"],
                "request_id": provenance["request_id"],
                "status": payload["status"],
                "code": payload.get("code"),
                "message": payload.get("message"),
                "event_refs": list(payload.get("event_refs") or ()),
                "replayed": replayed,
            },
            "result": payload.get("result"),
        }

    def _idempotency_conflict(
        self,
        conn,
        *,
        operation: str,
        provenance: InvocationProvenance,
        fingerprint: str,
        prior: dict[str, Any],
    ) -> dict[str, Any]:
        prior_payload = db._event_payload(prior)
        event = db.insert_event(
            conn,
            self.schema,
            event_type="invocation.decided",
            actor=provenance.actor,
            payload={
                "operation": operation,
                "status": "rejected",
                "code": "idempotency-key-conflict",
                "message": "idempotency key was already used with different arguments",
                "event_refs": [_decision_ref(int(prior["id"]))],
                "result": None,
                "request_fingerprint": fingerprint,
                "idempotency_owner": False,
                "provenance": provenance.as_event_payload(operation=operation),
                "original_request_id": prior_payload["provenance"]["request_id"],
            },
        )
        return self._decision_result(event=event, replayed=False)

    def _mutate(
        self,
        *,
        operation: str,
        arguments: dict[str, Any],
        provenance: InvocationProvenance | None,
        mutation: Callable[[Any, dict[str, Any] | None], Any],
    ) -> Any:
        if provenance is None:
            with self.connection() as conn:
                return mutation(conn, None)
        if not provenance.idempotency_key:
            raise db.ActionQError("served mutations require an idempotency key")

        fingerprint = _fingerprint(
            {
                "arguments": arguments,
                "basis_revision": provenance.basis_revision,
            }
        )
        with self.connection() as conn:
            with conn.transaction():
                lock_key = "\x1f".join(
                    (
                        operation,
                        provenance.actor,
                        provenance.environment,
                        provenance.idempotency_key,
                    )
                )
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (lock_key,),
                )
                prior = self._prior_decision(
                    conn, operation=operation, provenance=provenance
                )
                if prior is not None:
                    prior_payload = db._event_payload(prior)
                    if prior_payload.get("request_fingerprint") != fingerprint:
                        return self._idempotency_conflict(
                            conn,
                            operation=operation,
                            provenance=provenance,
                            fingerprint=fingerprint,
                            prior=prior,
                        )
                    db.insert_event(
                        conn,
                        self.schema,
                        event_type="invocation.replayed",
                        action_id=prior.get("action_id"),
                        actor=provenance.actor,
                        payload={
                            "operation": operation,
                            "decision_ref": _decision_ref(int(prior["id"])),
                            "provenance": provenance.as_event_payload(
                                operation=operation
                            ),
                        },
                    )
                    return self._decision_result(event=prior, replayed=True)

                request_event = db.insert_event(
                    conn,
                    self.schema,
                    event_type="invocation.requested",
                    actor=provenance.actor,
                    payload={
                        "operation": operation,
                        "request_fingerprint": fingerprint,
                        "provenance": provenance.as_event_payload(operation=operation),
                    },
                )
                action_id: int | None = None
                status = "accepted"
                code: str | None = None
                message: str | None = None
                result: Any = None
                event_provenance = provenance.as_event_payload(operation=operation)
                try:
                    result = _json_value(mutation(conn, event_provenance))
                    if isinstance(result, dict):
                        if operation == "execution.session.record":
                            recorded_action_id = result.get("action_id")
                            if isinstance(recorded_action_id, int):
                                action_id = recorded_action_id
                        elif operation.startswith("execution.action.") or operation == (
                            "execution.dispatch.enqueue"
                        ):
                            result_id = result.get("id")
                            if isinstance(result_id, int):
                                action_id = result_id
                except db.ClaimRejected as error:
                    status = "rejected"
                    code = "claim-rejected"
                    message = error.reason
                    action_id = error.action_id
                except db.NoActionAvailable as error:
                    status = "rejected"
                    code = "no-action-available"
                    message = str(error)
                except db.ActionQError as error:
                    status = "rejected"
                    code = "actionq-rejected"
                    message = str(error)

                event_refs = [_decision_ref(int(request_event["id"]))]
                event_refs.extend(
                    self._lifecycle_event_refs(
                        conn, operation=operation, provenance=provenance
                    )
                )
                decision = db.insert_event(
                    conn,
                    self.schema,
                    event_type="invocation.decided",
                    action_id=action_id,
                    actor=provenance.actor,
                    payload={
                        "operation": operation,
                        "status": status,
                        "code": code,
                        "message": message,
                        "event_refs": event_refs,
                        "result": result,
                        "request_fingerprint": fingerprint,
                        "idempotency_owner": True,
                        "provenance": event_provenance,
                    },
                )
                return self._decision_result(event=decision, replayed=False)

    def enqueue(
        self,
        *,
        action_type: str,
        project: str | None,
        target_ref: str | None,
        source_refs: list[str],
        priority: int,
        parent_id: int | None,
        created_by: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        arguments = {
            "action_type": action_type,
            "project": project,
            "target_ref": target_ref,
            "source_refs": source_refs,
            "priority": priority,
            "parent_id": parent_id,
            "created_by": created_by,
        }
        return self._mutate(
            operation="execution.action.enqueue",
            arguments=arguments,
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.enqueue(
                conn,
                self.schema,
                action_type=action_type,
                project=project,
                target_ref=target_ref,
                source_refs=source_refs,
                priority=priority,
                parent_id=parent_id,
                created_by=created_by,
                provenance=event_provenance,
            ),
        )

    def list_actions(self, **filters: Any) -> list[dict[str, Any]]:
        return self._read(lambda conn: db.list_actions(conn, self.schema, **filters))

    def show_action(self, action_id: int) -> dict[str, Any] | None:
        def read(conn):
            action = db.get_action(conn, self.schema, action_id)
            if action is None:
                return None
            return {
                "action": action,
                "events": db.action_events(conn, self.schema, action_id),
            }

        return self._read(read)

    def claim(
        self,
        *,
        worker: str,
        timeout_minutes: int,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        return self._mutate(
            operation="execution.action.claim",
            arguments={"worker": worker, "timeout_minutes": timeout_minutes},
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.claim(
                conn,
                self.schema,
                worker=worker,
                timeout_minutes=timeout_minutes,
                provenance=event_provenance,
            ),
        )

    def renew(
        self,
        *,
        action_id: int,
        worker: str,
        timeout_minutes: int,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        return self._mutate(
            operation="execution.action.renew",
            arguments={
                "action_id": action_id,
                "worker": worker,
                "timeout_minutes": timeout_minutes,
            },
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.renew(
                conn,
                self.schema,
                action_id=action_id,
                worker=worker,
                timeout_minutes=timeout_minutes,
                provenance=event_provenance,
            ),
        )

    def _terminal(
        self,
        *,
        operation: str,
        action_id: int,
        actor: str,
        arguments: dict[str, Any],
        provenance: InvocationProvenance | None,
        transition: Callable[[Any, dict[str, Any] | None], dict[str, Any]],
    ) -> Any:
        return self._mutate(
            operation=operation,
            arguments={"action_id": action_id, "actor": actor, **arguments},
            provenance=provenance,
            mutation=transition,
        )

    def complete(
        self,
        *,
        action_id: int,
        result_ref: str,
        actor: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        return self._terminal(
            operation="execution.action.complete",
            action_id=action_id,
            actor=actor,
            arguments={"result_ref": result_ref},
            provenance=provenance,
            transition=lambda conn, event_provenance: db.complete(
                conn,
                self.schema,
                action_id,
                result_ref,
                actor=actor,
                provenance=event_provenance,
            ),
        )

    def fail(
        self,
        *,
        action_id: int,
        reason: str,
        actor: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        return self._terminal(
            operation="execution.action.fail",
            action_id=action_id,
            actor=actor,
            arguments={"reason": reason},
            provenance=provenance,
            transition=lambda conn, event_provenance: db.fail(
                conn,
                self.schema,
                action_id,
                reason,
                actor=actor,
                provenance=event_provenance,
            ),
        )

    def reject(
        self,
        *,
        action_id: int,
        reason: str,
        validator: str,
        actor: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        return self._terminal(
            operation="execution.action.reject",
            action_id=action_id,
            actor=actor,
            arguments={"reason": reason, "validator": validator},
            provenance=provenance,
            transition=lambda conn, event_provenance: db.reject(
                conn,
                self.schema,
                action_id,
                reason=reason,
                validator=validator,
                actor=actor,
                provenance=event_provenance,
            ),
        )

    def cancel(
        self,
        *,
        action_id: int,
        reason: str,
        actor: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        return self._terminal(
            operation="execution.action.cancel",
            action_id=action_id,
            actor=actor,
            arguments={"reason": reason},
            provenance=provenance,
            transition=lambda conn, event_provenance: db.cancel(
                conn,
                self.schema,
                action_id,
                reason,
                actor=actor,
                provenance=event_provenance,
            ),
        )

    def sweep(
        self,
        *,
        actor: str = "actionctl:sweep",
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        return self._mutate(
            operation="execution.action.sweep",
            arguments={"actor": actor},
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.sweep(
                conn,
                self.schema,
                actor=actor,
                provenance=event_provenance,
            ),
        )

    def list_events(self, **filters: Any) -> list[dict[str, Any]]:
        return self._read(lambda conn: db.list_events(conn, self.schema, **filters))

    def emit_event(
        self,
        *,
        event_type: str,
        action_id: int | None,
        payload: dict[str, Any],
        actor: str | None,
    ) -> dict[str, Any]:
        with self.connection() as conn:
            return db.insert_event(
                conn,
                self.schema,
                event_type=event_type,
                action_id=action_id,
                actor=actor,
                payload=payload,
            )

    def list_sessions(self, **filters: Any) -> list[dict[str, Any]]:
        return self._read(lambda conn: db.list_sessions(conn, self.schema, **filters))

    def record_session(
        self,
        *,
        event_type: str,
        action_id: int | None,
        payload: dict[str, Any],
        actor: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        if event_type not in db.SESSION_EVENT_TYPES:
            raise db.ActionQError(f"unsupported session event type: {event_type}")
        return self._mutate(
            operation="execution.session.record",
            arguments={
                "event_type": event_type,
                "action_id": action_id,
                "payload": payload,
                "actor": actor,
            },
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.insert_event(
                conn,
                self.schema,
                event_type=event_type,
                action_id=action_id,
                actor=actor,
                payload=db.event_payload_with_provenance(payload, event_provenance),
            ),
        )

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


__all__ = ["ActionQApplication", "InvocationProvenance"]
