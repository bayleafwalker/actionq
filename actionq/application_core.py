"""Core connection, authorization, and idempotent mutation services for ActionQ.

This module owns the shared application mechanics; operation-specific services
are composed by :class:`actionq.application.ActionQApplication`.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Iterator
import time
import uuid

from . import db
from .action_resource import ActionResourceOwner, ResourceNotFound, serialize_envelope
from .runner_auth import VerifiedRunner, verify_runner_proof
from .cas import _DaemonCAS
from .completion_log import CompletionLog


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
_DISPATCH_V2_FIELDS = (
    "contract_version", "action_type", "output_expectation", "repo_id", "sprint_id",
    "work_item_id", "title", "prompt", "harness", "model", "priority", "refs",
    "dispatch_group_id", "requested_by",
)
_OUTPUT_EXPECTATIONS = {"plan", "audit-event", "draft-work-items", "sprint-proposal", "implementation", "review"}
_HARNESSES = {"claude", "codex", "copilot-cli", "codestral"}
_PRIORITIES = {"normal", "high"}
CANONICALIZATION_VERSION = "json-sort-utf8-v1"


@dataclass(frozen=True)
class InvocationProvenance:
    actor: str
    environment: str
    request_id: str
    catalog_revision: str
    idempotency_key: str
    basis_revision: str | None = None
    authorized_repositories: tuple[str, ...] = ()

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


@dataclass(frozen=True)
class AuthenticatedActionResourcePrincipal:
    provenance: InvocationProvenance
    principal_scope: str

    @classmethod
    def derive(cls, provenance: InvocationProvenance) -> "AuthenticatedActionResourcePrincipal":
        if not provenance.actor or not provenance.environment:
            raise db.ActionQError("authenticated action-resource provenance is incomplete")
        material = f"action-resource/v1\x1f{provenance.environment}\x1f{provenance.actor}".encode()
        return cls(provenance, "aqp1_" + hashlib.sha256(material).hexdigest())


ACTION_RESOURCE_NOT_FOUND = (
    404,
    (("content-type", "application/json"), ("content-length", "112"), ("cache-control", "no-store")),
    b'{"error":{"code":"resource_not_found","message":"resource not found"},"schema_version":"resource-reference/v1"}\n',
)


def _json_value(value: Any) -> Any:
    return json.loads(db.to_json(value))


def _fingerprint(arguments: dict[str, Any]) -> str:
    encoded = json.dumps(
        _json_value(arguments), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _decision_ref(event_id: int) -> str:
    return f"actionq:event:{event_id}"



class ActionQCore:
    """One application boundary over a compatible Actionq runtime role."""

    def __init__(
        self,
        *,
        schema: str | None = None,
        connection_factory: Callable[[], Any] | None = None,
        authorizer: Callable[[InvocationProvenance, str, str], bool] | None = None,
        resource_cursor_secret: bytes | None = None,
        artifact_root: Path | str | None = None,
        cas_factory: Callable[[Path], Any] | None = None,
        completion_ingest_connection_factory: Callable[[], Any] | None = None,
        completion_read_connection_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.schema = db.schema_name(schema)
        self._connection_factory = connection_factory
        self._authorizer = authorizer
        self._resource_cursor_secret = resource_cursor_secret
        configured_root = artifact_root if artifact_root is not None else os.environ.get("ACTIONQ_ARTIFACT_ROOT")
        self.artifact_root = Path(configured_root).expanduser() if configured_root else None
        self._cas_factory = cas_factory or _DaemonCAS
        self._completion_ingest_connection_factory = completion_ingest_connection_factory
        self._completion_read_connection_factory = completion_read_connection_factory

    def _verified_result_store(self, result_ref: str) -> Any:
        if self.artifact_root is None:
            raise db.ActionQError("verified settlement requires a configured durable artifact_root")
        try:
            store = self._cas_factory(self.artifact_root)
            store.get(result_ref)
            return store
        except db.ActionQError:
            raise
        except Exception as exc:  # noqa: BLE001 - authority must fail closed
            raise db.ActionQError("dispatch result referent is not durable") from exc

    def _project_owned_action(self, conn: Any, action_id: int, *, claim_receipt: str | None = None) -> None:
        if self._resource_cursor_secret is not None:
            self._action_resource_owner().project_state(
                conn, action_id=action_id, expected_claim_receipt=claim_receipt,
            )

    def _transition_owned_action(self, conn: Any, action_id: int, transition: Callable[[], Any], *, claim_receipt: str | None = None) -> Any:
        result = transition()
        self._project_owned_action(conn, action_id, claim_receipt=claim_receipt)
        return result

    def _action_resource_owner(self) -> ActionResourceOwner:
        if self._resource_cursor_secret is None:
            raise db.ActionQError("action resource cursor secret is not configured")
        return ActionResourceOwner(
            schema=self.schema, connection=self.connection,
            cursor_secret=self._resource_cursor_secret,
        )

    @staticmethod
    def serialize_action_resource_envelope(value: dict[str, Any]) -> bytes:
        return serialize_envelope(value)

    def enqueue_action_resource(
        self, *, principal: AuthenticatedActionResourcePrincipal, operation: str, idempotency_key: str,
        request: dict[str, Any], action_type: str, project: str | None,
        target_ref: str | None, source_refs: list[str], priority: int,
        parent_id: int | None, created_by: str,
    ) -> dict[str, Any]:
        """Create one ActionQ-owned action root; no transport route is implied."""
        self._authorize(principal.provenance, f"execution.action-resource.scope:{principal.principal_scope}", "enqueue")
        owner = self._action_resource_owner()
        result = owner.enqueue(
            principal_scope=principal.principal_scope, operation=operation,
            idempotency_key=idempotency_key, request=request,
            create_action=lambda conn: db.enqueue(
                conn, self.schema, action_type=action_type, project=project,
                target_ref=target_ref, source_refs=source_refs, priority=priority,
                parent_id=parent_id, created_by=created_by,
            ),
        )
        return {
            "schema_version": "resource-reference/v1",
            "resource_ref": result.resource_ref,
            "revision": result.revision,
        }

    def action_resource_snapshot(self, *, resource_ref: str, principal: AuthenticatedActionResourcePrincipal) -> dict[str, Any]:
        self._authorize(principal.provenance, f"execution.action-resource.scope:{principal.principal_scope}", "read")
        return self._action_resource_owner().snapshot(
            resource_ref=resource_ref, principal_scope=principal.principal_scope,
        )

    def action_resource_snapshot_response(self, *, resource_ref: str, principal: AuthenticatedActionResourcePrincipal) -> tuple[int, tuple[tuple[str, str], ...], bytes]:
        try:
            value = self.action_resource_snapshot(resource_ref=resource_ref, principal=principal)
        except (ResourceNotFound, db.ActionQError):
            return ACTION_RESOURCE_NOT_FOUND
        body = serialize_envelope(value)
        return 200, (("content-type", "application/json"), ("content-length", str(len(body))), ("cache-control", "no-store")), body

    def action_resource_changes(
        self, *, resource_ref: str, principal: AuthenticatedActionResourcePrincipal, cursor: str,
        wait_seconds: int = 0,
        cancel_event: Any | None = None,
    ) -> dict[str, Any]:
        self._authorize(principal.provenance, f"execution.action-resource.scope:{principal.principal_scope}", "read")
        return self._action_resource_owner().changes(
            resource_ref=resource_ref, principal_scope=principal.principal_scope,
            cursor=cursor, wait_seconds=wait_seconds, cancel_event=cancel_event,
        )

    def project_action_resource(
        self, conn: Any, *, action_id: int, expected_claim_receipt: str | None = None,
        kind: str = "state_changed",
    ) -> int | None:
        """Project authoritative state inside the caller's lifecycle transaction."""
        return self._action_resource_owner().project_state(
            conn, action_id=action_id, expected_claim_receipt=expected_claim_receipt,
            kind=kind,
        )

    def _authorize(self, provenance: InvocationProvenance, resource: str, verb: str) -> None:
        """Fail-closed resource-scoped hook for served dispatch resources."""
        if self._authorizer is None or not self._authorizer(provenance, resource, verb):
            raise db.ActionQError("authorization denied for dispatch resource")

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

    def _completion_log(self, capability: str) -> CompletionLog:
        configured = (
            self._completion_ingest_connection_factory
            if capability == "ingest"
            else self._completion_read_connection_factory
        )
        if configured is None:
            variable = (
                "ACTIONQ_COMPLETION_INGEST_URL"
                if capability == "ingest"
                else "ACTIONQ_COMPLETION_READ_URL"
            )
            url = os.environ.get(variable)
            if url:
                configured = lambda url=url: db.connect(url)
        return CompletionLog(schema=self.schema, connection_factory=configured or self._open)

    def ingest_session_completion(self, event: dict[str, Any]) -> dict[str, Any]:
        """Accept one observation through the narrow completion-log owner API."""
        return self._completion_log("ingest").ingest(event)

    def list_session_completions(
        self, *, cursor: str | int | None = None, limit: int = 100, replay: bool = False
    ) -> dict[str, Any]:
        """Read the replayable completion log; this does not read queue tables."""
        return self._completion_log("read").list(cursor=cursor, limit=limit, replay=replay)

    def session_completion_health(self) -> dict[str, Any]:
        return self._completion_log("read").health()

    def compact_session_completions(self, *, older_than_seconds: int | None = None) -> int:
        # Compaction is an owner/operator operation, never a completion bearer
        # capability; retain the application's ordinary owner connection.
        return CompletionLog(schema=self.schema, connection_factory=self._open).compact(
            older_than_seconds=older_than_seconds
        )

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
                            result_id = result.get("id", result.get("action_id"))
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
                durable_result = (
                    db.redact_action(result)
                    if operation == "execution.action.claim" and isinstance(result, dict)
                    else result
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
                        "result": durable_result,
                        "request_fingerprint": fingerprint,
                        "idempotency_owner": True,
                        "provenance": event_provenance,
                    },
                )
                response = self._decision_result(event=decision, replayed=False)
                if operation == "execution.action.claim" and isinstance(result, dict):
                    # Claim capabilities are disclosed only on the original
                    # authenticated response, never in history or replay.
                    response["result"] = result
                return response

