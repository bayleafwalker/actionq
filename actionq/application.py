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
import time
import uuid

from . import db
from .runner_auth import VerifiedRunner, verify_runner_proof


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
        authorizer: Callable[[InvocationProvenance, str, str], bool] | None = None,
    ) -> None:
        self.schema = db.schema_name(schema)
        self._connection_factory = connection_factory
        self._authorizer = authorizer

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

    def create_immutable_action(
        self,
        *,
        request: dict[str, Any],
        spec: dict[str, Any],
        input_refs: list[str],
        project: str | None,
        priority: int,
        created_by: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        """Persist a compiler-frozen candidate action as an ordinary action."""
        if provenance is not None:
            self._authorize(provenance, "execution.candidate-action.create", "create")
        role = request.get("role") if isinstance(request, dict) else None
        if role not in {"candidate-verification", "candidate-integration", "candidate-review"}:
            raise db.ActionQError("immutable candidate action role is invalid")
        arguments = {
            "request": request, "spec": spec, "input_refs": input_refs, "project": project,
            "priority": priority, "created_by": created_by,
        }
        return self._mutate(
            operation="execution.action.create-immutable-candidate", arguments=arguments,
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.create_immutable_action(
                conn, self.schema, request=request, spec=spec, input_refs=input_refs,
                action_type=role, project=project, priority=priority, created_by=created_by,
                provenance=event_provenance,
            ),
        )

    def list_actions(self, **filters: Any) -> list[dict[str, Any]]:
        return self._read(lambda conn: [db.redact_action(row) for row in db.list_actions(conn, self.schema, **filters)])

    def show_action(self, action_id: int) -> dict[str, Any] | None:
        def read(conn):
            action = db.get_action(conn, self.schema, action_id)
            if action is None:
                return None
            return {
                "action": db.redact_action(action),
                "events": db.action_events(conn, self.schema, action_id),
            }

        return self._read(read)

    def realize_execution_group(
        self, *, plan_ref: str, max_parallel: int, failure_policy: str,
        members: list[dict[str, Any]], created_by: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        if provenance is not None:
            self._authorize(provenance, "execution.group.manage", "create")
        arguments = {
            "plan_ref": plan_ref, "max_parallel": max_parallel,
            "failure_policy": failure_policy, "members": members, "created_by": created_by,
        }
        return self._mutate(
            operation="execution.group.realize", arguments=arguments, provenance=provenance,
            mutation=lambda conn, event_provenance: db.realize_execution_group(
                conn, self.schema, plan_ref=plan_ref, max_parallel=max_parallel,
                failure_policy=failure_policy, members=members, created_by=created_by,
                provenance=event_provenance,
            ),
        )

    def stop_execution_group(
        self, *, group_id: str, actor: str, reason: str,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        if provenance is not None:
            self._authorize(provenance, "execution.group.manage", "update")
        return self._mutate(
            operation="execution.group.stop-new-claims",
            arguments={"group_id": group_id, "actor": actor, "reason": reason},
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.stop_execution_group(
                conn, self.schema, group_id=group_id, actor=actor, reason=reason,
                provenance=event_provenance,
            ),
        )

    def show_execution_group(self, group_id: str) -> dict[str, Any] | None:
        return self._read(lambda conn: db.get_execution_group(conn, self.schema, group_id))

    def list_execution_groups(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._read(lambda conn: db.list_execution_groups(conn, self.schema, limit=limit))

    def claim(
        self,
        *,
        runner_proof: dict[str, Any] | None = None,
        worker: str | None = None,
        timeout_minutes: int,
        provenance: InvocationProvenance | None = None,
    ) -> Any:
        if runner_proof is not None:
            identity = verify_runner_proof(
                runner_proof, operation="execution.action.claim", resource="queue:next"
            )
            proof_authenticated = True
        elif provenance is not None and worker == provenance.actor:
            identity = VerifiedRunner(provenance.actor, provenance.request_id, "execution.action.claim")
            proof_authenticated = False
        else:
            raise db.ActionQError("claim requires signed runner proof or authenticated served identity")
        def claim_mutation(conn, event_provenance):
            with conn.transaction():
                if proof_authenticated:
                    db.consume_runner_request(
                        conn, self.schema, runner_id=identity.runner_id,
                        request_id=identity.request_id, operation=identity.operation,
                        resource="queue:next",
                    )
                return db.claim(
                    conn, self.schema, worker=identity.runner_id,
                    timeout_minutes=timeout_minutes, provenance=event_provenance,
                )
        return self._mutate(
            operation="execution.action.claim",
            arguments={"worker": identity.runner_id, "runner_request_id": identity.request_id,
                       "timeout_minutes": timeout_minutes},
            provenance=provenance,
            mutation=claim_mutation,
        )

    def renew(
        self,
        *,
        action_id: int,
        worker: str | None,
        timeout_minutes: int,
        claim_receipt: str,
        provenance: InvocationProvenance | None = None,
        runner_proof: dict[str, Any] | None = None,
    ) -> Any:
        if runner_proof is not None:
            identity = verify_runner_proof(
                runner_proof, operation="execution.action.renew", resource=f"action:{action_id}"
            )
            worker = identity.runner_id
        elif provenance is None or worker != provenance.actor:
            raise db.ActionQError("renew requires signed runner proof or authenticated served identity")
        assert worker is not None
        return self._mutate(
            operation="execution.action.renew",
            arguments={
                "action_id": action_id,
                "worker": worker,
                "timeout_minutes": timeout_minutes,
                "claim_receipt_digest": db.receipt_digest(claim_receipt),
            },
            provenance=provenance,
            mutation=lambda conn, event_provenance: db.renew(
                conn,
                self.schema,
                action_id=action_id,
                worker=worker,
                timeout_minutes=timeout_minutes,
                claim_receipt=claim_receipt,
                provenance=event_provenance,
            ),
        )

    def _terminal(
        self,
        *,
        operation: str,
        action_id: int,
        actor: str | None,
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
        actor: str | None,
        claim_receipt: str,
        provenance: InvocationProvenance | None = None,
        runner_proof: dict[str, Any] | None = None,
    ) -> Any:
        if runner_proof is not None:
            actor = verify_runner_proof(
                runner_proof, operation="execution.action.complete", resource=f"action:{action_id}"
            ).runner_id
        elif provenance is None or actor != provenance.actor:
            raise db.ActionQError("complete requires signed runner proof or authenticated served identity")
        assert actor is not None
        return self._terminal(
            operation="execution.action.complete",
            action_id=action_id,
            actor=actor,
            arguments={"result_ref": result_ref, "claim_receipt_digest": db.receipt_digest(claim_receipt)},
            provenance=provenance,
            transition=lambda conn, event_provenance: db.complete(
                conn,
                self.schema,
                action_id,
                result_ref,
                actor=actor,
                worker=actor,
                claim_receipt=claim_receipt,
                provenance=event_provenance,
            ),
        )

    def fail(
        self,
        *,
        action_id: int,
        reason: str,
        actor: str | None,
        claim_receipt: str,
        provenance: InvocationProvenance | None = None,
        runner_proof: dict[str, Any] | None = None,
    ) -> Any:
        if runner_proof is not None:
            actor = verify_runner_proof(
                runner_proof, operation="execution.action.fail", resource=f"action:{action_id}"
            ).runner_id
        elif provenance is None or actor != provenance.actor:
            raise db.ActionQError("fail requires signed runner proof or authenticated served identity")
        assert actor is not None
        return self._terminal(
            operation="execution.action.fail",
            action_id=action_id,
            actor=actor,
            arguments={"reason": reason, "claim_receipt_digest": db.receipt_digest(claim_receipt)},
            provenance=provenance,
            transition=lambda conn, event_provenance: db.fail(
                conn,
                self.schema,
                action_id,
                reason,
                actor=actor,
                worker=actor,
                claim_receipt=claim_receipt,
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
        claim_receipt: str,
        provenance: InvocationProvenance | None = None,
        runner_proof: dict[str, Any] | None = None,
    ) -> Any:
        if runner_proof is not None:
            actor = verify_runner_proof(
                runner_proof, operation="execution.action.reject", resource=f"action:{action_id}"
            ).runner_id
        elif provenance is None or actor != provenance.actor:
            raise db.ActionQError("reject requires signed runner proof or authenticated served identity")
        assert actor is not None
        return self._terminal(
            operation="execution.action.reject",
            action_id=action_id,
            actor=actor,
            arguments={"reason": reason, "validator": validator, "claim_receipt_digest": db.receipt_digest(claim_receipt)},
            provenance=provenance,
            transition=lambda conn, event_provenance: db.reject(
                conn,
                self.schema,
                action_id,
                reason=reason,
                validator=validator,
                actor=actor,
                worker=actor,
                claim_receipt=claim_receipt,
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
            mutation=lambda conn, event_provenance: {
                "reclaimed": db.sweep(conn, self.schema, actor=actor, provenance=event_provenance),
                "cancelled": db.reap_cancellations(conn, self.schema, actor=actor),
            },
        )

    def acknowledge_cancellation(self, *, action_id: int, cancel_request_id: str,
                                 former_claim_receipt: str, runner_auth_token: str,
                                 runner_proof: dict[str, Any]) -> Any:
        identity = verify_runner_proof(
            runner_proof, operation="execution.action.cancel-ack",
            resource=f"action:{action_id}:cancel:{cancel_request_id}",
        )
        def ack_mutation(conn, _provenance):
            with conn.transaction():
                resource = f"action:{action_id}:cancel:{cancel_request_id}"
                db.consume_runner_request(
                    conn, self.schema, runner_id=identity.runner_id,
                    request_id=identity.request_id, operation=identity.operation,
                    resource=resource, action_id=action_id, allow_replay=True,
                )
                return db.acknowledge_cancellation(
                    conn, self.schema, action_id, cancel_request_id=cancel_request_id,
                    former_claim_receipt=former_claim_receipt, runner_auth_token=runner_auth_token,
                    authenticated_runner=identity.runner_id,
                )
        return self._mutate(
            operation="execution.action.cancel-ack",
            arguments={"action_id": action_id, "cancel_request_id": cancel_request_id,
                       "former_claim_receipt_digest": db.receipt_digest(former_claim_receipt),
                       "runner_auth_digest": db.receipt_digest(runner_auth_token),
                       "runner_id": identity.runner_id, "runner_request_id": identity.request_id},
            provenance=None,
            mutation=ack_mutation,
        )

    def register_publication(
        self,
        *,
        action_id: int,
        attempt_id: str,
        journal_ref: str,
        source_commit: str,
        candidate_commit: str,
        claim_receipt: str,
        runner_proof: dict[str, Any],
    ) -> dict[str, Any]:
        """Record an immutable publication receipt under the current claim."""
        identity = verify_runner_proof(
            runner_proof,
            operation="execution.action.register-publication",
            resource=f"action:{action_id}:publication:{attempt_id}",
        )
        with self.connection() as conn:
            return db.register_publication(
                conn,
                self.schema,
                action_id=action_id,
                runner_id=identity.runner_id,
                runner_request_id=identity.request_id,
                claim_receipt=claim_receipt,
                attempt_id=attempt_id,
                journal_ref=journal_ref,
                source_commit=source_commit,
                candidate_commit=candidate_commit,
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


__all__ = ["ActionQApplication", "InvocationProvenance"]
