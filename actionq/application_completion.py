"""Terminal transition and publication service."""

from __future__ import annotations

from .application_core import *
from .application_core import _json_value


class CompletionService:
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
            transition=lambda conn, event_provenance: self._transition_owned_action(conn, action_id, lambda: db.complete(
                conn,
                self.schema,
                action_id,
                result_ref,
                actor=actor,
                worker=actor,
                claim_receipt=claim_receipt,
                provenance=event_provenance,
            )),
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
            transition=lambda conn, event_provenance: self._transition_owned_action(conn, action_id, lambda: db.fail(
                conn,
                self.schema,
                action_id,
                reason,
                actor=actor,
                worker=actor,
                claim_receipt=claim_receipt,
                provenance=event_provenance,
            )),
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
            transition=lambda conn, event_provenance: self._transition_owned_action(conn, action_id, lambda: db.reject(
                conn,
                self.schema,
                action_id,
                reason=reason,
                validator=validator,
                actor=actor,
                worker=actor,
                claim_receipt=claim_receipt,
                provenance=event_provenance,
            )),
        )

    def settle(
        self,
        *,
        action_id: int,
        result: dict[str, Any],
        actor: str | None,
        claim_receipt: str,
        provenance: InvocationProvenance | None = None,
        runner_proof: dict[str, Any] | None = None,
    ) -> Any:
        """Settle a claimed action from one verified dispatch-result/v1 packet."""

        if runner_proof is not None:
            actor = verify_runner_proof(
                runner_proof, operation="execution.action.settle", resource=f"action:{action_id}"
            ).runner_id
        elif provenance is None or actor != provenance.actor:
            raise db.ActionQError("settle requires signed runner proof or authenticated served identity")
        assert actor is not None
        result_ref = db.validate_dispatch_result(result, action_id=action_id)
        artifact_store = self._verified_result_store(result_ref)
        return self._terminal(
            operation="execution.action.settle",
            action_id=action_id,
            actor=actor,
            arguments={
                "dispatch_result": result,
                "claim_receipt_digest": db.receipt_digest(claim_receipt),
            },
            provenance=provenance,
            transition=lambda conn, event_provenance: self._transition_owned_action(
                conn,
                action_id,
                lambda: db.settle_dispatch_result(
                    conn,
                    self.schema,
                    action_id=action_id,
                    result=result,
                    actor=actor,
                    worker=actor,
                    claim_receipt=claim_receipt,
                    provenance=event_provenance,
                    artifact_root=self.artifact_root,
                    _artifact_store=artifact_store,
                ),
                claim_receipt=claim_receipt,
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


