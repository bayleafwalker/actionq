"""Claim and lease-renewal service."""

from __future__ import annotations

from .application_core import *


class ClaimService:
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
                claimed = db.claim(
                    conn, self.schema, worker=identity.runner_id,
                    timeout_minutes=timeout_minutes, provenance=event_provenance,
                )
                if claimed:
                    self._project_owned_action(conn, claimed["id"], claim_receipt=claimed["claim_receipt"])
                return claimed
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


