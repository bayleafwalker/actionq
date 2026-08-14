"""Action enqueue service for the ActionQ application boundary."""

from __future__ import annotations

from .application_core import *


class EnqueueService:
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


