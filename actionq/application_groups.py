"""Execution-group and action inspection service."""

from __future__ import annotations

from .application_core import *


class GroupService:
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


