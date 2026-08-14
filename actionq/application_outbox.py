"""Event, session, and completion-log observation service."""

from __future__ import annotations

from .application_core import *


class OutboxService:
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


