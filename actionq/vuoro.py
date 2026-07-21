"""Vuoro execution-domain adapter and operation catalog.

The module owns Actionq's operation names, schemas, and handlers.  It imports
Vuoro protocol classes only when ``register_operations`` is called, keeping the
standalone Actionq distribution free of a service-shell dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable

from . import db
from .application import ActionQApplication, InvocationProvenance


SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
API_VERSION = "v1"


def _transport(value: Any) -> Any:
    return json.loads(db.to_json(value))


def _object(
    properties: dict[str, Any],
    *,
    required: tuple[str, ...] = (),
    additional: bool = False,
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "$schema": SCHEMA_DIALECT,
        "type": "object",
        "properties": properties,
        "additionalProperties": additional,
    }
    if required:
        schema["required"] = list(required)
    return schema


_NULLABLE_STRING = {"type": ["string", "null"]}
_NULLABLE_INTEGER = {"type": ["integer", "null"]}
_ACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "action_type": {"type": "string"},
        "project": _NULLABLE_STRING,
        "target_ref": _NULLABLE_STRING,
        "source_refs": {"type": "array"},
        "priority": {"type": "integer"},
        "status": {
            "enum": [
                "pending",
                "claimed",
                "completed",
                "failed",
                "rejected",
                "cancelled",
            ]
        },
        "claimed_by": _NULLABLE_STRING,
        "claim_deadline": _NULLABLE_STRING,
        "parent_id": _NULLABLE_INTEGER,
        "chain_depth": {"type": "integer"},
    },
    "required": ["id", "action_type", "priority", "status"],
    "additionalProperties": True,
}
_EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "action_id": _NULLABLE_INTEGER,
        "event_type": {"type": "string"},
        "actor": _NULLABLE_STRING,
        "timestamp": {"type": "string"},
        "payload": {"type": "object"},
    },
    "required": ["id", "event_type", "timestamp", "payload"],
    "additionalProperties": True,
}
_DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        "decision_ref": {"type": "string", "pattern": "^actionq:event:[0-9]+$"},
        "operation": {"type": "string", "pattern": "^execution\\."},
        "request_id": {"type": "string", "minLength": 1},
        "status": {"enum": ["accepted", "rejected"]},
        "code": _NULLABLE_STRING,
        "message": _NULLABLE_STRING,
        "event_refs": {"type": "array", "items": {"type": "string"}},
        "replayed": {"type": "boolean"},
    },
    "required": [
        "decision_ref",
        "operation",
        "request_id",
        "status",
        "code",
        "message",
        "event_refs",
        "replayed",
    ],
    "additionalProperties": False,
}
_DECISION_RESULT_SCHEMA = _object(
    {"decision": _DECISION_SCHEMA, "result": {}},
    required=("decision", "result"),
)


@dataclass(frozen=True)
class AdapterOperation:
    definition: dict[str, Any]
    handler: Callable[[dict[str, Any], Any], Any]


def _definition(
    name: str,
    *,
    input_schema: dict[str, Any],
    result_schema: dict[str, Any],
    authority: str,
    semantics: str,
    idempotency: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "owning_domain": "execution",
        "input_schema": input_schema,
        "result_schema": result_schema,
        "required_authority": authority,
        "execution_semantics": semantics,
        "idempotency": idempotency,
        "deprecation": {
            "deprecated": False,
            "replacement": None,
            "sunset_at": None,
        },
        "required_client_schema_features": ["json-schema-draft-2020-12"],
    }


def _provenance(context: Any, *, operation: str) -> InvocationProvenance:
    identity = context.identity
    return InvocationProvenance(
        actor=identity.actor,
        environment=identity.environment,
        request_id=context.request_id,
        catalog_revision=context.catalog_revision,
        basis_revision=context.basis_revision,
        idempotency_key=context.idempotency_key or "",
    )


def build_operations(
    application: ActionQApplication | None = None,
) -> tuple[AdapterOperation, ...]:
    app = application or ActionQApplication()

    action_id = {"type": "integer", "minimum": 1}
    timeout = {"type": "integer", "minimum": 1, "maximum": 1440}
    limit = {"type": "integer", "minimum": 1, "maximum": 1000}

    def served(operation: str, callback: Callable[..., Any]):
        return lambda arguments, context: callback(
            arguments, _provenance(context, operation=operation), context.identity.actor
        )

    operations: list[AdapterOperation] = []

    name = "execution.action.enqueue"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    {
                        "action_type": {"type": "string", "minLength": 1},
                        "project": _NULLABLE_STRING,
                        "target_ref": _NULLABLE_STRING,
                        "source_refs": {
                            "type": "array",
                            "items": {"type": "string"},
                            "default": [],
                        },
                        "priority": {"type": "integer", "default": 100},
                        "parent_id": _NULLABLE_INTEGER,
                    },
                    required=("action_type",),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="execution.enqueue",
                semantics="enqueue",
                idempotency="required",
            ),
            served(
                name,
                lambda a, p, actor: app.enqueue(
                    action_type=a["action_type"],
                    project=a.get("project"),
                    target_ref=a.get("target_ref"),
                    source_refs=list(a.get("source_refs") or ()),
                    priority=a.get("priority", 100),
                    parent_id=a.get("parent_id"),
                    created_by=actor,
                    provenance=p,
                ),
            ),
        )
    )

    name = "execution.action.list"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    {
                        "status": _NULLABLE_STRING,
                        "action_type": _NULLABLE_STRING,
                        "project": _NULLABLE_STRING,
                        "limit": limit,
                    }
                ),
                result_schema={
                    "$schema": SCHEMA_DIALECT,
                    "type": "array",
                    "items": _ACTION_SCHEMA,
                },
                authority="execution.read",
                semantics="read",
                idempotency="not-allowed",
            ),
            lambda a, _context: _transport(
                app.list_actions(
                    status=a.get("status"),
                    action_type=a.get("action_type"),
                    project=a.get("project"),
                    limit=a.get("limit", 50),
                )
            ),
        )
    )

    name = "execution.action.show"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object({"action_id": action_id}, required=("action_id",)),
                result_schema=_object(
                    {
                        "action": {"anyOf": [_ACTION_SCHEMA, {"type": "null"}]},
                        "events": {"type": "array", "items": _EVENT_SCHEMA},
                    },
                    required=("action", "events"),
                ),
                authority="execution.read",
                semantics="read",
                idempotency="not-allowed",
            ),
            lambda a, _context: _transport(
                app.show_action(a["action_id"]) or {"action": None, "events": []}
            ),
        )
    )

    name = "execution.action.claim"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object({"timeout_minutes": timeout}),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="execution.claim",
                semantics="write",
                idempotency="required",
            ),
            served(
                name,
                lambda a, p, actor: app.claim(
                    worker=actor,
                    timeout_minutes=a.get("timeout_minutes", 30),
                    provenance=p,
                ),
            ),
        )
    )

    name = "execution.action.renew"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    {"action_id": action_id, "timeout_minutes": timeout},
                    required=("action_id",),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="execution.claim",
                semantics="write",
                idempotency="required",
            ),
            served(
                name,
                lambda a, p, actor: app.renew(
                    action_id=a["action_id"],
                    worker=actor,
                    timeout_minutes=a.get("timeout_minutes", 30),
                    provenance=p,
                ),
            ),
        )
    )

    terminal_specs = (
        (
            "complete",
            {"action_id": action_id, "result_ref": {"type": "string", "minLength": 1}},
            ("action_id", "result_ref"),
            lambda a, p, actor: app.complete(
                action_id=a["action_id"],
                result_ref=a["result_ref"],
                actor=actor,
                provenance=p,
            ),
        ),
        (
            "fail",
            {"action_id": action_id, "reason": {"type": "string", "minLength": 1}},
            ("action_id", "reason"),
            lambda a, p, actor: app.fail(
                action_id=a["action_id"],
                reason=a["reason"],
                actor=actor,
                provenance=p,
            ),
        ),
        (
            "reject",
            {
                "action_id": action_id,
                "reason": {"type": "string", "minLength": 1},
                "validator": {"type": "string", "minLength": 1},
            },
            ("action_id", "reason", "validator"),
            lambda a, p, actor: app.reject(
                action_id=a["action_id"],
                reason=a["reason"],
                validator=a["validator"],
                actor=actor,
                provenance=p,
            ),
        ),
        (
            "cancel",
            {"action_id": action_id, "reason": {"type": "string", "minLength": 1}},
            ("action_id", "reason"),
            lambda a, p, actor: app.cancel(
                action_id=a["action_id"],
                reason=a["reason"],
                actor=actor,
                provenance=p,
            ),
        ),
    )
    for suffix, properties, required, callback in terminal_specs:
        name = f"execution.action.{suffix}"
        operations.append(
            AdapterOperation(
                _definition(
                    name,
                    input_schema=_object(properties, required=required),
                    result_schema=_DECISION_RESULT_SCHEMA,
                    authority="execution.transition",
                    semantics="write",
                    idempotency="required",
                ),
                served(name, callback),
            )
        )

    name = "execution.action.sweep"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object({}),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="execution.sweep",
                semantics="admin",
                idempotency="required",
            ),
            served(
                name,
                lambda _a, p, actor: app.sweep(actor=actor, provenance=p),
            ),
        )
    )

    name = "execution.event.list"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    {
                        "since": _NULLABLE_STRING,
                        "event_type": _NULLABLE_STRING,
                        "action_id": _NULLABLE_INTEGER,
                        "limit": limit,
                    }
                ),
                result_schema={
                    "$schema": SCHEMA_DIALECT,
                    "type": "array",
                    "items": _EVENT_SCHEMA,
                },
                authority="execution.read",
                semantics="read",
                idempotency="not-allowed",
            ),
            lambda a, _context: _transport(
                app.list_events(
                    since=a.get("since"),
                    event_type=a.get("event_type"),
                    action_id=a.get("action_id"),
                    limit=a.get("limit", 100),
                )
            ),
        )
    )

    name = "execution.session.list"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    {
                        "project": _NULLABLE_STRING,
                        "active_only": {"type": "boolean"},
                        "limit": limit,
                    }
                ),
                result_schema={
                    "$schema": SCHEMA_DIALECT,
                    "type": "array",
                    "items": {"type": "object"},
                },
                authority="execution.read",
                semantics="read",
                idempotency="not-allowed",
            ),
            lambda a, _context: _transport(
                app.list_sessions(
                    project=a.get("project"),
                    active_only=a.get("active_only", False),
                    limit=a.get("limit", 100),
                )
            ),
        )
    )

    name = "execution.session.record"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    {
                        "event_type": {"enum": list(db.SESSION_EVENT_TYPES)},
                        "action_id": _NULLABLE_INTEGER,
                        "payload": {"type": "object"},
                    },
                    required=("event_type", "payload"),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="execution.session.report",
                semantics="write",
                idempotency="required",
            ),
            served(
                name,
                lambda a, p, actor: app.record_session(
                    event_type=a["event_type"],
                    action_id=a.get("action_id"),
                    payload=a["payload"],
                    actor=actor,
                    provenance=p,
                ),
            ),
        )
    )

    dispatch_properties = {
        "contract_version": {"const": "v1"},
        "repo_id": {"type": "string", "minLength": 1},
        "kind": {
            "enum": sorted(
                ("implement", "review", "test", "investigate", "document", "custom")
            )
        },
        "title": {"type": "string", "minLength": 1},
        "priority": {"enum": ["normal", "high"]},
        "refs": {"type": "array", "items": {"type": "string"}},
        "work_item_id": _NULLABLE_STRING,
        "output_expectation": _NULLABLE_STRING,
        "harness": _NULLABLE_STRING,
        "model": _NULLABLE_STRING,
        "prompt": _NULLABLE_STRING,
        "sprint_id": _NULLABLE_INTEGER,
        "dispatch_group_id": _NULLABLE_STRING,
    }
    name = "execution.dispatch.enqueue"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    dispatch_properties,
                    required=("contract_version", "repo_id", "kind", "title"),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="execution.dispatch.enqueue",
                semantics="enqueue",
                idempotency="required",
            ),
            served(
                name,
                lambda a, p, actor: app.dispatch(a, actor=actor, provenance=p),
            ),
        )
    )

    name = "execution.dispatch.list"
    operations.append(
        AdapterOperation(
            _definition(
                name,
                input_schema=_object(
                    {
                        "project": _NULLABLE_STRING,
                        "status": _NULLABLE_STRING,
                        "limit": limit,
                    }
                ),
                result_schema={
                    "$schema": SCHEMA_DIALECT,
                    "type": "array",
                    "items": {"type": "object"},
                },
                authority="execution.read",
                semantics="read",
                idempotency="not-allowed",
            ),
            lambda a, _context: _transport(
                app.list_dispatches(
                    project=a.get("project"),
                    status=a.get("status"),
                    limit=a.get("limit", 100),
                )
            ),
        )
    )
    return tuple(operations)


def catalog_metadata() -> list[dict[str, Any]]:
    """Return deterministic, data-only catalog definitions for composition."""

    return [operation.definition for operation in build_operations()]


def compatibility_record(
    application: ActionQApplication | None = None,
) -> dict[str, Any]:
    compatibility = (application or ActionQApplication()).compatibility()
    return {
        "api_version": API_VERSION,
        "schema_version": str(
            compatibility["observed_schema_version"]
            if compatibility["observed_schema_version"] is not None
            else compatibility["maximum_schema_version"]
        ),
        "state": "compatible" if compatibility["compatible"] else "incompatible",
        "reason": None if compatibility["compatible"] else compatibility["detail"],
    }


def register_operations(
    registry: Any,
    *,
    application: ActionQApplication | None = None,
    definition_factory: Callable[..., Any] | None = None,
) -> None:
    """Register this owner-provided catalog in a Vuoro service registry."""

    if definition_factory is None:
        try:
            from vuoro_service.contracts import OperationDefinition
        except ModuleNotFoundError as error:  # pragma: no cover - composition error
            raise RuntimeError(
                "vuoro-service must be installed to register Actionq operations"
            ) from error
        definition_factory = OperationDefinition
    for operation in build_operations(application):
        registry.register(definition_factory(**operation.definition), operation.handler)


__all__ = [
    "API_VERSION",
    "AdapterOperation",
    "build_operations",
    "catalog_metadata",
    "compatibility_record",
    "register_operations",
]
