"""Vuoro serving surface for `federation.resource/v1`.

Its own module, not part of ``actionq.vuoro``, and that separation is the point
rather than tidiness. An adapter record names exactly one module and one
``register``, so putting federation registration inside the execution adapter's
module would make the two inseparable at exactly the layer that is supposed to
keep them apart: ``execution/v1`` is frozen and federation iterates, and a
repin of one must never be a repin of the other.

For the same reason this stands beside ``ActionQApplication`` rather than
inside it. ``ActionQCore.connection()`` asserts *execution* schema
compatibility on every call, so serving federation through it would make every
federation request claim execution readiness -- a claim the tranche-4 freeze
forbids in as many words. ``FederationAuthority`` needs only a connection
factory and a schema, and does its own compatibility handshake.

What is served here is the append-only resource ledger, which is what
``federation.resource/v1`` is. `federation.principal/v1` and
`federation.grant/v1` are declared contracts with no operations yet: identity
is asserted by the issuer rather than administered here (see
``principal_from_identity`` below), and grants are today the authority set on
the asserted principal.

Every write declares its idempotency key **required**. That is not a style
choice copied from the execution adapter -- it is the opposite of what the
execution adapter does. ``FederationAuthority._execute`` records a durable
decision keyed by ``(environment, principal_id, operation, idempotency_key)``
and replays it on repeat; a missing key would collapse distinct commands into
one ledger row, and the execution adapter's tolerance of an empty key is
exactly the behaviour that must not be carried across.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import re
from typing import Any, Callable

from vuoro_adapter_kit import (
    CatalogRegistry,
    SCHEMA_DIALECT,
    SCHEMA_FEATURES as _ADAPTER_SCHEMA_FEATURES,
    object_schema,
)

from . import db
from . import federation_schema
from .federation import FederationAuthority, FederationPrincipal


SCHEMA_FEATURES = list(_ADAPTER_SCHEMA_FEATURES)
CAPABILITY = "federation.resource/v1"
API_VERSION = federation_schema.API_VERSION

#: A minted principal identifier, ``<issuer>:<subject>:<epoch>``. ActionQ does
#: not mint these and cannot verify that an epoch was incremented -- that is the
#: issuer's conformance evidence. What it can verify locally is the shape, and
#: it refuses anything else, because the alternative is accepting a bare actor
#: name as a principal id. Ownership is compared against ``owner_principal_id``
#: forever and v1 has no transfer operation, so a reissued actor name that
#: reached this ledger would take ownership of the previous holder's resources
#: and nothing could put it back.
MINTED_PRINCIPAL_ID = re.compile(r"^.+:[^:\s]+:(?:0|[1-9][0-9]*)$")

#: Reserved system principals, pinned literals rather than minted identities --
#: ``federation-backfill/v1`` is the one that exists, and it is half of what
#: makes a backfilled change distinguishable from a native one in a ledger with
#: no provenance column.
SYSTEM_PRINCIPAL_ID = re.compile(r"^[a-z][a-z0-9-]*/v[1-9][0-9]*$")


@dataclass(frozen=True)
class AdapterOperation:
    definition: dict[str, Any]
    handler: Callable[[dict[str, Any], Any], Any]


class FederationPrincipalError(db.ActionQError):
    """The caller's identity cannot become a federation principal."""


def principal_from_identity(identity: Any) -> FederationPrincipal:
    """Map a Vuoro identity onto a federation principal, or refuse.

    The refusals are the substance. ``actor`` is a display name chosen for
    humans and expected to change; ``principal_id`` is minted once and never
    reissued. Falling back from one to the other -- which is what any default
    here would amount to -- is the failure mode this whole mapping exists to
    prevent, so an identity that asserts no principal is refused rather than
    named after its actor.
    """
    principal_id = getattr(identity, "principal_id", None)
    if not isinstance(principal_id, str) or not principal_id:
        raise FederationPrincipalError(
            "federation requires an identity that asserts a minted principal_id"
        )
    if not (
        MINTED_PRINCIPAL_ID.fullmatch(principal_id)
        or SYSTEM_PRINCIPAL_ID.fullmatch(principal_id)
    ):
        raise FederationPrincipalError(
            "principal_id must be <issuer>:<subject>:<epoch> or a reserved system principal"
        )
    environment = getattr(identity, "environment", None)
    if not isinstance(environment, str) or not environment:
        raise FederationPrincipalError("federation requires an environment-bound identity")
    return FederationPrincipal.authenticated(
        environment=environment,
        principal_id=principal_id,
        authorities=tuple(getattr(identity, "authorities", ()) or ()),
    )


def _object(properties: dict[str, Any], *, required: tuple[str, ...] = ()) -> dict[str, Any]:
    return object_schema(properties, required=required)


_RESOURCE_REF = {"type": "string", "minLength": 1}
_EXPECTED_REVISION = {"type": "integer", "minimum": 0}

_DECISION_RESULT_SCHEMA = _object(
    {
        "schema_version": {"type": "string"},
        "status": {"type": "string"},
        "code": {"type": "string"},
        "message": {"type": "string"},
        "operation": {"type": "string"},
        "resource_ref": {"type": ["string", "null"]},
        "before_revision": {"type": ["integer", "null"]},
        "after_revision": {"type": ["integer", "null"]},
    },
    required=("schema_version", "status", "code", "message", "operation"),
)

_SNAPSHOT_RESULT_SCHEMA = _object(
    {
        "schema_version": {"type": "string"},
        "resource_ref": {"type": "string"},
        "owner_principal_id": {"type": "string"},
        "state": {"type": "string"},
        "revision": {"type": "integer"},
        "recovery_floor": {"type": "integer"},
    },
    required=(
        "schema_version", "resource_ref", "owner_principal_id", "state",
        "revision", "recovery_floor",
    ),
)


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
        "owning_domain": "federation",
        "input_schema": deepcopy(input_schema),
        "result_schema": deepcopy(result_schema),
        "required_authority": authority,
        "execution_semantics": semantics,
        "idempotency": idempotency,
        "deprecation": {"deprecated": False, "replacement": None, "sunset_at": None},
        "required_client_schema_features": list(SCHEMA_FEATURES),
    }


def build_operations(authority: FederationAuthority | None = None) -> tuple[AdapterOperation, ...]:
    """The served ledger, one operation per authority command.

    ``authority`` is optional so the catalog can be described without a
    database -- ``catalog_metadata`` is data only, and composition needs to
    digest it before anything connects.
    """
    served_authority = authority

    def command(callback: Callable[..., Any]) -> Callable[[dict[str, Any], Any], Any]:
        def handler(arguments: dict[str, Any], context: Any) -> Any:
            principal = principal_from_identity(context.identity)
            key = context.idempotency_key
            if not isinstance(key, str) or not key:
                # The catalog declares the key required and Vuoro enforces that
                # before dispatch; this is the second line, because the ledger's
                # replay identity has no meaning without one and a silent empty
                # key would write a row that can never be matched again.
                raise FederationPrincipalError("federation commands require an idempotency key")
            decision = callback(served_authority, arguments, principal, key)
            return decision.response
        return handler

    def read(callback: Callable[..., Any]) -> Callable[[dict[str, Any], Any], Any]:
        def handler(arguments: dict[str, Any], context: Any) -> Any:
            return callback(served_authority, arguments, principal_from_identity(context.identity))
        return handler

    operations: list[AdapterOperation] = [
        AdapterOperation(
            _definition(
                "federation.resource.create",
                input_schema=_object(
                    {"expected_revision": _EXPECTED_REVISION,
                     "resource_ref": {"type": ["string", "null"]}},
                    required=("expected_revision",),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="federation.create",
                semantics="write",
                idempotency="required",
            ),
            command(lambda auth, a, principal, key: auth.create(
                principal=principal, idempotency_key=key,
                expected_revision=a["expected_revision"],
                resource_ref=a.get("resource_ref"),
            )),
        ),
        AdapterOperation(
            _definition(
                "federation.resource.relate",
                input_schema=_object(
                    {"source_ref": _RESOURCE_REF, "relation_type": {"type": "string", "minLength": 1},
                     "target_ref": _RESOURCE_REF, "expected_revision": _EXPECTED_REVISION},
                    required=("source_ref", "relation_type", "target_ref", "expected_revision"),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="federation.relate",
                semantics="write",
                idempotency="required",
            ),
            command(lambda auth, a, principal, key: auth.add_relation(
                principal=principal, idempotency_key=key, source_ref=a["source_ref"],
                relation_type=a["relation_type"], target_ref=a["target_ref"],
                expected_revision=a["expected_revision"],
            )),
        ),
        AdapterOperation(
            _definition(
                "federation.resource.execution-ref",
                # An external *reference* and an assurance type, and nothing
                # else. The freeze requires that a served reference cannot
                # carry a launch instruction, and the way to guarantee that is
                # to give the schema nowhere to put one: no command, no
                # arguments, no payload, additionalProperties false.
                input_schema=_object(
                    {"resource_ref": _RESOURCE_REF,
                     "execution_ref": {"type": "string", "minLength": 1},
                     "assurance_type": {"type": "string", "minLength": 1},
                     "expected_revision": _EXPECTED_REVISION},
                    required=("resource_ref", "execution_ref", "assurance_type",
                              "expected_revision"),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="federation.relate",
                semantics="write",
                idempotency="required",
            ),
            command(lambda auth, a, principal, key: auth.record_execution_ref(
                principal=principal, idempotency_key=key, resource_ref=a["resource_ref"],
                execution_ref=a["execution_ref"], assurance_type=a["assurance_type"],
                expected_revision=a["expected_revision"],
            )),
        ),
        AdapterOperation(
            _definition(
                "federation.acceptance.decide",
                input_schema=_object(
                    {"resource_ref": _RESOURCE_REF, "outcome": {"enum": ["accepted", "rejected"]},
                     "policy_ref": {"type": "string", "minLength": 1},
                     "evidence_ref": {"type": ["string", "null"]},
                     "expected_revision": _EXPECTED_REVISION},
                    required=("resource_ref", "outcome", "policy_ref", "expected_revision"),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="federation.acceptance.decide",
                semantics="write",
                idempotency="required",
            ),
            command(lambda auth, a, principal, key: auth.decide_acceptance(
                principal=principal, idempotency_key=key, resource_ref=a["resource_ref"],
                outcome=a["outcome"], policy_ref=a["policy_ref"],
                evidence_ref=a.get("evidence_ref"), expected_revision=a["expected_revision"],
            )),
        ),
        AdapterOperation(
            _definition(
                "federation.settlement.record",
                input_schema=_object(
                    {"resource_ref": _RESOURCE_REF, "fact_ref": {"type": "string", "minLength": 1},
                     "expected_revision": _EXPECTED_REVISION},
                    required=("resource_ref", "fact_ref", "expected_revision"),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="federation.settlement.record",
                semantics="write",
                idempotency="required",
            ),
            command(lambda auth, a, principal, key: auth.record_settlement(
                principal=principal, idempotency_key=key, resource_ref=a["resource_ref"],
                fact_ref=a["fact_ref"], expected_revision=a["expected_revision"],
            )),
        ),
        AdapterOperation(
            _definition(
                "federation.resource.supersede",
                input_schema=_object(
                    {"resource_ref": _RESOURCE_REF, "expected_revision": _EXPECTED_REVISION},
                    required=("resource_ref", "expected_revision"),
                ),
                result_schema=_DECISION_RESULT_SCHEMA,
                authority="federation.supersede",
                semantics="write",
                idempotency="required",
            ),
            command(lambda auth, a, principal, key: auth.supersede(
                principal=principal, idempotency_key=key, resource_ref=a["resource_ref"],
                expected_revision=a["expected_revision"],
            )),
        ),
        AdapterOperation(
            _definition(
                "federation.resource.snapshot",
                input_schema=_object({"resource_ref": _RESOURCE_REF}, required=("resource_ref",)),
                result_schema=_SNAPSHOT_RESULT_SCHEMA,
                authority="federation.read",
                semantics="read",
                idempotency="not-allowed",
            ),
            read(lambda auth, a, principal: auth.snapshot(
                principal=principal, resource_ref=a["resource_ref"],
            )),
        ),
    ]
    return tuple(operations)


def catalog_metadata() -> list[dict[str, Any]]:
    """Deterministic, data-only catalog definitions for composition."""

    return [operation.definition for operation in build_operations()]


def build(runtime: Any) -> FederationAuthority:
    """Uniform construction (Vuoro composition v4 §3.3).

    Constructs lazily on purpose: the connection factory is a closure, so
    composing a catalog does not open a database and a validator can prove the
    served surface without one.
    """
    dsn = runtime.require("dsn")

    def connection():
        import psycopg

        return psycopg.connect(dsn)

    return FederationAuthority(connection=connection, schema=runtime.require("schema"))


def register(registry: CatalogRegistry, application: FederationAuthority) -> None:
    """Uniform construction's second half."""

    register_operations(registry, authority=application)


def register_operations(
    registry: CatalogRegistry,
    *,
    authority: FederationAuthority | None = None,
    definition_factory: Callable[..., Any] | None = None,
) -> None:
    """Register this owner-provided catalog in a Vuoro service registry."""

    if definition_factory is None:
        try:
            from vuoro_service.contracts import OperationDefinition
        except ModuleNotFoundError as error:  # pragma: no cover - composition error
            raise RuntimeError(
                "vuoro-service must be installed to register federation operations"
            ) from error
        definition_factory = OperationDefinition
    for operation in build_operations(authority):
        registry.register(definition_factory(**operation.definition), operation.handler)


def compatibility_record(authority: FederationAuthority | None = None) -> dict[str, Any]:
    """The federation schema's own handshake, never the execution schema's.

    Reported as ``uninitialized`` until the federation migration runs, which is
    what keeps a served federation surface from claiming readiness on a
    database that has never had one.
    """
    if authority is None:
        return {
            "api_version": API_VERSION,
            "schema_version": federation_schema.COMPATIBILITY_LABEL,
            "state": "unknown",
            "reason": "no federation authority was supplied to inspect",
        }
    with authority.connection() as conn:
        compatibility = federation_schema.check_compatibility(conn, authority.schema)
    return {
        "api_version": compatibility.api_version,
        "schema_version": federation_schema.COMPATIBILITY_LABEL,
        "state": "compatible" if compatibility.compatible else "incompatible",
        "reason": None if compatibility.compatible else compatibility.detail,
    }


__all__ = [
    "API_VERSION",
    "CAPABILITY",
    "AdapterOperation",
    "FederationPrincipalError",
    "SCHEMA_DIALECT",
    "SCHEMA_FEATURES",
    "build",
    "build_operations",
    "catalog_metadata",
    "compatibility_record",
    "principal_from_identity",
    "register",
    "register_operations",
]
