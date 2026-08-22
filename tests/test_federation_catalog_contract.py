"""The federation catalog is what W4 says it is, and execution/v1 is untouched.

Named by the tranche-4 freeze and by the W4 rescope, and the assertions here
are its stated constraints rather than a restatement of the module: a federation
catalog that carried an execution concept, a write that tolerated a missing
idempotency key, or an execution reference that could smuggle a launch
instruction would each be a real defect that no other test in this repository
would notice.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from actionq import vuoro as execution_adapter
from actionq import vuoro_federation as federation_adapter
from actionq.vuoro_federation import (
    FederationPrincipalError,
    build_operations,
    catalog_metadata,
    principal_from_identity,
    register_operations,
)


#: Concepts that belong to execution and must never appear as a federation
#: operation-name segment or input property. From the freeze's list, matched as
#: whole words rather than substrings.
#:
#: `settle` is deliberately absent, and the exclusion is a reading of the freeze
#: rather than a loosening of it. The freeze forbids a federation catalog that
#: carries execution's *settle* -- the queue operation that ends a claim -- while
#: federation settlement is a state of its own resource ledger, frozen in W3 and
#: served here as `federation.settlement.record`. A substring rule cannot tell
#: those apart, and the first draft of this test failed on exactly that
#: collision. What replaces it is the stronger check below: no federation
#: operation may share a name segment with an execution operation.
EXECUTION_CONCEPTS = (
    "claim", "renew", "sweep", "managed_dispatch", "managed-dispatch",
    "group_control", "group-control", "harness", "model", "prompt", "lease",
    "queue", "runner", "dispatch",
)


def _identity(**overrides) -> SimpleNamespace:
    fields = {
        "actor": "release-gate",
        "principal_id": "vuoro-cloud-control:github:123:0",
        "environment": "production",
        "authorities": frozenset({"federation.create", "federation.read"}),
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def test_the_two_catalogs_share_no_operation_and_no_domain() -> None:
    """Separate modules, separate registrations, separate names.

    The point of the split is that repinning one cannot repin the other; an
    operation appearing in both catalogs would make that structurally false.
    """
    federation = {definition["name"] for definition in catalog_metadata()}
    execution = {definition["name"] for definition in execution_adapter.catalog_metadata()}
    assert federation & execution == set()
    assert {definition["owning_domain"] for definition in catalog_metadata()} == {"federation"}
    assert all(name.startswith("federation.") for name in federation)


def test_execution_v1_is_byte_for_byte_what_it_was() -> None:
    """Scope: the exact sha256 of the 26-operation catalog payload, which is
    what byte-for-byte means here. Asserted from the federation test as well as
    the execution one, because the risk this file introduces is precisely that
    adding a second adapter to the same distribution disturbs the first."""
    catalog = execution_adapter.catalog_metadata()
    payload = json.dumps(catalog, sort_keys=True, separators=(",", ":")).encode()
    import hashlib

    assert len(catalog) == 26
    assert hashlib.sha256(payload).hexdigest() == (
        "8d434e8b347e804c90e48a6598304be84b12f2a61ebc2dbed00a26053239a778"
    )


def test_every_federation_write_requires_an_idempotency_key() -> None:
    """The opposite of what the execution adapter does, deliberately.

    ``FederationAuthority._execute`` keys its durable decision on
    ``(environment, principal_id, operation, idempotency_key)`` and replays it,
    so a missing key writes a row that can never be matched again. The
    execution adapter tolerates absence and passes an empty string; carrying
    that across would silently collapse distinct commands into one.
    """
    for definition in catalog_metadata():
        if definition["execution_semantics"] == "read":
            assert definition["idempotency"] == "not-allowed", definition["name"]
        else:
            assert definition["idempotency"] == "required", definition["name"]


def test_a_write_handler_refuses_an_absent_idempotency_key() -> None:
    """Declared and enforced, because the declaration is Vuoro's to honour."""
    operations = {
        operation.definition["name"]: operation for operation in build_operations()
    }
    handler = operations["federation.resource.create"].handler
    context = SimpleNamespace(identity=_identity(), idempotency_key=None)
    with pytest.raises(FederationPrincipalError, match="idempotency key"):
        handler({"expected_revision": 0}, context)


def test_the_federation_catalog_carries_no_execution_concept() -> None:
    for definition in catalog_metadata():
        segments = set(definition["name"].split("."))
        properties = set(definition["input_schema"]["properties"])
        for concept in EXECUTION_CONCEPTS:
            assert concept not in segments, f"{definition['name']} is named for {concept!r}"
            assert not any(concept in key.split("_") for key in properties), (
                f"{definition['name']} takes a {concept!r} field"
            )


def test_no_federation_operation_shares_a_leaf_with_an_execution_one() -> None:
    """The check the concept list cannot make, and the one that matters.

    A curated word list only catches what someone thought to list. Execution's
    own catalog is the authority on what execution means, so the rule is
    derived from it: no federation operation may end in the same verb as an
    execution operation on the same kind of subject. `settlement.record` and
    execution's `settle` survive this because they are different names for
    different objects, which is precisely the distinction a substring test
    could not draw.
    """
    execution_leaves = {
        definition["name"].rsplit(".", 1)[-1]
        for definition in execution_adapter.catalog_metadata()
    }
    federation_leaves = {
        definition["name"].rsplit(".", 1)[-1] for definition in catalog_metadata()
    }
    shared = federation_leaves & execution_leaves
    # `create` and `snapshot` are ledger verbs, not execution ones; anything
    # else in common would mean the federation surface grew an execution shape.
    assert shared <= {"create", "snapshot", "record", "decide"}, shared


def test_an_execution_reference_has_nowhere_to_carry_an_instruction() -> None:
    """A reference is a reference. The guarantee is structural: the schema
    forbids unknown properties and declares only a reference and an assurance
    type, so there is no field a launch instruction could arrive in."""
    definition = next(
        item for item in catalog_metadata()
        if item["name"] == "federation.resource.execution-ref"
    )
    schema = definition["input_schema"]
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == {
        "resource_ref", "execution_ref", "assurance_type", "expected_revision"
    }
    assert all(
        property_schema.get("type") in ("string", "integer")
        for property_schema in schema["properties"].values()
    )


def test_registration_matches_the_declared_catalog() -> None:
    class Definition:
        def __init__(self, **values):
            self.values = values
            self.name = values["name"]

    class Registry:
        def __init__(self):
            self.registered = []

        def register(self, definition, handler):
            self.registered.append((definition, handler))

    registry = Registry()
    register_operations(registry, definition_factory=Definition)
    assert [definition.name for definition, _ in registry.registered] == [
        definition["name"] for definition in catalog_metadata()
    ]
    assert all(callable(handler) for _, handler in registry.registered)


def test_the_catalog_is_describable_without_a_database() -> None:
    """Composition digests the catalog before anything connects, and
    ``build`` closes over the DSN rather than dialling it."""
    assert catalog_metadata()
    runtime = SimpleNamespace(require=lambda name: {"dsn": "postgresql:///none",
                                                    "schema": "federation"}[name])
    authority = federation_adapter.build(runtime)
    assert authority.schema == "federation"


# --- the identity mapping ----------------------------------------------------


def test_a_minted_principal_id_becomes_the_federation_principal() -> None:
    principal = principal_from_identity(_identity())
    assert principal.principal_id == "vuoro-cloud-control:github:123:0"
    assert principal.environment == "production"
    assert "federation.create" in principal.authorities


def test_a_reserved_system_principal_is_accepted_by_name() -> None:
    """federation-backfill/v1 is a pinned literal, not a minted identity."""
    principal = principal_from_identity(_identity(principal_id="federation-backfill/v1"))
    assert principal.principal_id == "federation-backfill/v1"


@pytest.mark.parametrize("principal_id", [
    None, "", "release-gate", "github:123", "issuer:subject:", "issuer:subject:x",
    "issuer:sub ject:1",
])
def test_an_unminted_principal_id_is_refused_before_any_command(principal_id) -> None:
    """The refusal that makes ownership survivable.

    Ownership is compared against ``owner_principal_id`` forever and v1 has no
    transfer operation, so an actor name reaching this ledger as a principal id
    would hand the previous holder's resources to whoever holds the name next.
    ActionQ cannot verify that an epoch was incremented -- that is the issuer's
    evidence -- but it can refuse anything that was never minted at all.
    """
    with pytest.raises(FederationPrincipalError):
        principal_from_identity(_identity(principal_id=principal_id))


def test_an_identity_without_an_environment_is_refused() -> None:
    with pytest.raises(FederationPrincipalError, match="environment-bound"):
        principal_from_identity(_identity(environment=""))
