"""Database-backed proof that a reissued identity cannot inherit ownership.

The companion shape-refusal falsifier, w4r-unminted-principal-refused, lives
in
tests/test_federation_catalog_contract.py::test_an_unminted_principal_id_is_refused_before_any_command
(a refusal at the serving edge of ids that were never minted at all -- a
different claim from this one). This file is the ownership half: given two
*validly minted* principal ids that share issuer and subject but differ only
in epoch, the later epoch must not be able to mutate a resource the earlier
epoch created, because owner_principal_id is compared by plain string
equality (actionq/federation.py:183) and a reissued identity's id is a
different string.
"""

from __future__ import annotations

import uuid

import pytest

from actionq import db, federation_schema
from actionq.federation import FederationAuthority, FederationPrincipal
from actionq.vuoro_federation import MINTED_PRINCIPAL_ID


def _factory(url: str):
    return lambda: db.connect(url)


def _principal(principal_id: str, *authorities: str) -> FederationPrincipal:
    return FederationPrincipal.authenticated(environment="test", principal_id=principal_id, authorities=authorities)


def _new_schema(prefix: str = "fed_own") -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _migrate(url: str, selected: str) -> None:
    with db.connect(url) as conn:
        federation_schema.migrate(conn, selected)


# Same issuer and subject, differing only in the trailing epoch segment --
# once for the shipped post-E-8 subject-derived shape
# (vuoro-cloud-local:<ulid>:<epoch>) and once for the pre-E-8 actor-derived
# shape (prod:github:123:<epoch>). Both match actionq's local shape check,
# MINTED_PRINCIPAL_ID (actionq/vuoro_federation.py:64), which parses
# right-to-left and is asserted below. The issuer's own conformance record
# (/projects/dev/vuoro-cloud/docs/evidence/principal-id-conformance.json)
# separately lists "prod:github:123:0" as a rejected_example -- actor
# strings are never subjects on that side of the boundary -- but that is a
# stricter, issuer-side rule this test does not and cannot enforce; it
# proves only what actionq's local regex and ownership comparison do with
# an id of this shape, not whether the issuer would ever mint one.
_EPOCH_PAIRS = [
    pytest.param(
        "vuoro-cloud-local:01J8Z6Q4N0X6X6X6X6X6X6X6X6:0",
        "vuoro-cloud-local:01J8Z6Q4N0X6X6X6X6X6X6X6X6:1",
        id="post-e8-subject-derived",
    ),
    pytest.param(
        "prod:github:123:0",
        "prod:github:123:1",
        id="pre-e8-actor-derived",
    ),
]


@pytest.mark.parametrize("original_epoch_id, reissued_epoch_id", _EPOCH_PAIRS)
def test_a_reissued_epoch_cannot_mutate_the_prior_epochs_resource(
    postgres_urls, original_epoch_id, reissued_epoch_id,
) -> None:
    """a principal_id whose epoch differs from a resource owner's principal_id fails the ownership comparison

    Covers both minted id shapes actionq accepts (post-E-8 subject-derived
    and pre-E-8 actor-derived): same issuer and subject, differing only in
    epoch. The original epoch can mutate its own resource; the reissued
    epoch, despite sharing issuer and subject, cannot -- ownership is a
    plain string comparison against owner_principal_id with no epoch-aware
    successor logic, so a reissued identity cannot inherit the prior
    identity's ownership.
    """
    # Load-bearing premise: both ids are validly minted shapes. Asserted in
    # code, not merely by comment, so a narrowed MINTED_PRINCIPAL_ID fails
    # this test loudly instead of silently degrading it to a proof about
    # two arbitrary strings differing in a suffix.
    assert MINTED_PRINCIPAL_ID.fullmatch(original_epoch_id)
    assert MINTED_PRINCIPAL_ID.fullmatch(reissued_epoch_id)

    selected = _new_schema()
    _migrate(postgres_urls["admin"], selected)
    authority = FederationAuthority(connection=_factory(postgres_urls["admin"]), schema=selected)

    original = _principal(original_epoch_id, "federation.create", "federation.relate", "federation.supersede")
    reissued = _principal(reissued_epoch_id, "federation.create", "federation.relate", "federation.supersede")

    resource = authority.create(principal=original, idempotency_key="create", expected_revision=0).resource_ref
    assert resource

    # The reissued epoch is refused on every owner-gated command, despite
    # sharing issuer and subject with the resource's actual owner.
    other_owned = authority.create(principal=reissued, idempotency_key="create-other", expected_revision=0).resource_ref
    assert other_owned

    rejected_relate = authority.add_relation(
        principal=reissued, idempotency_key="reissued-relate", source_ref=resource,
        relation_type="depends-on", target_ref=other_owned, expected_revision=1,
    )
    rejected_execution_ref = authority.record_execution_ref(
        principal=reissued, idempotency_key="reissued-execution-ref", resource_ref=resource,
        execution_ref="provider:reissued", assurance_type="observation", expected_revision=1,
    )
    rejected_supersede = authority.supersede(
        principal=reissued, idempotency_key="reissued-supersede", resource_ref=resource, expected_revision=1,
    )
    for decision in (rejected_relate, rejected_execution_ref, rejected_supersede):
        assert decision.status == "rejected"
        assert decision.code == "owner-mismatch"

    # The original epoch, unaffected, can still mutate its own resource --
    # so this test cannot pass vacuously against a broken owner_required
    # wiring that rejects everyone.
    accepted_relate = authority.add_relation(
        principal=original, idempotency_key="original-relate", source_ref=resource,
        relation_type="depends-on", target_ref=other_owned, expected_revision=1,
    )
    assert accepted_relate.status == "accepted"
    accepted_execution_ref = authority.record_execution_ref(
        principal=original, idempotency_key="original-execution-ref", resource_ref=resource,
        execution_ref="provider:original", assurance_type="observation", expected_revision=2,
    )
    assert accepted_execution_ref.status == "accepted"
    accepted_supersede = authority.supersede(
        principal=original, idempotency_key="original-supersede", resource_ref=resource, expected_revision=3,
    )
    assert accepted_supersede.status == "accepted"

    snapshot = authority.snapshot(principal=_principal("reader", "federation.read"), resource_ref=resource)
    assert snapshot["owner_principal_id"] == original_epoch_id
    assert snapshot["state"] == "superseded"
