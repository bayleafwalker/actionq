"""Dormant federation v1 aggregate and command-decision authority.

The module records ActionQ-local federation facts only.  It has no execution,
Sprintctl, catalog, CLI, or provider side effects.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from actionq_contracts import canonical_bytes, sha256_digest as contract_digest

from . import db
from . import federation_schema


RESOURCE_RE = re.compile(r"^aqf1_[A-Za-z0-9_-]{43}$")
RELATION_TYPES = frozenset({"parent-of", "depends-on", "derived-from", "supersedes"})
CYCLIC_RELATION_TYPES = frozenset({"parent-of", "depends-on"})


@dataclass(frozen=True)
class FederationPrincipal:
    environment: str
    principal_id: str
    authorities: frozenset[str]

    def __post_init__(self) -> None:
        if not isinstance(self.environment, str) or not self.environment:
            raise ValueError("authenticated federation principal requires a non-empty string environment")
        if not isinstance(self.principal_id, str) or not self.principal_id:
            raise ValueError("authenticated federation principal requires a non-empty string principal_id")
        if not all(isinstance(item, str) and item for item in self.authorities):
            raise ValueError("federation authorities must be non-empty strings")

    @classmethod
    def authenticated(cls, *, environment: str, principal_id: str, authorities: Iterable[str]) -> "FederationPrincipal":
        return cls(environment, principal_id, frozenset(authorities))


@dataclass(frozen=True)
class CommandDecision:
    status: str
    code: str
    message: str
    response_bytes: bytes
    response_digest: str
    resource_ref: str | None
    before_revision: int | None
    after_revision: int | None
    replayed: bool = False

    @property
    def response(self) -> dict[str, Any]:
        return json.loads(self.response_bytes)


class _Rejected(Exception):
    def __init__(self, code: str, message: str, *, resource_ref: str | None = None, before_revision: int | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.resource_ref = resource_ref
        self.before_revision = before_revision


def _new_resource_ref() -> str:
    return "aqf1_" + base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")


def _raw_digest(raw: bytes) -> str:
    # Plain sha256 of already-final bytes, with no canonicalization step --
    # unlike actionq_contracts.sha256_digest (imported below as
    # contract_digest), which JSON-canonicalizes its input before hashing
    # and so cannot be used for raw evidence bytes or a repr() descriptor.
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _decision_from_row(row: Any, *, replayed: bool) -> CommandDecision:
    return CommandDecision(
        status=str(row["status"]), code=str(row["code"]), message=str(row["message"]),
        response_bytes=bytes(row["response_bytes"]), response_digest=str(row["response_digest"]),
        resource_ref=row["resource_ref"],
        before_revision=int(row["before_revision"]) if row["before_revision"] is not None else None,
        after_revision=int(row["after_revision"]) if row["after_revision"] is not None else None,
        replayed=replayed,
    )


class FederationAuthority:
    """Transactional command service for an independent federation schema."""

    def __init__(self, *, connection: Callable[[], Any], schema: str | None = None):
        self.connection = connection
        self.schema = db.schema_name(schema or federation_schema.configured_schema())

    def _q(self, table: str) -> str:
        return db.qname(self.schema, table)

    def _response(self, *, status: str, code: str, message: str, operation: str,
                  resource_ref: str | None, before_revision: int | None,
                  after_revision: int | None) -> tuple[bytes, str]:
        payload = {
            "schema_version": "federation-command-decision/v1",
            "status": status,
            "code": code,
            "message": message,
            "operation": operation,
            "resource_ref": resource_ref,
            "before_revision": before_revision,
            "after_revision": after_revision,
        }
        return canonical_bytes(payload), contract_digest(payload)

    def _record_decision(self, conn: Any, *, principal: FederationPrincipal, operation: str,
                         idempotency_key: str, request_digest: str, status: str,
                         code: str, message: str, resource_ref: str | None,
                         before_revision: int | None, after_revision: int | None) -> CommandDecision:
        raw, response_digest = self._response(
            status=status, code=code, message=message, operation=operation,
            resource_ref=resource_ref, before_revision=before_revision, after_revision=after_revision,
        )
        row = conn.execute(
            f"INSERT INTO {self._q('federation_command_decisions')} "
            "(environment, principal_id, operation, idempotency_key, request_digest, status, code, message, "
            "response_bytes, response_digest, resource_ref, before_revision, after_revision) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
            (principal.environment, principal.principal_id, operation, idempotency_key, request_digest,
             status, code, message, raw, response_digest, resource_ref, before_revision, after_revision),
        ).fetchone()
        return _decision_from_row(row, replayed=False)

    def _resource(self, conn: Any, resource_ref: str, *, lock: bool = True) -> Any:
        suffix = " FOR UPDATE" if lock else ""
        return conn.execute(
            f"SELECT resource_ref, owner_principal_id, state, revision, recovery_floor "
            f"FROM {self._q('federation_resources')} WHERE resource_ref=%s{suffix}",
            (resource_ref,),
        ).fetchone()

    def _require_resource_ref(self, resource_ref: str) -> None:
        if not RESOURCE_RE.fullmatch(resource_ref):
            raise _Rejected("invalid-resource-ref", "resource_ref is not a federation v1 reference")

    def _require_revision(self, expected_revision: int) -> None:
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
            raise _Rejected("invalid-expected-revision", "expected_revision must be a non-negative integer")

    def _require_authority(self, principal: FederationPrincipal, authority: str) -> None:
        if authority not in principal.authorities:
            raise _Rejected("authority-denied", f"{authority} authority is required")

    def _locked_existing(self, conn: Any, *, principal: FederationPrincipal, resource_ref: str,
                         expected_revision: int, owner_required: bool = False) -> Any:
        self._require_resource_ref(resource_ref)
        self._require_revision(expected_revision)
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"federation-resource/v1:{resource_ref}",))
        row = self._resource(conn, resource_ref)
        if row is None:
            raise _Rejected("resource-not-found", "federation resource does not exist", resource_ref=resource_ref)
        before = int(row["revision"])
        if before != expected_revision:
            raise _Rejected("stale-revision", "expected_revision does not match current revision", resource_ref=resource_ref, before_revision=before)
        if row["state"] == "superseded":
            raise _Rejected("resource-superseded", "superseded resources are immutable", resource_ref=resource_ref, before_revision=before)
        if owner_required and row["owner_principal_id"] != principal.principal_id:
            raise _Rejected("owner-mismatch", "command actor does not own the source resource", resource_ref=resource_ref, before_revision=before)
        return row

    def _advance(self, conn: Any, *, row: Any, principal: FederationPrincipal, operation: str,
                 state: str, payload: dict[str, Any], side_effect: Callable[[int], None] | None = None) -> tuple[int, int]:
        before = int(row["revision"])
        after = before + 1
        updated = conn.execute(
            f"UPDATE {self._q('federation_resources')} SET state=%s, revision=%s, updated_at=now() "
            "WHERE resource_ref=%s AND revision=%s RETURNING revision",
            (state, after, row["resource_ref"], before),
        ).fetchone()
        if updated is None:
            raise _Rejected("stale-revision", "resource revision changed concurrently", resource_ref=row["resource_ref"], before_revision=before)
        if side_effect is not None:
            side_effect(after)
        conn.execute(
            f"INSERT INTO {self._q('federation_resource_changes')} "
            "(resource_ref, revision, operation, state, actor_principal_id, payload_bytes, payload_digest) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (row["resource_ref"], after, operation, state, principal.principal_id, canonical_bytes(payload), contract_digest(payload)),
        )
        return before, after

    def _execute(self, *, principal: FederationPrincipal, operation: str, idempotency_key: str,
                 request: dict[str, Any], apply: Callable[[Any], tuple[str, int | None, int | None]]) -> CommandDecision:
        if not isinstance(idempotency_key, str) or not idempotency_key:
            # Type-checked here, not just truthiness: identity below is
            # joined into a single advisory-lock key string, and a non-str
            # idempotency_key (or a non-str principal.environment/
            # principal_id -- see FederationPrincipal.__post_init__, which
            # rejects those the same way) would otherwise leak a bare
            # TypeError from that join instead of the module's public
            # exception, after a transaction is already open.
            raise db.ActionQError("federation commands require a non-empty string idempotency key")
        try:
            request_digest = contract_digest(request)
        except (TypeError, ValueError) as malformed:
            # This cannot become a durable "malformed-command" rejected
            # decision the way apply()'s failures do: request_digest, the
            # value that keys that decision, is exactly what failed to
            # compute. Raise a public exception instead of letting the
            # underlying TypeError (e.g. "Object of type bytes is not JSON
            # serializable") leak straight from a caller's malformed
            # argument -- every other command-argument failure in this
            # module surfaces as db.ActionQError or a rejected decision.
            raise db.ActionQError(f"command request could not be serialized: {malformed}") from malformed
        with self.connection() as conn, conn.transaction():
            federation_schema.require_compatible(conn, self.schema)
            identity = (principal.environment, principal.principal_id, operation, idempotency_key)
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("\x1f".join(("federation-command/v1", *identity)),))
            bound = conn.execute(
                f"SELECT request_digest FROM {self._q('federation_idempotency_bindings')} "
                "WHERE environment=%s AND principal_id=%s AND operation=%s AND idempotency_key=%s",
                identity,
            ).fetchone()
            prior = conn.execute(
                f"SELECT * FROM {self._q('federation_command_decisions')} "
                "WHERE environment=%s AND principal_id=%s AND operation=%s AND idempotency_key=%s AND request_digest=%s",
                (*identity, request_digest),
            ).fetchone()
            if prior is not None:
                return _decision_from_row(prior, replayed=True)
            if bound is not None and str(bound["request_digest"]) != request_digest:
                return self._record_decision(
                    conn, principal=principal, operation=operation, idempotency_key=idempotency_key,
                    request_digest=request_digest, status="rejected", code="idempotency-key-conflict",
                    message="idempotency key was first bound to different canonical request bytes",
                    resource_ref=None, before_revision=None, after_revision=None,
                )
            if bound is None:
                conn.execute(
                    f"INSERT INTO {self._q('federation_idempotency_bindings')} "
                    "(environment, principal_id, operation, idempotency_key, request_digest) VALUES (%s,%s,%s,%s,%s)",
                    (*identity, request_digest),
                )
            try:
                resource_ref, before, after = apply(conn)
            except _Rejected as rejected:
                return self._record_decision(
                    conn, principal=principal, operation=operation, idempotency_key=idempotency_key,
                    request_digest=request_digest, status="rejected", code=rejected.code, message=rejected.message,
                    resource_ref=rejected.resource_ref, before_revision=rejected.before_revision, after_revision=None,
                )
            except db.ActionQError:
                # A real service-level failure (e.g. the schema became
                # incompatible mid-transaction) is not a malformed *command*
                # -- it must keep normal exception/retry semantics instead of
                # being durably recorded as a permanently-replayed rejection.
                # db.ActionQError subclasses ValueError, so it has to be
                # excluded explicitly before the broad except below.
                raise
            except (TypeError, ValueError, AttributeError) as malformed:
                # A malformed payload must still be durably recorded once its
                # idempotency identity is bound above -- otherwise a retry with
                # the same identity re-attempts the same crash on every call
                # instead of replaying a stored decision.
                return self._record_decision(
                    conn, principal=principal, operation=operation, idempotency_key=idempotency_key,
                    request_digest=request_digest, status="rejected", code="malformed-command",
                    message=f"command payload could not be validated: {malformed}",
                    resource_ref=None, before_revision=None, after_revision=None,
                )
            return self._record_decision(
                conn, principal=principal, operation=operation, idempotency_key=idempotency_key,
                request_digest=request_digest, status="accepted", code="accepted", message="federation command recorded",
                resource_ref=resource_ref, before_revision=before, after_revision=after,
            )

    def create(self, *, principal: FederationPrincipal, idempotency_key: str,
               expected_revision: int, resource_ref: str | None = None) -> CommandDecision:
        request = {"expected_revision": expected_revision, "resource_ref": resource_ref}
        def apply(conn: Any) -> tuple[str, int, int]:
            self._require_authority(principal, "federation.create")
            self._require_revision(expected_revision)
            if expected_revision != 0:
                raise _Rejected("expected-absence-required", "create requires expected_revision 0")
            selected = resource_ref or _new_resource_ref()
            self._require_resource_ref(selected)
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"federation-resource/v1:{selected}",))
            if self._resource(conn, selected) is not None:
                raise _Rejected("resource-exists", "federation resource already exists", resource_ref=selected)
            conn.execute(
                f"INSERT INTO {self._q('federation_resources')} "
                "(resource_ref, owner_principal_id, state, revision, recovery_floor) VALUES (%s,%s,'registered',1,0)",
                (selected, principal.principal_id),
            )
            change_payload = {"owner_principal_id": principal.principal_id}
            conn.execute(
                f"INSERT INTO {self._q('federation_resource_changes')} "
                "(resource_ref, revision, operation, state, actor_principal_id, payload_bytes, payload_digest) "
                "VALUES (%s,1,'create','registered',%s,%s,%s)",
                (selected, principal.principal_id, canonical_bytes(change_payload), contract_digest(change_payload)),
            )
            return selected, 0, 1
        return self._execute(principal=principal, operation="create", idempotency_key=idempotency_key, request=request, apply=apply)

    def add_relation(self, *, principal: FederationPrincipal, idempotency_key: str,
                     source_ref: str, relation_type: str, target_ref: str,
                     expected_revision: int) -> CommandDecision:
        request = {"source_ref": source_ref, "relation_type": relation_type, "target_ref": target_ref, "expected_revision": expected_revision}
        def apply(conn: Any) -> tuple[str, int, int]:
            self._require_authority(principal, "federation.relate")
            self._require_resource_ref(source_ref)
            self._require_resource_ref(target_ref)
            if relation_type not in RELATION_TYPES:
                raise _Rejected("invalid-relation-type", "relation type is not supported")
            # Opposite-direction relation attempts must not each inspect a
            # graph that predates the other edge.  Lock both endpoints in a
            # stable order before checking ownership, existence and cycles.
            for locked_ref in sorted((source_ref, target_ref)):
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"federation-resource/v1:{locked_ref}",),
                )
            if relation_type in CYCLIC_RELATION_TYPES:
                # Endpoint locks alone only serialize writes that share an
                # endpoint. Two concurrent adds with pairwise-disjoint
                # endpoints (e.g. existing A->B, C->D; concurrent B->C and
                # D->A) would each pass a cycle probe against committed-only
                # state and jointly create a cycle. A lock scoped to the
                # relation type serializes every cycle-checked write of that
                # type, closing the gap.
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"federation-cycle/v1:{relation_type}",),
                )
            source = self._locked_existing(conn, principal=principal, resource_ref=source_ref, expected_revision=expected_revision, owner_required=True)
            if source_ref == target_ref:
                raise _Rejected("self-relation", "self relations are forbidden", resource_ref=source_ref, before_revision=int(source["revision"]))
            target = self._resource(conn, target_ref, lock=False)
            if target is None:
                raise _Rejected("target-not-found", "relation target does not exist", resource_ref=source_ref, before_revision=int(source["revision"]))
            if target["state"] == "superseded":
                raise _Rejected("target-superseded", "relation target is superseded and immutable", resource_ref=source_ref, before_revision=int(source["revision"]))
            exists = conn.execute(
                f"SELECT 1 FROM {self._q('federation_relations')} WHERE source_ref=%s AND relation_type=%s AND target_ref=%s",
                (source_ref, relation_type, target_ref),
            ).fetchone()
            if exists:
                raise _Rejected("relation-exists", "directed relation already exists", resource_ref=source_ref, before_revision=int(source["revision"]))
            if relation_type in CYCLIC_RELATION_TYPES:
                cycle = conn.execute(
                    f"WITH RECURSIVE reachable(ref) AS ("
                    f"SELECT target_ref FROM {self._q('federation_relations')} WHERE source_ref=%s AND relation_type=%s "
                    f"UNION SELECT relation.target_ref FROM {self._q('federation_relations')} relation JOIN reachable ON relation.source_ref=reachable.ref WHERE relation.relation_type=%s) "
                    "SELECT 1 FROM reachable WHERE ref=%s LIMIT 1",
                    (target_ref, relation_type, relation_type, source_ref),
                ).fetchone()
                if cycle:
                    raise _Rejected("relation-cycle", "relation would create a directed cycle", resource_ref=source_ref, before_revision=int(source["revision"]))
            def insert(after: int) -> None:
                conn.execute(
                    f"INSERT INTO {self._q('federation_relations')} (source_ref, relation_type, target_ref, source_revision) VALUES (%s,%s,%s,%s)",
                    (source_ref, relation_type, target_ref, after),
                )
            before, after = self._advance(conn, row=source, principal=principal, operation="add-relation", state=str(source["state"]), payload=request, side_effect=insert)
            return source_ref, before, after
        return self._execute(principal=principal, operation="add-relation", idempotency_key=idempotency_key, request=request, apply=apply)

    def record_execution_ref(self, *, principal: FederationPrincipal, idempotency_key: str,
                             resource_ref: str, execution_ref: str, assurance_type: str,
                             expected_revision: int) -> CommandDecision:
        request = {"resource_ref": resource_ref, "execution_ref": execution_ref, "assurance_type": assurance_type, "expected_revision": expected_revision}
        def apply(conn: Any) -> tuple[str, int, int]:
            self._require_authority(principal, "federation.relate")
            row = self._locked_existing(conn, principal=principal, resource_ref=resource_ref, expected_revision=expected_revision, owner_required=True)
            if not isinstance(execution_ref, str) or not execution_ref:
                raise _Rejected("invalid-execution-reference", "execution_ref is required", resource_ref=resource_ref, before_revision=int(row["revision"]))
            if not isinstance(assurance_type, str) or not assurance_type:
                raise _Rejected("invalid-assurance-type", "assurance_type is required", resource_ref=resource_ref, before_revision=int(row["revision"]))
            exists = conn.execute(f"SELECT 1 FROM {self._q('federation_execution_refs')} WHERE resource_ref=%s AND execution_ref=%s", (resource_ref, execution_ref)).fetchone()
            if exists:
                raise _Rejected("execution-reference-exists", "execution reference already exists", resource_ref=resource_ref, before_revision=int(row["revision"]))
            def insert(after: int) -> None:
                conn.execute(f"INSERT INTO {self._q('federation_execution_refs')} (resource_ref, execution_ref, assurance_type, source_revision) VALUES (%s,%s,%s,%s)", (resource_ref, execution_ref, assurance_type, after))
            before, after = self._advance(conn, row=row, principal=principal, operation="record-execution-ref", state=str(row["state"]), payload=request, side_effect=insert)
            return resource_ref, before, after
        return self._execute(principal=principal, operation="record-execution-ref", idempotency_key=idempotency_key, request=request, apply=apply)

    def record_evidence(self, *, principal: FederationPrincipal, idempotency_key: str,
                        resource_ref: str, evidence_ref: str, evidence_bytes: bytes,
                        assurance_type: str, expected_revision: int) -> CommandDecision:
        # evidence_bytes may be malformed (wrong type); the digest still has to
        # be deterministic over the raw request so idempotency replay is
        # correct, and the type is validated inside apply() so a bad type is
        # durably recorded as a rejected decision rather than raised before
        # the command even reaches the idempotency machinery.
        valid_evidence_bytes = isinstance(evidence_bytes, (bytes, bytearray))
        if valid_evidence_bytes:
            actual_digest = _raw_digest(bytes(evidence_bytes))
        else:
            try:
                descriptor = repr(evidence_bytes)
            except Exception:
                # A malformed value's own __repr__ can itself raise; fall
                # back to something deterministic per type so the digest
                # below (and idempotent replay of the resulting rejection)
                # doesn't depend on succeeding at describing the bad input.
                descriptor = f"<unrepresentable {type(evidence_bytes).__name__}>"
            actual_digest = _raw_digest(descriptor.encode("utf-8", errors="replace"))
        request = {
            "resource_ref": resource_ref, "evidence_ref": evidence_ref, "evidence_digest": actual_digest,
            # A raw byte string is an arbitrary, externally-fixed content-
            # addressing input (actual_digest above must stay plain
            # sha256(evidence_bytes) with no added prefix, since honest
            # callers independently compute that same hash to construct
            # evidence_ref) -- so its value space can, by construction,
            # contain the same bytes as some non-bytes value's repr()
            # descriptor. Without this field, that coincidence collides the
            # two paths' canonical request bytes on the same idempotency
            # identity, and a malformed submission silently replays an
            # unrelated valid one's accepted decision instead of being
            # independently evaluated. Recording which path produced
            # evidence_digest keeps the two paths' request bytes distinct
            # regardless of any digest coincidence.
            "evidence_bytes_valid": valid_evidence_bytes,
            "assurance_type": assurance_type, "expected_revision": expected_revision,
        }
        def apply(conn: Any) -> tuple[str, int, int]:
            self._require_authority(principal, "federation.evidence.ingest")
            if not valid_evidence_bytes:
                raise _Rejected("invalid-evidence-bytes", "evidence_bytes must be bytes")
            row = self._locked_existing(conn, principal=principal, resource_ref=resource_ref, expected_revision=expected_revision)
            if str(row["state"]) not in {"registered", "evidence-recorded"}:
                raise _Rejected("invalid-state-transition", "evidence may only be recorded before acceptance", resource_ref=resource_ref, before_revision=int(row["revision"]))
            if not isinstance(assurance_type, str) or not assurance_type:
                raise _Rejected("invalid-assurance-type", "assurance_type is required", resource_ref=resource_ref, before_revision=int(row["revision"]))
            expected_ref = "artifact:" + actual_digest
            if evidence_ref != expected_ref:
                raise _Rejected("evidence-digest-mismatch", "evidence bytes do not match the content-addressed reference", resource_ref=resource_ref, before_revision=int(row["revision"]))
            exists = conn.execute(f"SELECT 1 FROM {self._q('federation_evidence')} WHERE resource_ref=%s AND evidence_ref=%s", (resource_ref, evidence_ref)).fetchone()
            if exists:
                raise _Rejected("evidence-exists", "evidence reference already exists", resource_ref=resource_ref, before_revision=int(row["revision"]))
            def insert(after: int) -> None:
                conn.execute(f"INSERT INTO {self._q('federation_evidence')} (resource_ref, evidence_ref, evidence_digest, assurance_type, source_revision) VALUES (%s,%s,%s,%s,%s)", (resource_ref, evidence_ref, actual_digest, assurance_type, after))
            before, after = self._advance(conn, row=row, principal=principal, operation="record-evidence", state="evidence-recorded", payload=request, side_effect=insert)
            return resource_ref, before, after
        return self._execute(principal=principal, operation="record-evidence", idempotency_key=idempotency_key, request=request, apply=apply)

    def decide_acceptance(self, *, principal: FederationPrincipal, idempotency_key: str,
                          resource_ref: str, outcome: str, policy_ref: str,
                          evidence_ref: str | None, expected_revision: int) -> CommandDecision:
        request = {"resource_ref": resource_ref, "outcome": outcome, "policy_ref": policy_ref, "evidence_ref": evidence_ref, "expected_revision": expected_revision}
        def apply(conn: Any) -> tuple[str, int, int]:
            self._require_authority(principal, "federation.acceptance.decide")
            row = self._locked_existing(conn, principal=principal, resource_ref=resource_ref, expected_revision=expected_revision)
            state = str(row["state"])
            if outcome not in {"accepted", "rejected"} or not isinstance(policy_ref, str) or not policy_ref:
                raise _Rejected("invalid-acceptance", "outcome and named policy are required", resource_ref=resource_ref, before_revision=int(row["revision"]))
            if evidence_ref is not None and not isinstance(evidence_ref, str):
                # Checked unconditionally, not only on the accepted branch
                # below: on the rejected branch evidence_ref (nullable) goes
                # straight to the INSERT with no type guard at all, so a
                # non-str/non-None value either got silently assignment-cast
                # and durably stored under a status="accepted" response, or
                # (for a type psycopg can't cast at all) leaked a bare
                # psycopg error past _execute's durability handler.
                raise _Rejected("invalid-evidence-reference", "evidence_ref must be a string or None", resource_ref=resource_ref, before_revision=int(row["revision"]))
            allowed = state == "evidence-recorded" if outcome == "accepted" else state in {"registered", "evidence-recorded"}
            if not allowed:
                raise _Rejected("invalid-state-transition", "acceptance decision is invalid for current state", resource_ref=resource_ref, before_revision=int(row["revision"]))
            if outcome == "accepted" and (not isinstance(evidence_ref, str) or not evidence_ref):
                raise _Rejected("evidence-required", "acceptance requires cited verified evidence", resource_ref=resource_ref, before_revision=int(row["revision"]))
            if evidence_ref is not None:
                # Checked for both outcomes, not just accepted: a rejection
                # citing an evidence_ref that was never actually submitted via
                # record_evidence would otherwise be durably recorded with a
                # fabricated citation, asymmetric with the accepted path's
                # verification below.
                found = conn.execute(f"SELECT 1 FROM {self._q('federation_evidence')} WHERE resource_ref=%s AND evidence_ref=%s", (resource_ref, evidence_ref)).fetchone()
                if found is None:
                    raise _Rejected("evidence-not-found", "cited evidence was not verified for this resource", resource_ref=resource_ref, before_revision=int(row["revision"]))
            def insert(after: int) -> None:
                conn.execute(f"INSERT INTO {self._q('federation_acceptance_decisions')} (resource_ref, source_revision, outcome, policy_ref, evidence_ref, decided_by) VALUES (%s,%s,%s,%s,%s,%s)", (resource_ref, after, outcome, policy_ref, evidence_ref, principal.principal_id))
            before, after = self._advance(conn, row=row, principal=principal, operation="decide-acceptance", state=outcome, payload=request, side_effect=insert)
            return resource_ref, before, after
        return self._execute(principal=principal, operation="decide-acceptance", idempotency_key=idempotency_key, request=request, apply=apply)

    def record_settlement(self, *, principal: FederationPrincipal, idempotency_key: str,
                          resource_ref: str, fact_ref: str, expected_revision: int) -> CommandDecision:
        request = {"resource_ref": resource_ref, "fact_ref": fact_ref, "expected_revision": expected_revision}
        def apply(conn: Any) -> tuple[str, int, int]:
            self._require_authority(principal, "federation.settlement.record")
            row = self._locked_existing(conn, principal=principal, resource_ref=resource_ref, expected_revision=expected_revision)
            if str(row["state"]) not in {"accepted", "rejected"} or not isinstance(fact_ref, str) or not fact_ref:
                raise _Rejected("invalid-state-transition", "settlement requires accepted/rejected state and a cited fact", resource_ref=resource_ref, before_revision=int(row["revision"]))
            exists = conn.execute(f"SELECT 1 FROM {self._q('federation_settlements')} WHERE resource_ref=%s", (resource_ref,)).fetchone()
            if exists:
                raise _Rejected("settlement-exists", "resource has already been settled", resource_ref=resource_ref, before_revision=int(row["revision"]))
            def insert(after: int) -> None:
                conn.execute(f"INSERT INTO {self._q('federation_settlements')} (resource_ref, source_revision, fact_ref, reconciled_by) VALUES (%s,%s,%s,%s)", (resource_ref, after, fact_ref, principal.principal_id))
            before, after = self._advance(conn, row=row, principal=principal, operation="record-settlement", state=str(row["state"]), payload=request, side_effect=insert)
            return resource_ref, before, after
        return self._execute(principal=principal, operation="record-settlement", idempotency_key=idempotency_key, request=request, apply=apply)

    def supersede(self, *, principal: FederationPrincipal, idempotency_key: str,
                  resource_ref: str, expected_revision: int) -> CommandDecision:
        request = {"resource_ref": resource_ref, "expected_revision": expected_revision}
        def apply(conn: Any) -> tuple[str, int, int]:
            self._require_authority(principal, "federation.supersede")
            row = self._locked_existing(conn, principal=principal, resource_ref=resource_ref, expected_revision=expected_revision, owner_required=True)
            before, after = self._advance(conn, row=row, principal=principal, operation="supersede", state="superseded", payload=request)
            return resource_ref, before, after
        return self._execute(principal=principal, operation="supersede", idempotency_key=idempotency_key, request=request, apply=apply)

    def snapshot(self, *, principal: FederationPrincipal, resource_ref: str) -> dict[str, Any]:
        if not isinstance(resource_ref, str) or not RESOURCE_RE.fullmatch(resource_ref):
            raise db.ActionQError("resource_ref is not a federation v1 reference")
        if "federation.read" not in principal.authorities:
            raise db.ActionQError("federation.read authority is required")
        with self.connection() as conn, conn.transaction():
            federation_schema.require_compatible(conn, self.schema)
            row = self._resource(conn, resource_ref, lock=False)
            if row is None:
                raise db.ActionQError("federation resource not found")
            return {
                "schema_version": "federation-resource/v1", "resource_ref": row["resource_ref"],
                "owner_principal_id": row["owner_principal_id"], "state": row["state"],
                "revision": int(row["revision"]), "recovery_floor": int(row["recovery_floor"]),
            }
