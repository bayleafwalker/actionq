"""ActionQ-owned observable action-root resources.

This module deliberately does not expose lifecycle commands.  It projects the
existing queue state into the frozen ``actionq.action-resource-owner/v1``
contract and keeps its revision/retention namespace independent of queue event
ids.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag

from . import db


RESOURCE_RE = re.compile(r"^aqr1_[A-Za-z0-9_-]{43}$")
TERMINAL = {"completed", "failed", "rejected", "cancelled"}
OUTCOMES = TERMINAL
PROJECTION_KEYS = frozenset({"resource_ref", "revision", "state", "terminal", "outcome", "created_at", "updated_at", "execution_sessions"})
SESSION_KEYS = frozenset({"ordinal", "state", "started_at", "ended_at"})
EVENT_KEYS = frozenset({"revision", "kind", "state", "terminal", "outcome", "occurred_at"})
ROOT_STORAGE_KEYS = frozenset({"resource_ref", "action_id", "principal_scope", "operation", "idempotency_key", "request_digest", "request_snapshot", "revision", "enqueue_revision", "recovery_floor", "state", "created_at", "updated_at"})
SESSION_STORAGE_KEYS = SESSION_KEYS
EVENT_STORAGE_KEYS = frozenset({"revision", "kind", "state", "occurred_at"})


class ResourceNotFound(db.ActionQError):
    pass


class CursorExpired(db.ActionQError):
    def __init__(self, recovery_floor: int):
        super().__init__("cursor expired")
        self.recovery_floor = recovery_floor


class IdempotencyConflict(db.ActionQError):
    pass


def canonical_request(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def serialize_envelope(value: dict[str, Any]) -> bytes:
    """Serialize an owner envelope in the frozen UTF-8 canonical wire form."""
    return canonical_request(value) + b"\n"


def new_resource_ref() -> str:
    return db.new_opaque_ref("aqr1_")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _projection(row: dict[str, Any], sessions: list[dict[str, Any]]) -> dict[str, Any]:
    unknown_root = set(row) - ROOT_STORAGE_KEYS
    unknown_sessions = [set(session) - SESSION_STORAGE_KEYS for session in sessions]
    if unknown_root or any(unknown_sessions):
        raise db.ActionQError("unknown action resource owner field")
    state = str(row["state"])
    result = {
        "resource_ref": row["resource_ref"], "revision": int(row["revision"]),
        "state": state, "terminal": state in TERMINAL,
        "outcome": state if state in OUTCOMES else None,
        "created_at": _iso(row["created_at"]), "updated_at": _iso(row["updated_at"]),
        "execution_sessions": [
            {"ordinal": int(session["ordinal"]), "state": session["state"],
             "started_at": _iso(session["started_at"]), "ended_at": _iso(session["ended_at"])}
            for session in sessions
        ],
    }
    if set(result) != PROJECTION_KEYS or not all(set(item) == SESSION_KEYS for item in result["execution_sessions"]):
        raise db.ActionQError("action resource projection allowlist violation")
    return result


def _event(row: dict[str, Any]) -> dict[str, Any]:
    if set(row) != EVENT_STORAGE_KEYS:
        raise db.ActionQError("unknown action resource event field")
    state = str(row["state"])
    result = {"revision": int(row["revision"]), "kind": row["kind"], "state": state,
              "terminal": state in TERMINAL, "outcome": state if state in OUTCOMES else None,
              "occurred_at": _iso(row["occurred_at"])}
    if set(result) != EVENT_KEYS:
        raise db.ActionQError("action resource event allowlist violation")
    return result


@dataclass(frozen=True)
class EnqueueResult:
    resource_ref: str
    revision: int
    original_retry: bool


class ActionResourceOwner:
    def __init__(self, *, schema: str, connection: Callable[[], Any], cursor_secret: bytes):
        if len(cursor_secret) < 32:
            raise ValueError("cursor secret must contain at least 32 bytes")
        self.schema, self.connection, self.cursor_secret = schema, connection, cursor_secret

    def _cursor(self, resource_ref: str, principal_scope: str, revision: int) -> str:
        raw = json.dumps([resource_ref, principal_scope, revision], separators=(",", ":")).encode()
        nonce = secrets.token_bytes(12)
        encrypted = AESGCM(hashlib.sha256(self.cursor_secret).digest()).encrypt(nonce, raw, b"actionq-cursor/v1")
        return "aqc1_" + base64.urlsafe_b64encode(nonce + encrypted).rstrip(b"=").decode()

    def _parse_cursor(self, cursor: str, resource_ref: str, principal_scope: str) -> int:
        try:
            prefix, payload = cursor.split("_", 1)
            packed = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
            raw = AESGCM(hashlib.sha256(self.cursor_secret).digest()).decrypt(packed[:12], packed[12:], b"actionq-cursor/v1")
            decoded = json.loads(raw)
            if prefix != "aqc1" or decoded[:2] != [resource_ref, principal_scope]:
                raise ValueError
            revision = decoded[2]
            if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
                raise ValueError
            return revision
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, UnicodeError, binascii.Error, InvalidTag) as exc:
            raise ResourceNotFound("resource not found") from exc

    def enqueue(self, *, principal_scope: str, operation: str, idempotency_key: str,
                request: dict[str, Any], create_action: Callable[[Any], dict[str, Any]]) -> EnqueueResult:
        raw = canonical_request(request)
        digest = hashlib.sha256(raw).hexdigest()
        with self.connection() as conn, conn.transaction():
            lock = "\x1f".join(("action-resource/v1", principal_scope, operation, idempotency_key))
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock,))
            prior = conn.execute(
                f"SELECT resource_ref, enqueue_revision, request_digest, request_snapshot FROM {db.qname(self.schema, 'action_resources')} WHERE principal_scope=%s AND operation=%s AND idempotency_key=%s",
                (principal_scope, operation, idempotency_key),
            ).fetchone()
            if prior:
                if bytes(prior["request_snapshot"]) != raw or prior["request_digest"] != digest:
                    raise IdempotencyConflict("idempotency conflict")
                return EnqueueResult(prior["resource_ref"], int(prior["enqueue_revision"]), True)
            action = create_action(conn)
            ref = new_resource_ref()
            inserted = conn.execute(
                f"INSERT INTO {db.qname(self.schema, 'action_resources')} (resource_ref, action_id, principal_scope, operation, idempotency_key, request_digest, request_snapshot, revision, enqueue_revision, recovery_floor, state) VALUES (%s,%s,%s,%s,%s,%s,%s,1,1,0,'pending') RETURNING resource_ref, enqueue_revision",
                (ref, action["id"], principal_scope, operation, idempotency_key, digest, raw),
            ).fetchone()
            conn.execute(f"INSERT INTO {db.qname(self.schema, 'action_resource_changes')} (resource_ref, revision, kind, state) VALUES (%s,1,'state_changed','pending')", (ref,))
            return EnqueueResult(inserted["resource_ref"], int(inserted["enqueue_revision"]), False)

    def _lookup(self, conn: Any, resource_ref: str, principal_scope: str, *, lock: bool = False) -> dict[str, Any]:
        if not RESOURCE_RE.fullmatch(resource_ref):
            raise ResourceNotFound("resource not found")
        suffix = " FOR UPDATE" if lock else ""
        row = conn.execute(f"SELECT * FROM {db.qname(self.schema, 'action_resources')} WHERE resource_ref=%s AND principal_scope=%s{suffix}", (resource_ref, principal_scope)).fetchone()
        if not row:
            raise ResourceNotFound("resource not found")
        return row

    def snapshot(self, *, resource_ref: str, principal_scope: str) -> dict[str, Any]:
        if not RESOURCE_RE.fullmatch(resource_ref):  # required zero-DB malformed path
            raise ResourceNotFound("resource not found")
        with self.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = self._lookup(conn, resource_ref, principal_scope)
            sessions = conn.execute(f"SELECT ordinal, state, started_at, ended_at FROM {db.qname(self.schema, 'action_resource_sessions')} WHERE resource_ref=%s ORDER BY ordinal", (resource_ref,)).fetchall()
            revision = int(row["revision"])
            return {"schema_version": "resource-snapshot/v1", "resource_ref": resource_ref,
                    "revision": revision, "cursor": self._cursor(resource_ref, principal_scope, revision),
                    "recovery_floor": int(row["recovery_floor"]), "projection": _projection(row, list(sessions))}

    def changes(self, *, resource_ref: str, principal_scope: str, cursor: str, wait_seconds: int = 0,
                cancel_event: Any | None = None) -> dict[str, Any]:
        if isinstance(wait_seconds, bool) or not isinstance(wait_seconds, int) or not 0 <= wait_seconds <= 30:
            raise ValueError("wait_seconds must be an integer from 0 through 30")
        after = self._parse_cursor(cursor, resource_ref, principal_scope)
        deadline = time.monotonic() + wait_seconds
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise ConnectionError("observer disconnected")
            with self.connection() as conn, conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                root = self._lookup(conn, resource_ref, principal_scope)
                floor, latest = int(root["recovery_floor"]), int(root["revision"])
                if after < floor:
                    raise CursorExpired(floor)
                if after > latest:
                    raise ResourceNotFound("resource not found")
                rows = conn.execute(f"SELECT revision, kind, state, occurred_at FROM {db.qname(self.schema, 'action_resource_changes')} WHERE resource_ref=%s AND revision>%s ORDER BY revision", (resource_ref, after)).fetchall()
                events = [_event(dict(row)) for row in rows]
                position = events[-1]["revision"] if events else after
                result = {"schema_version": "resource-changes/v1", "resource_ref": resource_ref,
                          "from_cursor": cursor, "cursor": self._cursor(resource_ref, principal_scope, position),
                          "recovery_floor": floor, "changes": events}
                if events or root["state"] in TERMINAL or wait_seconds == 0 or time.monotonic() >= deadline:
                    return result
            time.sleep(min(.1, max(0, deadline - time.monotonic())))

    def project_state(self, conn: Any, *, action_id: int, expected_claim_receipt: str | None = None,
                      kind: str = "state_changed") -> int | None:
        """Project authoritative queue state after its fenced mutation.

        The caller cannot supply lifecycle state.  When projecting a claimed
        action it must present the receipt consumed by the authoritative
        transaction, preventing a stale session from publishing a transition.
        """
        action = conn.execute(
            f"SELECT status, claim_receipt FROM {db.qname(self.schema, 'actions')} WHERE id=%s",
            (action_id,),
        ).fetchone()
        if not action:
            return None
        state = action["status"]
        if expected_claim_receipt is not None and not hmac.compare_digest(str(action["claim_receipt"] or ""), expected_claim_receipt):
            raise db.ClaimRejected("stale action-resource projection", reason="claim-receipt-mismatch", action_id=action_id, requested_by="action-resource-owner")
        if state == "claimed" and expected_claim_receipt is None:
            raise db.ClaimRejected("stale action-resource projection", reason="claim-receipt-required", action_id=action_id, requested_by="action-resource-owner")
        row = conn.execute(f"UPDATE {db.qname(self.schema, 'action_resources')} SET revision=revision+1, state=%s, updated_at=now() WHERE action_id=%s RETURNING resource_ref, revision", (state, action_id)).fetchone()
        if not row:
            return None
        conn.execute(f"INSERT INTO {db.qname(self.schema, 'action_resource_changes')} (resource_ref, revision, kind, state) VALUES (%s,%s,%s,%s)", (row["resource_ref"], row["revision"], kind, state))
        return int(row["revision"])

    def prune(self, *, resource_ref: str, through_revision: int) -> int:
        if isinstance(through_revision, bool) or not isinstance(through_revision, int) or through_revision < 0:
            raise ValueError("through_revision must be non-negative")
        with self.connection() as conn, conn.transaction():
            root = conn.execute(f"SELECT revision, recovery_floor FROM {db.qname(self.schema, 'action_resources')} WHERE resource_ref=%s FOR UPDATE", (resource_ref,)).fetchone()
            if not root:
                raise ResourceNotFound("resource not found")
            floor = max(int(root["recovery_floor"]), min(through_revision, int(root["revision"])))
            deleted = conn.execute(f"DELETE FROM {db.qname(self.schema, 'action_resource_changes')} WHERE resource_ref=%s AND revision<=%s RETURNING revision", (resource_ref, floor)).fetchall()
            conn.execute(f"UPDATE {db.qname(self.schema, 'action_resources')} SET recovery_floor=%s WHERE resource_ref=%s", (floor, resource_ref))
            return len(deleted)
