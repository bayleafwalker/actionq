"""ActionQ's durable served session-completion observation log.

This module is deliberately separate from ``events``.  Completion observations
describe a harness/session terminal fact; they are not action settlement or a
queue command.  The tables are append-only apart from the independent
acknowledgement and recovery-watermark records.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any

from . import db


SCHEMA_VERSION = "session.completion-observed/v1"
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_PROJECT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TERMINAL_REASONS = {
    "succeeded": {"completed"},
    "failed": {"process-exit", "start-failed"},
    "cancelled": {"cancelled"},
    "timed-out": {"timeout"},
    "usage-limited": {"usage-limit"},
    "end-inferred": {"crash-inferred"},
}
_PROHIBITED = {
    "prompt", "transcript", "rawoutput", "rawoutputref", "environment", "env",
    "token", "secret", "secrets", "credential", "credentials", "password",
    "claimproof", "claimtoken", "accesstoken", "clientsecret", "apikey",
    "worktree", "commandoutput", "failuredetails", "failuredetail",
    "requestsnapshot", "requestbody", "headers", "provenance", "stdout",
    "stderr", "stacktrace",
}
_SECRET_RE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+/=-]{8,}|\b(?:sk|ghp|github_pat)_[A-Za-z0-9_-]{8,})",
    re.IGNORECASE,
)


class CompletionValidationError(db.ActionQError):
    """The supplied observation is not a safe v1 completion event."""


class CompletionConflictError(db.ActionQError):
    """An idempotency identity was reused with different bytes."""

    def __init__(self, message: str, *, code: str = "digest-conflict"):
        super().__init__(message)
        self.code = code


class CompletionCursorExpired(db.ActionQError):
    """The requested cursor predates the durable replay floor."""

    def __init__(self, recovery_floor: int):
        super().__init__("completion cursor is below the durable recovery floor")
        self.recovery_floor = recovery_floor


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def payload_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def _require_string(value: Any, name: str, *, uuid: bool = False) -> None:
    if not isinstance(value, str) or not value.strip():
        raise CompletionValidationError(f"{name} must be a non-empty string")
    if uuid and not _UUID_RE.fullmatch(value):
        raise CompletionValidationError(f"{name} must be a lowercase UUID")


def _reject_prohibited(value: Any, location: str = "event") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[._\s-]", "", str(key).lower())
            if normalized in _PROHIBITED:
                raise CompletionValidationError(f"{location}.{key} is prohibited")
            _reject_prohibited(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_prohibited(child, f"{location}[{index}]")
    elif isinstance(value, str):
        if _SECRET_RE.search(value) or value.startswith(("/", "\\")) or re.match(r"^[A-Za-z]:[\\/]", value):
            raise CompletionValidationError(f"{location} contains prohibited secret or absolute path content")


def _utc(value: Any, name: str) -> datetime:
    _require_string(value, name)
    if not value.endswith("Z"):
        raise CompletionValidationError(f"{name} must be an ISO UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise CompletionValidationError(f"{name} must be an ISO UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise CompletionValidationError(f"{name} must be an ISO UTC timestamp")
    return parsed


def validate_event(event: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise CompletionValidationError("completion event must be an object")
    _reject_prohibited(event)
    required = (
        "schema_version", "event_id", "origin_stream_id", "origin_sequence",
        "runtime_session_id", "attempt_id", "action_id", "repo", "harness",
        "model", "terminal", "started_at", "completed_at", "observed_at",
        "duration_ms", "refs", "evidence", "privacy",
    )
    for field in required:
        if field not in event:
            raise CompletionValidationError(f"completion event missing {field}")
    if event["schema_version"] != SCHEMA_VERSION:
        raise CompletionValidationError(f"schema_version must be {SCHEMA_VERSION}")
    _require_string(event["event_id"], "event_id", uuid=True)
    _require_string(event["origin_stream_id"], "origin_stream_id", uuid=True)
    if not isinstance(event["origin_sequence"], int) or isinstance(event["origin_sequence"], bool) or event["origin_sequence"] < 1:
        raise CompletionValidationError("origin_sequence must be a positive integer")
    _require_string(event["runtime_session_id"], "runtime_session_id")
    for field in ("attempt_id", "action_id"):
        if event[field] is not None:
            _require_string(event[field], field)
    if (event["attempt_id"] is None) != (event["action_id"] is None):
        raise CompletionValidationError("attempt_id and action_id must both be present or both be null")
    repo = event["repo"]
    if not isinstance(repo, dict):
        raise CompletionValidationError("repo must be an object")
    _require_string(repo.get("project"), "repo.project")
    if not _PROJECT_RE.fullmatch(repo["project"]):
        raise CompletionValidationError("repo.project must be a portable identifier")
    if repo.get("repo_id") is not None:
        _require_string(repo["repo_id"], "repo.repo_id", uuid=True)
    _require_string(event["harness"], "harness")
    if event.get("actor") is not None:
        _require_string(event["actor"], "actor")
    if event["model"] is not None:
        if not isinstance(event["model"], dict):
            raise CompletionValidationError("model must be an object or null")
        _require_string(event["model"].get("name"), "model.name")
        if event["model"].get("version") is not None and not isinstance(event["model"]["version"], str):
            raise CompletionValidationError("model.version must be a string")
    terminal = event["terminal"]
    if not isinstance(terminal, dict) or terminal.get("kind") not in _TERMINAL_REASONS:
        raise CompletionValidationError("terminal.kind is not recognized")
    if terminal.get("reason_code") not in _TERMINAL_REASONS[terminal["kind"]]:
        raise CompletionValidationError("terminal.reason_code does not match terminal.kind")
    if not isinstance(terminal.get("retryable"), bool):
        raise CompletionValidationError("terminal.retryable must be boolean")
    exit_code = terminal.get("exit_code")
    if exit_code is not None and (not isinstance(exit_code, int) or isinstance(exit_code, bool)):
        raise CompletionValidationError("terminal.exit_code must be an integer or null")
    if terminal["kind"] == "succeeded" and (exit_code != 0 or terminal["retryable"]):
        raise CompletionValidationError("succeeded completion must have exit_code 0 and retryable false")
    if terminal["reason_code"] == "process-exit" and (exit_code is None or exit_code == 0):
        raise CompletionValidationError("process-exit must have a non-zero exit code")
    if terminal["kind"] in {"cancelled", "timed-out", "usage-limited", "end-inferred"} and exit_code is not None:
        raise CompletionValidationError(f"{terminal['kind']} must have a null exit code")
    started, completed, observed = (_utc(event[field], field) for field in ("started_at", "completed_at", "observed_at"))
    if completed < started or observed < completed:
        raise CompletionValidationError("completion timestamps are out of order")
    duration = event["duration_ms"]
    if duration is not None and (not isinstance(duration, int) or isinstance(duration, bool) or duration < 0):
        raise CompletionValidationError("duration_ms must be a non-negative integer or null")
    if not isinstance(event["refs"], list):
        raise CompletionValidationError("refs must be an array")
    for ref in event["refs"]:
        if not isinstance(ref, dict):
            raise CompletionValidationError("refs entries must be objects")
        for field in ("kind", "source", "revision"):
            _require_string(ref.get(field), f"refs[].{field}")
    evidence = event["evidence"]
    verification = evidence.get("verification") if isinstance(evidence, dict) else None
    if not isinstance(evidence, dict) or not isinstance(evidence.get("dirty"), bool) or not isinstance(evidence.get("commit_count"), int) or evidence["commit_count"] < 0 or not isinstance(verification, dict):
        raise CompletionValidationError("evidence is invalid")
    for field in ("pass", "fail", "error"):
        if not isinstance(verification.get(field), int) or verification[field] < 0:
            raise CompletionValidationError(f"evidence.verification.{field} must be a non-negative integer")
    privacy = event["privacy"]
    if not isinstance(privacy, dict) or any(privacy.get(field) is not True for field in (
        "prompt_absent", "transcript_absent", "raw_output_absent", "environment_absent",
        "credentials_absent", "absolute_paths_absent", "claim_proofs_absent",
    )):
        raise CompletionValidationError("privacy assertions must all be true")
    return event


def _cursor(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        raise db.ActionQError("cursor must be a non-negative integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise db.ActionQError("cursor must be a non-negative integer") from exc
    if result < 0:
        raise db.ActionQError("cursor must be a non-negative integer")
    return result


def _safe_limit(value: Any) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise db.ActionQError("limit must be between 1 and 500") from exc
    if not 1 <= result <= 500:
        raise db.ActionQError("limit must be between 1 and 500")
    return result


def _event_row(row: Any) -> dict[str, Any]:
    result = dict(row)
    result.pop("server_cursor", None)
    return result


class CompletionLog:
    """PostgreSQL owner for the served completion observation stream."""

    def __init__(self, *, schema: str, connection_factory):
        self.schema = db.schema_name(schema)
        self.connection_factory = connection_factory

    def _q(self, relation: str) -> str:
        return db.qname(self.schema, relation)

    def _require_compatible(self, conn: Any) -> None:
        # Completion capability roles intentionally cannot inspect the queue
        # tables.  Their compatibility gate therefore checks only the
        # migration ledger and completion projection, never the authoritative
        # action/event shape.
        from . import schema as schema_contract
        row = conn.execute(
            f"SELECT max(version) AS version FROM {self._q(schema_contract.MIGRATION_TABLE)} WHERE domain=%s",
            (schema_contract.DOMAIN,),
        ).fetchone()
        if row is None or row["version"] is None or int(row["version"]) < schema_contract.MAX_SCHEMA_VERSION:
            raise schema_contract.SchemaCompatibilityError(
                "completion log requires the current ActionQ migration set"
            )
        conn.rollback()

    def ingest(self, event: dict[str, Any]) -> dict[str, Any]:
        validate_event(event)
        digest = payload_digest(event)
        with self.connection_factory() as conn:
            self._require_compatible(conn)
            with conn.transaction():
                meta = conn.execute(
                    f"SELECT last_cursor FROM {self._q('session_completion_watermarks')} WHERE singleton=1 FOR UPDATE"
                ).fetchone()
                if meta is None:
                    raise db.ActionQError("completion log watermark is not initialized")
                by_id = conn.execute(
                    f"SELECT server_cursor, event_digest FROM {self._q('session_completion_events')} WHERE event_id=%s",
                    (event["event_id"],),
                ).fetchone()
                by_position = conn.execute(
                    f"SELECT event_id, server_cursor, event_digest FROM {self._q('session_completion_stream_positions')} WHERE origin_stream_id=%s AND origin_sequence=%s",
                    (event["origin_stream_id"], event["origin_sequence"]),
                ).fetchone()
                if by_id is not None and by_id["event_digest"] != digest:
                    self._quarantine_durable(event, digest, "event-id-digest-conflict")
                    raise CompletionConflictError("event_id was reused with a different digest")
                if by_position is not None and (str(by_position["event_id"]) != event["event_id"] or by_position["event_digest"] != digest):
                    self._quarantine_durable(event, digest, "stream-position-digest-conflict")
                    raise CompletionConflictError("origin stream position was reused with a different digest", code="stream-position-conflict")
                if by_id is not None:
                    cursor = int(by_id["server_cursor"])
                    replayed = True
                elif by_position is not None:
                    # The event row may have crossed the server retention
                    # boundary, but the independent position and ACK records
                    # still make a producer retry safely idempotent.
                    cursor = int(by_position["server_cursor"])
                    replayed = True
                else:
                    row = conn.execute(
                        f"INSERT INTO {self._q('session_completion_events')} (event_id, origin_stream_id, origin_sequence, event_digest, payload) VALUES (%s,%s,%s,%s,%s::jsonb) RETURNING server_cursor, received_at",
                        (event["event_id"], event["origin_stream_id"], event["origin_sequence"], digest, json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
                    ).fetchone()
                    cursor = int(row["server_cursor"])
                    conn.execute(
                        f"INSERT INTO {self._q('session_completion_stream_positions')} (origin_stream_id, origin_sequence, event_id, server_cursor, event_digest) VALUES (%s,%s,%s,%s,%s)",
                        (event["origin_stream_id"], event["origin_sequence"], event["event_id"], cursor, digest),
                    )
                    conn.execute(
                        f"INSERT INTO {self._q('session_completion_acknowledgements')} (event_id, server_cursor, event_digest) VALUES (%s,%s,%s)",
                        (event["event_id"], cursor, digest),
                    )
                    conn.execute(
                        f"UPDATE {self._q('session_completion_watermarks')} SET last_cursor=%s, last_acknowledged_at=now() WHERE singleton=1",
                        (cursor,),
                    )
                    replayed = False
                ack = conn.execute(
                    f"SELECT acknowledged_at FROM {self._q('session_completion_acknowledgements')} WHERE event_id=%s",
                    (event["event_id"],),
                ).fetchone()
                return {
                    "schema_version": "session-completion-ack/v1",
                    "event_id": event["event_id"],
                    "origin_stream_id": event["origin_stream_id"],
                    "origin_sequence": event["origin_sequence"],
                    "event_digest": digest,
                    "server_cursor": cursor,
                    "acknowledged": ack is not None,
                    "acknowledged_at": db.json_default(ack["acknowledged_at"]) if ack else None,
                    "replayed": replayed,
                }

    def _quarantine(self, conn: Any, event: dict[str, Any], digest: str, reason: str) -> None:
        conn.execute(
            f"INSERT INTO {self._q('session_completion_quarantine')} (event_id, origin_stream_id, origin_sequence, event_digest, conflict_kind, summary) VALUES (%s,%s,%s,%s,%s,%s::jsonb)",
            (event.get("event_id"), event.get("origin_stream_id"), event.get("origin_sequence"), digest, reason, json.dumps({"schema_version": event.get("schema_version"), "event_id": event.get("event_id"), "origin_stream_id": event.get("origin_stream_id"), "origin_sequence": event.get("origin_sequence")}, sort_keys=True)),
        )

    def _quarantine_durable(self, event: dict[str, Any], digest: str, reason: str) -> None:
        # Conflict rejection rolls back the ingest transaction.  Quarantine is
        # an independent diagnostic fact and must survive that rollback.
        with self.connection_factory() as conn:
            with conn.transaction():
                self._quarantine(conn, event, digest, reason)

    def list(self, *, cursor: Any = None, limit: Any = 100, replay: bool = False) -> dict[str, Any]:
        requested = _cursor(cursor)
        page_size = _safe_limit(limit)
        with self.connection_factory() as conn:
            self._require_compatible(conn)
            meta = conn.execute(f"SELECT recovery_floor, last_cursor, retention_seconds FROM {self._q('session_completion_watermarks')} WHERE singleton=1").fetchone()
            if meta is None:
                raise db.ActionQError("completion log watermark is not initialized")
            floor, last_cursor = int(meta["recovery_floor"]), int(meta["last_cursor"])
            if requested < floor:
                # A stale cursor is not a resumable page.  In particular,
                # replay must not translate an omitted history into a new
                # checkpoint: the caller must explicitly recover from the
                # advertised floor (or a durable archive, if one is added).
                health = self.health(_conn=conn, _meta=meta)
                return {
                    "schema_version": "session-completion-cursor/v1",
                    "status": "cursor_expired",
                    "error": {
                        "code": "cursor_expired",
                        "message": "completion cursor is below the durable recovery floor",
                        "recovery_floor": floor,
                    },
                    "cursor": requested,
                    "recovery_floor": floor,
                    "server_cursor": last_cursor,
                    "next_cursor": requested,
                    "events": [],
                    "advance_cursor": False,
                    "gap": False,
                    "requires_replay": False,
                    "health": health,
                }
            start = requested
            gap = False
            rows = conn.execute(
                f"SELECT server_cursor, event_id, origin_stream_id, origin_sequence, event_digest, payload, received_at FROM {self._q('session_completion_events')} WHERE server_cursor>%s ORDER BY server_cursor LIMIT %s",
                (start, page_size),
            ).fetchall()
            events = []
            for row in rows:
                event = dict(row["payload"])
                events.append(event)
            next_cursor = int(rows[-1]["server_cursor"]) if rows else (start if gap and replay else requested)
            health = self.health(_conn=conn, _meta=meta)
            return {
                "schema_version": "session-completion-page/v1",
                "status": "ok",
                "cursor": requested,
                "events": events,
                "next_cursor": next_cursor,
                "server_cursor": last_cursor,
                "recovery_floor": floor,
                "advance_cursor": True,
                "gap": gap and not replay,
                "requires_replay": gap and not replay,
                "health": health,
            }

    def health(self, *, _conn: Any = None, _meta: Any = None) -> dict[str, Any]:
        owned = _conn is None
        if owned:
            context = self.connection_factory()
            context.__enter__()
            conn = context
        else:
            conn = _conn
        try:
            if owned:
                self._require_compatible(conn)
            meta = _meta or conn.execute(f"SELECT recovery_floor, last_cursor, last_acknowledged_at, retention_seconds FROM {self._q('session_completion_watermarks')} WHERE singleton=1").fetchone()
            counts = conn.execute(f"SELECT count(*) AS count, min(received_at) AS oldest FROM {self._q('session_completion_events')}").fetchone()
            quarantine = conn.execute(f"SELECT count(*) AS count FROM {self._q('session_completion_quarantine')}").fetchone()
            now = datetime.now(timezone.utc)
            oldest = counts["oldest"]
            age = max(0, int((now - oldest).total_seconds())) if oldest else None
            return {
                "schema_version": "session-completion-health/v1",
                "cursor": int(meta["last_cursor"]),
                "recovery_floor": int(meta["recovery_floor"]),
                "event_count": int(counts["count"]),
                "quarantine_count": int(quarantine["count"]),
                "oldest_event_age_seconds": age,
                "ingest_lag_seconds": age,
                "stream_lag_seconds": age,
                "retry_age_seconds": None,
                "retention_seconds": int(meta["retention_seconds"]),
                "last_acknowledged_at": db.json_default(meta["last_acknowledged_at"]) if meta.get("last_acknowledged_at") else None,
            }
        finally:
            if owned:
                context.__exit__(None, None, None)

    def compact(self, *, older_than_seconds: int | None = None) -> int:
        with self.connection_factory() as conn:
            self._require_compatible(conn)
            with conn.transaction():
                if older_than_seconds is None:
                    policy = conn.execute(
                        f"SELECT retention_seconds FROM {self._q('session_completion_watermarks')} WHERE singleton=1"
                    ).fetchone()
                    older_than_seconds = int(policy["retention_seconds"])
                if older_than_seconds < 1:
                    raise db.ActionQError("completion retention must be positive")
                cutoff = datetime.now(timezone.utc).timestamp() - older_than_seconds
                row = conn.execute(
                    f"DELETE FROM {self._q('session_completion_events')} WHERE received_at < to_timestamp(%s) AND event_id IN (SELECT event_id FROM {self._q('session_completion_acknowledgements')}) RETURNING server_cursor",
                    (cutoff,),
                )
                deleted = [int(item["server_cursor"]) for item in row.fetchall()]
                if deleted:
                    conn.execute(
                        f"UPDATE {self._q('session_completion_watermarks')} SET recovery_floor=GREATEST(recovery_floor,%s) WHERE singleton=1",
                        (max(deleted),),
                    )
                return len(deleted)
