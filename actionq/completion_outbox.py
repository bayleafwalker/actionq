"""Durable producer outbox for ``session.completion-observed/v1`` events."""

from __future__ import annotations

import hashlib
import argparse
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "session.completion-observed/v1"
DEFAULT_OUTBOX_PATH = Path("~/.local/state/actionq/session-wrapper/completion-outbox.sqlite3")
MAX_RETRY_SECONDS = 300
DEFAULT_ACK_RETENTION_SECONDS = 7 * 24 * 60 * 60
MAX_ACK_RETENTION_SECONDS = 365 * 24 * 60 * 60
_SAFE_ERROR_CLASSES = {
    "connection-error",
    "delivery-error",
    "permission-error",
    "timeout-error",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def payload_digest(payload: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(payload)).hexdigest()


class CompletionOutbox:
    """SQLite WAL outbox with one durable identity and monotonic sequence."""

    def __init__(
        self,
        path: Path,
        *,
        acknowledged_retention_seconds: int = DEFAULT_ACK_RETENTION_SECONDS,
    ):
        if not 1 <= acknowledged_retention_seconds <= MAX_ACK_RETENTION_SECONDS:
            raise ValueError("acknowledged retention must be between 1 second and 1 year")
        self.path = Path(path).expanduser()
        self.acknowledged_retention_seconds = acknowledged_retention_seconds
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self.compact()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS producer (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    stream_id TEXT NOT NULL,
                    next_sequence INTEGER NOT NULL CHECK (next_sequence >= 1),
                    acknowledged_retention_seconds INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS completion_events (
                    event_id TEXT PRIMARY KEY,
                    origin_sequence INTEGER NOT NULL UNIQUE,
                    capsule_id TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('pending','acknowledged','quarantined')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry_at REAL,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    acknowledged_at REAL
                );
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(producer)")
            }
            if "acknowledged_retention_seconds" not in columns:
                connection.execute(
                    "ALTER TABLE producer ADD COLUMN acknowledged_retention_seconds INTEGER"
                )
            connection.execute(
                "INSERT OR IGNORE INTO producer(singleton, stream_id, next_sequence, "
                "acknowledged_retention_seconds) VALUES (1, ?, 1, ?)",
                (str(uuid.uuid4()), self.acknowledged_retention_seconds),
            )
            connection.execute(
                "UPDATE producer SET acknowledged_retention_seconds = ? "
                "WHERE singleton = 1 AND acknowledged_retention_seconds IS NULL",
                (self.acknowledged_retention_seconds,),
            )
            self.acknowledged_retention_seconds = int(
                connection.execute(
                    "SELECT acknowledged_retention_seconds FROM producer WHERE singleton = 1"
                ).fetchone()["acknowledged_retention_seconds"]
            )

    @property
    def stream_id(self) -> str:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT stream_id FROM producer WHERE singleton = 1"
            ).fetchone()
        assert row is not None
        return str(row["stream_id"])

    def record(self, capsule_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        """Atomically allocate and store an event, or return its first bytes."""

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT event_id, origin_sequence, payload_json, payload_digest, state "
                "FROM completion_events WHERE capsule_id = ?",
                (capsule_id,),
            ).fetchone()
            if existing is not None:
                try:
                    original = json.loads(existing["payload_json"])
                    original_digest = payload_digest(original)
                except (json.JSONDecodeError, TypeError, ValueError):
                    original = None
                    original_digest = "invalid"
                if original_digest != existing["payload_digest"]:
                    connection.execute(
                        "UPDATE completion_events SET state = 'quarantined', "
                        "last_error = 'stored-payload-corrupt' WHERE capsule_id = ?",
                        (capsule_id,),
                    )
                    connection.commit()
                    raise CompletionConflictError(capsule_id)
                assert isinstance(original, dict)
                candidate = {
                    "schema_version": SCHEMA_VERSION,
                    "event_id": existing["event_id"],
                    "origin_stream_id": original["origin_stream_id"],
                    "origin_sequence": existing["origin_sequence"],
                    **fields,
                }
                if payload_digest(candidate) != existing["payload_digest"]:
                    connection.execute(
                        "UPDATE completion_events SET state = 'quarantined', "
                        "last_error = 'payload-conflict' WHERE capsule_id = ?",
                        (capsule_id,),
                    )
                    connection.commit()
                    raise CompletionConflictError(capsule_id)
                connection.commit()
                return original
            producer = connection.execute(
                "SELECT stream_id, next_sequence FROM producer WHERE singleton = 1"
            ).fetchone()
            assert producer is not None
            stream_id = str(producer["stream_id"])
            sequence = int(producer["next_sequence"])
            event_id = str(
                uuid.uuid5(uuid.UUID(stream_id), f"session-completion:{capsule_id}")
            )
            payload = {
                "schema_version": SCHEMA_VERSION,
                "event_id": event_id,
                "origin_stream_id": stream_id,
                "origin_sequence": sequence,
                **fields,
            }
            encoded = canonical_bytes(payload).decode("utf-8")
            digest = payload_digest(payload)
            connection.execute(
                "INSERT INTO completion_events "
                "(event_id, origin_sequence, capsule_id, payload_json, payload_digest, "
                "state, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (event_id, sequence, capsule_id, encoded, digest, time.time()),
            )
            connection.execute(
                "UPDATE producer SET next_sequence = ? WHERE singleton = 1",
                (sequence + 1,),
            )
            connection.commit()
        return payload

    def pending(self, *, now: float | None = None, limit: int = 100) -> list[dict[str, Any]]:
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT event_id, payload_json, payload_digest FROM completion_events "
                "WHERE state = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= ?) "
                "ORDER BY origin_sequence LIMIT ?",
                (timestamp, limit),
            ).fetchall()
            verified = self._verify_rows(connection, rows)
            connection.commit()
        return verified

    def retry(
        self, event_id: str, error: str | BaseException, *, now: float | None = None
    ) -> float:
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT event_id, attempts, state, payload_json, payload_digest "
                "FROM completion_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None or row["state"] != "pending":
                raise KeyError(event_id)
            if not self._verify_rows(connection, [row]):
                connection.commit()
                raise KeyError(event_id)
            attempts = int(row["attempts"]) + 1
            base_delay = min(MAX_RETRY_SECONDS, 2 ** min(attempts - 1, 20))
            jitter_unit = int(
                hashlib.sha256(f"{event_id}:{attempts}".encode()).hexdigest()[:8], 16
            ) / 0xFFFFFFFF
            delay = min(MAX_RETRY_SECONDS, base_delay * (0.75 + 0.5 * jitter_unit))
            next_retry = timestamp + delay
            connection.execute(
                "UPDATE completion_events SET attempts = ?, next_retry_at = ?, "
                "last_error = ? WHERE event_id = ?",
                (attempts, next_retry, _error_class(error), event_id),
            )
            connection.commit()
        return next_retry

    def acknowledge(self, event_id: str, digest: str) -> bool:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT event_id, payload_json, payload_digest, state "
                "FROM completion_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None:
                connection.rollback()
                return False
            if not self._verify_rows(connection, [row]):
                connection.commit()
                return False
            if row["payload_digest"] != digest:
                connection.execute(
                    "UPDATE completion_events SET state = 'quarantined', "
                    "last_error = 'ack-digest-mismatch' WHERE event_id = ?",
                    (event_id,),
                )
                connection.commit()
                return False
            connection.execute(
                "UPDATE completion_events SET state = 'acknowledged', "
                "acknowledged_at = ?, next_retry_at = NULL, last_error = NULL "
                "WHERE event_id = ?",
                (time.time(), event_id),
            )
            connection.commit()
            self.compact()
            return True

    def compact(
        self,
        *,
        now: float | None = None,
        acknowledged_before: float | None = None,
    ) -> int:
        if acknowledged_before is None:
            timestamp = time.time() if now is None else now
            acknowledged_before = timestamp - self.acknowledged_retention_seconds
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM completion_events WHERE state = 'acknowledged' "
                "AND acknowledged_at < ?",
                (acknowledged_before,),
            )
            return cursor.rowcount

    def health(self) -> dict[str, Any]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT event_id, payload_json, payload_digest FROM completion_events"
            ).fetchall()
            self._verify_rows(connection, rows)
            counts = {
                row["state"]: int(row["count"])
                for row in connection.execute(
                    "SELECT state, count(*) AS count FROM completion_events GROUP BY state"
                )
            }
            oldest = connection.execute(
                "SELECT min(created_at) AS value FROM completion_events "
                "WHERE state IN ('pending','quarantined')"
            ).fetchone()["value"]
            connection.commit()
        return {
            "stream_id": self.stream_id,
            "pending": counts.get("pending", 0),
            "acknowledged": counts.get("acknowledged", 0),
            "quarantined": counts.get("quarantined", 0),
            "oldest_unresolved_at": oldest,
            "healthy": counts.get("quarantined", 0) == 0,
            "acknowledged_retention_seconds": self.acknowledged_retention_seconds,
        }

    def status(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return payload-free operator metadata after digest verification."""

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT event_id, origin_sequence, payload_json, payload_digest, state, "
                "attempts, next_retry_at, last_error FROM completion_events "
                "ORDER BY origin_sequence LIMIT ?",
                (limit,),
            ).fetchall()
            self._verify_rows(connection, rows)
            result = []
            for row in rows:
                current = connection.execute(
                    "SELECT state, last_error FROM completion_events WHERE event_id = ?",
                    (row["event_id"],),
                ).fetchone()
                result.append({
                    "event_id": row["event_id"],
                    "origin_sequence": row["origin_sequence"],
                    "digest": row["payload_digest"],
                    "state": current["state"],
                    "attempts": row["attempts"],
                    "next_retry_at": row["next_retry_at"],
                    "error_class": current["last_error"],
                })
            connection.commit()
        return result

    def contains_capsule(self, capsule_id: str) -> bool:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT event_id, payload_json, payload_digest FROM completion_events "
                "WHERE capsule_id = ?",
                (capsule_id,),
            ).fetchall()
            verified = self._verify_rows(connection, rows)
            connection.commit()
        return bool(verified)

    def _verify_rows(
        self, connection: sqlite3.Connection, rows: list[sqlite3.Row]
    ) -> list[dict[str, Any]]:
        verified: list[dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
                actual = payload_digest(payload)
            except (json.JSONDecodeError, TypeError, ValueError):
                actual = "invalid"
                payload = None
            if actual != row["payload_digest"]:
                connection.execute(
                    "UPDATE completion_events SET state = 'quarantined', "
                    "last_error = 'stored-payload-corrupt' WHERE event_id = ?",
                    (row["event_id"],),
                )
                continue
            verified.append({"payload": payload, "digest": row["payload_digest"]})
        return verified


class CompletionConflictError(RuntimeError):
    pass


def _error_class(error: str | BaseException) -> str:
    if isinstance(error, PermissionError):
        return "permission-error"
    if isinstance(error, TimeoutError):
        return "timeout-error"
    if isinstance(error, ConnectionError):
        return "connection-error"
    if isinstance(error, str) and error in _SAFE_ERROR_CLASSES:
        return error
    return "delivery-error"


def completion_fields(
    capsule: dict[str, Any],
    capsule_path: Path,
    *,
    attempt_id: str | None = None,
    action_id: str | None = None,
) -> dict[str, Any]:
    """Project a placed capsule into the canonical privacy-minimal event."""

    exit_code = capsule["end"]["exit_code"]
    if capsule["end"].get("reason") == "wrapped command interrupted":
        terminal = {
            "kind": "cancelled",
            "exit_code": None,
            "reason_code": "cancelled",
            "retryable": False,
        }
    elif capsule["end"].get("reason") == "wrapped command usage limited":
        terminal = {
            "kind": "usage-limited",
            "exit_code": None,
            "reason_code": "usage-limit",
            "retryable": False,
        }
    elif capsule["end"].get("reason") == "wrapped command failed to start":
        terminal = {
            "kind": "failed",
            "exit_code": None,
            "reason_code": "start-failed",
            "retryable": False,
        }
    elif capsule["end"].get("reason") == "wrapped command timed out":
        terminal = {
            "kind": "timed-out",
            "exit_code": None,
            "reason_code": "timeout",
            "retryable": False,
        }
    elif capsule["end"]["kind"] == "end-inferred":
        terminal = {
            "kind": "end-inferred",
            "exit_code": None,
            "reason_code": "crash-inferred",
            "retryable": False,
        }
    elif exit_code == 0:
        terminal = {
            "kind": "succeeded",
            "exit_code": 0,
            "reason_code": "completed",
            "retryable": False,
        }
    else:
        terminal = {
            "kind": "failed",
            "exit_code": exit_code,
            "reason_code": "process-exit",
            "retryable": False,
        }
    verification = {"pass": 0, "fail": 0, "error": 0}
    for item in capsule["verification"]:
        if item["result"] in verification:
            verification[item["result"]] += 1
    capsule_bytes = capsule_path.read_bytes()
    capsule_revision = "sha256:" + hashlib.sha256(capsule_bytes).hexdigest()
    model = capsule["model"]
    return {
        "runtime_session_id": capsule["runtime_session_id"],
        "attempt_id": attempt_id,
        "action_id": action_id,
        "repo": capsule["repo"],
        "harness": capsule["harness"],
        "model": model,
        "terminal": terminal,
        "started_at": capsule["started_at"],
        "completed_at": capsule["ended_at"],
        # Frozen at first observation and reproducible during capsule recovery.
        "observed_at": capsule["ended_at"],
        "duration_ms": _duration_ms(capsule["started_at"], capsule["ended_at"]),
        "refs": [
            {
                "kind": "artifact",
                "source": f"{capsule['repo']['project']}:session-capsules/{capsule_path.name}",
                "revision": capsule_revision,
            }
        ],
        "evidence": {
            "dirty": capsule["git"]["dirty"],
            "commit_count": len(capsule["git"]["commits"]),
            "verification": verification,
        },
        "privacy": {
            "prompt_absent": True,
            "transcript_absent": True,
            "raw_output_absent": True,
            "environment_absent": True,
            "credentials_absent": True,
            "absolute_paths_absent": True,
            "claim_proofs_absent": True,
        },
    }


def _duration_ms(started_at: str, completed_at: str) -> int | None:
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        end = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def recover_capsules(outbox: CompletionOutbox, capsule_dir: Path) -> int:
    """Idempotently enqueue placed capsules after a producer restart."""

    recovered = 0
    if not capsule_dir.exists():
        return recovered
    for path in sorted(capsule_dir.glob("*.json")):
        try:
            capsule = json.loads(path.read_text(encoding="utf-8"))
            if outbox.contains_capsule(capsule["capsule_id"]):
                continue
            outbox.record(capsule["capsule_id"], completion_fields(capsule, path))
            recovered += 1
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    return recovered


def inspect_outbox(path: Path, *, limit: int = 100) -> dict[str, Any]:
    """Read producer policy and event metadata without any database mutation."""

    resolved = Path(path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        producer = connection.execute(
            "SELECT stream_id, acknowledged_retention_seconds FROM producer "
            "WHERE singleton = 1"
        ).fetchone()
        if producer is None:
            raise RuntimeError("completion outbox producer metadata is missing")
        rows = connection.execute(
            "SELECT event_id, origin_sequence, payload_json, payload_digest, state, "
            "attempts, next_retry_at, last_error, created_at FROM completion_events "
            "ORDER BY origin_sequence",
        ).fetchall()
        status: list[dict[str, Any]] = []
        counts = {"pending": 0, "acknowledged": 0, "quarantined": 0}
        oldest: float | None = None
        for index, row in enumerate(rows):
            try:
                payload = json.loads(row["payload_json"])
                valid = payload_digest(payload) == row["payload_digest"]
            except (json.JSONDecodeError, TypeError, ValueError):
                valid = False
            state = row["state"] if valid else "quarantined"
            error_class = row["last_error"] if valid else "stored-payload-corrupt"
            counts[state] += 1
            if state in {"pending", "quarantined"}:
                created = float(row["created_at"])
                oldest = created if oldest is None else min(oldest, created)
            if index >= limit:
                continue
            status.append(
                {
                    "event_id": row["event_id"],
                    "origin_sequence": row["origin_sequence"],
                    "digest": row["payload_digest"],
                    "state": state,
                    "attempts": row["attempts"],
                    "next_retry_at": row["next_retry_at"],
                    "error_class": error_class,
                }
            )
        return {
            "health": {
                "stream_id": producer["stream_id"],
                **counts,
                "oldest_unresolved_at": oldest,
                "healthy": counts["quarantined"] == 0,
                "acknowledged_retention_seconds": int(
                    producer["acknowledged_retention_seconds"]
                ),
            },
            "status": status,
        }
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    """Payload-free operator inspection for health and replay state."""

    parser = argparse.ArgumentParser(prog="actionq-completion-outbox")
    parser.add_argument("--path", type=Path, default=DEFAULT_OUTBOX_PATH)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("command", choices=("health", "status", "replay"))
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")
    inspection = inspect_outbox(args.path, limit=args.limit)
    if args.command == "health":
        result: Any = inspection["health"]
    else:
        rows = inspection["status"]
        result = rows if args.command == "status" else [
            row for row in rows if row["state"] == "pending"
        ]
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
