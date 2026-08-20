from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Lock

import pytest

from actionq.completion_outbox import (
    CompletionConflictError,
    CompletionOutbox,
    payload_digest,
)


@pytest.fixture()
def git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Test"], check=True
    )
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "initial"], check=True
    )
    return repo


def _fields(session: str) -> dict:
    return {
        "runtime_session_id": session,
        "attempt_id": None,
        "action_id": None,
        "repo": {"project": "actionq"},
        "harness": "manual",
        "model": None,
        "terminal": {
            "kind": "succeeded",
            "exit_code": 0,
            "reason_code": "completed",
            "retryable": False,
        },
        "started_at": "2026-08-09T10:00:00Z",
        "completed_at": "2026-08-09T10:00:01Z",
        "observed_at": "2026-08-09T10:00:01Z",
        "duration_ms": 1000,
        "refs": [],
        "evidence": {
            "dirty": False,
            "commit_count": 0,
            "verification": {"pass": 0, "fail": 0, "error": 0},
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


def test_stream_identity_sequence_and_event_ids_survive_restart(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    first = CompletionOutbox(path)
    event = first.record("capsule-a", _fields("session-a"))
    retried = first.record("capsule-a", _fields("session-a"))
    second = CompletionOutbox(path)
    next_event = second.record("capsule-b", _fields("session-b"))

    assert retried == event
    assert second.stream_id == event["origin_stream_id"]
    assert next_event["origin_sequence"] == event["origin_sequence"] + 1
    assert next_event["event_id"] != event["event_id"]


def test_same_capsule_with_different_payload_is_quarantined(tmp_path: Path) -> None:
    outbox = CompletionOutbox(tmp_path / "outbox.sqlite3")
    outbox.record("capsule-a", _fields("session-a"))
    with pytest.raises(CompletionConflictError):
        outbox.record("capsule-a", _fields("different-session"))
    assert outbox.health()["quarantined"] == 1
    assert outbox.pending() == []


def test_concurrent_allocation_is_gap_free_and_unique(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"

    def record(index: int) -> dict:
        return CompletionOutbox(path).record(
            f"capsule-{index}", _fields(f"session-{index}")
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        events = list(pool.map(record, range(24)))
    assert sorted(event["origin_sequence"] for event in events) == list(range(1, 25))
    assert len({event["event_id"] for event in events}) == 24


def test_wal_initialization_recovers_when_concurrent_opener_wins_lock() -> None:
    class Result:
        def __init__(self, row: tuple[str]) -> None:
            self.row = row

        def fetchone(self) -> tuple[str]:
            return self.row

    class SharedJournal:
        mode = "delete"
        setters = Barrier(2)
        lock = Lock()

    class ConcurrentOpenConnection:
        def __init__(self) -> None:
            self.commands: list[str] = []

        def execute(self, command: str):
            self.commands.append(command)
            if command == "PRAGMA journal_mode":
                return Result((SharedJournal.mode,))
            if command == "PRAGMA journal_mode=WAL":
                SharedJournal.setters.wait()
                with SharedJournal.lock:
                    if SharedJournal.mode == "delete":
                        SharedJournal.mode = "wal"
                        return Result(("wal",))
                error = sqlite3.OperationalError("database is locked")
                error.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise error
            raise AssertionError(command)

    connections = [ConcurrentOpenConnection(), ConcurrentOpenConnection()]

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(CompletionOutbox._enable_wal, connections))  # type: ignore[arg-type]

    assert SharedJournal.mode == "wal"
    assert sorted(len(connection.commands) for connection in connections) == [2, 3]
    assert sum(
        connection.commands.count("PRAGMA journal_mode=WAL")
        for connection in connections
    ) == 2


def test_lost_ack_retry_digest_quarantine_and_health(tmp_path: Path) -> None:
    outbox = CompletionOutbox(tmp_path / "outbox.sqlite3")
    event = outbox.record("capsule-a", _fields("session-a"))
    pending = outbox.pending()
    assert pending[0]["payload"] == event
    digest = pending[0]["digest"]

    # A lost acknowledgement leaves the exact same event pending.
    assert outbox.pending()[0]["payload"]["event_id"] == event["event_id"]
    next_retry = outbox.retry(event["event_id"], "secret=https://token", now=100.0)
    jitter_unit = int(
        hashlib.sha256(f"{event['event_id']}:1".encode()).hexdigest()[:8], 16
    ) / 0xFFFFFFFF
    assert next_retry == pytest.approx(100.0 + 0.75 + 0.5 * jitter_unit)
    assert outbox.pending(now=100.5) == []
    assert outbox.pending(now=next_retry)[0]["digest"] == digest
    status = outbox.status()
    assert status[0]["error_class"] == "delivery-error"
    assert "secret" not in json.dumps(status)

    assert not outbox.acknowledge(event["event_id"], "sha256:" + "0" * 64)
    assert outbox.pending(now=1000) == []
    assert outbox.health()["quarantined"] == 1
    assert outbox.health()["healthy"] is False


def test_retention_removes_only_acknowledged_rows(tmp_path: Path) -> None:
    outbox = CompletionOutbox(tmp_path / "outbox.sqlite3")
    acknowledged = outbox.record("capsule-a", _fields("session-a"))
    pending = outbox.record("capsule-b", _fields("session-b"))
    assert outbox.acknowledge(acknowledged["event_id"], payload_digest(acknowledged))
    assert outbox.compact(acknowledged_before=float("inf")) == 1
    assert outbox.pending()[0]["payload"]["event_id"] == pending["event_id"]


def test_configured_retention_runs_automatically_and_never_prunes_unresolved(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outbox.sqlite3"
    outbox = CompletionOutbox(path, acknowledged_retention_seconds=10)
    acknowledged = outbox.record("capsule-a", _fields("session-a"))
    pending = outbox.record("capsule-b", _fields("session-b"))
    assert outbox.acknowledge(acknowledged["event_id"], payload_digest(acknowledged))
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE completion_events SET acknowledged_at = 1 WHERE event_id = ?",
            (acknowledged["event_id"],),
        )
    restarted = CompletionOutbox(path, acknowledged_retention_seconds=10)
    assert [row["event_id"] for row in restarted.status()] == [pending["event_id"]]


def test_pending_read_quarantines_stored_payload_corruption(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    outbox = CompletionOutbox(path)
    event = outbox.record("capsule-a", _fields("session-a"))
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE completion_events SET payload_json = ? WHERE event_id = ?",
            ('{"tampered":true}', event["event_id"]),
        )
    assert outbox.pending() == []
    status = outbox.status()[0]
    assert status["state"] == "quarantined"
    assert status["error_class"] == "stored-payload-corrupt"


def test_operator_cli_is_payload_free_and_reports_replay_state(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    event = CompletionOutbox(path).record("capsule-a", _fields("private-session"))
    for command in ("health", "status", "replay"):
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "actionq.completion_outbox",
                "--path",
                str(path),
                command,
            ],
            text=True,
            capture_output=True,
            check=True,
        )
        parsed = json.loads(completed.stdout)
        assert "private-session" not in completed.stdout
        if command != "health":
            assert parsed[0]["event_id"] == event["event_id"]


def test_retention_policy_is_persisted_and_reused(tmp_path: Path) -> None:
    path = tmp_path / "outbox.sqlite3"
    year = 365 * 24 * 60 * 60
    first = CompletionOutbox(path, acknowledged_retention_seconds=year)
    second = CompletionOutbox(path, acknowledged_retention_seconds=1)
    assert first.acknowledged_retention_seconds == year
    assert second.acknowledged_retention_seconds == year


def test_cli_inspection_never_compacts_or_overrides_persisted_policy(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outbox.sqlite3"
    year = 365 * 24 * 60 * 60
    outbox = CompletionOutbox(path, acknowledged_retention_seconds=year)
    event = outbox.record("capsule-a", _fields("session-a"))
    assert outbox.acknowledge(event["event_id"], payload_digest(event))
    eight_days_ago = time.time() - 8 * 24 * 60 * 60
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE completion_events SET acknowledged_at = ? WHERE event_id = ?",
            (eight_days_ago, event["event_id"]),
        )
    before = path.read_bytes()

    for command in ("health", "status", "replay"):
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "actionq.completion_outbox",
                "--path",
                str(path),
                command,
            ],
            text=True,
            capture_output=True,
            check=True,
        )
        if command == "health":
            assert str(year) in completed.stdout

    assert path.read_bytes() == before
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM completion_events").fetchone()[0] == 1
        assert connection.execute(
            "SELECT acknowledged_retention_seconds FROM producer"
        ).fetchone()[0] == year


