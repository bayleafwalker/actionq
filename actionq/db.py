from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any


MAX_CHAIN_DEPTH = int(os.environ.get("ACTIONQ_MAX_CHAIN_DEPTH", "3"))
DEFAULT_RATE_LIMIT_PER_HOUR = int(os.environ.get("ACTIONQ_RATE_LIMIT_PER_HOUR", "20"))
SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ActionQError(ValueError):
    pass


class NoActionAvailable(ActionQError):
    pass


def _import_psycopg():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for actionq. Install with `pip install -e .` "
            "or `uv tool install /projects/dev/actionq/`."
        ) from exc
    return psycopg, dict_row


def schema_name(value: str | None = None) -> str:
    name = value or os.environ.get("ACTIONQ_SCHEMA", "actionq")
    if not SCHEMA_RE.match(name):
        raise ActionQError(
            "ACTIONQ_SCHEMA must be a simple Postgres identifier "
            "(letters, digits, underscore; not starting with a digit)."
        )
    return name


def qname(schema: str, table: str) -> str:
    schema_name(schema)
    return f'"{schema}"."{table}"'


def connect(url: str | None = None):
    db_url = url or os.environ.get("ACTIONQ_URL")
    if not db_url:
        raise ActionQError("ACTIONQ_URL is required")
    psycopg, dict_row = _import_psycopg()
    return psycopg.connect(db_url, row_factory=dict_row)


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def to_json(value: Any, *, pretty: bool = False) -> str:
    return json.dumps(value, default=json_default, indent=2 if pretty else None)


def parse_json(raw: str | None, *, default: Any) -> Any:
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ActionQError(f"Invalid JSON: {exc}") from exc


def migrate(conn, schema: str) -> None:
    schema = schema_name(schema)
    with conn.transaction():
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {qname(schema, "actions")} (
                id              BIGSERIAL PRIMARY KEY,
                action_type     TEXT        NOT NULL,
                project         TEXT,
                target_ref      TEXT,
                source_refs     JSONB       NOT NULL DEFAULT '[]'::jsonb,
                priority        INTEGER     NOT NULL DEFAULT 100,
                status          TEXT        NOT NULL DEFAULT 'pending'
                                            CHECK (status IN ('pending', 'claimed', 'completed', 'failed', 'rejected', 'cancelled')),
                parent_id       BIGINT      REFERENCES {qname(schema, "actions")}(id),
                chain_depth     INTEGER     NOT NULL DEFAULT 0,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                claimed_at      TIMESTAMPTZ,
                claimed_by      TEXT,
                claim_deadline  TIMESTAMPTZ,
                completed_at    TIMESTAMPTZ,
                result_ref      TEXT,
                failure_reason  TEXT,
                created_by      TEXT        NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {qname(schema, "events")} (
                id          BIGSERIAL   PRIMARY KEY,
                action_id   BIGINT      REFERENCES {qname(schema, "actions")}(id),
                event_type  TEXT        NOT NULL,
                timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
                actor       TEXT,
                payload     JSONB       NOT NULL DEFAULT '{{}}'::jsonb
            )
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_actionq_actions_claim_lookup
                ON {qname(schema, "actions")}(status, priority, created_at)
                WHERE status = 'pending'
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_actionq_actions_parent ON {qname(schema, 'actions')}(parent_id)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_actionq_actions_project ON {qname(schema, 'actions')}(project)"
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_actionq_actions_deadline
                ON {qname(schema, "actions")}(claim_deadline)
                WHERE status = 'claimed'
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_actionq_events_action ON {qname(schema, 'events')}(action_id)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_actionq_events_timestamp ON {qname(schema, 'events')}(timestamp DESC)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_actionq_events_type_time ON {qname(schema, 'events')}(event_type, timestamp DESC)"
        )


def insert_event(
    conn,
    schema: str,
    *,
    event_type: str,
    action_id: int | None = None,
    actor: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict:
    row = conn.execute(
        f"""
        INSERT INTO {qname(schema, "events")} (action_id, event_type, actor, payload)
        VALUES (%s, %s, %s, %s::jsonb)
        RETURNING *
        """,
        (action_id, event_type, actor, json.dumps(payload or {})),
    ).fetchone()
    return dict(row)


def get_action(conn, schema: str, action_id: int) -> dict | None:
    row = conn.execute(
        f"SELECT * FROM {qname(schema, 'actions')} WHERE id = %s",
        (action_id,),
    ).fetchone()
    return dict(row) if row else None


def _rate_limited_source(created_by: str) -> bool:
    return created_by.startswith("agent:") or created_by.startswith("script:")


def enqueue(
    conn,
    schema: str,
    *,
    action_type: str,
    project: str | None,
    target_ref: str | None,
    source_refs: list[str],
    priority: int,
    parent_id: int | None,
    created_by: str,
) -> dict:
    chain_depth = 0
    if parent_id is not None:
        parent = get_action(conn, schema, parent_id)
        if parent is None:
            raise ActionQError(f"Parent action #{parent_id} not found")
        chain_depth = int(parent["chain_depth"]) + 1
    if chain_depth > MAX_CHAIN_DEPTH:
        raise ActionQError(f"chain_depth {chain_depth} exceeds max {MAX_CHAIN_DEPTH}")

    with conn.transaction():
        if _rate_limited_source(created_by):
            count = conn.execute(
                f"""
                SELECT count(*) AS count
                FROM {qname(schema, "events")}
                WHERE event_type = 'action_enqueued'
                  AND payload->>'created_by' = %s
                  AND timestamp > now() - interval '1 hour'
                """,
                (created_by,),
            ).fetchone()["count"]
            if count >= DEFAULT_RATE_LIMIT_PER_HOUR:
                raise ActionQError(
                    f"Rate limit exceeded for {created_by}: {count}/hour"
                )

        action = conn.execute(
            f"""
            INSERT INTO {qname(schema, "actions")}
                (action_type, project, target_ref, source_refs, priority, parent_id, chain_depth, created_by)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                action_type,
                project,
                target_ref,
                json.dumps(source_refs),
                priority,
                parent_id,
                chain_depth,
                created_by,
            ),
        ).fetchone()
        insert_event(
            conn,
            schema,
            action_id=action["id"],
            event_type="action_enqueued",
            actor=created_by,
            payload={
                "action_type": action_type,
                "project": project,
                "target_ref": target_ref,
                "created_by": created_by,
                "parent_id": parent_id,
                "chain_depth": chain_depth,
            },
        )
    return dict(action)


def list_actions(
    conn,
    schema: str,
    *,
    status: str | None = None,
    action_type: str | None = None,
    project: str | None = None,
    limit: int = 50,
) -> list[dict]:
    clauses = []
    params: list[Any] = []
    if status:
        clauses.append("status = %s")
        params.append(status)
    if action_type:
        clauses.append("action_type = %s")
        params.append(action_type)
    if project:
        clauses.append("project = %s")
        params.append(project)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    rows = conn.execute(
        f"""
        SELECT * FROM {qname(schema, "actions")}
        {where}
        ORDER BY priority ASC, created_at ASC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def action_events(conn, schema: str, action_id: int) -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT * FROM {qname(schema, "events")}
        WHERE action_id = %s
        ORDER BY timestamp ASC, id ASC
        """,
        (action_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def claim(conn, schema: str, *, worker: str, timeout_minutes: int) -> dict:
    with conn.transaction():
        row = conn.execute(
            f"""
            UPDATE {qname(schema, "actions")}
            SET status = 'claimed',
                claimed_at = now(),
                claimed_by = %s,
                claim_deadline = now() + (%s * interval '1 minute')
            WHERE id = (
                SELECT id FROM {qname(schema, "actions")}
                WHERE status = 'pending'
                ORDER BY priority ASC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
            """,
            (worker, timeout_minutes),
        ).fetchone()
        if row is None:
            raise NoActionAvailable("no pending actions")
        insert_event(
            conn,
            schema,
            action_id=row["id"],
            event_type="action_claimed",
            actor=worker,
            payload={
                "claimed_by": worker,
                "claim_deadline": json_default(row["claim_deadline"]),
            },
        )
    return dict(row)


def _transition_terminal(
    conn,
    schema: str,
    *,
    action_id: int,
    status: str,
    event_type: str,
    actor: str | None,
    result_ref: str | None = None,
    failure_reason: str | None = None,
    payload: dict[str, Any] | None = None,
    allowed_statuses: tuple[str, ...] = ("claimed",),
) -> dict:
    allowed_sql = ", ".join(f"'{status}'" for status in allowed_statuses)
    with conn.transaction():
        row = conn.execute(
            f"""
            UPDATE {qname(schema, "actions")}
            SET status = %s,
                completed_at = now(),
                result_ref = COALESCE(%s, result_ref),
                failure_reason = COALESCE(%s, failure_reason)
            WHERE id = %s
              AND status IN ({allowed_sql})
            RETURNING *
            """,
            (status, result_ref, failure_reason, action_id),
        ).fetchone()
        if row is None:
            raise ActionQError(f"Action #{action_id} cannot transition to {status}")
        insert_event(
            conn,
            schema,
            action_id=action_id,
            event_type=event_type,
            actor=actor,
            payload=payload or {},
        )
    return dict(row)


def complete(conn, schema: str, action_id: int, result_ref: str, actor: str | None = None) -> dict:
    return _transition_terminal(
        conn,
        schema,
        action_id=action_id,
        status="completed",
        event_type="action_completed",
        actor=actor,
        result_ref=result_ref,
        payload={"result_ref": result_ref},
        allowed_statuses=("claimed",),
    )


def fail(conn, schema: str, action_id: int, reason: str, actor: str | None = None) -> dict:
    return _transition_terminal(
        conn,
        schema,
        action_id=action_id,
        status="failed",
        event_type="action_failed",
        actor=actor,
        failure_reason=reason,
        payload={"failure_reason": reason},
        allowed_statuses=("claimed",),
    )


def reject(
    conn,
    schema: str,
    action_id: int,
    *,
    reason: str,
    validator: str,
    actor: str | None = None,
) -> dict:
    return _transition_terminal(
        conn,
        schema,
        action_id=action_id,
        status="rejected",
        event_type="action_rejected",
        actor=actor,
        failure_reason=reason,
        payload={"rejection_reason": reason, "validator": validator},
        allowed_statuses=("claimed",),
    )


def cancel(conn, schema: str, action_id: int, reason: str, actor: str | None = "human") -> dict:
    return _transition_terminal(
        conn,
        schema,
        action_id=action_id,
        status="cancelled",
        event_type="action_cancelled",
        actor=actor,
        failure_reason=reason,
        payload={"reason": reason, "cancelled_by": actor},
        allowed_statuses=("pending", "claimed"),
    )


def sweep(conn, schema: str) -> list[dict]:
    with conn.transaction():
        rows = conn.execute(
            f"""
            SELECT * FROM {qname(schema, "actions")}
            WHERE status = 'claimed' AND claim_deadline < now()
            ORDER BY claim_deadline ASC
            FOR UPDATE
            """
        ).fetchall()
        swept: list[dict] = []
        for row in rows:
            updated = conn.execute(
                f"""
                UPDATE {qname(schema, "actions")}
                SET status = 'pending',
                    claimed_at = NULL,
                    claimed_by = NULL,
                    claim_deadline = NULL
                WHERE id = %s
                RETURNING *
                """,
                (row["id"],),
            ).fetchone()
            insert_event(
                conn,
                schema,
                action_id=row["id"],
                event_type="claim_timed_out",
                actor="actionctl:sweep",
                payload={
                    "previous_claimed_by": row["claimed_by"],
                    "timeout_seconds": None,
                },
            )
            swept.append(dict(updated))
    return swept


def list_events(
    conn,
    schema: str,
    *,
    since: str | None = None,
    event_type: str | None = None,
    action_id: int | None = None,
    limit: int = 100,
) -> list[dict]:
    clauses = []
    params: list[Any] = []
    if since:
        clauses.append("timestamp >= %s")
        params.append(since)
    if event_type:
        clauses.append("event_type = %s")
        params.append(event_type)
    if action_id is not None:
        clauses.append("action_id = %s")
        params.append(action_id)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    rows = conn.execute(
        f"""
        SELECT * FROM {qname(schema, "events")}
        {where}
        ORDER BY timestamp ASC, id ASC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def follow_events(conn, schema: str, *, event_type: str | None, action_id: int | None):
    last_id = 0
    while True:
        clauses = ["id > %s"]
        params: list[Any] = [last_id]
        if event_type:
            clauses.append("event_type = %s")
            params.append(event_type)
        if action_id is not None:
            clauses.append("action_id = %s")
            params.append(action_id)
        rows = conn.execute(
            f"""
            SELECT * FROM {qname(schema, "events")}
            WHERE {" AND ".join(clauses)}
            ORDER BY id ASC
            LIMIT 100
            """,
            params,
        ).fetchall()
        for row in rows:
            last_id = row["id"]
            yield dict(row)
        time.sleep(1)
