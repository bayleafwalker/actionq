import json
import io
from datetime import datetime, timezone

import pytest
import click
from click.testing import CliRunner

from actionq import db
from actionq import cli as cli_module
from actionq.cli import cli, list_cmd, sessions


def test_schema_name_defaults(monkeypatch):
    monkeypatch.delenv("ACTIONQ_SCHEMA", raising=False)
    assert db.schema_name() == "actionq"


def test_schema_name_rejects_unsafe_identifier():
    with pytest.raises(db.ActionQError):
        db.schema_name("bad-name")


def test_parse_json_rejects_invalid_payload():
    with pytest.raises(db.ActionQError):
        db.parse_json("{", default={})


def test_cli_requires_actionq_url_for_migrate(monkeypatch):
    monkeypatch.delenv("ACTIONQ_URL", raising=False)
    result = CliRunner().invoke(cli, ["migrate"])
    assert result.exit_code != 0
    assert "ACTIONQ_URL is required" in result.output
    assert "postgresql://USER:PASSWORD@HOST:5432/DATABASE" in result.output
    assert "Traceback" not in result.output


def test_cli_connection_refusal_is_actionable_and_temporary(monkeypatch):
    monkeypatch.setenv("ACTIONQ_URL", "postgresql://nobody@127.0.0.1:1/none")
    result = CliRunner().invoke(cli, ["ls"])
    assert result.exit_code == 75
    assert "error: PostgreSQL connection failed for host 127.0.0.1" in result.output
    assert "why: the database could not be reached" in result.output
    assert "verify ACTIONQ_URL" in result.output
    assert "Traceback" not in result.output


def test_cli_authentication_failure_uses_exit_four(monkeypatch):
    import psycopg

    error = psycopg.OperationalError("password authentication failed")
    error.sqlstate = "28P01"

    class FakeApp:
        def list_actions(self, **_filters):
            raise error

    monkeypatch.setenv("ACTIONQ_URL", "postgresql://nobody@db.internal:5432/actionq")
    monkeypatch.setattr("actionq.cli._app", lambda _ctx: FakeApp())
    result = CliRunner().invoke(cli, ["ls"])
    assert result.exit_code == 4
    assert "host db.internal" in result.output
    assert "database authentication was rejected" in result.output
    assert "ACTIONQ_URL" in result.output
    assert "Traceback" not in result.output


def test_cli_json_flag_is_accepted_after_subcommand(monkeypatch):
    class FakeApp:
        def list_actions(self, **_filters):
            return [{"id": 9, "action_type": "build", "status": "pending", "project": "demo"}]

    monkeypatch.setattr("actionq.cli._app", lambda _ctx: FakeApp())
    result = CliRunner().invoke(cli, ["ls", "--json"])
    assert result.exit_code == 0
    assert json.loads(result.output) == [
        {"id": 9, "action_type": "build", "status": "pending", "project": "demo"}
    ]


def test_list_and_sessions_render_tables_on_a_tty(monkeypatch):
    class FakeApp:
        def list_actions(self, **_filters):
            return [{"id": 9, "action_type": "build", "status": "pending", "project": "demo"}]

        def list_sessions(self, **_filters):
            return [{"session_id": "aqs:9", "action_id": 9, "status": "running", "project": "demo"}]

    class TTYBuffer(io.StringIO):
        def isatty(self):
            return True

    output = TTYBuffer()
    monkeypatch.setattr(cli_module.sys, "stdout", output)
    monkeypatch.setattr(cli_module, "_app", lambda _ctx: FakeApp())
    ctx = click.Context(cli)
    ctx.obj = {"schema": "actionq"}
    with ctx:
        list_cmd.callback(None, None, None, 50)
        sessions.callback(None, False, 100)
    assert "ID  TYPE   STATUS    PROJECT" in output.getvalue()
    assert "9   build  ○ queued  demo" in output.getvalue()
    assert "SESSION  ACTION  STATUS     PROJECT" in output.getvalue()
    assert "aqs:9" in output.getvalue() and "◐ running" in output.getvalue()
    assert "1 actions" in output.getvalue() and "1 sessions" in output.getvalue()


def test_root_help_groups_commands_and_shows_examples():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Start here" in result.output
    assert "Change" in result.output and "Plumbing" in result.output
    assert "Examples:" in result.output
    assert "actionctl ls" in result.output


def test_json_default_serializes_plain_values():
    payload = json.loads(db.to_json({"x": 1}))
    assert payload == {"x": 1}


def test_json_default_decodes_postgres_text_bytes():
    payload = json.loads(db.to_json({"text": b"scope-iterate"}))
    assert payload == {"text": "scope-iterate"}


def test_sessions_reducer_treats_inferred_end_as_terminal():
    rows = [
        {
            "id": 1,
            "action_id": 7,
            "event_type": "session.dispatch",
            "actor": "daemon:test",
            "timestamp": _ts("2026-07-19T08:00:00Z"),
            "payload": {"session_id": "aqs:7", "ttl_seconds": 60},
        },
        {
            "id": 2,
            "action_id": 7,
            "event_type": "session.end-inferred",
            "actor": "daemon:test",
            "timestamp": _ts("2026-07-19T08:02:00Z"),
            "payload": {"session_id": "aqs:7", "outcome": "end-inferred"},
        },
    ]

    sessions = db.summarize_sessions(rows)

    assert sessions[0]["status"] == "exited"
    assert sessions[0]["outcome"] == "end-inferred"


def _ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_summarize_dispatches_joins_request_metadata_and_latest_session():
    actions = [
        {
            "id": 7,
            "action_type": "scope-iterate",
            "project": "agentops",
            "target_ref": "wi:42",
            "source_refs": ["wi:42", "sprint:3", "ad:01ARZ3NDEKTSV4RRFFQ69G5FAV"],
            "status": "completed",
            "priority": 50,
            "created_at": _ts("2026-05-13T08:00:00Z"),
            "claimed_at": _ts("2026-05-13T08:01:00Z"),
            "completed_at": _ts("2026-05-13T08:05:00Z"),
            "claimed_by": "dispatcher:test",
            "result_ref": "sha:abc",
            "failure_reason": None,
            "parent_id": None,
            "chain_depth": 0,
        }
    ]
    events = [
        {
            "id": 1,
            "action_id": 7,
            "event_type": "dispatch.requested",
            "actor": "operator:test",
            "timestamp": _ts("2026-05-13T08:00:01Z"),
            "payload": {
                "title": "Refine backlog",
                "kind": "investigate",
                "output_expectation": "sprint-proposal",
                "dispatch_group_id": "dg:refine",
                "sprint_id": None,
                "harness": "codex",
                "model": "gpt-5.3-codex",
            },
        },
        {
            "id": 2,
            "action_id": 7,
            "event_type": "session.dispatch",
            "actor": "daemon:test",
            "timestamp": _ts("2026-05-13T08:01:00Z"),
            "payload": {
                "session_id": "aqs:7",
                "runtime_session_id": "aqs:7",
                "project": "agentops",
                "target_ref": "wi:42",
                "harness": "codex",
                "model": "gpt-5.3-codex",
            },
        },
        {
            "id": 3,
            "action_id": 7,
            "event_type": "session.exited",
            "actor": "daemon:test",
            "timestamp": _ts("2026-05-13T08:05:00Z"),
            "payload": {
                "session_id": "aqs:7",
                "runtime_session_id": "aqs:7",
                "outcome": "completed",
                "exit_code": 0,
            },
        },
    ]

    rows = db.summarize_dispatches(actions, events)

    assert rows[0]["id"] == 7
    assert rows[0]["kind"] == "investigate"
    assert rows[0]["output_expectation"] == "sprint-proposal"
    assert rows[0]["dispatch_group_id"] == "dg:refine"
    assert rows[0]["audit_refs"] == ["ad:01ARZ3NDEKTSV4RRFFQ69G5FAV"]
    assert rows[0]["session"]["session_id"] == "aqs:7"
    assert rows[0]["session"]["status"] == "exited"
    assert rows[0]["session"]["outcome"] == "completed"
